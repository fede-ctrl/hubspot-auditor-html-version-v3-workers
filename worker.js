/* AuditPulse Pro worker.js
   Optimized for Render background worker:
   - Atomic job claim via Supabase RPC claim_audit_job
   - Streaming CSV parse (bounded memory) for multimillion-row exports
   - Robust HubSpot fetch with 429/5xx retry and Retry-After handling
   - Heartbeats and lease extension while processing
   - Rich KPIs and bounded drill-down sampling
   - Hard safety cap for per-record enrichers at 500k (aggregate KPIs still computed)
*/

'use strict';

// ============================
// Dependencies and setup
// ============================
const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');            // keep node-fetch to ensure Node stream body
const { parse } = require('csv-parse');
const { v4: uuidv4 } = require('uuid');

// Env
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const HUBSPOT_API_BASE = process.env.HUBSPOT_API_BASE || 'https://api.hubapi.com';
const HUBSPOT_CLIENT_ID = process.env.HUBSPOT_CLIENT_ID;
const HUBSPOT_CLIENT_SECRET = process.env.HUBSPOT_CLIENT_SECRET;
const NODE_ENV = process.env.NODE_ENV || 'development';

// OAuth token endpoint must use global HubSpot OAuth domain
const HUBSPOT_TOKEN_URL = 'https://api.hubapi.com/oauth/v1/token';

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('[Worker] Missing SUPABASE_URL or SUPABASE_SERVICE_KEY.');
  process.exit(1);
}

// Supabase client with service_role
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false }
});

// Worker identity
const WORKER_ID = process.env.RENDER_SERVICE_NAME || `worker-${process.pid}-${uuidv4().slice(0,8)}`;

// Limits and cadence
const MAX_RECORDS_ENRICHED = 500000;         // cap for per-record drill-downs and expensive enrichers
const SAMPLE_CAP_PER_LIST = 5000;            // max sample size for each drill-down list
const DUP_SAMPLE_PER_KEY = 10;               // per duplicate key
const PROGRESS_EVERY_N = 25000;              // update progress after every N parsed rows
const HEARTBEAT_MS = 60000;                  // extend lease every 60 s

// ============================
// Utility: resilient fetch for HubSpot
// ============================
async function hsFetch(url, options = {}, attempt = 1) {
  const res = await fetch(url, options);
  if (res.status === 429 || res.status >= 500) {
    const retryAfterHeader = Number(res.headers.get('Retry-After')) || 0;
    const baseDelay = retryAfterHeader ? retryAfterHeader * 1000 : Math.min(30000, 500 * (2 ** attempt));
    const jitter = Math.floor(Math.random() * 500);
    const delay = baseDelay + jitter;
    await new Promise(r => setTimeout(r, delay));
    return hsFetch(url, options, attempt + 1);
  }
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`[HubSpot] ${res.status} ${res.statusText}: ${body.slice(0, 400)}`);
  }
  return res;
}

// ============================
// Token management (inline helper, no new files)
// ============================
async function getValidAccessToken(portal_id) {
  // Reads installations row and refreshes if needed, atomically where possible
  const { data: instRows, error: instErr } = await supabase
    .from('installations')
    .select('portal_id, access_token, refresh_token, expires_at')
    .eq('portal_id', portal_id)
    .limit(1);

  if (instErr) throw new Error(`[Supabase] installations read failed: ${instErr.message}`);
  if (!instRows || instRows.length === 0) throw new Error(`No installation found for portal_id=${portal_id}`);

  const inst = instRows[0];
  const expiresAt = inst.expires_at ? new Date(inst.expires_at).getTime() : 0;
  const now = Date.now() + 30000; // 30 s skew

  if (expiresAt > now && inst.access_token) {
    return inst.access_token;
  }

  // Refresh
  if (!inst.refresh_token) throw new Error('Missing refresh_token, cannot refresh access token.');

  const params = new URLSearchParams();
  params.append('grant_type', 'refresh_token');
  params.append('client_id', HUBSPOT_CLIENT_ID);
  params.append('client_secret', HUBSPOT_CLIENT_SECRET);
  params.append('refresh_token', inst.refresh_token);

  const res = await fetch(HUBSPOT_TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8' },
    body: params.toString()
  });

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`[HubSpot OAuth] Refresh failed: ${res.status} ${res.statusText} ${body.slice(0, 400)}`);
  }

  const token = await res.json();
  const newAccessToken = token.access_token;
  const newRefreshToken = token.refresh_token || inst.refresh_token;
  const expiresIn = token.expires_in || 1800; // seconds
  const newExpiresAt = new Date(Date.now() + (expiresIn * 1000)).toISOString();

  // Try to store new tokens
  const { error: updErr } = await supabase
    .from('installations')
    .update({ access_token: newAccessToken, refresh_token: newRefreshToken, expires_at: newExpiresAt })
    .eq('portal_id', portal_id);

  if (updErr) throw new Error(`[Supabase] installations update failed: ${updErr.message}`);

  return newAccessToken;
}

// ============================
// Supabase RPC helpers
// ============================
async function claimJob() {
  const { data, error } = await supabase.rpc('claim_audit_job', { p_worker_id: WORKER_ID });
  if (error) {
    console.error('[RPC] claim_audit_job error:', error.message);
    return null;
  }
  return data || null;
}

async function updateProgress(job_id, message, processed = null, total = null) {
  try {
    await supabase.rpc('update_audit_job_progress', {
      p_job_id: job_id,
      p_message: message || null,
      p_processed: processed,
      p_total: total
    });
  } catch (e) {
    // Non-fatal
    console.warn('[RPC] update_audit_job_progress failed:', e.message);
  }
}

async function extendLease(job_id, minutes = 15) {
  try {
    await supabase.rpc('extend_audit_job_lease', { p_job_id: job_id, p_minutes: minutes });
  } catch (e) {
    console.warn('[RPC] extend_audit_job_lease failed:', e.message);
  }
}

// ============================
// HubSpot Export API helpers
// ============================
async function startExport(objectType, accessToken) {
  const url = `${HUBSPOT_API_BASE}/crm/v3/exports/export/async`;
  const body = {
    exportType: 'CSV',
    format: 'CSV',
    objectType: objectType, // 'contacts' or 'companies'
    // propertyGroups: [], // optional
    // specifyProperties: false will export many defaults; omit to get broad coverage
  };

  const res = await hsFetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json;charset=utf-8'
    },
    body: JSON.stringify(body)
  });

  const json = await res.json();
  if (!json || !json.id) throw new Error('Export start did not return an id.');
  return json.id;
}

async function waitForExportReady(taskId, accessToken, job_id) {
  // Poll status until COMPLETED. Backoff handled by hsFetch on 429/5xx.
  // Typical status payload includes: state, status, result: { url: "https://..." }
  const statusUrl = `${HUBSPOT_API_BASE}/crm/v3/exports/export/async/tasks/${encodeURIComponent(taskId)}/status`;
  let attempts = 0;

  while (true) {
    attempts++;
    const res = await hsFetch(statusUrl, {
      headers: { Authorization: `Bearer ${accessToken}` }
    });
    const json = await res.json();

    const state = (json.state || json.status || '').toString().toUpperCase();
    if (state === 'CANCELED' || state === 'FAILED') {
      throw new Error(`Export task failed: ${JSON.stringify(json).slice(0, 400)}`);
    }

    if ((state === 'COMPLETED' || state === 'COMPLETE') && json?.result?.url) {
      return { downloadUrl: json.result.url, total: json?.result?.rowCount || null };
    }

    // progress message if present
    const pct = json?.progress?.percentage || null;
    if (job_id) {
      const msg = pct != null ? `Export preparing at ${pct}%` : 'Export preparing...';
      await updateProgress(job_id, msg);
    }

    // Simple linear wait, rely on hsFetch for retry pressure
    await new Promise(r => setTimeout(r, Math.min(10000, 2000 + attempts * 500)));
  }
}

// ============================
// CSV stream processing per object type
// ============================
function normalizeEmail(value) {
  return (value || '').trim().toLowerCase();
}
function normalizeDomain(value) {
  return (value || '').trim().toLowerCase().replace(/^https?:\/\//, '').replace(/^www\./, '');
}
function normalizePhone(value) {
  return (value || '').replace(/\D+/g, '');
}
function parseMaybeDate(value) {
  if (!value) return null;
  // HubSpot may export ms epoch or ISO
  const n = Number(value);
  if (!Number.isNaN(n) && n > 1000000000) return new Date(n);
  const d = new Date(value);
  return isNaN(d.getTime()) ? null : d;
}
function isValidEmailSimple(e) {
  if (!e) return false;
  // pragmatic, not perfect
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e);
}

// ============================
// KPI computation
// ============================
async function processCsvStream({ res, objectType, job_id }) {
  return new Promise((resolve, reject) => {
    const parser = parse({ columns: true, skip_empty_lines: true });

    let total = 0;
    let enrichedCount = 0;

    // Common KPIs
    const fillCounts = new Map();          // property -> non-empty count
    const uniqueProps = new Set();         // all property names discovered
    let ownerlessCount = 0;                // missing hubspot_owner_id
    let lifecycleMissing = 0;              // missing lifecyclestage
    let invalidEmailCount = 0;             // contacts only
    let staleCount = 0;                    // last modified older than threshold
    const STALE_DAYS = 180;
    const staleCutoff = new Date(Date.now() - STALE_DAYS * 86400000);

    // Orphans / empties
    const orphanedRecords = [];            // sample list
    // Duplicates
    const seenEmail = new Map();           // key -> { count, records: [] }
    const seenDomain = new Map();
    const seenPhone = new Map();

    const duplicateSummary = { byEmail: 0, byDomain: 0, byPhone: 0 };

    let heartbeatTimer = setInterval(() => extendLease(job_id).catch(() => {}), HEARTBEAT_MS);

    res.body
      .on('error', (err) => {
        clearInterval(heartbeatTimer);
        reject(err);
      })
      .pipe(parser)
      .on('data', async (row) => {
        total++;

        // Update progress occasionally
        if (total % PROGRESS_EVERY_N === 0) {
          await updateProgress(job_id, `Processing ${total.toLocaleString()} records...`, total, null);
        }

        // Record properties seen
        Object.keys(row).forEach(k => uniqueProps.add(k));

        // Fill counts
        for (const [k, v] of Object.entries(row)) {
          if (v !== '' && v !== null && v !== undefined) {
            fillCounts.set(k, (fillCounts.get(k) || 0) + 1);
          }
        }

        // Common columns
        const recordId = row['Record ID'] || row['record_id'] || row['hs_object_id'] || null;
        const ownerId = row['hubspot_owner_id'] || row['owner_id'] || row['HubSpot Owner ID'] || '';
        if (!ownerId) ownerlessCount++;

        const lifecycle = row['lifecyclestage'] || row['Lifecycle Stage'] || '';
        if (!lifecycle) lifecycleMissing++;

        const modified = row['hs_lastmodified_date'] || row['Last Modified Date'] || row['lastmodifieddate'] || '';
        const modifiedDate = parseMaybeDate(modified);
        if (modifiedDate && modifiedDate < staleCutoff) staleCount++;

        // Object-specific KPIs
        if (objectType === 'contacts') {
          const email = normalizeEmail(row.email || row['Email'] || '');
          if (email && !isValidEmailSimple(email)) invalidEmailCount++;

          // Orphaned: no associated company id
          const assocCompanyId = row.associatedcompanyid || row['Associated Company ID'] || '';
          if (!assocCompanyId && orphanedRecords.length < SAMPLE_CAP_PER_LIST && enrichedCount < MAX_RECORDS_ENRICHED) {
            orphanedRecords.push({ id: recordId, email, properties: row });
          }

          // Duplicates by email
          if (email) {
            const entry = seenEmail.get(email) || { count: 0, records: [] };
            entry.count++;
            if (entry.records.length < DUP_SAMPLE_PER_KEY && enrichedCount < MAX_RECORDS_ENRICHED) {
              entry.records.push({ id: recordId, email });
            }
            seenEmail.set(email, entry);
          }

          // Duplicates by phone
          const phone = normalizePhone(row.phone || row['Phone Number'] || row['phone'] || '');
          if (phone) {
            const entry = seenPhone.get(phone) || { count: 0, records: [] };
            entry.count++;
            if (entry.records.length < DUP_SAMPLE_PER_KEY && enrichedCount < MAX_RECORDS_ENRICHED) {
              entry.records.push({ id: recordId, phone });
            }
            seenPhone.set(phone, entry);
          }
        } else {
          // companies
          const numAssocContacts = Number(row.num_associated_contacts || row['Number of Associated Contacts'] || 0);
          if (!numAssocContacts && orphanedRecords.length < SAMPLE_CAP_PER_LIST && enrichedCount < MAX_RECORDS_ENRICHED) {
            orphanedRecords.push({ id: recordId, properties: row });
          }

          // Duplicates by domain
          const domain = normalizeDomain(row.domain || row['Company Domain Name'] || '');
          if (domain) {
            const entry = seenDomain.get(domain) || { count: 0, records: [] };
            entry.count++;
            if (entry.records.length < DUP_SAMPLE_PER_KEY && enrichedCount < MAX_RECORDS_ENRICHED) {
              entry.records.push({ id: recordId, domain });
            }
            seenDomain.set(domain, entry);
          }
        }

        if (enrichedCount < MAX_RECORDS_ENRICHED) enrichedCount++;
      })
      .on('end', async () => {
        clearInterval(heartbeatTimer);

        // Summaries
        for (const [, v] of seenEmail) if (v.count > 1) duplicateSummary.byEmail += v.count;
        for (const [, v] of seenDomain) if (v.count > 1) duplicateSummary.byDomain += v.count;
        for (const [, v] of seenPhone) if (v.count > 1) duplicateSummary.byPhone += v.count;

        // Property fill rates
        const fillRates = [];
        for (const prop of uniqueProps) {
          const count = fillCounts.get(prop) || 0;
          fillRates.push({ property: prop, filled: count, fillRate: total ? count / total : 0 });
        }
        fillRates.sort((a, b) => a.fillRate - b.fillRate);

        // Build samples
        const duplicateSamples = {
          byEmail: Array.from(seenEmail.entries())
            .filter(([, v]) => v.count > 1)
            .slice(0, 100)
            .map(([key, v]) => ({ value: key, count: v.count, sample: v.records })),
          byDomain: Array.from(seenDomain.entries())
            .filter(([, v]) => v.count > 1)
            .slice(0, 100)
            .map(([key, v]) => ({ value: key, count: v.count, sample: v.records })),
          byPhone: Array.from(seenPhone.entries())
            .filter(([, v]) => v.count > 1)
            .slice(0, 100)
            .map(([key, v]) => ({ value: key, count: v.count, sample: v.records }))
        };

        resolve({
          totals: { totalRecords: total },
          counts: {
            ownerless: ownerlessCount,
            lifecycleMissing,
            invalidEmails: objectType === 'contacts' ? invalidEmailCount : null,
            staleRecords: staleCount
          },
          orphaned: {
            count: orphanedRecords.length > 0 ? undefined : 0, // true count not tracked to avoid memory; UI uses sample length
            sample: orphanedRecords
          },
          duplicates: {
            summary: duplicateSummary,
            samples: duplicateSamples
          },
          properties: {
            fillRates,
            top20Sparsest: fillRates.slice(0, 20)
          }
        });
      })
      .on('error', (err) => {
        clearInterval(heartbeatTimer);
        reject(err);
      });
  });
}

// ============================
// Workflow audit (security and maintenance risks)
// ============================
async function performWorkflowAudit({ portal_id, accessToken, job_id }) {
  await updateProgress(job_id, 'Fetching workflows...');
  const results = { totalWorkflows: 0, risks: { exposedApiKeys: [], deprecatedEmailV1: [] } };

  let after = undefined;
  while (true) {
    const url = new URL(`${HUBSPOT_API_BASE}/automation/v3/workflows`);
    if (after) url.searchParams.set('after', after);

    const res = await hsFetch(url.toString(), {
      headers: { Authorization: `Bearer ${accessToken}` }
    });
    const json = await res.json();

    const workflows = json.results || json.workflows || [];
    results.totalWorkflows += workflows.length;

    for (const wf of workflows) {
      const actions = wf.actions || [];
      for (const a of actions) {
        // Security: detect hapikey in webhooks or custom code
        try {
          const blob = JSON.stringify(a).toLowerCase();
          if (/hapikey=/.test(blob)) {
            results.risks.exposedApiKeys.push({ workflowId: wf.id, name: wf.name, actionId: a.id });
          }
          if (/marketing-emails[\/\\]v1[\/\\]emails/.test(blob)) {
            results.risks.deprecatedEmailV1.push({ workflowId: wf.id, name: wf.name, actionId: a.id });
          }
        } catch (_) {}
      }
    }

    after = json.paging?.next?.after;
    if (!after) break;

    await updateProgress(job_id, `Workflows fetched: ${results.totalWorkflows}`);
  }

  return results;
}

// ============================
// CRM audit using Export API
// ============================
async function performCrmAudit({ portal_id, objectType, job_id }) {
  await updateProgress(job_id, `Starting ${objectType} export...`);

  const accessToken = await getValidAccessToken(portal_id);
  const taskId = await startExport(objectType, accessToken);
  const { downloadUrl, total } = await waitForExportReady(taskId, accessToken, job_id);

  await updateProgress(job_id, `Downloading ${objectType} CSV...`, 0, total || null);

  const res = await hsFetch(downloadUrl, { method: 'GET' });
  if (!res.ok) throw new Error(`Failed to download export: ${res.status} ${res.statusText}`);

  await updateProgress(job_id, `Processing ${objectType} CSV...`);

  const kpis = await processCsvStream({ res, objectType, job_id });

  // Final progress
  await updateProgress(job_id, `Finalizing ${objectType} audit...`, kpis.totals.totalRecords, kpis.totals.totalRecords);

  return {
    objectType,
    totals: kpis.totals,
    counts: kpis.counts,
    orphaned: kpis.orphaned,
    duplicates: kpis.duplicates,
    properties: kpis.properties,
    meta: {
      processedCap: MAX_RECORDS_ENRICHED,
      sampleCaps: { perList: SAMPLE_CAP_PER_LIST, dupPerKey: DUP_SAMPLE_PER_KEY }
    }
  };
}

// ============================
// Main job loop
// ============================
async function handleJob(job) {
  const job_id = job.job_id || job.id;
  const portal_id = job.portal_id;
  const type = job.object_type || job.type || 'contacts';

  try {
    await updateProgress(job_id, `Job claimed on ${new Date().toISOString()}`);

    const accessToken = await getValidAccessToken(portal_id); // verifies install
    // Basic sanity ping
    await hsFetch(`${HUBSPOT_API_BASE}/crm/v3/properties/contacts`, {
      headers: { Authorization: `Bearer ${accessToken}` }
    }).catch(() => { /* ignore, just a ping; property metadata optional */ });

    let results = null;
    if (type === 'contacts' || type === 'companies') {
      results = await performCrmAudit({ portal_id, objectType: type, job_id });
    } else if (type === 'workflows') {
      results = await performWorkflowAudit({ portal_id, accessToken, job_id });
    } else {
      throw new Error(`Unsupported object_type: ${type}`);
    }

    // Try to write results column. Fallback to result_json if needed.
    let writeOk = false;
    {
      const { error } = await supabase
        .from('audit_jobs')
        .update({ status: 'complete', results, completed_at: new Date().toISOString(), progress_message: 'Complete' })
        .eq('job_id', job_id);
      if (!error) writeOk = true;
    }
    if (!writeOk) {
      const { error } = await supabase
        .from('audit_jobs')
        .update({ status: 'complete', result_json: results, completed_at: new Date().toISOString(), progress_message: 'Complete' })
        .eq('job_id', job_id);
      if (error) throw new Error(`[Supabase] Failed writing results: ${error.message}`);
    }

    console.log(`[Worker] Job ${job_id} complete.`);
  } catch (err) {
    console.error(`[Worker] Job ${job_id} failed:`, err.message);

    // store error
    await supabase
      .from('audit_jobs')
      .update({
        status: 'failed',
        error: err.message.slice(0, 800),
        progress_message: 'Failed'
      })
      .eq('job_id', job_id)
      .then(() => null)
      .catch(() => null);
  }
}

async function pollForJobs() {
  try {
    const job = await claimJob();
    if (!job) {
      await new Promise(r => setTimeout(r, 5000));
      return pollForJobs();
    }
    await handleJob(job);
    // Immediately look for next job
    setImmediate(pollForJobs);
  } catch (e) {
    console.error('[Worker] Poll loop error:', e.message);
    await new Promise(r => setTimeout(r, 5000));
    return pollForJobs();
  }
}

// Entrypoint
pollForJobs();
