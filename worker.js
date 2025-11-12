/* AuditPulse Pro worker.js (API base sanitizer + hardened)
   --------------------------------------------------
   - Uses HubSpot global API base (api.hubapi.com). If env is mis-set to eu1 etc., auto-corrects.
   - Atomic job claim via Supabase RPC (claim_audit_job), treats all-null as "no job"
   - Streaming CSV parse (bounded memory); retries with backoff; heartbeats
   - KPIs + 500k per-record enrichment cap
   - Column-safe installations lookup & token refresh (no .or on unknown columns)
*/

'use strict';

const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');
const { parse } = require('csv-parse');
const { v4: uuidv4 } = require('uuid');

// ================= ENV =================
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const HUBSPOT_CLIENT_ID = process.env.HUBSPOT_CLIENT_ID;
const HUBSPOT_CLIENT_SECRET = process.env.HUBSPOT_CLIENT_SECRET;
const RAW_API_BASE = process.env.HUBSPOT_API_BASE || 'https://api.hubapi.com';
const HUBSPOT_TOKEN_URL = 'https://api.hubapi.com/oauth/v1/token'; // OAuth is always global

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('[Worker] Missing SUPABASE_URL or SUPABASE_SERVICE_KEY.');
  process.exit(1);
}

// Normalize API base (defend against bad values like https://api.eu1.hubapi.com or eu1.api.hubapi.com)
function sanitizeApiBase(input) {
  try {
    const u = new URL(input);
    const h = (u.hostname || '').toLowerCase();
    // Known good host
    if (h === 'api.hubapi.com') return 'https://api.hubapi.com';
    // Common bad variants -> correct them
    if (h.includes('eu1.') || h.includes('us1.') || h.includes('api.')) {
      return 'https://api.hubapi.com';
    }
    return 'https://api.hubapi.com';
  } catch {
    return 'https://api.hubapi.com';
  }
}
const HUBSPOT_API_BASE = sanitizeApiBase(RAW_API_BASE);
console.log('[Worker] Using HUBSPOT_API_BASE:', HUBSPOT_API_BASE);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false }
});

const WORKER_ID = process.env.RENDER_SERVICE_NAME || `worker-${process.pid}-${uuidv4().slice(0, 8)}`;

// =============== CONSTANTS ===============
const MAX_RECORDS_ENRICHED = 500000;   // per-record drill-down enrichment cap
const SAMPLE_CAP_PER_LIST = 5000;      // drill-down sample size per list
const DUP_SAMPLE_PER_KEY = 10;         // sample per duplicate key
const PROGRESS_EVERY_N = 25000;        // progress update cadence
const HEARTBEAT_MS = 60000;            // extend lease every 60s

// =============== UTILS ===================
const sleep = ms => new Promise(r => setTimeout(r, ms));
const pick = (obj, keys, fallback = null) => {
  for (const k of keys) if (obj && obj[k] !== undefined && obj[k] !== null && obj[k] !== '') return obj[k];
  return fallback;
};

// Robust HubSpot fetch with retries
async function hsFetch(url, options = {}, attempt = 1) {
  const res = await fetch(url, options);
  if (res.status === 429 || res.status >= 500) {
    const retryAfterHeader = Number(res.headers.get('Retry-After')) || 0;
    const baseDelay = retryAfterHeader ? retryAfterHeader * 1000 : Math.min(30000, 500 * (2 ** attempt));
    const jitter = Math.floor(Math.random() * 500);
    const delay = baseDelay + jitter;
    console.warn(`[hsFetch] ${res.status} retry in ${delay} ms`);
    await sleep(delay);
    return hsFetch(url, options, attempt + 1);
  }
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`[HubSpot] ${res.status} ${res.statusText}: ${body.slice(0, 400)}`);
  }
  return res;
}

/*
  Column-safe finder for installations:
  - Tries candidate portal columns one by one.
  - Skips columns that do not exist.
  - Returns { row, matchedColumn } on success.
*/
async function findInstallationByPortalId(portal) {
  const candidates = ['portal_id', 'hubspot_portal_id', 'portalid', 'account_id', 'hubspot_account_id'];
  for (const col of candidates) {
    try {
      const { data, error } = await supabase.from('installations').select('*').eq(col, portal).limit(1);
      if (error) {
        const msg = String(error.message || '').toLowerCase();
        if (msg.includes('column') && msg.includes('does not exist')) continue;
        throw error;
      }
      if (data && data.length) return { row: data[0], matchedColumn: col };
    } catch (e) {
      const msg = String(e.message || '').toLowerCase();
      if (msg.includes('column') && msg.includes('does not exist')) continue;
      throw e;
    }
  }
  return { row: null, matchedColumn: null };
}

// =============== TOKEN REFRESH (column-safe) ===============
async function getValidAccessToken(portal_id_input) {
  const portal = String(portal_id_input || '').trim();
  if (!portal) throw new Error('Portal id is empty in job.');

  const { row: inst, matchedColumn } = await findInstallationByPortalId(portal);
  if (!inst || !matchedColumn) throw new Error(`No installation found for portal=${portal}`);

  const accessToken = pick(inst, ['access_token', 'accesstoken', 'token', 'accessToken']);
  const refreshToken = pick(inst, ['refresh_token', 'refreshtoken', 'refreshToken']);
  const expiresAtRaw = pick(inst, ['expires_at', 'access_token_expires_at', 'expiresAt']);
  const expiresAt = expiresAtRaw ? new Date(expiresAtRaw).getTime() : 0;
  const now = Date.now() + 30000;

  if (accessToken && expiresAt > now) return accessToken;
  if (!refreshToken) throw new Error('Missing refresh_token, cannot refresh.');

  const params = new URLSearchParams();
  params.append('grant_type', 'refresh_token');
  params.append('client_id', HUBSPOT_CLIENT_ID);
  params.append('client_secret', HUBSPOT_CLIENT_SECRET);
  params.append('refresh_token', refreshToken);

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
  const newRefreshToken = token.refresh_token || refreshToken;
  const expiresIn = token.expires_in || 1800;
  const newExpiresAt = new Date(Date.now() + expiresIn * 1000).toISOString();

  const { error: updErr } = await supabase
    .from('installations')
    .update({
      access_token: newAccessToken,
      refresh_token: newRefreshToken,
      expires_at: newExpiresAt,
      updated_at: new Date().toISOString()
    })
    .eq(matchedColumn, portal);

  if (updErr) throw new Error(`[Supabase] installations update failed: ${updErr.message}`);
  return newAccessToken;
}

// =============== SUPABASE RPCS & HELPERS ===============
async function claimJobViaRpc() {
  const { data, error } = await supabase.rpc('claim_audit_job', { p_worker_id: WORKER_ID });
  if (error) {
    console.error('[RPC] claim_audit_job error:', error.message);
    return null;
  }
  const row = Array.isArray(data) ? data[0] : data;
  if (!row) return null;
  const hasAnyValue = Object.values(row).some(v => v !== null && v !== undefined);
  if (!hasAnyValue) return null;
  return row;
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

// =============== HUBSPOT EXPORT HELPERS ===============
async function startExport(objectType, accessToken) {
  const res = await hsFetch(`${HUBSPOT_API_BASE}/crm/v3/exports/export/async`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json;charset=utf-8' },
    body: JSON.stringify({ exportType: 'CSV', format: 'CSV', objectType })
  });
  const json = await res.json();
  if (!json || !json.id) throw new Error('Export start did not return id.');
  return json.id;
}

async function waitForExportReady(taskId, accessToken, job_id) {
  const statusUrl = `${HUBSPOT_API_BASE}/crm/v3/exports/export/async/tasks/${encodeURIComponent(taskId)}/status`;
  let attempts = 0;
  while (true) {
    attempts++;
    const res = await hsFetch(statusUrl, { headers: { Authorization: `Bearer ${accessToken}` } });
    const json = await res.json();
    const state = (json.state || json.status || '').toString().toUpperCase();

    if (state === 'CANCELED' || state === 'FAILED')
      throw new Error(`Export task failed: ${JSON.stringify(json).slice(0, 400)}`);

    if ((state === 'COMPLETED' || state === 'COMPLETE') && json?.result?.url)
      return { downloadUrl: json.result.url, total: json?.result?.rowCount || null };

    const pct = json?.progress?.percentage || null;
    const msg = pct != null ? `Export preparing at ${pct}%` : 'Export preparing...';
    await updateProgress(job_id, msg);
    await sleep(Math.min(10000, 2000 + attempts * 500));
  }
}

// =============== CSV STREAM & KPIs ===============
function normalizeEmail(e) { return (e || '').trim().toLowerCase(); }
function normalizeDomain(d) { return (d || '').trim().toLowerCase().replace(/^https?:\/\//, '').replace(/^www\./, ''); }
function normalizePhone(p) { return (p || '').replace(/\D+/g, ''); }
function parseMaybeDate(v) {
  if (!v) return null;
  const n = Number(v);
  if (!isNaN(n) && n > 1000000000) return new Date(n);
  const d = new Date(v);
  return isNaN(d.getTime()) ? null : d;
}
function isValidEmailSimple(e) { return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e || ''); }

async function processCsvStream({ res, objectType, job_id }) {
  return new Promise((resolve, reject) => {
    const parser = parse({ columns: true, skip_empty_lines: true });

    let total = 0, enrichedCount = 0;
    const fillCounts = new Map();
    const uniqueProps = new Set();
    let ownerlessCount = 0, lifecycleMissing = 0, invalidEmailCount = 0, staleCount = 0;
    const STALE_DAYS = 180, staleCutoff = new Date(Date.now() - STALE_DAYS * 86400000);

    const orphanedRecords = [];
    const seenEmail = new Map(), seenDomain = new Map(), seenPhone = new Map();
    const duplicateSummary = { byEmail: 0, byDomain: 0, byPhone: 0 };

    const heartbeat = setInterval(() => extendLease(job_id).catch(() => {}), HEARTBEAT_MS);

    res.body.on('error', err => { clearInterval(heartbeat); reject(err); })
      .pipe(parser)
      .on('data', async row => {
        total++;

        if (total % PROGRESS_EVERY_N === 0)
          await updateProgress(job_id, `Processing ${total.toLocaleString()} records...`, total, null);

        Object.keys(row).forEach(k => uniqueProps.add(k));
        for (const [k, v] of Object.entries(row))
          if (v !== '' && v != null) fillCounts.set(k, (fillCounts.get(k) || 0) + 1);

        const recordId = row['Record ID'] || row['record_id'] || row['hs_object_id'];
        const ownerId = row['hubspot_owner_id'] || row['owner_id'] || '';
        if (!ownerId) ownerlessCount++;
        const lifecycle = row['lifecyclestage'] || '';
        if (!lifecycle) lifecycleMissing++;
        const mod = parseMaybeDate(row['hs_lastmodified_date'] || row['lastmodifieddate'] || row['Last Modified Date']);
        if (mod && mod < staleCutoff) staleCount++;

        if (objectType === 'contacts') {
          const email = normalizeEmail(row.email || row['Email']);
          if (email && !isValidEmailSimple(email)) invalidEmailCount++;

          const assocCompany = row.associatedcompanyid || row['Associated Company ID'] || '';
          if (!assocCompany && orphanedRecords.length < SAMPLE_CAP_PER_LIST && enrichedCount < MAX_RECORDS_ENRICHED)
            orphanedRecords.push({ id: recordId, email });

          if (email) {
            const e = seenEmail.get(email) || { count: 0, records: [] };
            e.count++;
            if (e.records.length < DUP_SAMPLE_PER_KEY && enrichedCount < MAX_RECORDS_ENRICHED)
              e.records.push({ id: recordId, email });
            seenEmail.set(email, e);
          }

          const phone = normalizePhone(row.phone || row['Phone Number'] || row['phone']);
          if (phone) {
            const p = seenPhone.get(phone) || { count: 0, records: [] };
            p.count++;
            if (p.records.length < DUP_SAMPLE_PER_KEY && enrichedCount < MAX_RECORDS_ENRICHED)
              p.records.push({ id: recordId, phone });
            seenPhone.set(phone, p);
          }
        } else {
          const numAssoc = Number(row.num_associated_contacts || row['Number of Associated Contacts'] || 0);
          if (!numAssoc && orphanedRecords.length < SAMPLE_CAP_PER_LIST && enrichedCount < MAX_RECORDS_ENRICHED)
            orphanedRecords.push({ id: recordId });

          const domain = normalizeDomain(row.domain || row['Company Domain Name'] || '');
          if (domain) {
            const d = seenDomain.get(domain) || { count: 0, records: [] };
            d.count++;
            if (d.records.length < DUP_SAMPLE_PER_KEY && enrichedCount < MAX_RECORDS_ENRICHED)
              d.records.push({ id: recordId, domain });
            seenDomain.set(domain, d);
          }
        }

        if (enrichedCount < MAX_RECORDS_ENRICHED) enrichedCount++;
      })
      .on('end', async () => {
        clearInterval(heartbeat);

        for (const [, v] of seenEmail) if (v.count > 1) duplicateSummary.byEmail += v.count;
        for (const [, v] of seenDomain) if (v.count > 1) duplicateSummary.byDomain += v.count;
        for (const [, v] of seenPhone) if (v.count > 1) duplicateSummary.byPhone += v.count;

        const fillRates = [];
        for (const prop of uniqueProps) {
          const count = fillCounts.get(prop) || 0;
          fillRates.push({ property: prop, filled: count, fillRate: total ? count / total : 0 });
        }
        fillRates.sort((a, b) => a.fillRate - b.fillRate);

        resolve({
          totals: { totalRecords: total },
          counts: {
            ownerless: ownerlessCount,
            lifecycleMissing,
            invalidEmails: objectType === 'contacts' ? invalidEmailCount : null,
            staleRecords: staleCount
          },
          orphaned: { sample: orphanedRecords },
          duplicates: {
            summary: duplicateSummary,
            samples: {
              byEmail: Array.from(seenEmail.entries()).filter(([, v]) => v.count > 1).slice(0, 100)
                .map(([value, v]) => ({ value, count: v.count, sample: v.records })),
              byDomain: Array.from(seenDomain.entries()).filter(([, v]) => v.count > 1).slice(0, 100)
                .map(([value, v]) => ({ value, count: v.count, sample: v.records })),
              byPhone: Array.from(seenPhone.entries()).filter(([, v]) => v.count > 1).slice(0, 100)
                .map(([value, v]) => ({ value, count: v.count, sample: v.records }))
            }
          },
          properties: { fillRates, top20Sparsest: fillRates.slice(0, 20) }
        });
      })
      .on('error', err => { clearInterval(heartbeat); reject(err); });
  });
}

// =============== WORKFLOW AUDIT =================
async function performWorkflowAudit({ portal_id, accessToken, job_id }) {
  await updateProgress(job_id, 'Fetching workflows...');
  const results = { totalWorkflows: 0, risks: { exposedApiKeys: [], deprecatedEmailV1: [] } };
  let after = undefined;

  while (true) {
    const url = new URL(`${HUBSPOT_API_BASE}/automation/v3/workflows`);
    if (after) url.searchParams.set('after', after);

    const res = await hsFetch(url.toString(), { headers: { Authorization: `Bearer ${accessToken}` } });
    const json = await res.json();

    const workflows = json.results || json.workflows || [];
    results.totalWorkflows += workflows.length;

    for (const wf of workflows) {
      const actions = wf.actions || [];
      for (const a of actions) {
        const blob = JSON.stringify(a).toLowerCase();
        if (/hapikey=/.test(blob)) results.risks.exposedApiKeys.push({ workflowId: wf.id, name: wf.name });
        if (/marketing-emails[\/\\]v1[\/\\]emails/.test(blob)) results.risks.deprecatedEmailV1.push({ workflowId: wf.id, name: wf.name });
      }
    }

    after = json.paging?.next?.after;
    if (!after) break;
    await updateProgress(job_id, `Workflows fetched: ${results.totalWorkflows}`);
  }

  return results;
}

// =============== CRM AUDIT ======================
async function performCrmAudit({ portal_id, objectType, job_id }) {
  await updateProgress(job_id, `Starting ${objectType} export...`);
  const accessToken = await getValidAccessToken(portal_id);

  const taskId = await startExport(objectType, accessToken);
  const { downloadUrl, total } = await waitForExportReady(taskId, accessToken, job_id);

  await updateProgress(job_id, `Downloading ${objectType} CSV...`, 0, total || null);
  const res = await hsFetch(downloadUrl, { method: 'GET' });

  await updateProgress(job_id, `Processing ${objectType} CSV...`);
  const kpis = await processCsvStream({ res, objectType, job_id });

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

// =============== JOB HANDLER ====================
function extractJobFields(job) {
  const job_id = pick(job, ['job_id', 'id', 'jobId', 'JOB_ID', 'JobId', 'JobID']);
  const portal_id = pick(job, [
    'portal_id', 'portalId', 'hubspot_portal_id', 'account_id', 'hubspot_account_id',
    'PORTAL_ID', 'PortalId', 'HubSpot_Portal_ID', 'accountId'
  ]);
  const type = pick(job, ['object_type', 'type', 'objectType', 'OBJECT_TYPE']) || 'contacts';
  return { job_id, portal_id, type };
}

async function handleJob(rawJob) {
  const { job_id, portal_id, type } = extractJobFields(rawJob || {});
  if (!job_id || !portal_id) return; // all-null composite → no work

  try {
    await updateProgress(job_id, `Job claimed on ${new Date().toISOString()}`);

    // Optional ping
    const accessToken = await getValidAccessToken(portal_id);
    await hsFetch(`${HUBSPOT_API_BASE}/crm/v3/properties/contacts`, {
      headers: { Authorization: `Bearer ${accessToken}` }
    }).catch(() => {});

    let results = null;
    if (type === 'contacts' || type === 'companies') {
      results = await performCrmAudit({ portal_id, objectType: type, job_id });
    } else if (type === 'workflows') {
      results = await performWorkflowAudit({ portal_id, accessToken, job_id });
    } else {
      throw new Error(`Unsupported object_type: ${type}`);
    }

    // Store results
    let ok = false;
    {
      const { error } = await supabase
        .from('audit_jobs')
        .update({ status: 'complete', results, completed_at: new Date().toISOString(), progress_message: 'Complete' })
        .eq('job_id', job_id);
      if (!error) ok = true;
    }
    if (!ok) {
      const { error } = await supabase
        .from('audit_jobs')
        .update({ status: 'complete', result_json: results, completed_at: new Date().toISOString(), progress_message: 'Complete' })
        .eq('job_id', job_id);
      if (error) throw new Error(`[Supabase] Failed writing results: ${error.message}`);
    }

    console.log(`[Worker] Job ${job_id} complete.`);
  } catch (err) {
    console.error(`[Worker] Job ${job_id || 'unknown'} failed:`, err.message);
    await supabase
      .from('audit_jobs')
      .update({ status: 'failed', error: err.message.slice(0, 800), progress_message: 'Failed' })
      .eq('job_id', job_id || '00000000-0000-0000-0000-000000000000')
      .then(() => null)
      .catch(() => null);
  }
}

// =============== POLL LOOP =======================
async function pollForJobs() {
  try {
    const job = await claimJobViaRpc();
    if (!job) {
      await sleep(5000);
      return pollForJobs();
    }
    await handleJob(job);
    setImmediate(pollForJobs);
  } catch (e) {
    console.error('[Worker] Poll loop error:', e.message);
    await sleep(5000);
    return pollForJobs();
  }
}

// Entrypoint
pollForJobs();
