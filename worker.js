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
  const payload = {
    exportType: 'CSV',
    format: 'CSV',
    objectType: objectType === 'contacts' ? 'CONTACT' : objectType === 'companies' ? 'COMPANY' : objectType.toUpperCase()
  };
  const res = await hsFetch(`${HUBSPOT_API_BASE}/crm/v3/exports/export/async`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json;charset=utf-8' },
    body: JSON.stringify(payload)
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

    if (state === 'CANCELED' || state === 'FAILED') {
      throw new Error(`Export task failed: ${JSON.stringify(json).slice(0, 400)}`);
    }

    if ((state === 'COMPLETED' || state === 'COMPLETE') && json?.result?.url) {
      return { downloadUrl: json.result.url, total: json?.result?.rowCount || null };
    }

    const pct = json?.progress?.percentage || null;
    const msg = pct != null ? `Export preparing at ${pct}%` : 'Export preparing...';
    await updateProgress(job_id, msg);
    await sleep(Math.min(10000, 2000 + attempts * 500));
  }
}

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

function registerDuplicate(groupMap, key, type, snapshot) {
  let group = groupMap.get(key);
  if (!group) {
    group = { key, type, count: 0, records: [] };
    groupMap.set(key, group);
  }
  group.count += 1;
  if (group.records.length < DUP_SAMPLE_PER_KEY) {
    group.records.push(snapshot);
  }
}

async function processCsvStream({ res, objectType, job_id }) {
  return new Promise((resolve, reject) => {
    const parser = parse({ columns: true, skip_empty_lines: true });

    let totalRecords = 0;
    let enrichedCount = 0;
    const fillCounts = new Map();
    const uniqueProps = new Set();
    let ownerlessCount = 0;
    let lifecycleMissing = 0;
    let invalidEmailCount = 0;
    let staleCount = 0;
    const STALE_DAYS = 180;
    const staleCutoff = new Date(Date.now() - STALE_DAYS * 86400000);

    const orphanedRecords = [];
    const duplicateGroups = new Map();

    const heartbeat = setInterval(() => extendLease(job_id).catch(() => {}), HEARTBEAT_MS);

    res.body
      .on('error', err => {
        clearInterval(heartbeat);
        reject(err);
      })
      .pipe(parser)
      .on('data', async row => {
        totalRecords++;

        if (totalRecords % PROGRESS_EVERY_N === 0) {
          await updateProgress(job_id, `Processing ${totalRecords.toLocaleString()} records...`, totalRecords, null);
        }

        const properties = {};
        for (const [key, value] of Object.entries(row)) {
          uniqueProps.add(key);
          properties[key] = value;
          if (value !== '' && value != null) {
            fillCounts.set(key, (fillCounts.get(key) || 0) + 1);
          }
        }

        const recordId = row['Record ID'] || row['record_id'] || row['hs_object_id'] || row['objectId'];
        const snapshot = { id: recordId, properties };

        const ownerId = row['hubspot_owner_id'] || row['owner_id'] || row['hs_owner_id'] || '';
        if (!ownerId) ownerlessCount++;
        const lifecycle = row['lifecyclestage'] || row['lifecycle_stage'] || row['Lifecycle Stage'] || '';
        if (!lifecycle) lifecycleMissing++;
        const mod = parseMaybeDate(row['hs_lastmodified_date'] || row['lastmodifieddate'] || row['Last Modified Date']);
        if (mod && mod < staleCutoff) staleCount++;

        if (objectType === 'contacts') {
          const email = normalizeEmail(row.email || row['Email']);
          if (email && !isValidEmailSimple(email)) invalidEmailCount++;

          const assocCompany = row.associatedcompanyid || row['Associated Company ID'] || row['associatedCompanyId'] || '';
          if (!assocCompany && orphanedRecords.length < SAMPLE_CAP_PER_LIST && enrichedCount < MAX_RECORDS_ENRICHED) {
            orphanedRecords.push(snapshot);
          }

          if (email) {
            registerDuplicate(duplicateGroups, `email:${email}`, 'email', snapshot);
          }

          const phone = normalizePhone(row.phone || row['Phone Number'] || row['phone']);
          if (phone) {
            registerDuplicate(duplicateGroups, `phone:${phone}`, 'phone', snapshot);
          }
        } else if (objectType === 'companies') {
          const assoc = Number(row.num_associated_contacts || row['Number of Associated Contacts'] || row['associated_contacts'] || 0);
          if (!assoc && orphanedRecords.length < SAMPLE_CAP_PER_LIST && enrichedCount < MAX_RECORDS_ENRICHED) {
            orphanedRecords.push(snapshot);
          }
          const domain = normalizeDomain(row.domain || row['Company Domain Name'] || row['company_domain']);
          if (domain) {
            registerDuplicate(duplicateGroups, `domain:${domain}`, 'domain', snapshot);
          }
        }

        if (enrichedCount < MAX_RECORDS_ENRICHED) enrichedCount++;
      })
      .on('end', async () => {
        clearInterval(heartbeat);

        let totalDuplicates = 0;
        const duplicateRecords = [];
        const duplicateRecordIds = new Set();
        for (const group of duplicateGroups.values()) {
          if (group.count <= 1) continue;
          totalDuplicates += group.count - 1;
          for (const record of group.records) {
            if (!record || !record.id || duplicateRecordIds.has(record.id)) continue;
            duplicateRecordIds.add(record.id);
            duplicateRecords.push(record);
            if (duplicateRecords.length >= SAMPLE_CAP_PER_LIST) break;
          }
          if (duplicateRecords.length >= SAMPLE_CAP_PER_LIST) break;
        }

        resolve({
          totalRecords,
          fillCounts,
          uniqueProps,
          orphanedRecords,
          duplicateRecords,
          duplicateGroups,
          totalDuplicates,
          ownerlessCount,
          lifecycleMissing,
          invalidEmailCount,
          staleCount
        });
      })
      .on('error', err => {
        clearInterval(heartbeat);
        reject(err);
      });
  });
}

async function fetchPropertyMetadata(objectType, accessToken) {
  const res = await hsFetch(`${HUBSPOT_API_BASE}/crm/v3/properties/${objectType}`, {
    headers: { Authorization: `Bearer ${accessToken}` }
  });
  const json = await res.json();
  return Array.isArray(json?.results) ? json.results : [];
}

// =============== WORKFLOW AUDIT =================
async function performWorkflowAudit({ portal_id, accessToken, job_id }) {
  await updateProgress(job_id, 'Fetching workflows...');
  let totalWorkflows = 0;
  let inactiveWorkflows = 0;
  let noEnrollmentWorkflows = 0;
  const findings = [];
  let after = undefined;

  while (true) {
    const url = new URL(`${HUBSPOT_API_BASE}/automation/v3/workflows`);
    if (after) url.searchParams.set('after', after);

    const res = await hsFetch(url.toString(), { headers: { Authorization: `Bearer ${accessToken}` } });
    const json = await res.json();

    const workflows = json.results || json.workflows || [];
    totalWorkflows += workflows.length;

    for (const wf of workflows) {
      if (wf.enabled === false) inactiveWorkflows++;
      const enrolled = wf.currentlyEnrolledCount ?? wf.enrollmentCount ?? wf.stats?.enrolledCount ?? null;
      if (enrolled === 0) noEnrollmentWorkflows++;

      const actions = wf.actions || [];
      for (const action of actions) {
        const serialized = JSON.stringify(action || {}).toLowerCase();
        const common = {
          workflow_id: wf.id,
          workflow_name: wf.name || 'Unnamed workflow',
          action_type: action.type || action.actionType || 'action',
          last_updated: wf.updatedAt || wf.updated_at || wf.updatedOn || wf.lastUpdated || null
        };

        if (serialized.includes('hapikey=')) {
          findings.push({
            ...common,
            finding: 'Exposed hapikey parameter',
            details: action.name || 'Custom code / webhook contains hapikey='
          });
        }
        if (serialized.includes('marketing-emails') && serialized.includes('/v1/')) {
          findings.push({
            ...common,
            finding: 'Legacy Marketing Email API',
            details: action.name || 'Action references deprecated v1 Marketing Email API'
          });
        }
      }
    }

    after = json.paging?.next?.after;
    if (!after) break;
    await updateProgress(job_id, `Workflows fetched: ${totalWorkflows}`);
  }

  const payload = {
    auditType: 'workflows',
    kpis: {
      totalWorkflows,
      inactiveWorkflows,
      noEnrollmentWorkflows,
      isIncomplete: false
    },
    results: findings.map(item => ({
      ...item,
      last_updated: item.last_updated ? new Date(item.last_updated).toISOString() : 'Unknown'
    }))
  };

  return payload;
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

  const propertyMeta = await fetchPropertyMetadata(objectType, accessToken).catch(() => []);
  const metaMap = new Map();
  propertyMeta.forEach(meta => metaMap.set(meta.name, meta));

  const properties = [];
  for (const prop of kpis.uniqueProps) {
    const meta = metaMap.get(prop) || {};
    const fillCount = kpis.fillCounts.get(prop) || 0;
    const fillRate = kpis.totalRecords ? Math.round((fillCount / kpis.totalRecords) * 100) : 0;
    properties.push({
      label: meta.label || prop,
      internalName: prop,
      type: meta.type || meta.fieldType || 'string',
      description: meta.description || '',
      isCustom: meta.hubspotDefined === false ? true : false,
      fillCount,
      fillRate
    });
  }

  const customProps = properties.filter(p => p.isCustom);
  const avgCustomFillRate = customProps.length
    ? Number((customProps.reduce((sum, p) => sum + p.fillRate, 0) / customProps.length).toFixed(1))
    : 0;

  const payload = {
    auditType: 'crm',
    objectType,
    data: {
      totalRecords: kpis.totalRecords,
      limitHit: false,
      totalProperties: properties.length,
      averageCustomFillRate: avgCustomFillRate,
      properties: properties.sort((a, b) => a.label.localeCompare(b.label)),
      orphanedRecords: kpis.orphanedRecords,
      duplicateRecords: kpis.duplicateRecords,
      totalDuplicates: kpis.totalDuplicates,
      metrics: {
        ownerless: kpis.ownerlessCount,
        lifecycleMissing: kpis.lifecycleMissing,
        invalidEmails: objectType === 'contacts' ? kpis.invalidEmailCount : null,
        staleRecords: kpis.staleCount
      }
    }
  };

  return { payload, totals: kpis.totalRecords };
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

async function performWorkflowJob(job, accessToken) {
  return performWorkflowAudit({ portal_id: job.portal_id, accessToken, job_id: job.job_id });
}

async function handleJob(rawJob) {
  const { job_id, portal_id, type } = extractJobFields(rawJob || {});
  if (!job_id || !portal_id) return; // all-null composite → no work

  try {
    await updateProgress(job_id, `Job claimed on ${new Date().toISOString()}`);

    let payload;
    let totals = null;
    if (type === 'contacts' || type === 'companies') {
      const outcome = await performCrmAudit({ portal_id, objectType: type, job_id });
      payload = outcome.payload;
      totals = outcome.totals;
    } else if (type === 'workflows') {
      const accessToken = await getValidAccessToken(portal_id);
      payload = await performWorkflowJob({ job_id, portal_id }, accessToken);
    } else {
      throw new Error(`Unsupported object_type: ${type}`);
    }

    const { error } = await supabase
      .from('audit_jobs')
      .update({
        status: 'complete',
        result_json: payload,
        completed_at: new Date().toISOString(),
        progress_message: 'Complete',
        processed_records: totals || null,
        total_records: totals || null
      })
      .eq('job_id', job_id);

    if (error) throw new Error(`[Supabase] Failed writing results: ${error.message}`);
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
