'use strict';

const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');
const { parse } = require('csv-parse');
const { v4: uuidv4 } = require('uuid');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const HUBSPOT_CLIENT_ID = process.env.HUBSPOT_CLIENT_ID;
const HUBSPOT_CLIENT_SECRET = process.env.HUBSPOT_CLIENT_SECRET;
const HUBSPOT_API_BASE = 'https://api.hubapi.com';

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('[Worker] Missing Supabase configuration');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false }
});

const WORKER_ID = process.env.RENDER_SERVICE_NAME || `worker-${uuidv4().slice(0, 8)}`;
const HEARTBEAT_MS = 60_000;
const POLL_IDLE_MS = 5_000;
const PROGRESS_EVERY = 25_000;
const DUP_SAMPLE_PER_KEY = 10;
const SAMPLE_CAP = 5_000;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function hsFetch(url, options = {}, attempt = 1) {
  const res = await fetch(url, options);
  if (res.status === 429 || res.status >= 500) {
    const retryAfter = Number(res.headers.get('Retry-After') || 0) * 1000;
    const base = retryAfter || Math.min(30_000, 400 * 2 ** attempt);
    const jitter = Math.random() * 500;
    const delay = base + jitter;
    console.warn(`[Worker] HubSpot ${res.status}, retrying in ${Math.round(delay)}ms`);
    await sleep(delay);
    return hsFetch(url, options, attempt + 1);
  }
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`[HubSpot] ${res.status} ${res.statusText} ${text.slice(0, 400)}`);
  }
  return res;
}

async function getInstallation(portalId) {
  const { data, error } = await supabase
    .from('installations')
    .select('portal_id, access_token, refresh_token, expires_at')
    .eq('portal_id', portalId)
    .limit(1);
  if (error) throw new Error(`[Supabase] installations lookup failed: ${error.message}`);
  return data && data[0] ? data[0] : null;
}

async function refreshAccessToken(refreshToken) {
  const params = new URLSearchParams();
  params.append('grant_type', 'refresh_token');
  params.append('client_id', HUBSPOT_CLIENT_ID);
  params.append('client_secret', HUBSPOT_CLIENT_SECRET);
  params.append('refresh_token', refreshToken);

  const res = await fetch(`${HUBSPOT_API_BASE}/oauth/v1/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8' },
    body: params.toString()
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`[HubSpot OAuth] refresh failed: ${res.status} ${res.statusText} ${text.slice(0, 400)}`);
  }

  return res.json();
}

async function getValidAccessToken(portalId) {
  const install = await getInstallation(portalId);
  if (!install) throw new Error(`No installation found for portal ${portalId}`);

  const now = Date.now();
  if (install.access_token && install.expires_at && new Date(install.expires_at).getTime() > now + 30_000) {
    return install.access_token;
  }
  if (!install.refresh_token) throw new Error('Installation is missing a refresh_token');

  const refreshed = await refreshAccessToken(install.refresh_token);
  const accessToken = refreshed.access_token;
  const newRefresh = refreshed.refresh_token || install.refresh_token;
  const expiresAt = new Date(Date.now() + (refreshed.expires_in || 1800) * 1000).toISOString();

  const { error } = await supabase
    .from('installations')
    .update({
      access_token: accessToken,
      refresh_token: newRefresh,
      expires_at: expiresAt,
      updated_at: new Date().toISOString()
    })
    .eq('portal_id', portalId);

  if (error) throw new Error(`[Supabase] failed updating installation: ${error.message}`);
  return accessToken;
}

async function claimJob() {
  const { data, error } = await supabase.rpc('claim_audit_job', { p_worker_id: WORKER_ID });
  if (error) {
    console.error('[Worker] claim_audit_job error:', error.message);
    return null;
  }
  const job = Array.isArray(data) ? data[0] : data;
  if (!job) return null;
  const hasField = job.job_id || job.portal_id || job.object_type;
  return hasField ? job : null;
}

async function updateProgress(jobId, message, processed = null, total = null) {
  try {
    await supabase.rpc('update_audit_job_progress', {
      p_job_id: jobId,
      p_message: message || null,
      p_processed: processed,
      p_total: total
    });
  } catch (err) {
    console.warn('[Worker] update progress failed:', err.message);
  }
}

async function extendLease(jobId, minutes = 15) {
  try {
    await supabase.rpc('extend_audit_job_lease', { p_job_id: jobId, p_minutes: minutes });
  } catch (err) {
    console.warn('[Worker] extend lease failed:', err.message);
  }
}

async function startExport(objectType, accessToken) {
  const payload = {
    exportType: 'LIST',
    format: 'CSV',
    objectType: objectType === 'contacts' ? 'CONTACT' : 'COMPANY'
  };
  const res = await hsFetch(`${HUBSPOT_API_BASE}/crm/v3/exports/export/async`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
  });
  const json = await res.json();
  if (!json?.id) throw new Error('Export did not return an id');
  return json.id;
}

async function waitForExport(taskId, accessToken, jobId) {
  const statusUrl = `${HUBSPOT_API_BASE}/crm/v3/exports/export/async/tasks/${encodeURIComponent(taskId)}`;
  while (true) {
    const res = await hsFetch(statusUrl, { headers: { Authorization: `Bearer ${accessToken}` } });
    const json = await res.json();
    const state = (json.state || json.status || '').toString().toUpperCase();

    if (state === 'FAILED' || state === 'CANCELED') {
      throw new Error(`Export task ${taskId} failed: ${JSON.stringify(json).slice(0, 200)}`);
    }

    if ((state === 'COMPLETE' || state === 'COMPLETED') && json?.result?.url) {
      return { url: json.result.url, total: json.result.rowCount || null };
    }

    const pct = json?.progress?.percentage;
    await updateProgress(jobId, pct != null ? `Preparing export (${pct}%)` : 'Preparing export...');
    await sleep(2000);
  }
}

function normalizeEmail(email) {
  return (email || '').trim().toLowerCase();
}

function normalizeDomain(domain) {
  return (domain || '').trim().toLowerCase().replace(/^https?:\/\//, '').replace(/^www\./, '');
}

function normalizePhone(phone) {
  return (phone || '').replace(/\D+/g, '');
}

function parseDate(value) {
  if (!value) return null;
  const numeric = Number(value);
  if (!Number.isNaN(numeric) && numeric > 1_000_000_000_000) return new Date(numeric);
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

async function crunchCsv({ response, objectType, jobId }) {
  return new Promise((resolve, reject) => {
    const parser = parse({ columns: true, skip_empty_lines: true });
    const fillCounts = new Map();
    const properties = new Set();
    const duplicates = new Map();
    const orphanSample = [];
    let total = 0;
    let processedForProgress = 0;
    let ownerless = 0;
    let invalidEmails = 0;
    let stale = 0;
    const staleCutoff = new Date(Date.now() - 180 * 24 * 3600 * 1000);

    const heartbeat = setInterval(() => extendLease(jobId).catch(() => {}), HEARTBEAT_MS);

    response.body
      .on('error', err => {
        clearInterval(heartbeat);
        reject(err);
      })
      .pipe(parser)
      .on('data', async row => {
        total += 1;
        processedForProgress += 1;

        if (processedForProgress >= PROGRESS_EVERY) {
          processedForProgress = 0;
          await updateProgress(jobId, `Processed ${total.toLocaleString()} records`, total, null);
        }

        for (const [key, value] of Object.entries(row)) {
          properties.add(key);
          if (value !== '' && value != null) {
            fillCounts.set(key, (fillCounts.get(key) || 0) + 1);
          }
        }

        const ownerId = row.hubspot_owner_id || row.owner_id || '';
        if (!ownerId) ownerless += 1;

        const lastModified = parseDate(row.hs_lastmodified_date || row.lastmodifieddate || row['Last Modified Date']);
        if (lastModified && lastModified < staleCutoff) stale += 1;

        const recordId = row['Record ID'] || row['record_id'] || row['hs_object_id'];

        if (objectType === 'contacts') {
          const email = normalizeEmail(row.email || row['Email']);
          if (email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) invalidEmails += 1;

          const assocCompany = row.associatedcompanyid || row['Associated Company ID'];
          if (!assocCompany && orphanSample.length < SAMPLE_CAP) {
            orphanSample.push({ id: recordId, label: email });
          }

          if (email) {
            const key = `email:${email}`;
            const info = duplicates.get(key) || { type: 'email', key: email, count: 0, samples: [] };
            info.count += 1;
            if (info.samples.length < DUP_SAMPLE_PER_KEY) info.samples.push(recordId);
            duplicates.set(key, info);
          }

          const phone = normalizePhone(row.phone || row['Phone Number']);
          if (phone) {
            const key = `phone:${phone}`;
            const info = duplicates.get(key) || { type: 'phone', key: phone, count: 0, samples: [] };
            info.count += 1;
            if (info.samples.length < DUP_SAMPLE_PER_KEY) info.samples.push(recordId);
            duplicates.set(key, info);
          }
        } else {
          const domain = normalizeDomain(row.domain || row['Company Domain Name']);
          if (domain) {
            const key = `domain:${domain}`;
            const info = duplicates.get(key) || { type: 'domain', key: domain, count: 0, samples: [] };
            info.count += 1;
            if (info.samples.length < DUP_SAMPLE_PER_KEY) info.samples.push(recordId);
            duplicates.set(key, info);
          }

          const assocContacts = Number(row.num_associated_contacts || row['Number of Associated Contacts'] || 0);
          if (!assocContacts && orphanSample.length < SAMPLE_CAP) {
            orphanSample.push({ id: recordId, label: domain });
          }
        }
      })
      .on('end', () => {
        clearInterval(heartbeat);
        const sparsest = Array.from(properties).map(property => {
          const filled = fillCounts.get(property) || 0;
          const rate = total ? Math.round((filled / total) * 10000) / 100 : 0;
          return { property, filled, fillRate: `${rate.toFixed(2)}%` };
        });
        sparsest.sort((a, b) => parseFloat(a.fillRate) - parseFloat(b.fillRate));

        const duplicateKeys = Array.from(duplicates.values()).filter(item => item.count > 1);

        resolve({
          total,
          ownerless,
          invalidEmails,
          stale,
          orphanSample,
          duplicateKeys,
          propertyFill: sparsest.slice(0, 20)
        });
      })
      .on('error', err => {
        clearInterval(heartbeat);
        reject(err);
      });
  });
}

async function performCrmAudit(job) {
  const { job_id, portal_id, object_type } = job;
  await updateProgress(job_id, `Starting ${object_type} export`);
  const accessToken = await getValidAccessToken(portal_id);

  const taskId = await startExport(object_type, accessToken);
  const { url, total } = await waitForExport(taskId, accessToken, job_id);

  await updateProgress(job_id, 'Downloading CSV', 0, total || null);
  const response = await hsFetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });

  await updateProgress(job_id, 'Processing records');
  const kpis = await crunchCsv({ response, objectType: object_type, jobId: job_id });

  const summary = [
    { label: 'Total records', value: kpis.total.toLocaleString() },
    { label: 'Ownerless records', value: kpis.ownerless.toLocaleString() },
    { label: 'Stale (>180d)', value: kpis.stale.toLocaleString() },
  ];
  if (object_type === 'contacts') {
    summary.push({ label: 'Invalid emails', value: kpis.invalidEmails.toLocaleString() });
  }

  const payload = {
    summary,
    orphanSamples: kpis.orphanSample,
    duplicateKeys: kpis.duplicateKeys.map(item => ({
      type: item.type,
      key: item.key,
      count: item.count,
      samples: item.samples
    })).slice(0, 100),
    propertyFill: kpis.propertyFill,
    metadata: {
      portalId: portal_id,
      objectType: object_type,
      generatedAt: new Date().toISOString()
    }
  };

  return { payload, totals: kpis.total };
}

async function performWorkflowAudit(job) {
  const { job_id, portal_id } = job;
  await updateProgress(job_id, 'Fetching workflows');
  const accessToken = await getValidAccessToken(portal_id);

  let total = 0;
  const risks = { exposedApiKeys: [], legacyEmailApi: [] };
  let after = null;

  while (true) {
    const url = new URL(`${HUBSPOT_API_BASE}/automation/v3/workflows`);
    if (after) url.searchParams.set('after', after);
    const res = await hsFetch(url.toString(), { headers: { Authorization: `Bearer ${accessToken}` } });
    const json = await res.json();

    const workflows = json.results || json.workflows || [];
    total += workflows.length;

    workflows.forEach(wf => {
      const actions = wf.actions || [];
      actions.forEach(action => {
        const serialized = JSON.stringify(action).toLowerCase();
        if (serialized.includes('hapikey=')) risks.exposedApiKeys.push({ id: wf.id, name: wf.name });
        if (serialized.includes('marketing-emails/v1/emails')) risks.legacyEmailApi.push({ id: wf.id, name: wf.name });
      });
    });

    after = json.paging?.next?.after;
    if (!after) break;
    await updateProgress(job_id, `Fetched ${total} workflows`);
  }

  const payload = {
    summary: [
      { label: 'Total workflows', value: total.toLocaleString() },
      { label: 'Exposed API keys', value: risks.exposedApiKeys.length },
      { label: 'Legacy email API actions', value: risks.legacyEmailApi.length }
    ],
    orphanSamples: [],
    duplicateKeys: [],
    propertyFill: [],
    metadata: {
      portalId: portal_id,
      objectType: 'workflows',
      generatedAt: new Date().toISOString()
    },
    risks
  };

  return { payload, totals: total };
}

async function completeJob(jobId, result, totals) {
  const updates = {
    status: 'complete',
    result_json: result,
    results: result.summary,
    progress_message: 'Complete',
    processed_records: totals || 0,
    total_records: totals || 0,
    completed_at: new Date().toISOString()
  };
  const { error } = await supabase.from('audit_jobs').update(updates).eq('job_id', jobId);
  if (error) throw new Error(`[Supabase] failed to finalize job: ${error.message}`);
}

async function failJob(jobId, message) {
  await supabase
    .from('audit_jobs')
    .update({ status: 'failed', error: message.slice(0, 800), progress_message: 'Failed' })
    .eq('job_id', jobId)
    .then(() => null)
    .catch(() => null);
}

async function handleJob(job) {
  const jobId = job.job_id;
  const portalId = job.portal_id;
  const type = job.object_type;

  if (!jobId || !portalId) {
    return;
  }

  console.log(`[Worker] Claimed job ${jobId} portal=${portalId} type=${type}`);
  try {
    await updateProgress(jobId, `Job claimed at ${new Date().toISOString()}`);

    let outcome;
    if (type === 'contacts' || type === 'companies') {
      outcome = await performCrmAudit(job);
    } else if (type === 'workflows') {
      outcome = await performWorkflowAudit(job);
    } else {
      throw new Error(`Unsupported object_type ${type}`);
    }

    await completeJob(jobId, outcome.payload, outcome.totals);
    console.log(`[Worker] Job ${jobId} completed`);
  } catch (err) {
    console.error(`[Worker] Job ${jobId} failed:`, err.message);
    await failJob(jobId, err.message || 'Unknown error');
  }
}

async function loop() {
  try {
    const job = await claimJob();
    if (!job) {
      await sleep(POLL_IDLE_MS);
    } else {
      await handleJob(job);
    }
  } catch (err) {
    console.error('[Worker] Loop error:', err.message);
    await sleep(POLL_IDLE_MS);
  }
  setImmediate(loop);
}

loop();
