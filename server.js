require('dotenv').config();

const express = require('express');
const path = require('path');
const cors = require('cors');
const fetch = require('node-fetch');
const { v4: uuidv4 } = require('uuid');
const ExcelJS = require('exceljs');
const { createClient } = require('@supabase/supabase-js');

// ---------- Environment helpers ----------
function required(name) {
  const value = process.env[name];
  if (!value) {
    console.error(`[Web] Missing required environment variable ${name}`);
    process.exit(1);
  }
  return value;
}

const SUPABASE_URL = required('SUPABASE_URL');
const SUPABASE_SERVICE_KEY = required('SUPABASE_SERVICE_KEY');
const HUBSPOT_CLIENT_ID = required('HUBSPOT_CLIENT_ID');
const HUBSPOT_CLIENT_SECRET = required('HUBSPOT_CLIENT_SECRET');
const HUBSPOT_REDIRECT_URI = required('HUBSPOT_REDIRECT_URI');
const HUBSPOT_API_BASE = (process.env.HUBSPOT_API_BASE || 'https://api.hubapi.com').replace(/\/$/, '');
const CORS_ORIGIN = process.env.CORS_ORIGIN || null;
const PORT = process.env.PORT || 10000;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false }
});

const app = express();

if (CORS_ORIGIN) {
  app.use(cors({ origin: CORS_ORIGIN }));
} else {
  app.use(cors());
}

app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ---------- Utility functions ----------
function buildInstallRedirect() {
  const scopes = [
    'crm.objects.contacts.read',
    'crm.objects.companies.read',
    'automation.workflows.read',
    'crm.schemas.contacts.read',
    'crm.schemas.companies.read',
    'oauth'
  ];
  const params = new URLSearchParams({
    client_id: HUBSPOT_CLIENT_ID,
    redirect_uri: HUBSPOT_REDIRECT_URI,
    scope: scopes.join(' ')
  });
  return `https://app.hubspot.com/oauth/authorize?${params.toString()}`;
}

async function exchangeCodeForTokens(code) {
  const params = new URLSearchParams();
  params.append('grant_type', 'authorization_code');
  params.append('client_id', HUBSPOT_CLIENT_ID);
  params.append('client_secret', HUBSPOT_CLIENT_SECRET);
  params.append('redirect_uri', HUBSPOT_REDIRECT_URI);
  params.append('code', code);

  const res = await fetch('https://api.hubapi.com/oauth/v1/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8' },
    body: params.toString()
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`OAuth token exchange failed: ${res.status} ${res.statusText} ${text}`);
  }

  return res.json();
}

async function fetchTokenInfo(accessToken) {
  const res = await fetch(`https://api.hubapi.com/oauth/v1/access-tokens/${accessToken}`);
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Failed fetching access token metadata: ${res.status} ${res.statusText} ${text}`);
  }
  return res.json();
}

async function getInstallation(portalId) {
  const { data, error } = await supabase
    .from('installations')
    .select('portal_id, expires_at, updated_at')
    .eq('portal_id', portalId)
    .limit(1);

  if (error) throw error;
  return data && data.length ? data[0] : null;
}

async function assertInstallation(portalId) {
  const install = await getInstallation(portalId);
  if (!install) {
    const err = new Error(`No installation found for portal ${portalId}`);
    err.statusCode = 404;
    throw err;
  }
  return install;
}

function normalizeJob(job) {
  if (!job) return null;
  return {
    jobId: job.job_id,
    portalId: job.portal_id,
    objectType: job.object_type,
    status: job.status,
    progressMessage: job.progress_message,
    processedRecords: job.processed_records,
    totalRecords: job.total_records,
    error: job.error,
    createdAt: job.created_at,
    completedAt: job.completed_at,
    results: job.results,
    resultJson: job.result_json
  };
}

function buildWorkbook(job, result) {
  const workbook = new ExcelJS.Workbook();
  const metaSheet = workbook.addWorksheet('Summary');
  metaSheet.columns = [
    { header: 'Metric', key: 'metric', width: 40 },
    { header: 'Value', key: 'value', width: 25 },
  ];

  const summary = Array.isArray(result?.summary) ? result.summary : [];
  for (const row of summary) {
    metaSheet.addRow({ metric: row.label || row.metric, value: row.value });
  }

  metaSheet.addRow({});
  metaSheet.addRow({ metric: 'Portal ID', value: job.portalId });
  metaSheet.addRow({ metric: 'Object Type', value: job.objectType });
  metaSheet.addRow({ metric: 'Job ID', value: job.jobId });

  const orphanSheet = workbook.addWorksheet('Orphan Sample');
  orphanSheet.columns = [
    { header: 'Record ID', key: 'id', width: 25 },
    { header: 'Label', key: 'label', width: 40 },
  ];
  const orphanSample = Array.isArray(result?.orphanSamples) ? result.orphanSamples : [];
  orphanSample.forEach(item => orphanSheet.addRow({ id: item.id, label: item.label || item.email || '' }));

  const dupSheet = workbook.addWorksheet('Duplicate Keys');
  dupSheet.columns = [
    { header: 'Type', key: 'type', width: 15 },
    { header: 'Key', key: 'key', width: 35 },
    { header: 'Count', key: 'count', width: 10 },
    { header: 'Sample IDs', key: 'samples', width: 60 },
  ];
  const duplicates = Array.isArray(result?.duplicateKeys) ? result.duplicateKeys : [];
  for (const dup of duplicates) {
    dupSheet.addRow({
      type: dup.type,
      key: dup.key,
      count: dup.count,
      samples: Array.isArray(dup.samples) ? dup.samples.join(', ') : ''
    });
  }

  const propsSheet = workbook.addWorksheet('Property Fill');
  propsSheet.columns = [
    { header: 'Property', key: 'property', width: 35 },
    { header: 'Filled', key: 'filled', width: 12 },
    { header: 'Fill Rate', key: 'fillRate', width: 12 },
  ];
  const props = Array.isArray(result?.propertyFill) ? result.propertyFill : [];
  props.forEach(p => propsSheet.addRow({ property: p.property, filled: p.filled, fillRate: p.fillRate }));

  return workbook;
}

// ---------- Routes ----------
app.get('/api/install', (_req, res) => {
  res.redirect(buildInstallRedirect());
});

app.get('/api/oauth-callback', async (req, res) => {
  const { code } = req.query;
  if (!code) return res.status(400).send('Missing HubSpot authorization code.');

  try {
    const tokenResponse = await exchangeCodeForTokens(code);
    const { access_token, refresh_token, expires_in } = tokenResponse;
    const tokenInfo = await fetchTokenInfo(access_token);

    const hubId = String(tokenInfo.hub_id || tokenInfo.hubId || '');
    if (!hubId) throw new Error('HubSpot did not return a hub_id');

    const expiresAt = new Date(Date.now() + (expires_in || 1800) * 1000).toISOString();

    const { error } = await supabase.from('installations').upsert({
      portal_id: hubId,
      access_token,
      refresh_token,
      expires_at: expiresAt,
      updated_at: new Date().toISOString()
    });

    if (error) throw error;

    res.redirect(`/?portal_id=${encodeURIComponent(hubId)}&installed=1`);
  } catch (err) {
    console.error('[Web] OAuth callback error:', err.message);
    res.status(500).send(`OAuth failed: ${err.message}`);
  }
});

app.get('/api/install-status', async (req, res) => {
  const portalId = (req.query.portal_id || '').trim();
  if (!portalId) return res.status(400).json({ error: 'portal_id is required' });

  try {
    const install = await getInstallation(portalId);
    res.json({
      installed: Boolean(install),
      portalId,
      expiresAt: install?.expires_at || null,
      updatedAt: install?.updated_at || null
    });
  } catch (err) {
    console.error('[Web] install-status error:', err.message);
    res.status(500).json({ error: 'Failed to fetch installation status' });
  }
});

app.post('/api/create-job', async (req, res) => {
  const portalId = String(req.body.portalId || '').trim();
  const objectType = String(req.body.objectType || '').trim().toLowerCase();

  if (!portalId) return res.status(400).json({ error: 'portalId is required' });
  if (!['contacts', 'companies', 'workflows'].includes(objectType)) {
    return res.status(400).json({ error: 'Invalid objectType' });
  }

  try {
    await assertInstallation(portalId);
    const jobId = uuidv4();

    const { error } = await supabase.from('audit_jobs').insert({
      job_id: jobId,
      portal_id: portalId,
      object_type: objectType,
      status: 'pending',
      progress_message: 'Queued',
      processed_records: 0,
      total_records: null
    });

    if (error) throw error;

    res.status(201).json({ jobId });
  } catch (err) {
    const status = err.statusCode || 500;
    console.error('[Web] create-job error:', err.message);
    res.status(status).json({ error: err.message });
  }
});

app.get('/api/job-status', async (req, res) => {
  const jobId = String(req.query.job_id || '').trim();
  if (!jobId) return res.status(400).json({ error: 'job_id is required' });

  try {
    const { data, error } = await supabase
      .from('audit_jobs')
      .select(
        'job_id, portal_id, object_type, status, progress_message, processed_records, total_records, error, results, result_json, created_at, completed_at'
      )
      .eq('job_id', jobId)
      .limit(1);

    if (error) throw error;
    const job = data && data[0];
    if (!job) return res.status(404).json({ error: 'Job not found' });

    res.json(normalizeJob(job));
  } catch (err) {
    console.error('[Web] job-status error:', err.message);
    res.status(500).json({ error: 'Failed to fetch job status' });
  }
});

app.get('/api/download', async (req, res) => {
  const jobId = String(req.query.job_id || '').trim();
  if (!jobId) return res.status(400).json({ error: 'job_id is required' });

  try {
    const { data, error } = await supabase
      .from('audit_jobs')
      .select('job_id, portal_id, object_type, status, result_json, results')
      .eq('job_id', jobId)
      .limit(1);

    if (error) throw error;
    const job = data && data[0];
    if (!job) return res.status(404).json({ error: 'Job not found' });
    if (job.status !== 'complete') return res.status(409).json({ error: 'Job is not complete yet' });

    const normalized = normalizeJob(job);
    const result = normalized.resultJson || normalized.results || {};

    const workbook = buildWorkbook(normalized, result);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="audit-${normalized.objectType}-${normalized.portalId}.xlsx"`);

    await workbook.xlsx.write(res);
    res.end();
  } catch (err) {
    console.error('[Web] download error:', err.message);
    res.status(500).json({ error: 'Failed to generate Excel' });
  }
});

app.get('/api/health', (_req, res) => {
  res.json({ status: 'ok', time: new Date().toISOString() });
});

app.get('*', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`[Web] Listening on port ${PORT}`);
});
