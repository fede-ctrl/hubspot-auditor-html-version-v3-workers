require('dotenv').config();

const express = require('express');
const fetch = require('node-fetch');
const { createClient } = require('@supabase/supabase-js');
const cors = require('cors');
const path = require('path');
const ExcelJS = require('exceljs');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = process.env.PORT || 10000;

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

const requiredEnv = name => {
  const value = process.env[name];
  if (!value) {
    console.error(`Missing required environment variable ${name}`);
    process.exit(1);
  }
  return value;
};

const CLIENT_ID = requiredEnv('HUBSPOT_CLIENT_ID');
const CLIENT_SECRET = requiredEnv('HUBSPOT_CLIENT_SECRET');
const SUPABASE_URL = requiredEnv('SUPABASE_URL');
const SUPABASE_SERVICE_KEY = requiredEnv('SUPABASE_SERVICE_KEY');
const RENDER_EXTERNAL_URL = requiredEnv('RENDER_EXTERNAL_URL').replace(/\/$/, '');

const REDIRECT_URI = `${RENDER_EXTERNAL_URL}/api/oauth-callback`;
const HUBSPOT_API_BASE = 'https://api.hubapi.com';
const HUBSPOT_AUTH_BASE = 'https://app-eu1.hubspot.com';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false }
});

async function getInstallation(portalId) {
  const { data, error } = await supabase
    .from('installations')
    .select('portal_id, access_token, refresh_token, expires_at, updated_at')
    .eq('portal_id', portalId)
    .maybeSingle();

  if (error) {
    throw new Error(`Supabase lookup failed: ${error.message}`);
  }
  return data || null;
}

async function refreshAccessToken(refreshToken) {
  const params = new URLSearchParams();
  params.append('grant_type', 'refresh_token');
  params.append('client_id', CLIENT_ID);
  params.append('client_secret', CLIENT_SECRET);
  params.append('refresh_token', refreshToken);

  const res = await fetch(`${HUBSPOT_API_BASE}/oauth/v1/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: params.toString()
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Token refresh failed: ${res.status} ${res.statusText} ${text}`);
  }

  return res.json();
}

async function getValidAccessToken(portalId) {
  const install = await getInstallation(portalId);
  if (!install) {
    throw new Error(`Could not find installation for portal ${portalId}. Please reinstall the app.`);
  }

  const now = Date.now();
  const expiresAt = install.expires_at ? new Date(install.expires_at).getTime() : 0;
  if (install.access_token && expiresAt > now + 30_000) {
    return install.access_token;
  }

  if (!install.refresh_token) {
    throw new Error('Installation is missing a refresh token. Reinstall the app.');
  }

  const refreshed = await refreshAccessToken(install.refresh_token);
  const accessToken = refreshed.access_token;
  const newRefresh = refreshed.refresh_token || install.refresh_token;
  const expiresAtIso = new Date(Date.now() + (refreshed.expires_in || 1800) * 1000).toISOString();

  const { error } = await supabase
    .from('installations')
    .update({
      access_token: accessToken,
      refresh_token: newRefresh,
      expires_at: expiresAtIso,
      updated_at: new Date().toISOString()
    })
    .eq('portal_id', portalId);

  if (error) {
    throw new Error(`Supabase update failed: ${error.message}`);
  }

  return accessToken;
}

async function fetchTokenInfo(accessToken) {
  const res = await fetch(`${HUBSPOT_API_BASE}/oauth/v1/access-tokens/${accessToken}`);
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Failed to fetch HubSpot token info: ${res.status} ${text}`);
  }
  return res.json();
}

function buildAuthUrl() {
  const requiredScopes = [
    'crm.export',
    'oauth',
    'crm.objects.companies.read',
    'crm.schemas.contacts.read',
    'crm.objects.contacts.read',
    'crm.schemas.companies.read'
  ];
  const optionalScopes = [
    'tickets',
    'crm.schemas.deals.read',
    'automation',
    'business-intelligence',
    'crm.objects.owners.read',
    'crm.objects.deals.read'
  ];

  const params = new URLSearchParams({
    client_id: CLIENT_ID,
    redirect_uri: REDIRECT_URI,
    scope: requiredScopes.join(' '),
    optional_scope: optionalScopes.join(' ')
  });

  return `${HUBSPOT_AUTH_BASE}/oauth/authorize?${params.toString()}`;
}

app.get('/api/install', (_req, res) => {
  res.redirect(buildAuthUrl());
});

app.post('/api/disconnect', async (req, res) => {
  const portalId = (req.header('X-HubSpot-Portal-Id') || '').trim();
  if (!portalId) {
    return res.status(400).json({ message: 'HubSpot Portal ID is missing.' });
  }
  try {
    const { error } = await supabase
      .from('installations')
      .delete()
      .eq('portal_id', portalId);
    if (error) throw error;
    res.json({ message: 'Successfully disconnected.' });
  } catch (err) {
    console.error('Disconnect error:', err.message);
    res.status(500).json({ message: 'Failed to disconnect.' });
  }
});

app.get('/api/oauth-callback', async (req, res) => {
  const code = req.query.code;
  if (!code) {
    return res.status(400).send('HubSpot authorization code not found.');
  }

  try {
    const params = new URLSearchParams({
      grant_type: 'authorization_code',
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      redirect_uri: REDIRECT_URI,
      code
    });

    const tokenRes = await fetch(`${HUBSPOT_API_BASE}/oauth/v1/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: params.toString()
    });

    if (!tokenRes.ok) {
      const text = await tokenRes.text().catch(() => '');
      throw new Error(`OAuth token exchange failed: ${tokenRes.status} ${text}`);
    }

    const tokenJson = await tokenRes.json();
    const { access_token, refresh_token, expires_in } = tokenJson;
    const tokenInfo = await fetchTokenInfo(access_token);

    const portalId = String(tokenInfo.hub_id || tokenInfo.hubId || '').trim();
    if (!portalId) {
      throw new Error('HubSpot did not return a hub_id');
    }

    const expiresAt = new Date(Date.now() + (expires_in || 1800) * 1000).toISOString();

    const { error } = await supabase
      .from('installations')
      .upsert({
        portal_id: portalId,
        access_token,
        refresh_token,
        expires_at: expiresAt,
        updated_at: new Date().toISOString()
      }, { onConflict: 'portal_id' });

    if (error) {
      throw error;
    }

    res.redirect(`/?portalId=${encodeURIComponent(portalId)}&portal_id=${encodeURIComponent(portalId)}`);
  } catch (err) {
    console.error('OAuth Callback Error:', err.message);
    res.status(500).send(`<h1>Server Error</h1><p>${err.message}</p>`);
  }
});

app.get('/api/check-scopes', async (req, res) => {
  const portalId = (req.header('X-HubSpot-Portal-Id') || '').trim();
  if (!portalId) {
    return res.status(400).json({ message: 'HubSpot Portal ID is missing.' });
  }

  try {
    const accessToken = await getValidAccessToken(portalId);
    const info = await fetchTokenInfo(accessToken);
    const scopes = Array.isArray(info.scopes) ? info.scopes : [];
    res.json(scopes);
  } catch (err) {
    console.error('Scope check failed:', err.message);
    res.status(500).json({ message: 'Could not verify scopes.' });
  }
});

app.post('/api/create-audit-job', async (req, res) => {
  const portalId = (req.header('X-HubSpot-Portal-Id') || '').trim();
  const objectType = String(req.body?.objectType || '').trim().toLowerCase();

  if (!portalId) {
    return res.status(400).json({ message: 'HubSpot Portal ID is missing.' });
  }
  if (!['contacts', 'companies', 'workflows'].includes(objectType)) {
    return res.status(400).json({ message: 'objectType is missing or invalid.' });
  }

  try {
    const installation = await getInstallation(portalId);
    if (!installation) {
      return res.status(404).json({ message: `No installation found for portal ${portalId}.` });
    }

    const tenMinutesAgo = new Date(Date.now() - 10 * 60 * 1000).toISOString();
    const { data: existingJob } = await supabase
      .from('audit_jobs')
      .select('job_id, status')
      .eq('portal_id', portalId)
      .eq('object_type', objectType)
      .in('status', ['pending', 'running'])
      .gt('created_at', tenMinutesAgo)
      .maybeSingle();

    if (existingJob) {
      return res.status(202).json({
        message: 'An audit for this object is already in progress.',
        jobId: existingJob.job_id
      });
    }

    const jobId = uuidv4();
    const { error } = await supabase.from('audit_jobs').insert({
      job_id: jobId,
      portal_id: portalId,
      object_type: objectType,
      status: 'pending',
      progress_message: 'Job submitted to queue...'
    });

    if (error) {
      throw error;
    }

    res.status(202).json({ message: 'Audit job created successfully.', jobId });
  } catch (err) {
    console.error('Error creating audit job:', err.message);
    res.status(500).json({ message: `Error creating audit job: ${err.message}` });
  }
});

app.get('/api/audit-status/:jobId', async (req, res) => {
  const jobId = String(req.params.jobId || '').trim();
  if (!jobId) {
    return res.status(400).json({ message: 'Job ID is missing.' });
  }

  try {
    const { data, error } = await supabase
      .from('audit_jobs')
      .select('status, progress_message, processed_records, total_records, error, results, result_json, object_type, portal_id')
      .eq('job_id', jobId)
      .maybeSingle();

    if (error) {
      throw error;
    }
    if (!data) {
      return res.status(404).json({ message: 'Job not found.' });
    }

    let payload = data.result_json || data.results || null;
    if (typeof payload === 'string') {
      try {
        payload = JSON.parse(payload);
      } catch (err) {
        console.warn('Failed to parse result JSON:', err.message);
        payload = null;
      }
    }

    res.json({
      status: data.status,
      progress_message: data.progress_message,
      processed_records: data.processed_records,
      total_records: data.total_records,
      error_message: data.error,
      results: payload
    });
  } catch (err) {
    console.error('Error fetching job status:', err.message);
    res.status(500).json({ message: `Error fetching job status: ${err.message}` });
  }
});

app.post('/api/download-excel', async (req, res) => {
  const { auditData, auditType, objectType } = req.body || {};
  if (!auditData || !auditType) {
    return res.status(400).send('Audit data or type is missing.');
  }

  try {
    const workbook = new ExcelJS.Workbook();
    workbook.creator = 'AuditPulse';
    workbook.created = new Date();
    const timestamp = new Date().toISOString().split('T')[0];
    let fileName = `AuditPulse-Report-${auditType}-${timestamp}.xlsx`;

    if (auditType === 'crm') {
      if (!objectType || !auditData.data) {
        return res.status(400).send('CRM Audit data (objectType or data block) is invalid.');
      }
      fileName = `AuditPulse-Report-${objectType}-${timestamp}.xlsx`;
      const crmData = auditData.data;

      const propSheet = workbook.addWorksheet('Property Audit');
      propSheet.columns = [
        { header: 'Property Label', key: 'label', width: 30 },
        { header: 'Internal Name', key: 'internalName', width: 30 },
        { header: 'Type', key: 'type', width: 15 },
        { header: 'Source', key: 'source', width: 15 },
        { header: 'Description', key: 'description', width: 40 },
        { header: 'Filled Records', key: 'fillCount', width: 18 },
        { header: 'Fill Rate (%)', key: 'fillRate', width: 18 }
      ];
      (crmData.properties || []).forEach(p => {
        propSheet.addRow({ ...p, source: p.isCustom ? 'Custom' : 'Standard' });
      });

      const orphanLabel = objectType === 'contacts' ? 'Orphaned Contacts' : 'Empty Companies';
      const orphanSheet = workbook.addWorksheet(orphanLabel);
      if (objectType === 'contacts') {
        orphanSheet.columns = [
          { header: 'Name', key: 'name', width: 30 },
          { header: 'Email', key: 'email', width: 30 },
          { header: 'Create Date', key: 'createdate', width: 22 },
          { header: 'Record ID', key: 'id', width: 22 }
        ];
      } else {
        orphanSheet.columns = [
          { header: 'Company Name', key: 'name', width: 30 },
          { header: 'Domain', key: 'domain', width: 30 },
          { header: 'Create Date', key: 'createdate', width: 22 },
          { header: 'Record ID', key: 'id', width: 22 }
        ];
      }
      (crmData.orphanedRecords || []).forEach(r => {
        orphanSheet.addRow({
          name: objectType === 'contacts'
            ? `${r?.properties?.firstname || ''} ${r?.properties?.lastname || ''}`.trim()
            : r?.properties?.name || '',
          email: r?.properties?.email || '',
          domain: r?.properties?.domain || '',
          createdate: r?.properties?.createdate || '',
          id: r.id
        });
      });

      const duplicateSheet = workbook.addWorksheet('Duplicates');
      if (objectType === 'contacts') {
        duplicateSheet.columns = [
          { header: 'Name', key: 'name', width: 30 },
          { header: 'Email (Duplicate Identifier)', key: 'email', width: 35 },
          { header: 'Create Date', key: 'createdate', width: 22 },
          { header: 'Record ID', key: 'id', width: 22 }
        ];
      } else {
        duplicateSheet.columns = [
          { header: 'Company Name', key: 'name', width: 30 },
          { header: 'Domain (Duplicate Identifier)', key: 'domain', width: 35 },
          { header: 'Create Date', key: 'createdate', width: 22 },
          { header: 'Record ID', key: 'id', width: 22 }
        ];
      }
      (crmData.duplicateRecords || []).forEach(r => {
        duplicateSheet.addRow({
          name: objectType === 'contacts'
            ? `${r?.properties?.firstname || ''} ${r?.properties?.lastname || ''}`.trim()
            : r?.properties?.name || '',
          email: r?.properties?.email || '',
          domain: r?.properties?.domain || '',
          createdate: r?.properties?.createdate || '',
          id: r.id
        });
      });

      const missingDescSheet = workbook.addWorksheet('Properties Missing Description');
      missingDescSheet.columns = [
        { header: 'Property Label', key: 'label', width: 30 },
        { header: 'Internal Name', key: 'internalName', width: 30 },
        { header: 'Type', key: 'type', width: 20 }
      ];
      (crmData.properties || [])
        .filter(p => !p.description)
        .forEach(p => {
          missingDescSheet.addRow({ label: p.label, internalName: p.internalName, type: p.type });
        });
    } else if (auditType === 'workflows') {
      if (!Array.isArray(auditData.results)) {
        return res.status(400).send('Workflow Audit data (results array) is invalid or missing.');
      }
      fileName = `AuditPulse-Report-Workflows-${timestamp}.xlsx`;
      const wfSheet = workbook.addWorksheet('Workflow Audit');
      wfSheet.columns = [
        { header: 'Workflow Name', key: 'workflow_name', width: 40 },
        { header: 'Workflow ID', key: 'workflow_id', width: 20 },
        { header: 'Action Type', key: 'action_type', width: 25 },
        { header: 'Finding', key: 'finding', width: 30 },
        { header: 'Details', key: 'details', width: 45 },
        { header: 'Last Updated', key: 'last_updated', width: 25 }
      ];
      auditData.results.forEach(finding => {
        wfSheet.addRow({
          workflow_name: finding.workflow_name || 'N/A',
          workflow_id: finding.workflow_id || 'N/A',
          action_type: finding.action_type || 'N/A',
          finding: finding.finding || 'N/A',
          details: finding.details || 'N/A',
          last_updated: finding.last_updated || 'N/A'
        });
      });
    }

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
    await workbook.xlsx.write(res);
    res.end();
  } catch (err) {
    console.error('Excel generation error:', err.message);
    res.status(500).send(`Error generating Excel file: ${err.message}`);
  }
});

app.get('*', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.use((err, _req, res, _next) => {
  console.error('Unhandled Error:', err.stack || err);
  res.status(500).send('Something broke on the server!');
});

app.listen(PORT, () => {
  console.log(`Server (${process.env.RENDER_SERVICE_NAME || 'local'}) is live on port ${PORT}`);
});
