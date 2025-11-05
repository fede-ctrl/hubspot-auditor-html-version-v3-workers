// This script runs on the 'auditpulsepro-web' service.
// It handles web requests, creates jobs, and reports status.

require('dotenv').config();
const express = require('express');
const fetch = require('node-fetch');
const { createClient } = require('@supabase/supabase-js');
const cors = require('cors');
const path = require('path');
const ExcelJS = require('exceljs');

const app = express();
// Render sets the PORT environment variable.
const PORT = process.env.PORT || 10000; 

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// --- Environment Variables ---
const CLIENT_ID = process.env.HUBSPOT_CLIENT_ID;
const CLIENT_SECRET = process.env.HUBSPOT_CLIENT_SECRET;
const RENDER_EXTERNAL_URL = process.env.RENDER_EXTERNAL_URL; 
const REDIRECT_URI = `${RENDER_EXTERNAL_URL}/api/oauth-callback`;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// *** NEW: Define the API base URL ***
// This is critical for supporting EU portals
const HUBSPOT_API_BASE = 'https://api.eu1.hubapi.com';

// --- Helper: Get Access Token ---
async function getValidAccessToken(portalId) {
    const { data: installation, error } = await supabase.from('installations').select('refresh_token, access_token, expires_at').eq('hubspot_portal_id', portalId).single();
    if (error || !installation) throw new Error(`Could not find installation for portal ${portalId}. Please reinstall the app.`);
    let { refresh_token, access_token, expires_at } = installation;
    if (new Date() > new Date(expires_at)) {
        console.log(`Refreshing token for portal ${portalId} (EU)`);
        
        // *** FIX: Use EU1 domain for token refresh ***
        const response = await fetch(`${HUBSPOT_API_BASE}/oauth/v1/token`, { 
            method: 'POST', 
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, 
            body: new URLSearchParams({ 
                grant_type: 'refresh_token', 
                client_id: CLIENT_ID, 
                client_secret: CLIENT_SECRET, 
                refresh_token 
            }), 
        });

        if (!response.ok) throw new Error('Failed to refresh access token');
        const newTokens = await response.json();
        access_token = newTokens.access_token;
        const newExpiresAt = new Date(Date.now() + newTokens.expires_in * 1000).toISOString();
        await supabase.from('installations').update({ access_token, expires_at: newExpiresAt }).eq('hubspot_portal_id', portalId);
    }
    return access_token;
}

// --- API Routes ---

// 1. Install & OAuth
app.get('/api/install', (req, res) => {
    if (!RENDER_EXTERNAL_URL) {
        console.error("CRITICAL: RENDER_EXTERNAL_URL environment variable is not set.");
        return res.status(500).send("<h1>Configuration Error</h1><p>The server is missing the RENDER_EXTERNAL_URL environment variable. Cannot proceed with installation.</p>");
    }
    console.log("Redirect URI:", REDIRECT_URI);
    
    // *** FIX: Added 'crm.export' to the required scopes list ***
    // Using the scopes from your URL + the one we need
    const REQUIRED_SCOPES = 'oauth crm.objects.companies.read crm.schemas.contacts.read crm.objects.contacts.read crm.schemas.custom.read tickets automation crm.export';
    
    const OPTIONAL_SCOPES = 'cms.knowledge_base.settings.read crm.objects.deals.read';
    
    // *** FIX: Use EU1 domain for authorization ***
    const authUrl = `https://app-eu1.hubspot.com/oauth/authorize?client_id=${CLIENT_ID}&redirect_uri=${REDIRECT_URI}&scope=${REQUIRED_SCOPES.replace(/ /g, '%20')}&optional_scope=${OPTIONAL_SCOPES.replace(/ /g, '%20')}`;
    
    console.log("Generated Auth URL:", authUrl); 
    res.redirect(authUrl);
});

app.post('/api/disconnect', async (req, res) => {
    const portalId = req.header('X-HubSpot-Portal-Id');
    if (!portalId) return res.status(400).json({ message: 'HubSpot Portal ID is missing.' });
    try {
        const { error } = await supabase.from('installations').delete().eq('hubspot_portal_id', portalId);
        if (error) throw error;
        res.status(200).json({ message: 'Successfully disconnected.' });
    } catch (error) {
        console.error('Disconnect error:', error);
        res.status(500).json({ message: 'Failed to disconnect.' });
    }
});

app.get('/api/oauth-callback', async (req, res) => {
    const authCode = req.query.code;
    if (!authCode) return res.status(400).send('HubSpot authorization code not found.');
    if (!RENDER_EXTERNAL_URL) {
         return res.status(500).send("<h1>Configuration Error</h1><p>The server is missing the RENDER_EXTERNAL_URL environment variable. Cannot complete authentication.</p>");
    }
    try {
        // *** FIX: Use EU1 domain for token exchange ***
        const response = await fetch(`${HUBSPOT_API_BASE}/oauth/v1/token`, { 
            method: 'POST', 
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, 
            body: new URLSearchParams({ 
                grant_type: 'authorization_code', 
                client_id: CLIENT_ID, 
                client_secret: CLIENT_SECRET, 
                redirect_uri: REDIRECT_URI, 
                code: authCode 
            }), 
        });

        if (!response.ok) throw new Error(await response.text());
        const tokenData = await response.json();
        const { refresh_token, access_token, expires_in } = tokenData;

        // *** FIX: Use EU1 domain for token info ***
        const tokenInfoResponse = await fetch(`${HUBSPOT_API_BASE}/oauth/v1/access-tokens/${access_token}`);
        if (!tokenInfoResponse.ok) throw new Error('Failed to fetch HubSpot token info');
        const tokenInfo = await tokenInfoResponse.json();

        const hub_id = tokenInfo.hub_id;
        const granted_scopes = tokenInfo.scopes;
        const expiresAt = new Date(Date.now() + expires_in * 1000).toISOString();

        await supabase.from('installations').upsert(
            { hubspot_portal_id: hub_id, refresh_token, access_token, expires_at: expiresAt, granted_scopes: granted_scopes },
            { onConflict: 'hubspot_portal_id' }
        );
        res.redirect(`/?portalId=${hub_id}`);
    } catch (error) {
        console.error('OAuth Callback Error:', error);
        res.status(500).send(`<h1>Server Error</h1><p>${error.message}</p>`);
    }
});

// 2. Scope Check (No API calls, no change needed)
app.get('/api/check-scopes', async (req, res) => {
    // ... (no changes needed in this function)
    const portalId = req.header('X-HubSpot-Portal-Id');
    if (!portalId) return res.status(400).json({ message: 'HubSpot Portal ID is missing.' });
    try {
        const { data, error } = await supabase.from('installations').select('granted_scopes').eq('hubspot_portal_id', portalId).single();
        if (error) {
            console.error('Supabase error fetching scopes:', error);
            return res.status(500).json({ message: 'Could not fetch permissions.' });
        }
        if (!data || !data.granted_scopes) return res.json([]);
        let scopesArray;
        if (typeof data.granted_scopes === 'string') {
            try { scopesArray = JSON.parse(data.granted_scopes); } catch (e) { return res.json([]); }
        } else if (Array.isArray(data.granted_scopes)) {
            scopesArray = data.granted_scopes;
        } else {
            return res.json([]);
        }
        res.json(scopesArray);
    } catch (error) {
        console.error('Check scopes internal error:', error);
        res.status(500).json({ message: 'Server error checking scopes.' });
    }
});


// 3. Create Audit Job (No API calls, no change needed)
app.post('/api/create-audit-job', async (req, res) => {
    // ... (no changes needed in this function)
    const portalId = req.header('X-HubSpot-Portal-Id');
    const { objectType } = req.body; 
    if (!portalId) return res.status(400).json({ message: 'HubSpot Portal ID is missing.' });
    if (!objectType) return res.status(400).json({ message: 'objectType is missing.' });
    try {
        const tenMinutesAgo = new Date(Date.now() - 10 * 60 * 1000).toISOString();
        const { data: existingJob, error: existingError } = await supabase
            .from('audit_jobs')
            .select('job_id, status')
            .eq('portal_id', portalId)
            .eq('object_type', objectType)
            .in('status', ['pending', 'running'])
            .gt('created_at', tenMinutesAgo)
            .maybeSingle(); 
        
        if (existingJob) {
            console.log(`Job ${existingJob.job_id} is already ${existingJob.status}. Returning existing job.`);
            return res.status(202).json({ 
                message: "An audit for this object is already in progress.", 
                jobId: existingJob.job_id 
            });
        }
        if (existingError) {
            throw existingError;
        }
        const { data: newJob, error: createError } = await supabase
            .from('audit_jobs')
            .insert({
                portal_id: portalId,
                object_type: objectType,
                status: 'pending',
                progress_message: 'Job submitted to queue...'
            })
            .select('job_id') 
            .single();
        if (createError) throw createError;
        console.log(`Created new job ${newJob.job_id} for portal ${portalId}, type ${objectType}`);
        res.status(202).json({ 
            message: "Audit job created successfully.",
            jobId: newJob.job_id 
        });
    } catch (error) {
        console.error('Error creating audit job:', error.message);
        res.status(500).json({ message: `Error creating audit job: ${error.message}` });
    }
});


// 4. Check Audit Job Status (No API calls, no change needed)
app.get('/api/audit-status/:jobId', async (req, res) => {
    // ... (no changes needed in this function)
    const { jobId } = req.params;
    if (!jobId) return res.status(400).json({ message: 'Job ID is missing.' });
    try {
        const { data: job, error } = await supabase
            .from('audit_jobs')
            .select('status, progress_message, results, error_message')
            .eq('job_id', jobId)
            .single();
        if (error) throw error;
        if (!job) {
            return res.status(404).json({ message: 'Job not found.' });
        }
        if (job.status === 'complete') {
            res.status(200).json({
                status: job.status,
                progress_message: job.progress_message,
                results: job.results 
            });
        } else {
             res.status(200).json({ 
                status: job.status,
                progress_message: job.progress_message,
                error_message: job.error_message,
                results: null
            });
        }
    } catch (error) {
        console.error('Error fetching job status:', error.message);
        res.status(500).json({ message: `Error fetching job status: ${error.message}` });
    }
});


// 5. Excel Download (No API calls, no change needed)
app.post('/api/download-excel', async (req, res) => {
    // ... (no changes needed in this function)
    const { auditData, auditType, objectType } = req.body; 
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
            if (!objectType || !auditData.data) return res.status(400).send('CRM Audit data (objectType or data block) is invalid.');
            fileName = `AuditPulse-Report-${objectType}-${timestamp}.xlsx`;
            const crmData = auditData.data;
            const propSheet = workbook.addWorksheet('Property Audit');
            propSheet.columns = [
                { header: 'Property Label', key: 'label', width: 30 }, { header: 'Internal Name', key: 'internalName', width: 30 }, { header: 'Type', key: 'type', width: 15 }, { header: 'Source', key: 'source', width: 15 }, { header: 'Description', key: 'description', width: 40 }, { header: 'Filled Records', key: 'fillCount', width: 15 }, { header: 'Fill Rate (%)', key: 'fillRate', width: 15 }
            ];
            if (crmData.properties && Array.isArray(crmData.properties)) {
                crmData.properties.forEach(p => {
                    propSheet.addRow({ ...p, source: p.isCustom ? 'Custom' : 'Standard' });
                });
            }
            const orphanLabel = objectType === 'contacts' ? 'Orphaned Contacts' : 'Empty Companies';
            const orphanSheet = workbook.addWorksheet(orphanLabel);
            if (crmData.orphanedRecords && Array.isArray(crmData.orphanedRecords)) {
                if (objectType === 'contacts') {
                    orphanSheet.columns = [ { header: 'Name', key: 'name', width: 30 }, { header: 'Email', key: 'email', width: 30 }, { header: 'Create Date', key: 'createdate', width: 20 }, { header: 'Record ID', key: 'id', width: 20 } ];
                    crmData.orphanedRecords.forEach(r => {
                        orphanSheet.addRow({ name: `${r?.properties?.firstname || ''} ${r?.properties?.lastname || ''}`, email: r?.properties?.email, createdate: r?.properties?.createdate, id: r.id });
                    });
                } else { // Companies
                    orphanSheet.columns = [ { header: 'Company Name', key: 'name', width: 30 }, { header: 'Domain', key: 'domain', width: 30 }, { header: 'Create Date', key: 'createdate', width: 20 }, { header: 'Record ID', key: 'id', width: 20 } ];
                    crmData.orphanedRecords.forEach(r => {
                        orphanSheet.addRow({ name: r?.properties?.name, domain: r?.properties?.domain, createdate: r?.properties?.createdate, id: r.id });
                    });
                }
            }
            const duplicateSheet = workbook.addWorksheet('Duplicates');
            if (crmData.duplicateRecords && Array.isArray(crmData.duplicateRecords)) {
                 if (objectType === 'contacts') {
                    duplicateSheet.columns = [ { header: 'Name', key: 'name', width: 30 }, { header: 'Email (Duplicate Identifier)', key: 'email', width: 30 }, { header: 'Create Date', key: 'createdate', width: 20 }, { header: 'Record ID', key: 'id', width: 20 } ];
                    crmData.duplicateRecords.forEach(r => {
                        duplicateSheet.addRow({ name: `${r?.properties?.firstname || ''} ${r?.properties?.lastname || ''}`, email: r?.properties?.email, createdate: r?.properties?.createdate, id: r.id });
                    });
                } else { // Companies
                    duplicateSheet.columns = [ { header: 'Company Name', key: 'name', width: 30 }, { header: 'Domain (Duplicate Identifier)', key: 'domain', width: 30 }, { header: 'Create Date', key: 'createdate', width: 20 }, { header: 'Record ID', key: 'id', width: 20 } ];
                    crmData.duplicateRecords.forEach(r => {
                        duplicateSheet.addRow({ name: r?.properties?.name, domain: r?.properties?.domain, createdate: r?.properties?.createdate, id: r.id });
                    });
                }
            }
            const missingDescSheet = workbook.addWorksheet('Properties Missing Description');
            missingDescSheet.columns = [ { header: 'Property Label', key: 'label', width: 30 }, { header: 'Internal Name', key: 'internalName', width: 30 }, { header: 'Type', key: 'type', width: 20 } ];
            if (crmData.properties && Array.isArray(crmData.properties)) {
                crmData.properties.filter(p => !p.description).forEach(p => {
                    missingDescSheet.addRow({label: p.label, internalName: p.internalName, type: p.type});
                });
            }
        } else if (auditType === 'workflows') {
            if (!auditData.results || !Array.isArray(auditData.results)) {
                 return res.status(400).send('Workflow Audit data (results array) is invalid or missing.');
            }
            fileName = `AuditPulse-Report-Workflows-${timestamp}.xlsx`;
            const wfSheet = workbook.addWorksheet('Workflow Audit');
            wfSheet.columns = [
                { header: 'Workflow Name', key: 'workflow_name', width: 35 }, { header: 'Workflow ID', key: 'workflow_id', width: 15 }, { header: 'Action Type', key: 'action_type', width: 20 }, { header: 'Finding', key: 'finding', width: 30 }, { header: 'Details', key: 'details', width: 40 }, { header: 'Last Updated', key: 'last_updated', width: 20 }
            ];
            auditData.results.forEach(finding => {
                wfSheet.addRow({
                    workflow_name: finding.workflow_name || 'N/A', workflow_id: finding.workflow_id || 'N/A', action_type: finding.action_type || 'N/A', finding: finding.finding || 'N/A', details: finding.details || 'N/A', last_updated: finding.last_updated || 'N/A'
                 });
            });
        }
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
        await workbook.xlsx.write(res);
        res.end();
    } catch (error) {
        console.error('Excel generation error:', error);
        res.status(500).send(`Error generating Excel file: ${error.message}`);
    }
});


// Catch-all route MUST be last
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Basic Error Handling Middleware
app.use((err, req, res, next) => {
  console.error("Unhandled Error:", err.stack || err);
  res.status(500).send('Something broke on the server!');
});

app.listen(PORT, () => console.log(`Server (${process.env.RENDER_SERVICE_NAME || 'local'}) is live on port ${PORT}`));
