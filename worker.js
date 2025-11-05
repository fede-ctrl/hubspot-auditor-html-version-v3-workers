require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');
const { parse } = require('csv-parse');

// --- Supabase Client ---
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// --- HubSpot API Clients ---
const CLIENT_ID = process.env.HUBSPOT_CLIENT_ID;
const CLIENT_SECRET = process.env.HUBSPOT_CLIENT_SECRET;

// *** NEW: Define the API base URL ***
const HUBSPOT_API_BASE = 'https://api.eu1.hubapi.com';

// --- Helper: Rate Limit Delay ---
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ---------------------------------
// --- AUDIT HELPER FUNCTIONS ---
// ---------------------------------

/**
 * Gets a valid access token from Supabase, refreshing if needed.
 */
async function getValidAccessToken(portalId) {
    const { data: installation, error } = await supabase
        .from('installations')
        .select('refresh_token, access_token, expires_at')
        .eq('hubspot_portal_id', portalId)
        .single();

    if (error || !installation) throw new Error(`[Worker] Could not find installation for portal ${portalId}.`);
    
    let { refresh_token, access_token, expires_at } = installation;

    if (new Date() > new Date(expires_at)) {
        console.log(`[Worker] Refreshing token for portal ${portalId} (EU)`);
        
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

        if (!response.ok) throw new Error(`[Worker] Failed to refresh access token: ${await response.text()}`);
        
        const newTokens = await response.json();
        access_token = newTokens.access_token;
        const newExpiresAt = new Date(Date.now() + newTokens.expires_in * 1000).toISOString();
        
        await supabase
            .from('installations')
            .update({ access_token, expires_at: newExpiresAt })
            .eq('hubspot_portal_id', portalId);
    }
    return access_token;
}

/**
 * Fetches *all pages* of data from a HubSpot endpoint using GET.
 */
async function fetchAllHubSpotData(initialUrl, accessToken, resultsKey) {
    const allResults = [];
    let currentUrl = initialUrl;
    let pageCount = 0;
    const MAX_PAGES = 100; // Safety cap

    console.log(`[Worker] fetchAll: Starting fetch for key '${resultsKey}' from ${initialUrl}`);

    try {
        while (currentUrl && pageCount < MAX_PAGES) {
            pageCount++;
            await sleep(200); 
            const response = await fetch(currentUrl, { headers: { 'Authorization': `Bearer ${accessToken}` } });

            if (!response.ok) {
                 const errorBody = await response.text();
                 console.error(`[Worker] fetchAll: HubSpot API request failed: ${response.status} ${response.statusText} for URL: ${currentUrl}. Body: ${errorBody}`);
                 throw new Error(`[Worker] fetchAll: HubSpot API request failed: ${response.status} ${response.statusText}`);
            }

            const data = await response.json();
            const resultsOnPage = data[resultsKey]; 

            if (resultsOnPage && Array.isArray(resultsOnPage)) {
                allResults.push(...resultsOnPage);
            } else {
                 console.warn(`[Worker] fetchAll: Page ${pageCount}: No iterable data found under key '${resultsKey}'.`);
            }

            const hasPaging = data.paging && data.paging.next && (data.paging.next.link || data.paging.next.after);

            if (hasPaging) {
                if (data.paging.next.link) {
                    currentUrl = data.paging.next.link;
                } else if (data.paging.next.after) {
                    const baseUrl = initialUrl.split('?')[0];
                    const searchParams = new URLSearchParams(currentUrl.split('?')[1] || ''); 
                    searchParams.set('after', data.paging.next.after); 
                    currentUrl = `${baseUrl}?${searchParams.toString()}`;
                } else {
                     currentUrl = null; 
                }
            } else {
                console.log(`[Worker] fetchAll: Page ${pageCount}: No paging information found. Assuming fetch complete.`);
                currentUrl = null; // No more pages
            }
        }
        console.log(`[Worker] fetchAll: Finished fetch for key '${resultsKey}'. Total items retrieved: ${allResults.length}`);
        return allResults;
    } catch (error) {
        console.error(`[Worker] fetchAll: Error during fetch for key '${resultsKey}':`, error);
        throw error;
    }
}


// ---------------------------------
// --- MAIN AUDIT FUNCTIONS ---
// ---------------------------------

/**
 * Performs the full CRM audit (Contacts/Companies) using the 'POST /crm/v3/exports/export/async' API.
 */
async function performCrmAudit(job) {
    const { portal_id, object_type, job_id } = job;
    console.log(`[Worker] Job ${job_id}: Starting CRM Audit for ${object_type} (using EXPORT API)`);

    await supabase.from('audit_jobs').update({ progress_message: 'Fetching access token...' }).eq('job_id', job_id);
    const accessToken = await getValidAccessToken(portal_id);

    // 1. Fetch ALL Properties
    await supabase.from('audit_jobs').update({ progress_message: 'Fetching all properties...' }).eq('job_id', job_id);
    
    // *** FIX: Use EU1 domain ***
    const propertiesUrl = `${HUBSPOT_API_BASE}/crm/v3/properties/${object_type}?archived=false&limit=100`;
    const allProperties = await fetchAllHubSpotData(propertiesUrl, accessToken, 'results');
    console.log(`[Worker] Job ${job_id}: Fetched ${allProperties.length} properties.`);
    
    const baseProps = object_type === 'contacts' ? ['associatedcompanyid', 'email'] : ['num_associated_contacts', 'domain'];
    const propertyNames = allProperties.map(p => p.name).concat(baseProps);
    const uniquePropertyNames = [...new Set(propertyNames)];

    let totalRecords = 0;
    const fillCounts = {};
    uniquePropertyNames.forEach(propName => { fillCounts[propName] = 0; });
    
    const orphanedRecords = [];
    const seenDuplicateValues = new Map();
    const duplicateIdProp = object_type === 'contacts' ? 'email' : 'domain';

    // 1. Request the Export from HubSpot
    console.log(`[Worker] Job ${job_id}: Requesting HubSpot export for ${object_type}...`);
    await supabase.from('audit_jobs').update({ progress_message: 'Requesting HubSpot export...' }).eq('job_id', job_id);
    
    // *** FIX: Use EU1 domain ***
    const exportRequestUrl = `${HUBSPOT_API_BASE}/crm/v3/exports/export/async`;
    const exportRequestBody = {
        objectType: object_type,
        properties: uniquePropertyNames, 
        exportFormat: "CSV" 
    };

    const exportResponse = await fetch(exportRequestUrl, {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${accessToken}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(exportRequestBody)
    });

    if (!exportResponse.ok) {
        throw new Error(`[Worker] HubSpot Export Request failed: ${await exportResponse.text()}`);
    }

    const exportJob = await exportResponse.json();
    const exportId = exportJob.id;
    console.log(`[Worker] Job ${job_id}: HubSpot export job created (ID: ${exportId}). Polling for completion...`);

    // 2. Poll HubSpot for the export file URL
    let exportStatus = 'PENDING';
    let fileUrl = null;
    
    // *** FIX: Use EU1 domain ***
    const exportStatusUrl = `${HUBSPOT_API_BASE}/crm/v3/exports/export/async/tasks/${exportId}/status`;
    let pollCount = 0;

    while (exportStatus === 'PENDING' || exportStatus === 'PROCESSING') {
        pollCount++;
        await sleep(5000); 
        const progressMessage = `Waiting for HubSpot export (Status: ${exportStatus}, ${pollCount * 5}s)...`;
        await supabase.from('audit_jobs').update({ progress_message }).eq('job_id', job_id);
        
        const statusResponse = await fetch(exportStatusUrl, { headers: { 'Authorization': `Bearer ${accessToken}` } });
        if (!statusResponse.ok) {
            throw new Error(`[Worker] Failed to get export job status: ${await statusResponse.text()}`);
        }
        
        const statusData = await statusResponse.json();
        exportStatus = statusData.status;
        
        if (exportStatus === 'COMPLETED') {
            fileUrl = statusData.result; // This is the download URL
        } else if (exportStatus === 'FAILED') {
            throw new Error('[Worker] HubSpot export job failed.');
        }
    }

    // 3. Download and Process the Export File
    console.log(`[Worker] Job ${job_id}: Export complete. Downloading file...`);
    await supabase.from('audit_jobs').update({ progress_message: 'Downloading and processing export...' }).eq('job_id', job_id);

    const fileResponse = await fetch(fileUrl); // This URL is absolute from HubSpot
    if (!fileResponse.ok) {
        throw new Error(`[Worker] Failed to download export file: ${fileResponse.statusText}`);
    }
    
    const csvText = await fileResponse.text();
    const rows = csvText.split('\n');
    
    if (rows.length <= 1) {
         console.log(`[Worker] Job ${job_id}: Export file was empty (or contained header only).`);
         totalRecords = 0;
    } else {
        const records = parse(csvText, {
            columns: true,
            skip_empty_lines: true
        });

        totalRecords = records.length;
        console.log(`[Worker] Job ${job_id}: Parsed ${totalRecords} records from export file.`);
        
        // 4. Process all rows
        for (const record of records) {
            const recordId = record['Record ID'];
            
            for (const propName of uniquePropertyNames) {
                if (record[propName] !== null && record[propName] !== '' && record[propName] !== undefined) {
                    fillCounts[propName]++;
                }
            }

            if (object_type === 'contacts' && !record.associatedcompanyid) {
                if (orphanedRecords.length < 5000) orphanedRecords.push({ id: recordId, properties: record });
            } else if (object_type === 'companies') {
                const numContacts = record.num_associated_contacts;
                if (numContacts === null || numContacts === undefined || numContacts === '0' || numContacts === '') {
                    if (orphanedRecords.length < 5000) orphanedRecords.push({ id: recordId, properties: record });
                }
            }
            
            const value = record[duplicateIdProp]?.toLowerCase();
            if (value) {
                if (!seenDuplicateValues.has(value)) {
                    seenDuplicateValues.set(value, { count: 0, records: [] });
                }
                const entry = seenDuplicateValues.get(value);
                entry.count++;
                if (entry.records.length < 10) entry.records.push({ id: recordId, properties: record });
            }
        } 
    } 

    
    console.log(`[Worker] Job ${job_id}: Processed ${totalRecords} records from export file.`);
    await supabase.from('audit_jobs').update({ progress_message: 'Calculating final results...' }).eq('job_id', job_id);

    // 5. Final Calculations
    const auditResults = allProperties.map(prop => {
        const fillCount = fillCounts[prop.name] || 0;
        const fillRate = totalRecords > 0 ? Math.round((fillCount / totalRecords) * 100) : 0;
        return { label: prop.label, internalName: prop.name, type: prop.type, description: prop.description || '', isCustom: !prop.hubspotDefined, fillRate, fillCount };
    });

    let totalDuplicates = 0;
    const duplicateRecords = [];
    for (const [value, entry] of seenDuplicateValues.entries()) {
        if (entry.count > 1) {
            duplicateRecords.push(...entry.records);
            totalDuplicates += (entry.count - 1);
        }
    }

    const customProperties = auditResults.filter(p => p.isCustom);
    const averageCustomFillRate = customProperties.length > 0
        ? Math.round(customProperties.reduce((acc, p) => acc + p.fillRate, 0) / customProperties.length)
        : 0;

    // 6. Format Final Results Object
    const finalResults = {
        auditType: 'crm',
        objectType: object_type,
        data: {
            totalRecords: totalRecords,
            totalProperties: allProperties.length,
            averageCustomFillRate: averageCustomFillRate,
            properties: auditResults,
            orphanedRecords: orphanedRecords,
            duplicateRecords: duplicateRecords.slice(0, 5000),
            totalDuplicates: totalDuplicates,
            limitHit: false 
        }
    };
    
    console.log(`[Worker] Job ${job_id}: CRM Audit complete. ${orphanedRecords.length} orphans found (capped), ${totalDuplicates} duplicates found.`);
    return finalResults;
}

/**
 * **WORKFLOW AUDIT - V3**
 */
async function performWorkflowAudit(job) {
    const { portal_id, job_id } = job;
    console.log(`[Worker] Job ${job_id}: Starting Workflow Audit (using V3 endpoint)...`);

    await supabase.from('audit_jobs').update({ progress_message: 'Fetching access token...' }).eq('job_id', job_id);
    const accessToken = await getValidAccessToken(portal_id);

    // 1. Fetch all workflow summaries
    await supabase.from('audit_jobs').update({ progress_message: 'Fetching workflow list (V3)...' }).eq('job_id', job_id);
    
    // *** FIX: Use EU1 domain ***
    const workflowsUrl = `${HUBSPOT_API_BASE}/automation/v3/workflows?limit=100`;
    const allWorkflows = await fetchAllHubSpotData(workflowsUrl, accessToken, 'workflows');
    
    console.log(`[Worker] Job ${job_id}: Found ${allWorkflows.length} workflow summaries from V3.`);

    let kpis = {
        totalWorkflows: 0,
        inactiveWorkflows: 0,
        noEnrollmentWorkflows: 0,
        isIncomplete: false
    };
    if (allWorkflows && allWorkflows.length > 0) {
        kpis.totalWorkflows = allWorkflows.length;
        kpis.inactiveWorkflows = allWorkflows.filter(wf => wf.enabled === false).length;
        kpis.noEnrollmentWorkflows = allWorkflows.filter(wf => wf.contactCount === 0).length;
    }
    console.log(`[Worker] Job ${job_id}: Calculated KPIs:`, kpis);

    const findings = [];
    const v1EmailPattern = /marketing-emails[\/|\\]v1[\/|\\]emails/i;
    const hapikeyPattern = /hapikey=/i;
    let processedCount = 0;

    console.log(`[Worker] Job ${job_id}: V3 endpoint returned full definitions. Starting direct scan...`);
    
    for (const workflowDetail of allWorkflows) {
        processedCount++;
        
        if (processedCount % 20 === 0) { 
            const progressMessage = `Scanning workflow ${processedCount}/${allWorkflows.length}...`;
            console.log(`[Worker] Job ${job_id}: ${progressMessage}`);
            await supabase.from('audit_jobs').update({ progress_message: progressMessage }).eq('job_id', job_id);
        }

        try {
            if (!workflowDetail.actions || workflowDetail.actions.length === 0) continue;

            // 3. Analyze actions
            for (const action of workflowDetail.actions) {
                let foundIssue = null;
                let details = '';
                
                if (action.type === 'WEBHOOK' && action.url) {
                    if (hapikeyPattern.test(action.url)) {
                        foundIssue = 'HAPIkey in URL';
                        details = action.url;
                    } else if (v1EmailPattern.test(action.url)) {
                        foundIssue = 'V1 Marketing Email API URL';
                        details = action.url; 
                    }
                } else if (action.type === 'CUSTOM_CODE' && action.code) {
                     if (hapikeyPattern.test(action.code)) {
                        foundIssue = 'HAPIkey in Custom Code';
                        details = 'Custom Code Snippet';
                    } else if (v1EmailPattern.test(action.code)) {
                        foundIssue = 'V1 Marketing Email API in Custom Code';
                        details = 'Custom Code Snippet';
                    }
                }

                if (foundIssue) {
                    findings.push({
                        workflow_name: workflowDetail.name || `Unnamed (ID: ${workflowDetail.id})`,
                        workflow_id: workflowDetail.id,
                        action_type: action.type,
                        finding: foundIssue,
                        details: details,
                        last_updated: workflowDetail.updated ? new Date(workflowDetail.updated).toLocaleDateString() : 'N/A'
                    });
                }
            }
        } catch (err) {
            console.error(`[Worker] Job ${job_id}: Error processing workflow ${workflowDetail.id}:`, err.message);
            continue;
        }
    }

    // 4. Format Final Results
    console.log(`[Worker] Job ${job_id}: Workflow Audit complete. Found ${findings.length} issues.`);
    return { 
        auditType: 'workflows', 
        results: findings, 
        kpis: kpis 
    };
}


// ---------------------------------
// --- WORKER POLLING LOGIC ---
// ---------------------------------

/**
 * Main function to poll for pending jobs.
 */
async function pollForJobs() {
    let job = null;
    try {
        // 1. Find the oldest 'pending' job
        const { data: foundJob, error: findError }
 = await supabase
            .from('audit_jobs')
            .select('*')
            .eq('status', 'pending')
            .order('created_at', { ascending: true })
            .limit(1)
            .maybeSingle();
        
        if (findError) {
            console.error('[Worker] Error fetching job:', findError.message);
            setTimeout(pollForJobs, 10000); 
            return;
        }
        
        if (!foundJob) {
            setTimeout(pollForJobs, 5000); 
            return;
        }

        // 2. We found a job. Claim it.
        job = foundJob;
        console.log(`[Worker] Job ${job.job_id} found. Claiming...`);
        const { error: claimError } = await supabase
            .from('audit_jobs')
            .update({ status: 'running', progress_message: 'Job claimed by worker...' })
            .eq('job_id', job.job_id)
            .eq('status', 'pending'); 

        if (claimError) {
            console.error(`[Worker] Job ${job.job_id} failed to claim:`, claimError.message);
            setTimeout(pollForJobs, 5000);
            return;
        }

        // 3. Now that we've claimed it, execute the job
        console.log(`[Worker] Job ${job_id} claimed for portal ${job.portal_id}. Starting audit...`);
        let auditResults = {};
        try {
            // Run the audit based on type
            if (job.object_type === 'contacts' || job.object_type === 'companies') {
                auditResults = await performCrmAudit(job);
            } else if (job.object_type === 'workflows') {
                auditResults = await performWorkflowAudit(job);
            } else {
                throw new Error(`Unknown job object_type: ${job.object_type}`);
            }

            // 4. Mark job as 'complete'
            await supabase
                .from('audit_jobs')
                .update({
                    status: 'complete',
                    progress_message: 'Audit complete.',
                    results: auditResults 
                })
                .eq('job_id', job.job_id);
            
            console.log(`[Worker] Job ${job_id} completed successfully.`);

        } catch (auditError) {
            // 5. Mark job as 'failed' if an error occurs
            console.error(`[Worker] Job ${job_id} FAILED:`, auditError.message, auditError.stack);
            
            // This also includes the fix for the 'job_id is not defined' error
            await supabase
                .from('audit_jobs')
                .update({ 
                    status: 'failed', 
                    error_message: auditError.message.substring(0, 500), 
                    progress_message: 'Audit failed.' 
                })
                .eq('job_id', job.job_id); // Use job.job_id
        }
        
        // Immediately poll for the next job
        setTimeout(pollForJobs, 1000); 

    } catch (err) {
        // This is the outer "Fatal Error" catch block
        console.error('[Worker] Fatal Error in pollForJobs loop:', err.message);
        if (job && job.job_id) {
            try {
                await supabase
                    .from('audit_jobs')
                    .update({ status: 'failed', error_message: 'Worker fatal error: ' + err.message })
                    .eq('job_id', job.job_id);
            } catch (releaseError) {
                console.error(`[Worker] CRITICAL: Failed to release job ${job.job_id} after fatal error.`, releaseError.message);
            }
        }
        setTimeout(pollForJobs, 10000);
    }
}


/**
 * Starts the worker
 */
async function startWorker() {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY || !CLIENT_ID || !CLIENT_SECRET) {
        console.error('[Worker] Missing critical environment variables. Worker cannot start.');
        console.log('Ensure SUPABASE_URL, SUPABASE_SERVICE_KEY, HUBSPOT_CLIENT_ID, and HUBSPOT_CLIENT_SECRET are set.');
        return;
    }
    
    console.log('[Worker] AuditPulse Worker Service started. Polling for jobs...');
    pollForJobs(); // Start the polling loop
}

startWorker();
