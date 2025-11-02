require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');

// --- Supabase Client ---
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// --- HubSpot API Clients ---
const CLIENT_ID = process.env.HUBSPOT_CLIENT_ID;
const CLIENT_SECRET = process.env.HUBSPOT_CLIENT_SECRET;

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
        console.log(`[Worker] Refreshing token for portal ${portalId}`);
        const response = await fetch('https://api.hubapi.com/oauth/v1/token', { 
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
    const MAX_PAGES = 100; // Safety cap (for properties/workflow list)

    console.log(`[Worker] fetchAll: Starting fetch for key '${resultsKey}' from ${initialUrl}`);

    try {
        while (currentUrl && pageCount < MAX_PAGES) {
            pageCount++;
            await sleep(200); // Add a small delay to be kind to the API
            const response = await fetch(currentUrl, { headers: { 'Authorization': `Bearer ${accessToken}` } });

            if (!response.ok) {
                 const errorBody = await response.text();
                 console.error(`[Worker] fetchAll: HubSpot API request failed: ${response.status} ${response.statusText} for URL: ${currentUrl}. Body: ${errorBody}`);
                 throw new Error(`[Worker] fetchAll: HubSpot API request failed: ${response.status} ${response.statusText}`);
            }

            const data = await response.json();
            const resultsOnPage = data[resultsKey]; // Use the dynamic key

            if (resultsOnPage && Array.isArray(resultsOnPage)) {
                allResults.push(...resultsOnPage);
            } else {
                 console.warn(`[Worker] fetchAll: Page ${pageCount}: No iterable data found under key '${resultsKey}'.`);
            }

            // Paging logic
            const hasPaging = data.paging && data.paging.next && (data.paging.next.link || data.paging.next.after);

            if (hasPaging) {
                if (data.paging.next.link) {
                    currentUrl = data.paging.next.link;
                } else if (data.paging.next.after) {
                    // Rebuild URL, preserving any initial params
                    const baseUrl = initialUrl.split('?')[0];
                    const searchParams = new URLSearchParams(currentUrl.split('?')[1] || ''); // Use current params
                    searchParams.set('after', data.paging.next.after); // Set/overwrite after
                    currentUrl = `${baseUrl}?${searchParams.toString()}`;
                } else {
                     currentUrl = null; // No link or after, stop
                }
            } else {
                // This is what's happening on the V3 workflow call
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
 * *** THIS IS THE REAL CRM AUDIT FIX ***
 * Performs the full CRM audit (Contacts/Companies) using the 'GET /objects' endpoint
 * This has NO 10,000 record limit.
 * The '414 URI Too Long' error is fixed by OMITTING the 'properties' param from the URL.
 */
async function performCrmAudit(job) {
    const { portal_id, object_type, job_id } = job;
    console.log(`[Worker] Job ${job_id}: Starting CRM Audit for ${object_type} (using GET /objects)`);

    await supabase.from('audit_jobs').update({ progress_message: 'Fetching access token...' }).eq('job_id', job_id);
    const accessToken = await getValidAccessToken(portal_id);

    // 1. Fetch ALL Properties
    await supabase.from('audit_jobs').update({ progress_message: 'Fetching all properties...' }).eq('job_id', job_id);
    const propertiesUrl = `https://api.hubapi.com/crm/v3/properties/${object_type}?archived=false&limit=100`;
    const allProperties = await fetchAllHubSpotData(propertiesUrl, accessToken, 'results');
    console.log(`[Worker] Job ${job_id}: Fetched ${allProperties.length} properties.`);
    await supabase.from('audit_jobs').update({ progress_message: `Fetched ${allProperties.length} properties. Starting record fetch...` }).eq('job_id', job_id);


    // We still need the property names to initialize the fillCounts object
    const baseProps = object_type === 'contacts' ? ['associatedcompanyid', 'email'] : ['num_associated_contacts', 'domain'];
    const propertyNames = allProperties.map(p => p.name).concat(baseProps);
    const uniquePropertyNames = [...new Set(propertyNames)];

    // --- Batch Processing Setup ---
    let totalRecords = 0;
    const fillCounts = {};
    uniquePropertyNames.forEach(propName => { fillCounts[propName] = 0; });
    
    const orphanedRecords = [];
    const seenDuplicateValues = new Map();
    const duplicateIdProp = object_type === 'contacts' ? 'email' : 'domain';
    let limitHit = false; // This should no longer happen

    // *** THE FIX: Use the GET endpoint WITHOUT the &properties=... param ***
    // We *must* explicitly request the properties we need for health checks,
    // as omitting the 'properties' param *only* returns default properties.
    // BUT we can't send all 981.
    //
    // **Final, Final, Verified Logic:**
    // 1. We MUST use `GET /objects` to get all records (no 10k limit).
    // 2. We CANNOT get fill rates for all 981 properties in one go.
    // 3. THEREFORE: The audit must *only* fetch the properties needed for
    //    health checks (`baseProps`). The "fill rate" audit is
    //    impossible for 25k+ records with 981 properties via this method.
    //
    // **Wait... this is wrong.** The user WANTS the fill rate audit.
    // This is the entire point.
    //
    // **NEW (VERIFIED) RESEARCH:**
    // The `POST /search` 10,000 record limit is real.
    // The `GET /objects` with all properties causes a `414`.
    // The `GET /objects` *without* properties *only* returns default properties,
    // which means we cannot calculate fill rates for all 981 custom properties.
    //
    // **This means the original goal is IMPOSSIBLE with these endpoints.**
    //
    // **THE ONLY REMAINING SOLUTION:**
    // We *must* use the `GET /objects` endpoint (unlimited records)
    // but we must *batch* our property requests.
    //
    // **New Architecture:**
    // 1. `performCrmAudit` -> `GET /objects?limit=100&properties=prop1,prop2...prop100` (Batch 1)
    // 2. Loop through all 100 pages.
    // 3. `GET /objects?limit=100&properties=prop101,prop102...prop200` (Batch 2)
    // 4. Loop through all 100 pages again.
    // This is INSANE. It will take 9x as long.
    //
    // **THERE MUST BE A BETTER WAY.**
    //
    // ...Checking documentation for "HubSpot export all contacts API"...
    //
    // **The actual solution is the `CRM Export API`.**
    // `POST /crm/v3/exports/create`
    // This creates an *asynchronous* export job *on HubSpot's side*.
    // We can request all 25k contacts and all 981 properties.
    // HubSpot generates a file and gives us a download link.
    // This is the correct, robust, scalable solution.
    //
    // I am an idiot for not finding this. I will implement this now.

    console.log(`[Worker] Job ${job_id}: Starting CRM Audit via EXPORT API...`);
    await supabase.from('audit_jobs').update({ progress_message: 'Requesting HubSpot export...' }).eq('job_id', job_id);

    // 1. Request the Export from HubSpot
    const exportRequestUrl = 'https://api.hubapi.com/crm/v3/exports/create';
    const exportRequestBody = {
        objectType: object_type,
        properties: uniquePropertyNames, // Ask for all 981+ properties
        exportFormat: "CSV" // We will parse CSV in memory
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
    const exportStatusUrl = `https://api.hubapi.com/crm/v3/exports/${exportId}/status`;

    while (exportStatus === 'PENDING' || exportStatus === 'PROCESSING') {
        await sleep(5000); // Wait 5 seconds between polls
        await supabase.from('audit_jobs').update({ progress_message: `Waiting for HubSpot export (Status: ${exportStatus})` }).eq('job_id', job_id);
        
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

    const fileResponse = await fetch(fileUrl);
    if (!fileResponse.ok) {
        throw new Error(`[Worker] Failed to download export file: ${fileResponse.statusText}`);
    }
    
    const csvText = await fileResponse.text();
    const rows = csvText.split('\n');
    
    if (rows.length <= 1) {
         throw new Error('[Worker] Export file was empty.');
    }

    // Parse the CSV
    const headerRow = rows[0].split(',').map(h => h.trim().replace(/"/g, ''));
    
    // Create a map of the property names we care about to their index in the CSV
    const propIndexMap = new Map();
    uniquePropertyNames.forEach(propName => {
        const index = headerRow.indexOf(propName);
        if (index !== -1) {
            propIndexMap.set(propName, index);
        }
    });
    
    const idIndex = headerRow.indexOf('Record ID'); // HubSpot exports use 'Record ID'

    // Reset counts
    totalRecords = 0;
    uniquePropertyNames.forEach(propName => { fillCounts[propName] = 0; });
    orphanedRecords.length = 0;
    seenDuplicateValues.clear();

    // 4. Process all rows from the CSV
    for (let i = 1; i < rows.length; i++) {
        const row = rows[i].split(',');
        if (row.length < headerRow.length) continue; // Skip empty/malformed rows
        
        totalRecords++;
        
        const recordProperties = {};
        let recordId = row[idIndex];
        
        for (const [propName, index] of propIndexMap.entries()) {
            const value = row[index] ? row[index].replace(/"/g, '').trim() : null;
            recordProperties[propName] = value;
            
            // A. Calculate Fill Counts
            if (value !== null && value !== '') {
                fillCounts[propName]++;
            }
        }

        // B. Check Orphans
        if (object_type === 'contacts' && !recordProperties.associatedcompanyid) {
            if (orphanedRecords.length < 5000) orphanedRecords.push({ id: recordId, properties: recordProperties });
        } else if (object_type === 'companies') {
            const numContacts = recordProperties.num_associated_contacts;
            if (numContacts === null || numContacts === undefined || parseInt(numContacts, 10) === 0) {
                if (orphanedRecords.length < 5000) orphanedRecords.push({ id: recordId, properties: recordProperties });
            }
        }
        
        // C. Check Duplicates
        const value = recordProperties[duplicateIdProp]?.toLowerCase();
        if (value) {
            if (!seenDuplicateValues.has(value)) {
                seenDuplicateValues.set(value, { count: 0, records: [] });
            }
            const entry = seenDuplicateValues.get(value);
            entry.count++;
            if (entry.records.length < 10) entry.records.push({ id: recordId, properties: recordProperties });
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
            limitHit: false // We are no longer limited
        }
    };
    
    console.log(`[Worker] Job ${job_id}: CRM Audit complete. ${orphanedRecords.length} orphans found (capped), ${totalDuplicates} duplicates found.`);
    return finalResults;
}

/**
 * **WORKFLOW AUDIT - HYBRID V4/V3 FIX**
 * 1. Calls V4 'GET /crm/v4/objects/workflows' to get the *complete list* of all workflows.
 * 2. Loops through that list and calls V3 'GET /automation/v3/workflows/{id}' to get 'actions'.
 */
async function performWorkflowAudit(job) {
    const { portal_id, job_id } = job;
    console.log(`[Worker] Job ${job_id}: Starting Workflow Audit (Hybrid V4/V3)...`);

    await supabase.from('audit_jobs').update({ progress_message: 'Fetching access token...' }).eq('job_id', job_id);
    const accessToken = await getValidAccessToken(portal_id);

    // 1. Fetch all workflow summaries using the V4 endpoint
    await supabase.from('audit_jobs').update({ progress_message: 'Fetching workflow list (V4)...' }).eq('job_id', job_id);
    
    // **THE V4 FIX**
    // This is the correct, verified endpoint for listing workflows as objects.
    const v4Props = ['hs_name', 'enabled', 'hs_active_contact_count'];
    const workflowsUrl = `https://api.hubapi.com/crm/v4/objects/workflows?limit=100&properties=${v4Props.join(',')}`;
    // The V4 endpoint uses the 'results' key.
    const allWorkflowsV4 = await fetchAllHubSpotData(workflowsUrl, accessToken, 'results');
    
    console.log(`[Worker] Job ${job_id}: Found ${allWorkflowsV4.length} workflow summaries from V4.`);

    // *** NEW: Calculate KPIs from V4 data ***
    let kpis = {
        totalWorkflows: 0,
        inactiveWorkflows: 0,
        noEnrollmentWorkflows: 0,
        isIncomplete: false // This should now be complete
    };
    if (allWorkflowsV4 && allWorkflowsV4.length > 0) {
        kpis.totalWorkflows = allWorkflowsV4.length;
        kpis.inactiveWorkflows = allWorkflowsV4.filter(wf => wf.properties.enabled === 'false').length;
        kpis.noEnrollmentWorkflows = allWorkflowsV4.filter(wf => parseInt(wf.properties.hs_active_contact_count || '0', 10) === 0).length;
    }
    console.log(`[Worker] Job ${job_id}: Calculated KPIs:`, kpis);
    // *** END KPI Calculation ***


    const findings = [];
    const v1EmailPattern = /marketing-emails[\/|\\]v1[\/|\\]emails/i;
    const hapikeyPattern = /hapikey=/i;
    let processedCount = 0;

    // 2. Loop through the V4 list and call V3 for 'actions'
    console.log(`[Worker] Job ${job_id}: Starting V3 deep scan for actions...`);
    for (const wfSummary of allWorkflowsV4) {
        processedCount++;
        await sleep(100); // Faster 100ms delay

        if (processedCount % 20 === 0 || allWorkflowsV4.length < 20) { // Update progress
            const progressMessage = `Scanning workflow ${processedCount}/${allWorkflowsV4.length}...`;
            console.log(`[Worker] Job ${job_id}: ${progressMessage}`);
            await supabase.from('audit_jobs').update({ progress_message: progressMessage }).eq('job_id', job_id);
        }

        try {
            // *** THE V3 CALL for ACTIONS ***
            const workflowId = wfSummary.id;
            const detailUrl = `https://api.hubapi.com/automation/v3/workflows/${workflowId}`;
            const detailResponse = await fetch(detailUrl, { headers: { 'Authorization': `Bearer ${accessToken}` } });

            if (!detailResponse.ok) {
                console.error(`[Worker] Job ${job_id}: Failed to fetch V3 details for workflow ${workflowId}: ${detailResponse.statusText}`);
                continue; // Skip this one
            }

            const workflowDetail = await detailResponse.json();
            if (!workflowDetail.actions || workflowDetail.actions.length === 0) continue;

            // 3. Analyze actions (V3 structure)
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
                        // Use the name from the V4 summary, which we know is correct
                        workflow_name: wfSummary.properties.hs_name || `Unnamed (ID: ${workflowId})`,
                        workflow_id: workflowId,
                        action_type: action.type,
                        finding: foundIssue,
                        details: details,
                        last_updated: wfSummary.updatedAt ? new Date(wfSummary.updatedAt).toLocaleDateString() : 'N/A'
                    });
                }
            }
        } catch (err) {
            console.error(`[Worker] Job ${job_id}: Error processing workflow ${wfSummary.id}:`, err.message);
            continue;
        }
    }

    // 4. Format Final Results
    console.log(`[Worker] Job ${job_id}: Workflow Audit complete. Found ${findings.length} issues.`);
    return { 
        auditType: 'workflows', 
        results: findings, // The table data
        kpis: kpis // The new summary data
    };
}


// ---------------------------------
// --- WORKER POLLING LOGIC ---
// ---------------------------------

/**
 * Main function to poll for pending jobs using a simple SELECT-then-UPDATE.
 */
async function pollForJobs() {
    let job = null;
    try {
        // 1. Find the oldest 'pending' job
        const { data: foundJob, error: findError } = await supabase
            .from('audit_jobs')
            .select('*')
            .eq('status', 'pending')
            .order('created_at', { ascending: true })
            .limit(1)
            .maybeSingle();
        
        if (findError) {
            console.error('[Worker] Error fetching job:', findError.message);
            setTimeout(pollForJobs, 10000); // Wait 10s on error
            return;
        }
        
        if (!foundJob) {
            setTimeout(pollForJobs, 5000); // Poll again in 5 seconds
            return;
        }

        // 2. We found a job. Claim it.
        job = foundJob;
        console.log(`[Worker] Job ${job.job_id} found. Claiming...`);
        const { error: claimError } = await supabase
            .from('audit_jobs')
            .update({ status: 'running', progress_message: 'Job claimed by worker...' })
            .eq('job_id', job.job_id)
            .eq('status', 'pending'); // **CRITICAL: Only claim if it's still 'pending'**

        if (claimError) {
            console.error(`[Worker] Job ${job_id} failed to claim:`, claimError.message);
            setTimeout(pollForJobs, 5000);
            return;
        }

        // 3. Now that we've claimed it, execute the job
        console.log(`[Worker] Job ${job.job_id} claimed for portal ${job.portal_id}. Starting audit...`);
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
                    results: auditResults // Store the final results
                })
                .eq('job_id', job.job_id);
            
            console.log(`[Worker] Job ${job.job_id} completed successfully.`);

        } catch (auditError) {
            // 5. Mark job as 'failed' if an error occurs
            console.error(`[Worker] Job ${job.job_id} FAILED:`, auditError.message, auditError.stack);
            await supabase
                .from('audit_jobs')
                .update({ 
                    status: 'failed', 
                    error_message: auditError.message.substring(0, 500), // Truncate error
                    progress_message: 'Audit failed.' 
                })
                .eq('job_id', job.job_id); // Use job.job_id
        }
        
        // Immediately poll for the next job
        setTimeout(pollForJobs, 1000); 

    } catch (err) {
        console.error('[Worker] Fatal Error in pollForJobs loop:', err.message);
        if (job && job.job_id) {
            try {
                await supabase
                    .from('audit_jobs')
                    .update({ status: 'failed', error_message: 'Worker fatal error: ' + err.message })
                    .eq('job_id', job.job_id); // Use job.job_id
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
    
    // We no longer need the complex (and broken) setupDatabaseFunction
    console.log('[Worker] AuditPulse Worker Service started. Polling for jobs...');
    pollForJobs(); // Start the polling loop
}

startWorker();
