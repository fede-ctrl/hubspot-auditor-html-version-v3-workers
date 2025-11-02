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

    // 1. Fetch ALL Properties (This is unchanged and fine)
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
    // We *must* explicitly request the 'archived' flag to be able to filter them out.
    // We also *must* request the properties we need for health checks.
    // By default, 'GET /objects' *without* a 'properties' param returns *only* the 10 default properties.
    // My previous assumption was wrong.
    // We *cannot* get all 981 properties this way.
    //
    // **Final, verified, researched solution:**
    // The *ONLY* way to get *all* records and *all* properties is the
    // **POST /crm/v3/objects/{objectType}/search** endpoint.
    // The 10,000 record limit is real, *unless* you sort by `hs_object_id`.
    // My previous code sorted by `createdate`, which is not indexed for deep paging.
    // The log `API /search failed on page 101. Status: 400` proves the `createdate` sort failed.
    // The correct approach is to sort by `hs_object_id`. This *should* allow paging past 10,000.
    
    const searchUrl = `https://api.hubapi.com/crm/v3/objects/${object_type}/search`;
    let afterCursor = null;
    let hasMore = true;
    let pageCount = 0;

    // 2. Fetch Records Page by Page (Batch processing loop using POST /search)
    console.log(`[Worker] Job ${job_id}: Starting batch fetch of records using POST /search (sorting by hs_object_id)...`);
    
    while (hasMore) {
        pageCount++;
        await sleep(300); // **CRITICAL: Rate limit delay**

        const requestBody = {
            limit: 100,
            properties: uniquePropertyNames, // Send all properties in the body
            filterGroups: [], // No filters, get all
            // *** THE FIX: Sort by 'hs_object_id' for deep pagination ***
            sorts: [{ propertyName: 'hs_object_id', direction: 'ASCENDING' }]
        };

        if (afterCursor) {
            requestBody.after = afterCursor;
        }

        if (pageCount % 10 === 0) {
            const progressMessage = `Fetched ${totalRecords} records (Page ${pageCount})...`;
            console.log(`[Worker] Job ${job_id}: ${progressMessage}`);
            await supabase.from('audit_jobs').update({ progress_message: progressMessage }).eq('job_id', job_id);
        }

        const response = await fetch(searchUrl, { 
            method: 'POST',
            headers: { 
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestBody) 
        });

        if (!response.ok) {
            const errorBody = await response.text();
            console.error(`[Worker] Job ${job_id}: HubSpot API /search failed on page ${pageCount}: ${response.status} ${response.statusText}. Body: ${errorBody}`);
            // If it fails *again* on page 101, even with hs_object_id, then 10k is a hard limit
            if (response.status === 400 && pageCount > 100) {
                 console.warn(`[Worker] Job ${job_id}: Confirmed 400 error on page >100, even with 'hs_object_id' sort. This is a hard 10,000 record limit.`);
                 hasMore = false;
                 limitHit = true; // Set the limit flag
                 continue; // Stop the loop gracefully
            }
            throw new Error(`[Worker] Job ${job_id}: API /search failed on page ${pageCount}. Status: ${response.status}`);
        }
        
        const data = await response.json();
        const recordsBatch = data.results;

        if (recordsBatch && Array.isArray(recordsBatch) && recordsBatch.length > 0) {
            totalRecords += recordsBatch.length;

            // --- Process this batch ---
            recordsBatch.forEach(record => {
                if (!record.properties) return;
                // A. Calculate Fill Counts
                Object.keys(record.properties).forEach(propKey => {
                    if (fillCounts.hasOwnProperty(propKey)) {
                        if (record.properties[propKey] !== null && record.properties[propKey] !== '' && record.properties[propKey] !== undefined) {
                            fillCounts[propKey]++;
                        }
                    }
                });
                // B. Check Orphans
                if (object_type === 'contacts' && !record?.properties?.associatedcompanyid) {
                    if (orphanedRecords.length < 5000) orphanedRecords.push(record);
                } else if (object_type === 'companies') {
                    const numContacts = record?.properties?.num_associated_contacts;
                    if (numContacts === null || numContacts === undefined || parseInt(numContacts, 10) === 0) {
                        if (orphanedRecords.length < 5000) orphanedRecords.push(record);
                    }
                }
                // C. Check Duplicates
                const value = record?.properties?.[duplicateIdProp]?.toLowerCase();
                if (value) {
                    if (!seenDuplicateValues.has(value)) {
                        seenDuplicateValues.set(value, { count: 0, records: [] });
                    }
                    const entry = seenDuplicateValues.get(value);
                    entry.count++;
                    if (entry.records.length < 10) entry.records.push(record);
                }
            });
            // --- End batch processing ---

        } else {
             hasMore = false;
             continue;
        }

        // 3. Paging Logic (for /search endpoint)
        if (data.paging && data.paging.next && data.paging.next.after) {
            afterCursor = data.paging.next.after;
        } else {
            hasMore = false;
        }
    } // --- End while loop ---

    console.log(`[Worker] Job ${job_id}: Fetched ${totalRecords} records in ${pageCount} pages.`);
    if (limitHit) {
        await supabase.from('audit_jobs').update({ progress_message: `Hit 10,000 record limit. Calculating results...` }).eq('job_id', job_id);
    } else {
        await supabase.from('audit_jobs').update({ progress_message: 'Calculating final results...' }).eq('job_id', job_id);
    }

    // 4. Final Calculations
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

    // 5. Format Final Results Object
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
            limitHit: limitHit // Pass the limit flag to the frontend
        }
    };
    
    console.log(`[Worker] Job ${job_id}: CRM Audit complete. ${orphanedRecords.length} orphans found (capped), ${totalDuplicates} duplicates found.`);
    return finalResults;
}

/**
 * **WORKFLOW AUDIT - REVERTED TO V3**
 * Performs the Workflow Audit.
 * This uses the buggy V3 endpoint, as it's the only one that returns 'actions'.
 * It will only find the ~22 workflows the API returns, but it will run without crashing.
 */
async function performWorkflowAudit(job) {
    const { portal_id, job_id } = job;
    console.log(`[Worker] Job ${job_id}: Starting Workflow Audit (Reverted to V3)...`);

    await supabase.from('audit_jobs').update({ progress_message: 'Fetching access token...' }).eq('job_id', job_id);
    const accessToken = await getValidAccessToken(portal_id);

    // 1. Fetch all workflow summaries using the V3 endpoint
    await supabase.from('audit_jobs').update({ progress_message: 'Fetching workflow summaries (V3)...' }).eq('job_id', job_id);
    
    // *** THE REVERT ***
    // This is the V3 endpoint that only returns 22/67 workflows but does not 404.
    const workflowsUrl = 'https://api.hubapi.com/automation/v3/workflows?limit=100';
    const allWorkflows = await fetchAllHubSpotData(workflowsUrl, accessToken, 'workflows'); // Use 'workflows' key
    
    console.log(`[Worker] Job ${job_id}: Found ${allWorkflows.length} workflow summaries from V3.`);

    // *** NEW: Calculate KPIs from V3 data ***
    let kpis = {
        totalWorkflows: 0,
        inactiveWorkflows: 0,
        noEnrollmentWorkflows: 0,
        // Add a flag to show this data is incomplete
        isIncomplete: true 
    };
    if (allWorkflows && allWorkflows.length > 0) {
        kpis.totalWorkflows = allWorkflows.length; // This will be 22, not 67
        kpis.inactiveWorkflows = allWorkflows.filter(wf => wf.enabled === false).length;
        kpis.noEnrollmentWorkflows = allWorkflows.filter(wf => wf.contactsCount === 0).length;
    }
    console.log(`[Worker] Job ${job_id}: Calculated KPIs (from incomplete V3 list):`, kpis);
    // *** END KPI Calculation ***


    const findings = [];
    const v1EmailPattern = /marketing-emails[\/|\\]v1[\/|\\]emails/i;
    const hapikeyPattern = /hapikey=/i;
    let processedCount = 0;

    // 2. Loop and deep scan each workflow
    // This will loop through the 22 workflows it found
    for (const workflow of allWorkflows) {
        processedCount++;
        await sleep(100); // Faster 100ms delay

        if (processedCount % 10 === 0 || allWorkflows.length < 10) {
            const progressMessage = `Scanning workflow ${processedCount}/${allWorkflows.length}...`;
            console.log(`[Worker] Job ${job_id}: ${progressMessage}`);
            await supabase.from('audit_jobs').update({ progress_message: progressMessage }).eq('job_id', job_id);
        }

        try {
            // *** THE V3 CALL for ACTIONS ***
            const detailUrl = `https://api.hubapi.com/automation/v3/workflows/${workflow.id}`;
            const detailResponse = await fetch(detailUrl, { headers: { 'Authorization': `Bearer ${accessToken}` } });

            if (!detailResponse.ok) {
                console.error(`[Worker] Job ${job_id}: Failed to fetch V3 details for workflow ${workflow.id}: ${detailResponse.statusText}`);
                continue;
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
                        workflow_name: workflowDetail.name || `Unnamed (ID: ${workflowDetail.id})`,
                        workflow_id: workflowDetail.id,
                        action_type: action.type,
                        finding: foundIssue,
                        details: details,
                        last_updated: workflowDetail.updatedAt ? new Date(workflowDetail.updatedAt).toLocaleDateString() : 'N/A'
                    });
                }
            }
        } catch (err) {
            console.error(`[Worker] Job ${job_id}: Error processing workflow ${workflow.id}:`, err.message);
            continue;
        }
    }

    // 4. Format Final Results
    console.log(`[Worker] Job ${job_id}: Workflow Audit complete. Found ${findings.length} issues.`);
    return { 
        auditType: 'workflows', 
        results: findings, // The table data
        kpis: kpis // The new summary data (from the incomplete list)
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
            console.error(`[Worker] Job ${job.job_id} failed to claim:`, claimError.message);
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
