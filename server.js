require('dotenv').config();
const express = require('express');
const fetch = require('node-fetch');
const { createClient } = require('@supabase/supabase-js');
const cors = require('cors');
const path = require('path');
const ExcelJS = require('exceljs');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// --- Environment Variables ---
const CLIENT_ID = process.env.HUBSPOT_CLIENT_ID;
const CLIENT_SECRET = process.env.HUBSPOT_CLIENT_SECRET;
const RENDER_EXTERNAL_URL = 'https://auditpulse.onrender.com';
const REDIRECT_URI = `${RENDER_EXTERNAL_URL}/api/oauth-callback`;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// --- HubSpot API Helper ---
async function getValidAccessToken(portalId) {
    const { data: installation, error } = await supabase.from('installations').select('refresh_token, access_token, expires_at').eq('hubspot_portal_id', portalId).single();
    if (error || !installation) throw new Error(`Could not find installation for portal ${portalId}. Please reinstall the app.`);
    let { refresh_token, access_token, expires_at } = installation;
    if (new Date() > new Date(expires_at)) {
        console.log(`Refreshing token for portal ${portalId}`);
        const response = await fetch('https://api.hubapi.com/oauth/v1/token', { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ grant_type: 'refresh_token', client_id: CLIENT_ID, client_secret: CLIENT_SECRET, refresh_token }), });
        if (!response.ok) throw new Error(`Failed to refresh access token: ${await response.text()}`);
        const newTokens = await response.json();
        access_token = newTokens.access_token;
        const newExpiresAt = new Date(Date.now() + newTokens.expires_in * 1000).toISOString();
        const { error: updateError } = await supabase.from('installations').update({ access_token, expires_at: newExpiresAt }).eq('hubspot_portal_id', portalId);
        if (updateError) console.error("Error updating token in Supabase:", updateError);
    }
    return access_token;
}

// **REFINED HELPER FUNCTION** - More robust paging logic and logging
async function fetchAllHubSpotData(initialUrl, accessToken, resultsKey) {
    const allResults = [];
    let currentUrl = initialUrl;
    let pageCount = 0;
    const MAX_PAGES = 100; // Safety limit

    // Extract limit from initial URL for comparison later
    const initialUrlParams = new URLSearchParams(initialUrl.split('?')[1] || '');
    const requestedLimit = parseInt(initialUrlParams.get('limit') || '100', 10); // Default to 100 if not specified

    console.log(`[fetchAllHubSpotData] Starting fetch for key '${resultsKey}' from ${initialUrl} (Requested Limit: ${requestedLimit})`);

    try {
        while (currentUrl && pageCount < MAX_PAGES) {
            pageCount++;
            console.log(`[fetchAllHubSpotData] Fetching page ${pageCount}: ${currentUrl}`);
            const response = await fetch(currentUrl, { headers: { 'Authorization': `Bearer ${accessToken}` } });

            if (!response.ok) {
                 const errorBody = await response.text();
                 console.error(`[fetchAllHubSpotData] HubSpot API request failed: ${response.status} ${response.statusText} for URL: ${currentUrl}. Body: ${errorBody}`);
                 throw new Error(`HubSpot API request failed: ${response.status} ${response.statusText}`);
            }

            const data = await response.json();

            // Use the dynamic 'resultsKey'
            const resultsOnPage = data[resultsKey];
            let itemsAddedThisPage = 0;
            if (resultsOnPage && Array.isArray(resultsOnPage)) {
                allResults.push(...resultsOnPage);
                itemsAddedThisPage = resultsOnPage.length;
                console.log(`[fetchAllHubSpotData] Page ${pageCount}: Added ${itemsAddedThisPage} items using key '${resultsKey}'. Total items: ${allResults.length}`);
            } else {
                 console.warn(`[fetchAllHubSpotData] Page ${pageCount}: No iterable data found under key '${resultsKey}' or key is missing/not an array. Response keys: ${Object.keys(data)}`);
            }

            // Paging logic - Check carefully
            const hasPaging = data.paging && data.paging.next && (data.paging.next.link || data.paging.next.after);

            if (hasPaging) {
                 console.log(`[fetchAllHubSpotData] Page ${pageCount}: Paging info found.`);
                if (data.paging.next.link) {
                    currentUrl = data.paging.next.link;
                     console.log(`[fetchAllHubSpotData] Paging using 'link': ${currentUrl}`);
                } else if (data.paging.next.after) {
                    const baseUrl = initialUrl.split('?')[0];
                    const searchParams = new URLSearchParams(currentUrl.split('?')[1] || '');
                    searchParams.set('after', data.paging.next.after);
                    currentUrl = `${baseUrl}?${searchParams.toString()}`;
                    console.log(`[fetchAllHubSpotData] Paging using 'after': ${currentUrl}`);
                }
                // Check if the API returned fewer items than requested, even with paging info (could indicate end)
                if (itemsAddedThisPage < requestedLimit && currentUrl) {
                     console.warn(`[fetchAllHubSpotData] Page ${pageCount}: Received ${itemsAddedThisPage} items (less than requested limit ${requestedLimit}) but paging info exists. Continuing fetch, but this might be the last page.`);
                }
            } else {
                console.log(`[fetchAllHubSpotData] Page ${pageCount}: No valid paging.next.link or paging.next.after found.`);
                // **Explicitly log if fewer items were received than requested AND no paging**
                if (itemsAddedThisPage < requestedLimit) {
                    console.warn(`[fetchAllHubSpotData] Page ${pageCount}: Received ${itemsAddedThisPage} items (less than requested limit ${requestedLimit}) AND no further paging info. Assuming fetch complete.`);
                } else {
                    console.log("[fetchAllHubSpotData] Assuming fetch complete as no further paging info provided.");
                }
                currentUrl = null; // Stop pagination
            }
        } // End while loop

        if (pageCount >= MAX_PAGES) {
             console.warn(`[fetchAllHubSpotData] Reached maximum page limit (${MAX_PAGES}). Fetch may be incomplete.`);
        }

        console.log(`[fetchAllHubSpotData] Finished fetch for key '${resultsKey}'. Total items retrieved: ${allResults.length}`);
        return allResults;

    } catch (error) {
        console.error(`[fetchAllHubSpotData] Error during fetch for key '${resultsKey}':`, error);
        throw error;
    }
}


// --- API Routes ---
app.get('/api/install', (req, res) => {
    const REQUIRED_SCOPES = 'oauth crm.objects.companies.read crm.schemas.contacts.read crm.objects.contacts.read crm.schemas.companies.read';
    const OPTIONAL_SCOPES = 'automation crm.schemas.deals.read business-intelligence crm.objects.owners.read crm.objects.deals.read';
    const authUrl = `https://app.hubspot.com/oauth/authorize?client_id=${CLIENT_ID}&redirect_uri=${REDIRECT_URI}&scope=${REQUIRED_SCOPES}&optional_scope=${OPTIONAL_SCOPES}`;
    res.redirect(authUrl);
});

app.post('/api/disconnect', async (req, res) => {
    const portalId = req.header('X-HubSpot-Portal-Id');
    if (!portalId) return res.status(400).json({ message: 'HubSpot Portal ID is missing.' });
    try {
        const { error } = await supabase.from('installations').delete().eq('hubspot_portal_id', portalId);
        if (error) {
            console.error('Supabase disconnect error:', error);
            throw new Error('Database error during disconnect.');
        };
        res.status(200).json({ message: 'Successfully disconnected.' });
    } catch (error) {
        console.error('Disconnect error:', error);
        res.status(500).json({ message: error.message || 'Failed to disconnect.' });
    }
});


app.get('/api/oauth-callback', async (req, res) => {
    const authCode = req.query.code;
    if (!authCode) return res.status(400).send('HubSpot authorization code not found.');
    try {
        const response = await fetch('https://api.hubapi.com/oauth/v1/token', { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ grant_type: 'authorization_code', client_id: CLIENT_ID, client_secret: CLIENT_SECRET, redirect_uri: REDIRECT_URI, code: authCode }), });
        if (!response.ok) throw new Error(`Token exchange failed: ${await response.text()}`);
        const tokenData = await response.json();
        const { refresh_token, access_token, expires_in } = tokenData;

        const tokenInfoResponse = await fetch(`https://api.hubapi.com/oauth/v1/access-tokens/${access_token}`);
        if (!tokenInfoResponse.ok) throw new Error(`Failed to fetch HubSpot token info: ${await tokenInfoResponse.text()}`);
        const tokenInfo = await tokenInfoResponse.json();

        const hub_id = tokenInfo.hub_id;
        const granted_scopes = tokenInfo.scopes;
        const expiresAt = new Date(Date.now() + expires_in * 1000).toISOString();

        // Save to Supabase
        const { error: upsertError } = await supabase.from('installations').upsert(
            {
                hubspot_portal_id: hub_id,
                refresh_token,
                access_token,
                expires_at: expiresAt,
                granted_scopes: granted_scopes
            },
            { onConflict: 'hubspot_portal_id' }
        );
        if (upsertError) {
             console.error('Supabase upsert error during oauth:', upsertError);
             throw new Error('Database error saving installation details.');
        }

        res.redirect(`/?portalId=${hub_id}`);
    } catch (error) {
        console.error('OAuth Callback Error:', error);
        res.status(500).send(`<h1>Server Error</h1><p>${error.message}</p>`);
    }
});

app.get('/api/check-scopes', async (req, res) => {
    const portalId = req.header('X-HubSpot-Portal-Id');
    if (!portalId) return res.status(400).json({ message: 'HubSpot Portal ID is missing.' });
    try {
        const { data, error } = await supabase.from('installations').select('granted_scopes').eq('hubspot_portal_id', portalId).single();

        if (error) {
            console.error('Supabase error fetching scopes:', error);
            return res.status(500).json({ message: 'Could not fetch permissions.' });
        }
        if (!data || !data.granted_scopes) {
            return res.json([]);
        }

        let scopesArray;
        if (typeof data.granted_scopes === 'string') {
            try {
                scopesArray = JSON.parse(data.granted_scopes);
            } catch (parseError) {
                console.error('Failed to parse granted_scopes string:', parseError, 'Raw string:', data.granted_scopes);
                return res.json([]);
            }
        } else if (Array.isArray(data.granted_scopes)) {
            scopesArray = data.granted_scopes;
        } else {
            console.warn('granted_scopes has unexpected type:', typeof data.granted_scopes);
            return res.json([]);
        }

        res.json(scopesArray);

    } catch (error) {
        console.error('Check scopes internal error:', error);
        res.status(500).json({ message: 'Server error checking scopes.' });
    }
});


// CRM Audit Endpoint
app.get('/api/audit', async (req, res) => {
    const portalId = req.header('X-HubSpot-Portal-Id');
    const objectType = req.query.objectType || 'contacts';
    if (!portalId) return res.status(400).json({ message: 'HubSpot Portal ID is missing.' });
    if (objectType !== 'contacts' && objectType !== 'companies') {
         return res.status(400).json({ message: 'Invalid objectType. Must be contacts or companies.' });
    }

    try {
        const accessToken = await getValidAccessToken(portalId);

        const propertiesUrl = `https://api.hubapi.com/crm/v3/properties/${objectType}?archived=false&limit=100`;
        const allProperties = await fetchAllHubSpotData(propertiesUrl, accessToken, 'results');

        const baseProps = objectType === 'contacts' ? ['associatedcompanyid', 'email'] : ['num_associated_contacts', 'domain'];
        const propertyNames = allProperties.map(p => p.name).concat(baseProps);
        const uniquePropertyNames = [...new Set(propertyNames)];

        const propertiesQueryString = uniquePropertyNames.join(',');

        const recordsUrl = `https://api.hubapi.com/crm/v3/objects/${objectType}?limit=100&properties=${propertiesQueryString}`;
        console.log(`Fetching CRM records: ${recordsUrl}`);
        const allRecords = await fetchAllHubSpotData(recordsUrl, accessToken, 'results');
        const totalRecords = allRecords.length;
        console.log(`Fetched ${totalRecords} ${objectType} records.`);

        // Calculate Fill Rates
        const fillCounts = {};
        uniquePropertyNames.forEach(propName => { fillCounts[propName] = 0; });

        allRecords.forEach(r => {
            if (!r.properties) return;
            Object.keys(r.properties).forEach(p => {
                if (r.properties[p] !== null && r.properties[p] !== '' && r.properties[p] !== undefined) {
                    if (uniquePropertyNames.includes(p)) {
                         fillCounts[p] = (fillCounts[p] || 0) + 1;
                    }
                }
            });
        });

        const auditResults = allProperties.map(prop => {
            const fillCount = fillCounts[prop.name] || 0;
            const fillRate = totalRecords > 0 ? Math.round((fillCount / totalRecords) * 100) : 0;
            return {
                label: prop.label, internalName: prop.name, type: prop.type, description: prop.description || '', isCustom: !prop.hubspotDefined, fillRate, fillCount
            };
        });

        const { orphanedRecords, duplicateRecords } = calculateDataHealth(objectType, allRecords);

        // Calculate Total Duplicates Count
        const valueMap = new Map();
        const duplicateIdProp = objectType === 'contacts' ? 'email' : 'domain';
        duplicateRecords.forEach(rec => {
            const value = rec?.properties?.[duplicateIdProp]?.toLowerCase();
            if (value) {
                if (!valueMap.has(value)) valueMap.set(value, 0);
                valueMap.set(value, valueMap.get(value) + 1);
            }
        });
        let totalDuplicates = 0;
        for (const count of valueMap.values()) {
            if (count > 1) {
                totalDuplicates += (count - 1);
            }
        }

        const customProperties = auditResults.filter(p => p.isCustom);
        const averageCustomFillRate = customProperties.length > 0
            ? Math.round(customProperties.reduce((acc, p) => acc + p.fillRate, 0) / customProperties.length)
            : 0;

        res.json({
            auditType: 'crm',
            objectType: objectType,
            data: {
                totalRecords, totalProperties: auditResults.length, averageCustomFillRate, properties: auditResults, orphanedRecords, duplicateRecords, totalDuplicates
            }
        });
    } catch (error) {
        console.error(`Audit error for ${objectType}:`, error);
        res.status(500).json({ message: `Audit failed: ${error.message}. Check server logs for details.` });
    }
});


// Workflow Audit Endpoint
app.get('/api/workflow-audit', async (req, res) => {
    const portalId = req.header('X-HubSpot-Portal-Id');
    if (!portalId) return res.status(400).json({ message: 'HubSpot Portal ID is missing.' });

    console.log(`[DEBUG] Starting workflow audit for portal ${portalId}`);

    try {
        // Server-side scope check
        const { data: installation, error: scopeError } = await supabase.from('installations').select('granted_scopes').eq('hubspot_portal_id', portalId).single();

        let scopesArray = [];
        if (installation && installation.granted_scopes) {
             if (typeof installation.granted_scopes === 'string') {
                try { scopesArray = JSON.parse(installation.granted_scopes); } catch (e) { console.error("Error parsing scopes in workflow audit:", e); }
             } else if (Array.isArray(installation.granted_scopes)) {
                scopesArray = installation.granted_scopes;
             }
        }

        if (scopeError || !scopesArray.includes('automation')) {
             console.warn(`[DEBUG] Workflow audit attempt failed for portal ${portalId}: Missing 'automation' scope. Scopes found: ${scopesArray}`);
            return res.status(403).json({ message: 'Workflow Audit requires the "Automation" scope. Please disconnect and reinstall the app, ensuring you grant this permission.' });
        }

        const accessToken = await getValidAccessToken(portalId);

        // 1. Fetch ALL workflow summaries using the helper
        const workflowsUrl = 'https://api.hubapi.com/automation/v3/workflows?limit=100'; // Initial URL with limit
        console.log("[DEBUG] Fetching ALL workflows summaries using helper:", workflowsUrl);
        const allWorkflows = await fetchAllHubSpotData(workflowsUrl, accessToken, 'workflows'); // Use 'workflows' key and the helper
        console.log(`[DEBUG] Fetch helper completed. Found ${allWorkflows.length} total workflow summaries.`);

        // *** NEW LOGGING: Check if the target workflow ID is in the fetched list ***
        const targetWorkflowId = '2873517280'; // Your specific custom code workflow ID as a string
        const targetWorkflowSummary = allWorkflows.find(wf => wf.id.toString() === targetWorkflowId); // Find by ID (convert API ID to string)

        if (targetWorkflowSummary) {
            console.log(`[DEBUG] Target workflow ID ${targetWorkflowId} (Name: ${targetWorkflowSummary.name}) WAS FOUND in the initial list.`);
        } else {
            console.error(`[ERROR] Target workflow ID ${targetWorkflowId} WAS NOT FOUND in the fetched list of ${allWorkflows.length} summaries. Pagination might be incomplete or the ID is incorrect/inaccessible.`);
             if (allWorkflows.length < 50) { // Only log if the list isn't huge
                 console.log("[DEBUG] Fetched workflow IDs:", allWorkflows.map(wf => wf.id));
             }
        }
        // *** END NEW LOGGING ***


        const findings = [];
        const v1EmailPattern = /marketing-emails[\/|\\]v1[\/|\\]emails/i;
        const hapikeyPattern = /hapikey=/i;

        // 2. Loop and deep scan each workflow from the fetched list
        console.log("[DEBUG] Starting deep scan of workflows...");
        let processedCount = 0;
        for (const workflow of allWorkflows) {
            processedCount++;
            // Log less frequently, but always log the target ID if encountered
            if (workflow.id.toString() === targetWorkflowId || processedCount % 50 === 0 || allWorkflows.length < 50) {
                 console.log(`[DEBUG] Processing workflow ${processedCount}/${allWorkflows.length} (ID: ${workflow.id}, Name: ${workflow.name})`);
            }

            try {
                const detailUrl = `https://api.hubapi.com/automation/v3/workflows/${workflow.id}`;
                const detailResponse = await fetch(detailUrl, { headers: { 'Authorization': `Bearer ${accessToken}` } });

                if (!detailResponse.ok) {
                    console.error(`[DEBUG] Failed to fetch details for workflow ${workflow.id}: ${detailResponse.status} ${detailResponse.statusText}`);
                    continue;
                }

                const workflowDetail = await detailResponse.json();

                if (!workflowDetail.actions || !Array.isArray(workflowDetail.actions) || workflowDetail.actions.length === 0) {
                    continue;
                }

                // 3. Analyze target actions
                for (const action of workflowDetail.actions) {
                    let foundIssue = null;
                    let details = '';

                    // Log the action type for the target workflow specifically, or all if few workflows exist
                     if (workflow.id.toString() === targetWorkflowId || allWorkflows.length < 10) {
                        console.log(`[DEBUG] Workflow ID: ${workflow.id} - Checking action. Action Type found: '${action.type}'`);
                     }


                    if (action.type === 'WEBHOOK' && action.url) {
                         // console.log(`[DEBUG] Workflow ID: ${workflow.id} - Is WEBHOOK action. Testing URL.`);
                        if (hapikeyPattern.test(action.url)) {
                            foundIssue = 'HAPIkey in URL';
                            details = action.url;
                        } else if (v1EmailPattern.test(action.url)) {
                            foundIssue = 'V1 Marketing Email API URL';
                            details = action.url;
                        }
                    } else if (action.type === 'CUSTOM_CODE' && action.code) {
                        // console.log(`\n[DEBUG] Workflow ID: ${workflow.id} - Is CUSTOM_CODE action. Testing code.`);
                        if (action.code && typeof action.code === 'string') {
                             if (workflow.id.toString() === targetWorkflowId) {
                                console.log(`------ BEGIN action.code for ${workflow.id} ------`);
                                console.log(action.code);
                                console.log(`------ END action.code for ${workflow.id} ------`);
                             }
                            const v1Match = v1EmailPattern.test(action.code);
                            const hapikeyMatch = hapikeyPattern.test(action.code);
                             if (workflow.id.toString() === targetWorkflowId) {
                                console.log(`[DEBUG] Testing V1 Pattern Result for ${workflow.id}: ${v1Match}`);
                                console.log(`[DEBUG] Testing Hapikey Pattern Result for ${workflow.id}: ${hapikeyMatch}`);
                             }

                            if (hapikeyMatch) {
                                foundIssue = 'HAPIkey in Custom Code';
                                details = 'Custom Code Snippet';
                            } else if (v1Match) {
                                foundIssue = 'V1 Marketing Email API in Custom Code';
                                details = 'Custom Code Snippet';
                            }
                        } else {
                            console.log(`[DEBUG] Workflow ${workflow.id}: CUSTOM_CODE action has no 'code' property or it's not a string.`);
                        }
                    }

                    if (foundIssue) {
                        console.log(`[DEBUG] >>> Finding recorded for Workflow ID ${workflowDetail.id}: ${foundIssue}`);
                        findings.push({
                            workflow_name: workflowDetail.name || `Unnamed Workflow (ID: ${workflowDetail.id})`,
                            workflow_id: workflowDetail.id,
                            action_type: action.type,
                            finding: foundIssue,
                            details: details,
                            last_updated: workflowDetail.updatedAt ? new Date(workflowDetail.updatedAt).toLocaleDateString() : 'N/A'
                        });
                    }
                } // End loop through actions
            } catch (err) {
                console.error(`[DEBUG] Error processing workflow ${workflow.id}:`, err);
                continue;
            }
        } // End loop through workflows
        console.log(`[DEBUG] Workflow deep scan completed. Found ${findings.length} total issues.`);

        res.json({ auditType: 'workflows', results: findings });

    } catch (error) {
        console.error('[DEBUG] Workflow audit endpoint failed:', error);
        res.status(500).json({ message: `Workflow audit failed: ${error.message}` });
    }
}); // End of /api/workflow-audit


// Flexible Excel Download Endpoint
app.post('/api/download-excel', async (req, res) => {
    const { auditData, auditType, objectType } = req.body;

    if (!auditData || !auditType) {
        return res.status(400).send('Audit data or type is missing.');
    }
    if (auditType !== 'crm' && auditType !== 'workflows') {
        return res.status(400).send('Invalid auditType specified.');
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

            // Worksheet 1: Property Audit
            const propSheet = workbook.addWorksheet('Property Audit');
            propSheet.columns = [
                { header: 'Property Label', key: 'label', width: 30 }, { header: 'Internal Name', key: 'internalName', width: 30 }, { header: 'Type', key: 'type', width: 15 }, { header: 'Source', key: 'source', width: 15 }, { header: 'Description', key: 'description', width: 40 }, { header: 'Filled Records', key: 'fillCount', width: 15 }, { header: 'Fill Rate (%)', key: 'fillRate', width: 15 }
            ];
            if (crmData.properties && Array.isArray(crmData.properties)) {
                crmData.properties.forEach(p => {
                    propSheet.addRow({ ...p, source: p.isCustom ? 'Custom' : 'Standard' });
                });
            }


            // Worksheet 2: Orphaned/Empty Records
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


            // Worksheet 3: Duplicates
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


            // Worksheet 4: Properties Missing Description
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

        // Set Headers and Write File
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);

        await workbook.xlsx.write(res);
        res.end();

    } catch (error) {
        console.error('Excel generation error:', error);
        res.status(500).send(`Error generating Excel file: ${error.message}`);
    }
});


// Helper function for CRM Data Health
function calculateDataHealth(objectType, allRecords) {
    let orphanedRecords = [];
    try {
        if (objectType === 'contacts') {
            orphanedRecords = allRecords.filter(contact => !contact?.properties?.associatedcompanyid);
        } else { // 'companies'
            orphanedRecords = allRecords.filter(company => {
                const numContacts = company?.properties?.num_associated_contacts;
                return numContacts === null || numContacts === undefined || parseInt(numContacts, 10) === 0;
            });
        }
    } catch (e) {
        console.error("Error calculating orphaned records:", e);
    }


    const duplicateIdProp = objectType === 'contacts' ? 'email' : 'domain';
    const valueMap = new Map();
    const duplicateRecords = [];

    try {
        allRecords.forEach(record => {
            const value = record?.properties?.[duplicateIdProp]?.toLowerCase();
            if (value) {
                if (!valueMap.has(value)) {
                    valueMap.set(value, []);
                }
                valueMap.get(value).push(record);
            }
        });

        for (const recordsWithValue of valueMap.values()) {
            if (recordsWithValue.length > 1) {
                duplicateRecords.push(...recordsWithValue);
            }
        }
    } catch (e) {
        console.error("Error calculating duplicate records:", e);
    }

    return { orphanedRecords, duplicateRecords };
}

// Catch-all route MUST be last
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Basic Error Handling Middleware
app.use((err, req, res, next) => {
  console.error("Unhandled Error:", err.stack || err);
  res.status(500).send('Something broke on the server!');
});


app.listen(PORT, () => console.log(` Server is live on port ${PORT}`));

