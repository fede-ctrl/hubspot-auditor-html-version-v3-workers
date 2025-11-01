# AuditPulse

AuditPulse is a powerful, all-in-one tool designed for HubSpot administrators to quickly analyze and improve their portal's data quality. The application provides actionable insights into property utilization, data health, and asset management for both Contact and Company objects.

Built with a stable and efficient Node.js backend and a clean, dependency-free vanilla JavaScript frontend, this tool is designed for reliability and ease of use.

## Key Features

* **Property Fill Rate Audit:** Get a comprehensive report on which properties are being used and which are being neglected. The audit calculates an estimated fill rate and fill count for every property in your portal.
* **Data Health Overview:** Instantly identify common data hygiene issues with key performance indicators (KPIs) for:
    * **Orphaned Contacts:** Contacts that are not associated with any company.
    * **Empty Companies:** Companies that have no associated contacts.
    * **Duplicate Records:** An estimated count of duplicate contacts (by email) and companies (by domain) based on a data sample.
* **Actionable "Drill-Down" Feature:** Click on a data health KPI to see a sample of the specific records that need attention, with direct links to view them in your HubSpot portal.
* **Expandable Property Details:** A clean, responsive table shows you the most important information at a glance, with an expandable section for every property to reveal technical details like internal name, type, and description.
* **CSV Export:** Download your full property audit report as a CSV file for offline analysis or sharing.
* **Multi-Tenant Ready:** Securely connects to any HubSpot portal via OAuth 2.0, making it ready for the HubSpot App Marketplace.

## Shared Data

| Data | Permissions |
| :--- | :--- |
| Contact Properties | Read |
| Company Properties | Read |
| Workflows | Read |

## Technology Stack

* **Backend:** Node.js, Express.js
* **Frontend:** HTML, Tailwind CSS (via CDN), Vanilla JavaScript
* **Database:** Supabase (for secure storage of OAuth tokens)
* **Deployment:** Render (as a unified Web Service)
