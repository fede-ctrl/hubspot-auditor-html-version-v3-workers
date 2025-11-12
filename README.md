# AuditPulse Pro

AuditPulse Pro is a production-grade, asynchronous auditing platform for HubSpot administrators. It provides deep diagnostics for CRM hygiene, workflow safety, and property completeness while delegating heavy data processing to a dedicated worker service.

## Final Product Summary

* **No API limits:** Streams HubSpot's Async Export API so audits cover millions of records without pagination caps.
* **Asynchronous job engine:** Web tier stays responsive while a worker claims jobs via Supabase RPC leases.
* **Actionable outputs:** Excel workbooks summarize KPIs, duplicates, orphan records, property fill rates, and workflow risks.
* **Production hardened:** Built for Render + Supabase with strict OAuth handling, RLS-enabled tables, and environment-specific configuration.

## System Architecture

| Component | Role | Key Responsibilities |
| --- | --- | --- |
| Web service (`server.js`) | User-facing API and frontend host | OAuth 2.0 auth, installation status, job creation, status polling, Excel export |
| Worker service (`worker.js`) | Background processor | Claims jobs, runs HubSpot exports, streams CSV analysis, updates progress |
| Supabase (PostgreSQL + RPCs) | Shared state and coordination | Stores OAuth tokens, enforces leases, exposes RPCs for claim/progress/lease |
| Frontend (`public/index.html`) | Single-page UI | Install flow, status checks, audit launcher, progress tracker, results viewer |

No audit work runs inside the web process. Jobs persist in `audit_jobs`, are claimed exclusively through `claim_audit_job`, and advance asynchronously until completion.

## Key Features

1. **Asynchronous CRM audits**
   * Uses HubSpot Async Export API to pull Contacts and Companies without record limits.
   * Streams CSV line-by-line with bounded memory and automatic retry/backoff on 429/5xx responses.
   * Maintains live progress, processed counts, and job leases via Supabase RPCs.
2. **Workflow & security review**
   * Fetches all workflows and flags risky actions such as exposed API keys or deprecated Marketing Email API calls.
3. **Data health KPIs**
   * Total records, orphaned records, ownerless records, stale records, invalid emails (contacts), lifecycle stage gaps.
   * Property fill rates for sparsest fields and duplicate detection (email/phone/domain) with sample IDs.
4. **Excel report generation**
   * Multi-sheet workbook for summary KPIs, orphan samples, duplicate keys, property fill rates, and workflow risks.
5. **Reliable operations**
   * Lease/heartbeat model prevents double-processing and recovers from crashes.
   * Environment-driven configuration shared between services; OAuth tokens stored only in Supabase.

## Deployment Checklist

### 1. Supabase schema

Run the following SQL *once* in the Supabase SQL editor to normalize the schema around `portal_id`, enable RLS, and install RPC helpers for the worker.

```sql
-- =========================
-- BASIC SCHEMA (idempotent)
-- =========================

-- Installations
create table if not exists public.installations (
  portal_id text primary key,
  access_token text,
  refresh_token text,
  expires_at timestamptz,
  updated_at timestamptz default now()
);

create unique index if not exists installations_portal_id_key on public.installations(portal_id);

-- Jobs
create table if not exists public.audit_jobs (
  job_id uuid primary key,
  portal_id text not null,
  object_type text not null check (object_type in ('contacts','companies','workflows')),
  status text not null default 'pending' check (status in ('pending','running','complete','failed')),
  progress_message text,
  processed_records bigint,
  total_records bigint,
  results jsonb,
  result_json jsonb,
  error text,
  created_at timestamptz default now(),
  claimed_at timestamptz,
  completed_at timestamptz,
  lease_expires_at timestamptz
);

create index if not exists audit_jobs_status_idx on public.audit_jobs(status);
create index if not exists audit_jobs_portal_idx on public.audit_jobs(portal_id);
create index if not exists audit_jobs_lease_idx on public.audit_jobs(lease_expires_at);

-- =========================
-- RLS (service role only)
-- =========================

alter table public.installations enable row level security;
alter table public.audit_jobs enable row level security;

drop policy if exists service_rw_installations on public.installations;
drop policy if exists service_rw_audit_jobs on public.audit_jobs;

create policy service_rw_installations on public.installations
  for all using (auth.role() = 'service_role') with check (auth.role() = 'service_role');

create policy service_rw_audit_jobs on public.audit_jobs
  for all using (auth.role() = 'service_role') with check (auth.role() = 'service_role');

-- =========================
-- RPCs
-- =========================

drop function if exists public.claim_audit_job(p_worker_id text);

create or replace function public.claim_audit_job(p_worker_id text)
returns public.audit_jobs
language plpgsql
security definer
as $$
declare
  v_job public.audit_jobs;
begin
  update public.audit_jobs
     set status = 'running',
         claimed_at = now(),
         lease_expires_at = now() + interval '20 minutes',
         progress_message = coalesce(progress_message, 'Running')
   where job_id in (
     select job_id
       from public.audit_jobs
      where (status = 'pending')
         or (status = 'running' and (lease_expires_at is null or lease_expires_at < now()))
      order by created_at asc
      for update skip locked
      limit 1
   )
  returning * into v_job;
  if v_job.job_id is null then
    return null;
  end if;
  return v_job;
end
$$;

drop function if exists public.update_audit_job_progress(p_job_id uuid, p_message text, p_processed bigint, p_total bigint);

create or replace function public.update_audit_job_progress(
  p_job_id uuid,
  p_message text,
  p_processed bigint,
  p_total bigint
) returns void
language sql
security definer
as $$
  update public.audit_jobs
     set progress_message = coalesce(p_message, progress_message),
         processed_records = coalesce(p_processed, processed_records),
         total_records = coalesce(p_total, total_records)
   where job_id = p_job_id;
$$;

drop function if exists public.extend_audit_job_lease(p_job_id uuid, p_minutes int);

create or replace function public.extend_audit_job_lease(p_job_id uuid, p_minutes int)
returns void
language sql
security definer
as $$
  update public.audit_jobs
     set lease_expires_at = now() + make_interval(mins => greatest(1, coalesce(p_minutes,15)))
   where job_id = p_job_id;
$$;
```

### 2. Render configuration

Configure both Render services (Web and Worker) with the same environment variables:

```
SUPABASE_URL=your_supabase_url
SUPABASE_SERVICE_KEY=your_service_role_key
HUBSPOT_CLIENT_ID=...
HUBSPOT_CLIENT_SECRET=...
HUBSPOT_REDIRECT_URI=https://auditpulse.onrender.com/api/oauth-callback
HUBSPOT_API_BASE=https://api.hubapi.com
CORS_ORIGIN=https://auditpulse.onrender.com
RENDER_EXTERNAL_URL=https://auditpulsepro-web.onrender.com
NODE_ENV=production
```

### 3. Deploy commands

* **Web service:** `node server.js`
* **Worker service:** `node worker.js`

### 4. Smoke flow after deploy

1. Visit `/api/install`, authorize the HubSpot app, and return to `/?portal_id=...`.
2. Click **Check** in the UI to verify installation (badge should show **Installed**).
3. Launch a Contacts, Companies, or Workflows audit and watch the progress badge update.
4. Download the Excel workbook once the job reports **Complete**.

The repository keeps the battle-tested scripts intact—no extra files required. Configure Supabase and Render as described above to obtain a fully functional production deployment.
