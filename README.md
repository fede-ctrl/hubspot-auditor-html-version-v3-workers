# AuditPulse Minimal Working Setup

AuditPulse is a Render + Supabase + HubSpot stack that runs long running CRM audits in a background worker and exposes the progress through a lightweight web SPA.

This repository contains the production-ready minimal setup for the worker/web pair. Follow the steps below to provision Supabase, configure Render, and deploy both services.

## 1. Supabase schema

Run the following SQL *once* in the Supabase SQL editor. It normalizes the schema to a single `portal_id` column, adds indexes, enables row level security (RLS), and installs the RPC helpers that the worker relies on.

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

## 2. Render configuration

Configure both Render services (Web and Worker) with the exact same environment variables:

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

## 3. Deploy commands

* **Web service** start command: `node server.js`
* **Worker service** start command: `node worker.js`

After deployment:

1. Visit `/api/install` on the web service to authorize the HubSpot app.
2. You will be redirected back to `/?portal_id=...`.
3. Use the UI to verify installation status, run an audit, and download the Excel export when complete.

This README intentionally keeps the instructions tight so you can deploy the minimal production setup without adding new files or deviating from the battle-tested scripts in this repository.
