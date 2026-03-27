-- ══════════════════════════════════════════════════
-- BLAKC Returns Manager — Supabase Schema
-- Run this in Supabase SQL Editor (once)
-- ══════════════════════════════════════════════════

-- 1. REQUESTS TABLE
create table if not exists requests (
  id                    uuid default gen_random_uuid() primary key,
  req_id                text unique not null,
  req_num               integer,
  order_id              text not null,
  order_number          text,
  items                 jsonb default '[]',
  refund_method         text,
  shipping_preference   text default 'pickup',
  status                text default 'pending',
  request_type          text default 'return',
  total_price           numeric default 0,
  address               jsonb,
  is_cod                boolean default false,
  days_since_order      integer default 0,

  -- Customer info
  customer_name         text,
  customer_email        text,
  customer_phone        text,
  customer_id           text,

  -- AWB / Tracking
  awb                   text,
  awb_status            text,
  awb_status_code       text,
  awb_last_scan         jsonb,
  awb_last_checked      timestamptz,
  awb_final             boolean default false,

  -- Exchange
  exchange_order_id     text,
  exchange_order_name   text,
  exchange_order_number text,
  exchange_shopify_name text,

  -- Refund
  refund_id             text,
  refund_amount         numeric,
  utr_number            text,

  -- Timestamps
  auto_action           text,
  submitted_at          timestamptz default now(),
  approved_at           timestamptz,
  pickup_created_at     timestamptz,
  archived_at           timestamptz,
  created_at            timestamptz default now(),

  -- Human-readable request ID (RET001, EXC001, MIX001 — global sequential)
  request_id            text
);

-- Index for fast lookups
create index if not exists idx_requests_order_id    on requests(order_id);
create index if not exists idx_requests_status      on requests(status);
create index if not exists idx_requests_awb         on requests(awb);
create index if not exists idx_requests_approved_at on requests(approved_at);

-- 2. EXCHANGE COUNTER  (#EXC9001, #EXC9002...)
create table if not exists exc_counter (
  id          integer primary key default 1,
  last_number integer default 9000
);
insert into exc_counter(id, last_number) values(1, 9000)
  on conflict(id) do nothing;

-- Function for atomic increment
create or replace function increment_exc_counter()
returns integer language plpgsql as $$
declare
  new_number integer;
begin
  update exc_counter
  set last_number = last_number + 1
  where id = 1
  returning last_number into new_number;
  return new_number;
end;
$$;

-- 3. AUDIT LOG
create table if not exists audit_log (
  id         uuid default gen_random_uuid() primary key,
  order_id   text,
  req_id     text,
  action     text,
  actor      text,
  details    text,
  created_at timestamptz default now()
);
create index if not exists idx_audit_order_id on audit_log(order_id);
create index if not exists idx_audit_created  on audit_log(created_at desc);

-- 4. SETTINGS  (key-value store)
create table if not exists settings (
  key        text primary key,
  value      jsonb,
  updated_at timestamptz default now()
);

-- Enable Row Level Security (but allow service key full access)
alter table requests  enable row level security;
alter table audit_log enable row level security;
alter table settings  enable row level security;
alter table exc_counter enable row level security;

-- Service role bypass (your SUPABASE_SERVICE_KEY bypasses RLS automatically)
-- These policies allow access via service key:
drop policy if exists "Service role full access on requests" on requests;
create policy "Service role full access on requests"
  on requests for all using (true) with check (true);

drop policy if exists "Service role full access on audit_log" on audit_log;
create policy "Service role full access on audit_log"
  on audit_log for all using (true) with check (true);

drop policy if exists "Service role full access on settings" on settings;
create policy "Service role full access on settings"
  on settings for all using (true) with check (true);

drop policy if exists "Service role full access on exc_counter" on exc_counter;
create policy "Service role full access on exc_counter"
  on exc_counter for all using (true) with check (true);

-- ══════════════════════════════════════════════════
-- PAYMENTS TABLE (Easebuzz exchange difference payments)
-- ══════════════════════════════════════════════════
create table if not exists payments (
  id           uuid default gen_random_uuid() primary key,
  txnid        text unique not null,
  order_id     text not null,
  amount       numeric(10,2) not null,
  status       text not null default 'pending',  -- pending | paid | failed | hash_mismatch
  txn_response jsonb,
  created_at   timestamptz default now()
);

create index if not exists payments_order_id_idx on payments(order_id);
create index if not exists payments_txnid_idx    on payments(txnid);

alter table payments enable row level security;
drop policy if exists "Service role full access on payments" on payments;
create policy "Service role full access on payments"
  on payments for all using (true) with check (true);
