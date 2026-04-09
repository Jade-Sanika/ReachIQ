alter table public.campaigns
add column if not exists milestone_views integer default 0,
add column if not exists timeline_requirements text,
add column if not exists currency text default 'INR',
add column if not exists escrow_enabled boolean default true;

alter table public.offers
add column if not exists deliverable_url text,
add column if not exists deliverable_status text default 'not_submitted',
add column if not exists deliverable_submitted_at timestamptz,
add column if not exists review_notes text,
add column if not exists payment_status text default 'escrow_pending',
add column if not exists payment_amount numeric(12,2),
add column if not exists payment_released_at timestamptz;
