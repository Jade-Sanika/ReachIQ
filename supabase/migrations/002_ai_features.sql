alter table public.influencer_profiles
add column if not exists sample_video_transcript text;

create table if not exists public.ai_match_runs (
    id uuid primary key default gen_random_uuid(),
    campaign_id uuid not null references public.campaigns(id) on delete cascade,
    influencer_id uuid not null references public.profiles(id) on delete cascade,
    semantic_score numeric(6,4),
    weighted_score numeric(6,4),
    predicted_views integer,
    predicted_engagement_rate numeric(6,2),
    predicted_cpm numeric(10,2),
    conversion_likelihood numeric(6,2),
    pricing_assessment jsonb,
    created_at timestamptz not null default now()
);

create table if not exists public.deliverable_reviews (
    id uuid primary key default gen_random_uuid(),
    brand_id uuid not null references public.profiles(id) on delete cascade,
    video_url text,
    transcript text,
    review_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists public.offer_events (
    id uuid primary key default gen_random_uuid(),
    offer_id uuid not null references public.offers(id) on delete cascade,
    actor_id uuid references public.profiles(id) on delete set null,
    event_type text not null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists public.ai_recommendations (
    id uuid primary key default gen_random_uuid(),
    feature_name text not null,
    user_id uuid references public.profiles(id) on delete set null,
    entity_type text,
    entity_id text,
    payload jsonb not null default '{}'::jsonb,
    accepted boolean,
    outcome jsonb,
    created_at timestamptz not null default now()
);

alter table public.ai_match_runs enable row level security;
alter table public.deliverable_reviews enable row level security;
alter table public.offer_events enable row level security;
alter table public.ai_recommendations enable row level security;

create policy "brands view own ai match runs" on public.ai_match_runs
for select using (
    exists (
        select 1 from public.campaigns
        where campaigns.id = ai_match_runs.campaign_id
          and campaigns.brand_id = auth.uid()
    )
);

create policy "brands view own deliverable reviews" on public.deliverable_reviews
for select using (brand_id = auth.uid());

create policy "participants view offer events" on public.offer_events
for select using (
    exists (
        select 1
        from public.offers
        join public.campaigns on campaigns.id = offers.campaign_id
        where offers.id = offer_events.offer_id
          and (offers.influencer_id = auth.uid() or campaigns.brand_id = auth.uid())
    )
);

create policy "users view own ai recommendations" on public.ai_recommendations
for select using (user_id = auth.uid());
