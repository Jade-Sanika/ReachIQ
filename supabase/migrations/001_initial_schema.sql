create extension if not exists "pgcrypto";

create table if not exists public.profiles (
    id uuid primary key,
    username text unique,
    full_name text not null,
    role text not null check (role in ('brand', 'influencer')),
    avatar_url text,
    created_at timestamptz not null default now()
);

create table if not exists public.influencer_profiles (
    profile_id uuid primary key references public.profiles(id) on delete cascade,
    platform text default 'youtube',
    youtube_channel text,
    follower_count bigint default 0,
    total_views bigint default 0,
    video_count integer default 0,
    engagement_rate numeric(6,2) default 0,
    niche text default 'lifestyle',
    bio text,
    content_description text,
    rate_range text,
    location text,
    languages text[] default '{}',
    audience_age text,
    audience_gender text,
    audience_location text,
    audience_interests text,
    availability text default 'Available',
    channel_description text,
    instagram_handle text,
    twitter_handle text,
    tiktok_handle text,
    website_url text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.campaigns (
    id uuid primary key default gen_random_uuid(),
    brand_id uuid not null references public.profiles(id) on delete cascade,
    name text not null,
    brief_text text not null,
    target_audience text,
    goals text,
    budget_range text,
    status text not null default 'draft',
    deadline date,
    platforms text,
    created_at timestamptz not null default now()
);

create table if not exists public.offers (
    id uuid primary key default gen_random_uuid(),
    campaign_id uuid not null references public.campaigns(id) on delete cascade,
    influencer_id uuid not null references public.profiles(id) on delete cascade,
    status text not null default 'pending' check (status in ('pending', 'negotiating', 'accepted', 'rejected')),
    brand_notes text,
    brand_budget_range text,
    influencer_quote numeric(10,2),
    negotiated_amount numeric(10,2),
    created_at timestamptz not null default now()
);

alter table public.profiles enable row level security;
alter table public.influencer_profiles enable row level security;
alter table public.campaigns enable row level security;
alter table public.offers enable row level security;

create policy "profiles self read" on public.profiles
for select using (auth.uid() = id);

create policy "profiles self update" on public.profiles
for update using (auth.uid() = id);

create policy "influencer profile self read" on public.influencer_profiles
for select using (auth.uid() = profile_id);

create policy "influencer profile self update" on public.influencer_profiles
for update using (auth.uid() = profile_id);

create policy "brands manage own campaigns" on public.campaigns
for all using (auth.uid() = brand_id) with check (auth.uid() = brand_id);

create policy "influencers view campaigns tied to their offers" on public.campaigns
for select using (
    exists (
        select 1 from public.offers
        where offers.campaign_id = campaigns.id
          and offers.influencer_id = auth.uid()
    )
);

create policy "brands manage offers for own campaigns" on public.offers
for all using (
    exists (
        select 1 from public.campaigns
        where campaigns.id = offers.campaign_id
          and campaigns.brand_id = auth.uid()
    )
)
with check (
    exists (
        select 1 from public.campaigns
        where campaigns.id = offers.campaign_id
          and campaigns.brand_id = auth.uid()
    )
);

create policy "influencers view and update own offers" on public.offers
for select using (auth.uid() = influencer_id);

create policy "influencers update own offers" on public.offers
for update using (auth.uid() = influencer_id);
