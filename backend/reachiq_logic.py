import math
import re
from collections import Counter


def parse_yt_duration(duration_str):
    """Convert YouTube ISO 8601 durations into a human-readable clock format."""
    hours = re.search(r'(\d+)H', duration_str or '')
    minutes = re.search(r'(\d+)M', duration_str or '')
    seconds = re.search(r'(\d+)S', duration_str or '')

    h = int(hours.group(1)) if hours else 0
    m = int(minutes.group(1)) if minutes else 0
    s = int(seconds.group(1)) if seconds else 0

    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def calculate_rate_range(total_views, video_count, engagement_rate, niche):
    """Calculate fair creator pricing bounds based on views, engagement, and niche."""
    avg_views = total_views / video_count if video_count > 0 else 0
    base_rate = (avg_views / 1000) * 20

    premium_niches = {'finance', 'business', 'tech', 'education'}
    niche_multiplier = 1.4 if (niche or '').lower() in premium_niches else 1.0

    eng_multiplier = 1.0
    if engagement_rate and engagement_rate > 5.0:
        eng_multiplier = 1.3
    elif engagement_rate and engagement_rate > 3.0:
        eng_multiplier = 1.1

    recommended_rate = max(50, base_rate * niche_multiplier * eng_multiplier)
    return {
        'min_rate': round(recommended_rate * 0.8),
        'recommended_rate': round(recommended_rate),
        'max_rate': round(recommended_rate * 1.5),
    }


def normalize_text(value):
    """Normalize free-form text for lightweight semantic comparisons."""
    lowered = (value or '').lower()
    lowered = re.sub(r'[^a-z0-9\s]', ' ', lowered)
    return re.sub(r'\s+', ' ', lowered).strip()


def tokenize_text(value):
    """Return normalized tokens from a text blob."""
    return [token for token in normalize_text(value).split(' ') if token]


def cosine_similarity(vector_a, vector_b):
    """Compute cosine similarity between two vectors."""
    if not vector_a or not vector_b or len(vector_a) != len(vector_b):
        return 0.0

    dot_product = sum(a * b for a, b in zip(vector_a, vector_b))
    magnitude_a = math.sqrt(sum(a * a for a in vector_a))
    magnitude_b = math.sqrt(sum(b * b for b in vector_b))
    if magnitude_a == 0 or magnitude_b == 0:
        return 0.0
    return dot_product / (magnitude_a * magnitude_b)


def local_semantic_similarity(text_a, text_b):
    """Fallback semantic similarity using token frequency vectors."""
    tokens_a = tokenize_text(text_a)
    tokens_b = tokenize_text(text_b)
    if not tokens_a or not tokens_b:
        return 0.0

    vocabulary = sorted(set(tokens_a) | set(tokens_b))
    counter_a = Counter(tokens_a)
    counter_b = Counter(tokens_b)
    vector_a = [counter_a.get(token, 0) for token in vocabulary]
    vector_b = [counter_b.get(token, 0) for token in vocabulary]
    return cosine_similarity(vector_a, vector_b)


def build_campaign_matching_text(campaign):
    """Build a rich text blob representing campaign intent."""
    return ' '.join(
        filter(
            None,
            [
                campaign.get('name'),
                campaign.get('brief_text'),
                campaign.get('target_audience'),
                campaign.get('goals'),
                campaign.get('platforms'),
                campaign.get('budget_range'),
            ],
        )
    )


def build_creator_matching_text(influencer_profile):
    """Build a rich text blob representing creator identity and audience fit."""
    return ' '.join(
        filter(
            None,
            [
                influencer_profile.get('niche'),
                influencer_profile.get('bio'),
                influencer_profile.get('content_description'),
                influencer_profile.get('audience_interests'),
                influencer_profile.get('audience_age'),
                influencer_profile.get('audience_gender'),
                influencer_profile.get('audience_location'),
                influencer_profile.get('channel_description'),
                influencer_profile.get('sample_video_transcript'),
                influencer_profile.get('platform'),
                influencer_profile.get('availability'),
            ],
        )
    )


def parse_budget_value(range_text):
    """Parse a currency range string into a representative midpoint value."""
    if not range_text:
        return None

    numbers = [
        int(match.replace(',', ''))
        for match in re.findall(r'(\d[\d,]*)', str(range_text))
    ]
    if not numbers:
        return None
    if len(numbers) == 1:
        return float(numbers[0])
    return float(sum(numbers[:2]) / 2)


def score_platform_alignment(campaign_platforms, creator_platform):
    """Score how well the requested platforms match the creator's main platform."""
    if not campaign_platforms or not creator_platform:
        return 0.5

    platforms = [part.strip().lower() for part in str(campaign_platforms).split(',') if part.strip()]
    return 1.0 if creator_platform.lower() in platforms else 0.0


def score_budget_alignment(campaign_budget, creator_rate):
    """Score budget compatibility between campaign and creator pricing."""
    campaign_value = parse_budget_value(campaign_budget)
    creator_value = parse_budget_value(creator_rate)

    if campaign_value is None or creator_value is None:
        return 0.5

    relative_gap = abs(campaign_value - creator_value) / max(campaign_value, creator_value, 1)
    return max(0.0, 1.0 - relative_gap)


def score_audience_alignment(campaign, influencer_profile):
    """Score alignment between target audience and creator audience descriptors."""
    campaign_audience = ' '.join(
        filter(None, [campaign.get('target_audience'), campaign.get('goals')])
    )
    creator_audience = ' '.join(
        filter(
            None,
            [
                influencer_profile.get('audience_age'),
                influencer_profile.get('audience_gender'),
                influencer_profile.get('audience_location'),
                influencer_profile.get('audience_interests'),
                influencer_profile.get('bio'),
            ],
        )
    )
    if not campaign_audience or not creator_audience:
        return 0.45
    return min(1.0, local_semantic_similarity(campaign_audience, creator_audience) * 1.35)


def score_creator_quality(influencer_profile):
    """Score creator quality using audience scale and engagement."""
    followers = influencer_profile.get('follower_count') or 0
    engagement_rate = influencer_profile.get('engagement_rate') or 0

    follower_score = 0.25
    if followers >= 250000:
        follower_score = 1.0
    elif followers >= 100000:
        follower_score = 0.85
    elif followers >= 25000:
        follower_score = 0.7
    elif followers >= 5000:
        follower_score = 0.55
    elif followers >= 1000:
        follower_score = 0.4

    engagement_score = 0.25
    if engagement_rate >= 8:
        engagement_score = 1.0
    elif engagement_rate >= 5:
        engagement_score = 0.85
    elif engagement_rate >= 3:
        engagement_score = 0.65
    elif engagement_rate >= 1.5:
        engagement_score = 0.45

    return round((follower_score * 0.55) + (engagement_score * 0.45), 4)


def calculate_enhanced_match_score(campaign, influencer_profile, semantic_similarity=None):
    """Weighted semantic match score for campaign-to-creator ranking."""
    campaign_text = build_campaign_matching_text(campaign)
    creator_text = build_creator_matching_text(influencer_profile)
    semantic_score = (
        semantic_similarity
        if semantic_similarity is not None
        else local_semantic_similarity(campaign_text, creator_text)
    )

    platform_score = score_platform_alignment(campaign.get('platforms'), influencer_profile.get('platform'))
    budget_score = score_budget_alignment(campaign.get('budget_range'), influencer_profile.get('rate_range'))
    audience_score = score_audience_alignment(campaign, influencer_profile)
    quality_score = score_creator_quality(influencer_profile)

    availability = (influencer_profile.get('availability') or 'available').lower()
    availability_score = 1.0 if availability == 'available' else 0.6 if availability == 'busy' else 0.2

    weighted_score = (
        (semantic_score * 0.42)
        + (platform_score * 0.13)
        + (budget_score * 0.1)
        + (audience_score * 0.15)
        + (quality_score * 0.15)
        + (availability_score * 0.05)
    )
    return round(min(max(weighted_score, 0.0), 1.0), 4)


def assess_pricing_fairness(predicted_views, creator_rate, engagement_rate):
    """Assess whether a creator looks underpriced, fairly priced, or overpriced."""
    asking_rate = parse_budget_value(creator_rate)
    if asking_rate is None:
        return {
            'label': 'unknown',
            'score': 50,
            'summary': 'Not enough pricing data to assess value.',
        }

    expected_rate = max(50, (predicted_views / 1000) * (16 + min(engagement_rate or 0, 10)))
    ratio = asking_rate / expected_rate if expected_rate else 1

    if ratio <= 0.8:
        return {
            'label': 'underpriced',
            'score': 88,
            'summary': 'The creator appears underpriced versus expected reach.',
        }
    if ratio <= 1.15:
        return {
            'label': 'fair',
            'score': 72,
            'summary': 'The creator rate looks fair for the expected performance.',
        }
    return {
        'label': 'overpriced',
        'score': 42,
        'summary': 'The creator rate is high relative to projected reach.',
    }


def predict_campaign_performance(campaign, influencer_profile, semantic_similarity=None, match_score=None):
    """Estimate campaign outcomes for a creator using available campaign/profile signals."""
    semantic_score = (
        semantic_similarity
        if semantic_similarity is not None
        else local_semantic_similarity(
            build_campaign_matching_text(campaign),
            build_creator_matching_text(influencer_profile),
        )
    )
    match_score = (
        match_score
        if match_score is not None
        else calculate_enhanced_match_score(campaign, influencer_profile, semantic_similarity=semantic_score)
    )

    follower_count = influencer_profile.get('follower_count') or 0
    total_views = influencer_profile.get('total_views') or 0
    video_count = influencer_profile.get('video_count') or 0
    engagement_rate = float(influencer_profile.get('engagement_rate') or 0)

    historical_avg_views = total_views / video_count if video_count else max(follower_count * 0.16, 0)
    predicted_views = historical_avg_views * (0.72 + (semantic_score * 0.48) + (match_score * 0.22))
    predicted_views = round(max(predicted_views, 100))

    predicted_engagement_rate = max(0.8, engagement_rate * (0.85 + (semantic_score * 0.25)))
    predicted_engagement_rate = round(min(predicted_engagement_rate, 14.0), 2)

    creator_rate = influencer_profile.get('rate_range') or campaign.get('budget_range')
    asking_rate = parse_budget_value(creator_rate) or parse_budget_value(campaign.get('budget_range')) or 1000
    predicted_cpm = round((asking_rate / max(predicted_views, 1)) * 1000, 2)

    conversion_likelihood = round(
        min(95.0, max(6.0, (match_score * 68) + (predicted_engagement_rate * 2.8))),
        1,
    )

    pricing = assess_pricing_fairness(predicted_views, creator_rate, predicted_engagement_rate)
    value_score = round(
        min(
            100.0,
            max(
                1.0,
                (match_score * 55)
                + (semantic_score * 20)
                + (predicted_engagement_rate * 2.4)
                + ((100 - min(predicted_cpm, 100)) * 0.12)
                + (pricing['score'] * 0.08),
            ),
        ),
        1,
    )

    return {
        'predicted_views': predicted_views,
        'predicted_engagement_rate': predicted_engagement_rate,
        'predicted_cpm': predicted_cpm,
        'conversion_likelihood': conversion_likelihood,
        'value_score': value_score,
        'pricing_fairness': pricing,
    }


def summarize_match_reasons(campaign, influencer_profile, semantic_similarity=None, prediction=None):
    """Generate short deterministic reasons explaining why a creator is a fit."""
    reasons = []
    if semantic_similarity is None:
        semantic_similarity = local_semantic_similarity(
            build_campaign_matching_text(campaign),
            build_creator_matching_text(influencer_profile),
        )
    prediction = prediction or {}

    if semantic_similarity >= 0.45:
        reasons.append('Strong semantic fit between campaign brief and creator profile.')
    if score_platform_alignment(campaign.get('platforms'), influencer_profile.get('platform')) >= 1:
        reasons.append(f"Platform alignment on {influencer_profile.get('platform', 'the requested platform')}.")
    if score_budget_alignment(campaign.get('budget_range'), influencer_profile.get('rate_range')) >= 0.7:
        reasons.append('Creator pricing is close to the campaign budget.')
    if score_audience_alignment(campaign, influencer_profile) >= 0.45:
        reasons.append('Audience signals line up with the target market.')
    if (influencer_profile.get('engagement_rate') or 0) >= 4:
        reasons.append('Above-average engagement suggests strong audience response.')

    pricing = prediction.get('pricing_fairness', {})
    if pricing.get('label') == 'underpriced':
        reasons.append('This creator looks like a strong value buy for projected reach.')

    return reasons[:3] or ['Relevant niche and creator profile match the campaign brief.']
