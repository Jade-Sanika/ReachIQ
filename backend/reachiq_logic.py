import math
import math
import re
import unicodedata
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
    normalized = unicodedata.normalize('NFKD', str(value or ''))
    lowered = normalized.encode('ascii', 'ignore').decode('ascii').lower()
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


def _stringify_matching_value(value):
    """Normalize mixed campaign/profile fields into matching-safe text."""
    if value in (None, ''):
        return ''
    if isinstance(value, (list, tuple, set)):
        return ' '.join(str(item).strip() for item in value if str(item).strip())
    return str(value).strip()


def build_campaign_matching_text(campaign):
    """Build a rich text blob representing campaign intent."""
    return ' '.join(
        part for part in [
            _stringify_matching_value(campaign.get('name')),
            _stringify_matching_value(campaign.get('brief_text')),
            _stringify_matching_value(campaign.get('target_audience')),
            _stringify_matching_value(campaign.get('goals')),
            _stringify_matching_value(campaign.get('platforms')),
            _stringify_matching_value(campaign.get('budget_range')),
            _stringify_matching_value(campaign.get('milestone_views_target')),
            _stringify_matching_value(campaign.get('milestone_views')),
            _stringify_matching_value(campaign.get('timeline')),
            _stringify_matching_value(campaign.get('timeline_requirements')),
        ] if part
    )


def build_creator_matching_text(influencer_profile):
    """Build a rich text blob representing creator identity and audience fit."""
    return ' '.join(
        part for part in [
            _stringify_matching_value(influencer_profile.get('niche')),
            _stringify_matching_value(influencer_profile.get('bio')),
            _stringify_matching_value(influencer_profile.get('content_description')),
            _stringify_matching_value(influencer_profile.get('audience_interests')),
            _stringify_matching_value(influencer_profile.get('audience_age')),
            _stringify_matching_value(influencer_profile.get('audience_gender')),
            _stringify_matching_value(influencer_profile.get('audience_location')),
            _stringify_matching_value(influencer_profile.get('channel_description')),
            _stringify_matching_value(influencer_profile.get('sample_video_transcript')),
            _stringify_matching_value(influencer_profile.get('platform')),
            _stringify_matching_value(influencer_profile.get('availability')),
        ] if part
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


def clamp01(value):
    """Clamp any numeric value to the 0-1 range."""
    return min(1.0, max(0.0, float(value or 0.0)))


def safe_float(value, default=0.0):
    """Safely coerce values into floats."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def normalize_metric(value, minimum, maximum, default=0.5):
    """Min-max normalize a value while handling flat or missing cohorts."""
    if value is None:
        return float(default)
    minimum = safe_float(minimum, value)
    maximum = safe_float(maximum, value)
    value = safe_float(value, default)
    if maximum <= minimum:
        return float(default)
    return clamp01((value - minimum) / max(maximum - minimum, 1e-9))


def _parse_platforms(value):
    """Normalize campaign platform input into a lowercase list."""
    if isinstance(value, (list, tuple, set)):
        return [normalize_text(item) for item in value if normalize_text(item)]
    return [part.strip().lower() for part in str(value or '').split(',') if part.strip()]


def fallback_creator_rate(campaign, influencer_profile):
    """Estimate an asking price when explicit rate data is missing."""
    explicit_rate = parse_budget_value(
        influencer_profile.get('asking_price')
        or influencer_profile.get('rate')
        or influencer_profile.get('rate_range')
    )
    if explicit_rate is not None:
        return explicit_rate

    campaign_budget = parse_budget_value(campaign.get('budget_range'))
    if campaign_budget is not None:
        return campaign_budget

    niche = normalize_text(influencer_profile.get('niche') or campaign.get('niche') or '')
    default_rates = {
        'beauty': 120000.0,
        'fashion': 115000.0,
        'fitness': 105000.0,
        'tech': 140000.0,
        'food': 95000.0,
        'travel': 110000.0,
        'gaming': 125000.0,
        'education': 100000.0,
        'business': 135000.0,
        'sports': 115000.0,
        'lifestyle': 90000.0,
    }
    return default_rates.get(niche, 85000.0)


def compute_roi_value(predicted_views, creator_rate, campaign=None, influencer_profile=None):
    """Compute raw ROI as expected views per unit of asking price."""
    asking_price = parse_budget_value(creator_rate)
    if asking_price is None:
        asking_price = fallback_creator_rate(campaign or {}, influencer_profile or {})
    return safe_float(predicted_views, 0.0) / max(safe_float(asking_price, 0.0), 1.0)


def engagement_score_from_rate(engagement_rate):
    """Continuously score engagement with a conservative default when missing."""
    rate = safe_float(engagement_rate, 3.5)
    if rate <= 0:
        rate = 3.5
    return clamp01(rate / 10.0)


def _extract_age_range(text):
    """Extract a simple age range from free-form audience text."""
    normalized = normalize_text(text)
    if not normalized:
        return None

    explicit_ranges = re.findall(r'(\d{1,2})\s*(?:-|to)\s*(\d{1,2})', normalized)
    if explicit_ranges:
        low = min(int(start) for start, _ in explicit_ranges)
        high = max(int(end) for _, end in explicit_ranges)
        return low, high

    ages = [int(match) for match in re.findall(r'\b([1-6]\d)\b', normalized)]
    if ages:
        return min(ages), max(ages)
    return None


def _score_age_alignment(campaign_text, creator_text):
    """Score overlap between campaign and creator audience age ranges."""
    campaign_range = _extract_age_range(campaign_text)
    creator_range = _extract_age_range(creator_text)
    if not campaign_range or not creator_range:
        return 0.5

    overlap_start = max(campaign_range[0], creator_range[0])
    overlap_end = min(campaign_range[1], creator_range[1])
    if overlap_end < overlap_start:
        return 0.15

    overlap = overlap_end - overlap_start + 1
    union = max(campaign_range[1], creator_range[1]) - min(campaign_range[0], creator_range[0]) + 1
    return clamp01(overlap / max(union, 1))


def _extract_gender_hint(text):
    """Extract broad gender targeting hints from text."""
    normalized = normalize_text(text)
    if not normalized:
        return ''
    if any(token in normalized for token in ('women', 'woman', 'female', 'girls', 'girl')):
        return 'female'
    if any(token in normalized for token in ('men', 'man', 'male', 'boys', 'boy')):
        return 'male'
    if any(token in normalized for token in ('all genders', 'everyone', 'general audience', 'all')):
        return 'all'
    return ''


def _score_gender_alignment(campaign_text, creator_gender_text):
    """Score how well campaign gender intent aligns with creator audience gender."""
    campaign_gender = _extract_gender_hint(campaign_text)
    creator_gender = _extract_gender_hint(creator_gender_text)

    if not campaign_gender or not creator_gender:
        return 0.5
    if campaign_gender == 'all' or creator_gender == 'all':
        return 0.75
    if campaign_gender == creator_gender:
        return 1.0
    return 0.2


def _score_location_alignment(campaign_text, creator_location_text):
    """Score whether location hints appear aligned."""
    campaign_location = normalize_text(campaign_text)
    creator_location = normalize_text(creator_location_text)
    if not campaign_location or not creator_location:
        return 0.5

    creator_parts = [part.strip() for part in re.split(r'[,/|-]', creator_location) if part.strip()]
    if any(part and part in campaign_location for part in creator_parts):
        return 1.0
    return 0.4


def score_follower_scale(followers, minimum=None, maximum=None):
    """Score followers continuously using log scaling."""
    follower_value = max(safe_float(followers, 0.0), 0.0)
    log_value = math.log1p(follower_value)
    if minimum is not None and maximum is not None:
        return normalize_metric(log_value, math.log1p(max(minimum, 0.0)), math.log1p(max(maximum, 0.0)), default=0.5)
    return normalize_metric(log_value, math.log1p(1000), math.log1p(10000000), default=0.5)


def score_platform_alignment(campaign_platforms, creator_platform):
    """Score exact, related, or weak platform fit."""
    creator = normalize_text(creator_platform)
    platforms = _parse_platforms(campaign_platforms)
    if not platforms or not creator:
        return 0.4

    if creator in platforms:
        return 1.0

    related_map = {
        'instagram': {'tiktok', 'youtube'},
        'youtube': {'instagram', 'tiktok'},
        'tiktok': {'instagram', 'youtube'},
        'twitter': {'instagram'},
    }
    if any(creator in related_map.get(platform, set()) for platform in platforms):
        return 0.7
    return 0.4


def score_budget_alignment(campaign_budget, creator_rate):
    """Score budget compatibility between campaign and creator pricing."""
    campaign_value = parse_budget_value(campaign_budget)
    creator_value = parse_budget_value(creator_rate)

    if campaign_value is None or creator_value is None:
        return 0.5

    relative_gap = abs(campaign_value - creator_value) / max(campaign_value, creator_value, 1)
    return max(0.0, 1.0 - relative_gap)


def score_audience_alignment(campaign, influencer_profile):
    """Score audience alignment from age, gender, and location with safe fallbacks."""
    campaign_text = ' '.join(
        filter(None, [campaign.get('target_audience'), campaign.get('brief_text'), campaign.get('goals')])
    )
    creator_age = influencer_profile.get('audience_age')
    creator_gender = influencer_profile.get('audience_gender')
    creator_location = influencer_profile.get('audience_location')

    age_score = _score_age_alignment(campaign_text, creator_age)
    gender_score = _score_gender_alignment(campaign_text, creator_gender)
    location_score = _score_location_alignment(campaign_text, creator_location)

    if not creator_age and not creator_gender and not creator_location:
        return 0.5

    return round(
        clamp01((age_score * 0.45) + (gender_score * 0.25) + (location_score * 0.30)),
        4,
    )


def score_creator_quality(influencer_profile):
    """Score creator content quality from consistency, performance, and completion history."""
    precomputed = influencer_profile.get('content_quality_score')
    if precomputed not in (None, ''):
        return round(clamp01(precomputed), 4)

    video_count = max(safe_float(influencer_profile.get('video_count'), 0.0), 0.0)
    follower_count = max(safe_float(influencer_profile.get('follower_count'), 0.0), 1.0)
    total_views = max(safe_float(influencer_profile.get('total_views'), 0.0), 0.0)
    avg_views = safe_float(influencer_profile.get('average_views'), 0.0)
    if avg_views <= 0 and video_count > 0:
        avg_views = total_views / max(video_count, 1.0)

    consistency_score = clamp01(video_count / 60.0)
    performance_ratio = avg_views / max(follower_count, 1.0)
    performance_score = clamp01(performance_ratio / 1.2)

    completion_rate = influencer_profile.get('completion_rate')
    if completion_rate in (None, ''):
        completion_rate = influencer_profile.get('past_completion_rate')
    completion_rate = safe_float(completion_rate, 0.6)
    if completion_rate > 1:
        completion_rate /= 100.0
    completion_score = clamp01(completion_rate)

    return round(
        clamp01((consistency_score * 0.30) + (performance_score * 0.45) + (completion_score * 0.25)),
        4,
    )


def calculate_enhanced_match_score(campaign, influencer_profile, semantic_similarity=None):
    """Weighted creator-campaign match score using the normalized ranking inputs."""
    campaign_text = build_campaign_matching_text(campaign)
    creator_text = build_creator_matching_text(influencer_profile)
    semantic_score = (
        semantic_similarity
        if semantic_similarity is not None
        else local_semantic_similarity(campaign_text, creator_text)
    )

    roi_score = influencer_profile.get('roi_score')
    if roi_score in (None, ''):
        roi_score = 0.5

    engagement_score = influencer_profile.get('engagement_score')
    if engagement_score in (None, ''):
        engagement_score = engagement_score_from_rate(influencer_profile.get('engagement_rate'))

    audience_score = influencer_profile.get('audience_match_score')
    if audience_score in (None, ''):
        audience_score = score_audience_alignment(campaign, influencer_profile)

    follower_score = influencer_profile.get('follower_score')
    if follower_score in (None, ''):
        follower_score = score_follower_scale(influencer_profile.get('follower_count'))

    content_quality_score = influencer_profile.get('content_quality_score')
    if content_quality_score in (None, ''):
        content_quality_score = score_creator_quality(influencer_profile)

    platform_score = influencer_profile.get('platform_match_score')
    if platform_score in (None, ''):
        platform_score = score_platform_alignment(campaign.get('platforms'), influencer_profile.get('platform'))

    weighted_score = (
        (semantic_score * 0.25)
        + (safe_float(roi_score, 0.5) * 0.20)
        + (safe_float(engagement_score, 0.35) * 0.15)
        + (safe_float(audience_score, 0.5) * 0.15)
        + (safe_float(follower_score, 0.5) * 0.10)
        + (safe_float(content_quality_score, 0.5) * 0.10)
        + (safe_float(platform_score, 0.4) * 0.05)
    )

    niche_tier = influencer_profile.get('niche_tier')
    if niche_tier == 1:
        weighted_score *= 1.10

    engagement_rate = safe_float(influencer_profile.get('engagement_rate'), 3.5)
    if engagement_rate > 6 and safe_float(roi_score, 0.0) >= 0.7:
        weighted_score *= 1.08

    pricing_label = (influencer_profile.get('prediction', {}) or {}).get('pricing_fairness', {}).get('label')
    if safe_float(roi_score, 0.0) < 0.35 or pricing_label == 'overpriced':
        weighted_score *= 0.90
    if engagement_rate < 1.5:
        weighted_score *= 0.92

    return round(clamp01(weighted_score), 4)


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

    if influencer_profile.get('niche_tier') == 1:
        reasons.append('Same niche as the campaign, so relevance is especially strong.')
    elif influencer_profile.get('niche_tier') == 2:
        reasons.append('Related niche fit keeps the creator relevant to the campaign.')

    if semantic_similarity >= 0.45:
        reasons.append('Strong semantic fit between campaign brief and creator profile.')
    if score_platform_alignment(campaign.get('platforms'), influencer_profile.get('platform')) >= 1:
        reasons.append(f"Platform alignment on {influencer_profile.get('platform', 'the requested platform')}.")
    if influencer_profile.get('roi_score', 0) >= 0.7:
        reasons.append('Projected ROI is strong relative to the creator asking price.')
    if score_audience_alignment(campaign, influencer_profile) >= 0.55:
        reasons.append('Audience signals line up with the target market.')
    if engagement_score_from_rate(influencer_profile.get('engagement_rate')) >= 0.6:
        reasons.append('Above-average engagement suggests strong audience response.')

    pricing = prediction.get('pricing_fairness', {})
    if pricing.get('label') == 'underpriced':
        reasons.append('This creator looks like a strong value buy for projected reach.')

    return reasons[:3] or ['Relevant niche and creator profile match the campaign brief.']
