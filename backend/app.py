from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from supabase import create_client, Client
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import fitz  # PyMuPDF
import docx  # python-docx
import json
import google.generativeai as genai
import mimetypes
import requests
import urllib.parse as urlparse
from urllib.parse import parse_qs
import re
from reachiq_logic import (
    parse_yt_duration as format_youtube_duration,
    calculate_rate_range,
    calculate_enhanced_match_score as compute_match_score,
    build_campaign_matching_text,
    build_creator_matching_text,
    cosine_similarity,
    local_semantic_similarity,
    normalize_text,
    predict_campaign_performance,
    score_creator_quality,
    summarize_match_reasons,
    parse_budget_value,
)

# Load environment variables
load_dotenv()

app = Flask(__name__, static_folder='../frontend', static_url_path='')
CORS(app)

# Get Supabase credentials from environment
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_KEY')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY') # <-- NEW: YouTube Key


if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise ValueError("Supabase credentials not found in environment variables")

print(f"Supabase URL: {SUPABASE_URL}")
print("Supabase credentials loaded successfully")

# Initialize Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
LEGACY_USD_TO_INR = float(os.getenv('LEGACY_USD_TO_INR', '83'))
CAMPAIGN_META_PREFIX = '__REACHIQ_CAMPAIGN_META__::'
OFFER_META_PREFIX = '__REACHIQ_OFFER_META__::'

# --- NEW: Configure Gemini API Key ---
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in environment variables")
genai.configure(api_key=GEMINI_API_KEY)

def get_current_user():
    """Get current user from Supabase session"""
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return None
    
    token = auth_header.replace('Bearer ', '')
    
    try:
        # Use Supabase client to get the user from token
        response = supabase.auth.get_user(token)
        if response.user:
            return response.user.id
    except Exception as e:
        print(f"Error getting user from token: {e}")
    
    return None


def get_current_profile(user_id):
    """Fetch the current user's profile row."""
    profile_response = supabase.table('profiles').select('*').eq('id', user_id).single().execute()
    return profile_response.data


def get_offer_with_campaign(offer_id):
    """Fetch an offer together with its campaign."""
    offer_response = supabase.table('offers').select('*').eq('id', offer_id).single().execute()
    offer = offer_response.data
    if not offer:
        return None, None

    campaign_response = supabase.table('campaigns').select('*').eq('id', offer['campaign_id']).single().execute()
    return offer, campaign_response.data


def can_access_offer(user_id, profile, offer, campaign):
    """Return whether the current user is allowed to update/view the offer."""
    if not profile or not offer or not campaign:
        return False

    if profile.get('role') == 'brand':
        return campaign.get('brand_id') == user_id

    if profile.get('role') == 'influencer':
        return offer.get('influencer_id') == user_id

    return False


def serialize_brand_offer(offer, campaign, influencer):
    """Shape offer data for brand-facing pages."""
    events = get_offer_events(offer.get('id'))
    campaign_state = get_campaign_runtime_state(campaign)
    offer_state = get_offer_runtime_state(offer)
    resolved_campaign = {**campaign, **campaign_state}
    return {
        'id': offer.get('id'),
        'status': offer.get('status', 'pending'),
        'created_at': offer.get('created_at'),
        'brand_notes': offer_state.get('brand_notes', ''),
        'brand_budget_range': normalize_currency_text(offer.get('brand_budget_range', '')),
        'influencer_quote': normalize_offer_amount(offer.get('influencer_quote'), resolved_campaign),
        'negotiated_amount': normalize_offer_amount(offer.get('negotiated_amount'), resolved_campaign),
        'deliverable_url': offer_state.get('deliverable_url'),
        'deliverable_status': offer_state.get('deliverable_status', 'not_submitted'),
        'deliverable_submitted_at': offer_state.get('deliverable_submitted_at'),
        'review_notes': offer_state.get('review_notes', ''),
        'payment_status': offer_state.get('payment_status', 'escrow_pending'),
        'payment_amount': normalize_offer_amount(offer_state.get('payment_amount'), resolved_campaign),
        'payment_released_at': offer_state.get('payment_released_at'),
        'negotiation_summary': build_negotiation_summary({**offer, **offer_state}, resolved_campaign, events),
        'timeline': build_offer_timeline({**offer, **offer_state}, resolved_campaign, events),
        'campaigns': {
            'id': campaign.get('id'),
            'name': campaign.get('name'),
            'brief_text': campaign.get('brief_text'),
            'target_audience': campaign.get('target_audience'),
            'niche': campaign_state.get('niche', ''),
            'budget_range': normalize_currency_text(campaign.get('budget_range')),
            'status': campaign.get('status'),
            'milestone_views': campaign_state.get('milestone_views'),
            'timeline_requirements': campaign_state.get('timeline_requirements'),
            'currency': campaign_state.get('currency', 'INR'),
        },
        'influencer': {
            'id': influencer.get('id') if influencer else None,
            'full_name': influencer.get('full_name') if influencer else 'Unknown Creator',
            'username': influencer.get('username') if influencer else '',
        }
    }


def serialize_influencer_offer(offer, campaign, brand):
    """Shape offer data for influencer-facing pages."""
    events = get_offer_events(offer.get('id'))
    campaign_state = get_campaign_runtime_state(campaign)
    offer_state = get_offer_runtime_state(offer)
    resolved_campaign = {**campaign, **campaign_state}
    return {
        'id': offer.get('id'),
        'status': offer.get('status', 'pending'),
        'created_at': offer.get('created_at'),
        'brand_notes': offer_state.get('brand_notes', ''),
        'brand_budget_range': normalize_currency_text(offer.get('brand_budget_range', '')),
        'influencer_quote': normalize_offer_amount(offer.get('influencer_quote'), resolved_campaign),
        'negotiated_amount': normalize_offer_amount(offer.get('negotiated_amount'), resolved_campaign),
        'deliverable_url': offer_state.get('deliverable_url'),
        'deliverable_status': offer_state.get('deliverable_status', 'not_submitted'),
        'deliverable_submitted_at': offer_state.get('deliverable_submitted_at'),
        'review_notes': offer_state.get('review_notes', ''),
        'payment_status': offer_state.get('payment_status', 'escrow_pending'),
        'payment_amount': normalize_offer_amount(offer_state.get('payment_amount'), resolved_campaign),
        'payment_released_at': offer_state.get('payment_released_at'),
        'negotiation_summary': build_negotiation_summary({**offer, **offer_state}, resolved_campaign, events),
        'timeline': build_offer_timeline({**offer, **offer_state}, resolved_campaign, events),
        'brand_name': brand.get('full_name') if brand else 'Brand Partner',
        'campaigns': {
            'id': campaign.get('id'),
            'name': campaign.get('name'),
            'brief_text': campaign.get('brief_text'),
            'target_audience': campaign.get('target_audience'),
            'niche': campaign_state.get('niche', ''),
            'budget_range': normalize_currency_text(campaign.get('budget_range')),
            'brand_id': campaign.get('brand_id'),
            'status': campaign.get('status'),
            'milestone_views': campaign_state.get('milestone_views'),
            'timeline_requirements': campaign_state.get('timeline_requirements'),
            'currency': campaign_state.get('currency', 'INR'),
        }
    }


def convert_legacy_amount_to_inr(amount):
    """Convert legacy internal/USD-denominated numeric amounts into INR."""
    if amount in (None, ''):
        return None
    return round(float(amount) * LEGACY_USD_TO_INR, 2)


def is_inr_campaign_context(campaign=None):
    """Infer whether campaign-linked numeric values are already stored in INR."""
    if not campaign:
        return False
    if campaign.get('currency') == 'INR':
        return True
    if '₹' in str(campaign.get('budget_range') or ''):
        return True
    return False


def normalize_offer_amount(amount, campaign=None):
    """Normalize stored offer numeric amounts for display and analytics."""
    if amount in (None, ''):
        return None
    return round(float(amount), 2) if is_inr_campaign_context(campaign) else convert_legacy_amount_to_inr(amount)


def parse_budget_value_in_inr(range_text):
    """Parse budget-like text into an INR midpoint."""
    value = parse_budget_value(range_text)
    if value is None:
        return None
    return round(value * LEGACY_USD_TO_INR, 2) if '$' in str(range_text) else round(value, 2)


def format_inr_amount(amount, decimals=0):
    """Format a numeric INR amount for display."""
    if amount in (None, ''):
        return ''
    amount = float(amount)
    if decimals == 0 and amount.is_integer():
        return f"₹{int(amount):,}"
    return f"₹{amount:,.{decimals}f}"


def normalize_currency_text(value):
    """Normalize legacy USD ranges/amounts into INR display strings."""
    if value in (None, ''):
        return ''

    text = str(value)
    if '₹' in text:
        return text
    if '$' not in text:
        return text

    numbers = [int(match.replace(',', '')) for match in re.findall(r'(\d[\d,]*)', text)]
    converted = [round(number * LEGACY_USD_TO_INR) for number in numbers]
    if not converted:
        return text.replace('$', '₹')
    if '+' in text:
        return f"{format_inr_amount(converted[0])}+"
    if len(converted) == 1:
        return format_inr_amount(converted[0])
    return f"{format_inr_amount(converted[0])} - {format_inr_amount(converted[1])}"


BUDGET_RANGE_OPTIONS = [
    "₹50,000 - ₹1,00,000",
    "₹1,00,000 - ₹2,50,000",
    "₹2,50,000 - ₹5,00,000",
    "₹5,00,000 - ₹10,00,000",
    "₹10,00,000+",
]

PLATFORM_ALIASES = {
    'instagram': 'instagram',
    'insta': 'instagram',
    'reels': 'instagram',
    'youtube': 'youtube',
    'yt': 'youtube',
    'shorts': 'youtube',
    'tiktok': 'tiktok',
    'tik tok': 'tiktok',
    'twitter': 'twitter',
    'x': 'twitter',
}


def extract_inr_amounts_from_text(text):
    """Extract INR-like amounts from free-form text, including lakh/crore shorthand."""
    raw_text = str(text or '')
    normalized = normalize_text(raw_text)
    if not normalized:
        return []

    amounts = []
    for match in re.finditer(r'(\d+(?:\.\d+)?)\s*(crore|cr|lakh|lac|lakhs|lacs|k|thousand)?', normalized):
        raw_value = float(match.group(1))
        unit = (match.group(2) or '').lower()
        context_window = normalized[max(0, match.start() - 20): min(len(normalized), match.end() + 20)]
        has_budget_context = any(keyword in context_window for keyword in {'budget', 'spend', 'cost', 'pricing', 'price', 'rs', 'inr', 'rupee'})

        if unit in {'crore', 'cr'}:
            raw_value *= 10000000
        elif unit in {'lakh', 'lac', 'lakhs', 'lacs'}:
            raw_value *= 100000
        elif unit in {'k', 'thousand'}:
            raw_value *= 1000
        elif raw_value < 1000 or not has_budget_context:
            continue
        amounts.append(int(round(raw_value)))

    return amounts


def normalize_budget_range_for_form(value, source_text=''):
    """Convert parsed budget text into one of the supported budget dropdown values."""
    candidate = normalize_currency_text(value)
    for option in BUDGET_RANGE_OPTIONS:
        if candidate == option:
            return option

    combined_text = f"{candidate} {source_text}".strip()
    amounts = extract_inr_amounts_from_text(combined_text)
    if not amounts:
        return ''

    amount = amounts[0] if len(amounts) == 1 else int(round(sum(amounts[:2]) / 2))
    if amount < 100000:
        return BUDGET_RANGE_OPTIONS[0]
    if amount < 250000:
        return BUDGET_RANGE_OPTIONS[1]
    if amount < 500000:
        return BUDGET_RANGE_OPTIONS[2]
    if amount < 1000000:
        return BUDGET_RANGE_OPTIONS[3]
    return BUDGET_RANGE_OPTIONS[4]


def infer_platforms_from_text(text, preferred=''):
    """Return a normalized comma-separated platform list from text signals."""
    combined = normalize_text(' '.join(filter(None, [text, preferred])))
    detected = []
    for alias, canonical in PLATFORM_ALIASES.items():
        if alias in combined and canonical not in detected:
            detected.append(canonical)

    if not detected:
        if 'video' in combined or 'creator' in combined:
            detected = ['youtube', 'instagram']
        else:
            detected = ['instagram']

    return ', '.join(detected)


def infer_target_audience_from_text(text):
    """Infer a concise target audience from a short campaign goal."""
    raw_text = ' '.join(str(text or '').split())
    patterns = [
        r'(?:for|targeting|aimed at|focused on|towards|reach(?:ing)?)\s+([^.,;\n]+)',
        r'(?:to)\s+([^.,;\n]+?)\s+(?:through|via|using)\b',
    ]
    for pattern in patterns:
        match = re.search(pattern, raw_text, flags=re.IGNORECASE)
        if match:
            audience = match.group(1).strip(' ,.-')
            if audience:
                return audience[:140]
    return "Relevant audience aligned with the product category"


def infer_goals_from_text(text):
    """Infer structured campaign goals from free-form text."""
    normalized = normalize_text(text)
    goal_labels = []

    keyword_map = [
        ('Brand awareness', {'awareness', 'launch', 'introduce', 'visibility', 'buzz'}),
        ('Product sales', {'sales', 'sell', 'purchase', 'orders', 'revenue', 'conversion', 'conversions'}),
        ('Lead generation', {'lead', 'leads', 'signup', 'signups', 'register', 'registrations'}),
        ('App installs', {'install', 'installs', 'download', 'downloads', 'app users'}),
        ('Engagement', {'engagement', 'community', 'comments', 'shares', 'interaction'}),
        ('Website traffic', {'traffic', 'website visits', 'site visits', 'landing page'}),
    ]

    for label, keywords in keyword_map:
        if any(keyword in normalized for keyword in keywords):
            goal_labels.append(label)

    if not goal_labels:
        goal_labels = ['Brand awareness']

    return ', '.join(goal_labels[:3])


def infer_campaign_name_from_text(text):
    """Create a concise campaign name when one is not clearly provided."""
    normalized_text = ' '.join(str(text or '').split())
    if not normalized_text:
        return 'Campaign Draft'

    if re.search(r'\blaunch\b', normalized_text, flags=re.IGNORECASE):
        launch_match = re.search(r'launch\s+(?:our|the|a|an)?\s*([^.,;\n]+)', normalized_text, flags=re.IGNORECASE)
        if launch_match:
            subject = launch_match.group(1).strip()
            subject = re.split(r'\b(for|to|with|through|via)\b', subject, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            if subject:
                return f"{subject.title()} Launch Campaign"[:90]

    words = re.findall(r'[A-Za-z0-9]+', normalized_text)
    if not words:
        return 'Campaign Draft'
    return (' '.join(words[:6]).title() + ' Campaign')[:90]


def sanitize_parsed_campaign_data(parsed_data, source_text):
    """Normalize parser output so the create form gets reliable values."""
    parsed_data = parsed_data or {}
    source_text = ' '.join(str(source_text or '').split())
    inferred_niches = infer_campaign_niches({
        'name': parsed_data.get('name') or '',
        'brief_text': parsed_data.get('brief_text') or source_text,
        'target_audience': parsed_data.get('target_audience') or '',
        'goals': parsed_data.get('goals') or '',
        'niche': parsed_data.get('niche') or '',
    })

    brief_text = (parsed_data.get('brief_text') or source_text).strip()
    if brief_text and len(brief_text) > 320:
        brief_text = brief_text[:317].rstrip() + '...'

    target_audience = (parsed_data.get('target_audience') or '').strip()
    if not target_audience or target_audience.lower() in {'general audience', 'everyone', 'consumers'}:
        target_audience = infer_target_audience_from_text(source_text)

    goals = (parsed_data.get('goals') or '').strip()
    if not goals:
        goals = infer_goals_from_text(source_text)

    platforms = infer_platforms_from_text(source_text, preferred=parsed_data.get('platforms') or '')
    budget_range = normalize_budget_range_for_form(parsed_data.get('budget_range') or '', source_text=source_text)

    return {
        "name": (parsed_data.get('name') or '').strip() or infer_campaign_name_from_text(source_text),
        "brief_text": brief_text,
        "target_audience": target_audience,
        "niche": normalize_text(parsed_data.get('niche') or '') or (inferred_niches[0] if inferred_niches else ''),
        "goals": goals,
        "budget_range": budget_range,
        "platforms": platforms,
        "outreach_angle": (parsed_data.get('outreach_angle') or '').strip(),
        "budget_reasoning": (parsed_data.get('budget_reasoning') or '').strip(),
    }


def split_embedded_metadata(text, prefix):
    """Split stored text from ReachIQ fallback metadata."""
    raw_text = str(text or '')
    index = raw_text.rfind(prefix)
    if index == -1:
        return raw_text, {}

    candidate = raw_text[index + len(prefix):].strip()
    try:
        metadata = json.loads(candidate)
        if not isinstance(metadata, dict):
            return raw_text, {}
        return raw_text[:index].rstrip(), metadata
    except Exception:
        return raw_text, {}


def embed_metadata(text, prefix, metadata):
    """Append structured fallback metadata to a text field."""
    clean_text, _ = split_embedded_metadata(text, prefix)
    compact_metadata = {key: value for key, value in (metadata or {}).items() if value not in (None, '', [], {})}
    if not compact_metadata:
        return clean_text
    encoded = json.dumps(compact_metadata, separators=(',', ':'))
    return f"{clean_text}\n{prefix}{encoded}" if clean_text else f"{prefix}{encoded}"


def get_campaign_runtime_state(campaign):
    """Resolve campaign state using real columns first, then fallback metadata."""
    clean_goals, metadata = split_embedded_metadata(campaign.get('goals'), CAMPAIGN_META_PREFIX)
    return {
        'goals': clean_goals,
        'niche': campaign.get('niche') or metadata.get('niche', ''),
        'milestone_views': campaign.get('milestone_views') if campaign.get('milestone_views') not in (None, '') else metadata.get('milestone_views', 0),
        'timeline_requirements': campaign.get('timeline_requirements') or metadata.get('timeline_requirements', ''),
        'currency': campaign.get('currency') or metadata.get('currency') or 'INR',
    }


def get_offer_runtime_state(offer):
    """Resolve workflow fields using real columns first, then fallback metadata."""
    clean_notes, metadata = split_embedded_metadata(offer.get('brand_notes'), OFFER_META_PREFIX)
    return {
        'brand_notes': clean_notes,
        'deliverable_url': offer.get('deliverable_url') or metadata.get('deliverable_url'),
        'deliverable_status': offer.get('deliverable_status') or metadata.get('deliverable_status') or 'not_submitted',
        'deliverable_submitted_at': offer.get('deliverable_submitted_at') or metadata.get('deliverable_submitted_at'),
        'review_notes': offer.get('review_notes') or metadata.get('review_notes', ''),
        'payment_status': offer.get('payment_status') or metadata.get('payment_status') or 'escrow_pending',
        'payment_amount': offer.get('payment_amount') if offer.get('payment_amount') not in (None, '') else metadata.get('payment_amount'),
        'payment_released_at': offer.get('payment_released_at') or metadata.get('payment_released_at'),
    }


def get_offer_events(offer_id):
    """Fetch offer events safely."""
    try:
        response = supabase.table('offer_events').select('*').eq('offer_id', offer_id).order('created_at').execute()
        return response.data or []
    except Exception:
        return []


def humanize_status_label(value):
    """Convert stored status values into readable labels."""
    return str(value or '').replace('_', ' ').strip().title()


def maybe_log_milestone_hit_event(offer_id, actor_id, offer, campaign, analytics=None, events=None):
    """Persist a milestone-hit event once when a submitted video reaches its target."""
    try:
        campaign_state = get_campaign_runtime_state(campaign or {})
        offer_state = get_offer_runtime_state(offer or {})
        milestone_target = int(campaign_state.get('milestone_views') or 0)
        deliverable_url = offer_state.get('deliverable_url')
        if milestone_target <= 0 or not deliverable_url:
            return analytics

        known_events = events if events is not None else get_offer_events(offer_id)
        if any(event.get('event_type') == 'milestone_hit_detected' for event in known_events):
            return analytics

        analytics = analytics or analyze_video_payload(deliverable_url, milestone_target)
        if analytics.get('milestone', {}).get('is_reached'):
            log_offer_event(
                offer_id,
                actor_id,
                'milestone_hit_detected',
                {
                    'target': milestone_target,
                    'views': analytics.get('metrics', {}).get('views', 0),
                },
            )
        return analytics
    except Exception:
        return analytics


def analyze_video_payload(url, milestone_target):
    """Fetch and calculate video analytics for a YouTube URL."""
    yt_regex = r'(?:youtube\.com\/(?:[^\/]+\/.+\/|(?:v|e(?:mbed)?)\/|.*[?&]v=|shorts\/)|youtu\.be\/)([a-zA-Z0-9_-]{11})'
    match = re.search(yt_regex, url or '')
    if not match:
        raise ValueError("Could not extract a valid 11-character Video ID from the URL.")

    video_id = match.group(1)
    yt_url = f"https://youtube.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails&id={video_id}&key={YOUTUBE_API_KEY}"
    res = requests.get(yt_url).json()
    if not res.get("items"):
        raise LookupError("Video not found or is set to private.")

    video = res["items"][0]
    snippet = video["snippet"]
    stats = video["statistics"]
    content_details = video["contentDetails"]

    views = int(stats.get("viewCount", 0))
    likes = int(stats.get("likeCount", 0))
    comments = int(stats.get("commentCount", 0))
    engagement_rate = round(((likes + comments) / views * 100), 2) if views > 0 else 0
    like_ratio = round((likes / views * 100), 2) if views > 0 else 0
    is_reached = views >= milestone_target if milestone_target > 0 else False
    progress_pct = (views / milestone_target * 100) if milestone_target > 0 else 0
    readable_duration = parse_yt_duration(content_details.get("duration", "PT0S"))
    tags = snippet.get("tags", [])[:5]
    emv_inr = round((views / 1000) * (20 * LEGACY_USD_TO_INR), 2)

    return {
        "title": snippet["title"],
        "channel_name": snippet["channelTitle"],
        "published_at": snippet["publishedAt"][:10],
        "thumbnail": snippet["thumbnails"].get("maxres", snippet["thumbnails"]["high"])["url"],
        "video_meta": {
            "duration": readable_duration,
            "tags": tags,
            "description": snippet.get("description", ""),
        },
        "metrics": {
            "views": views,
            "likes": likes,
            "comments": comments,
            "engagement_rate": engagement_rate,
            "like_to_view_ratio": like_ratio,
            "earned_media_value_inr": emv_inr,
        },
        "milestone": {
            "target": milestone_target,
            "progress_percentage": progress_pct,
            "is_reached": is_reached,
            "views_remaining": max(0, milestone_target - views)
        }
    }


def build_negotiation_summary(offer, campaign, events=None):
    """Create a concise negotiation summary visible to both sides."""
    events = events or []
    offer_state = get_offer_runtime_state(offer)
    points = []

    opening_budget = normalize_currency_text(offer.get('brand_budget_range') or campaign.get('budget_range'))
    if opening_budget:
        points.append(f"Opening budget: {opening_budget}")
    if offer.get('influencer_quote') not in (None, ''):
        points.append(f"Creator countered at {format_inr_amount(normalize_offer_amount(offer.get('influencer_quote'), campaign))}")
    if offer.get('negotiated_amount') not in (None, ''):
        points.append(f"Latest agreed value: {format_inr_amount(normalize_offer_amount(offer.get('negotiated_amount'), campaign))}")
    if offer_state.get('brand_notes'):
        points.append(f"Latest brand note: {offer_state.get('brand_notes')}")

    history = []
    for event in events[-5:]:
        event_type = event.get('event_type', '').replace('_', ' ').title()
        metadata = event.get('metadata') or {}
        amount = metadata.get('negotiated_amount') or metadata.get('influencer_quote')
        snippet = event_type
        if amount not in (None, ''):
            snippet += f" at {format_inr_amount(normalize_offer_amount(amount, campaign))}"
        history.append(snippet)

    headline = points[0] if points else 'No negotiation changes yet.'
    return {
        'headline': headline,
        'points': points,
        'history': history,
    }


def build_offer_timeline(offer, campaign, events=None, analytics=None):
    """Return a milestone-style lifecycle timeline for campaign fulfillment."""
    events = events or []
    campaign_state = get_campaign_runtime_state(campaign)
    offer_state = get_offer_runtime_state(offer)
    timeline = [
        {
            'label': 'Offer Sent',
            'status': 'completed',
            'detail': 'Brand sent the collaboration offer.',
            'date': offer.get('created_at'),
        },
        {
            'label': 'Negotiation',
            'status': 'completed' if offer.get('status') in {'negotiating', 'accepted'} or offer.get('influencer_quote') else 'pending',
            'detail': build_negotiation_summary(offer, campaign, events).get('headline'),
            'date': next((event.get('created_at') for event in events if event.get('event_type') == 'offer_status_updated'), None),
        },
        {
            'label': 'Video Submitted',
            'status': 'completed' if offer_state.get('deliverable_url') else 'pending',
            'detail': offer_state.get('deliverable_url') or 'Waiting for creator submission.',
            'date': offer_state.get('deliverable_submitted_at'),
        },
        {
            'label': 'Brand Review',
            'status': 'completed' if offer_state.get('deliverable_status') in {'reviewed', 'milestone_hit', 'approved'} else 'pending',
            'detail': offer_state.get('review_notes') or 'Review not completed yet.',
            'date': next((event.get('created_at') for event in events if event.get('event_type') == 'deliverable_reviewed'), None),
        },
        {
            'label': 'Escrow / Payment',
            'status': 'completed' if offer_state.get('payment_status') == 'paid' else ('in_progress' if offer_state.get('payment_status') == 'payment_processing' else 'pending'),
            'detail': offer_state.get('payment_status', 'escrow_pending').replace('_', ' ').title(),
            'date': offer_state.get('payment_released_at'),
        },
    ]

    if analytics and analytics.get('milestone', {}).get('target'):
        timeline.insert(
            4,
            {
                'label': 'Milestone Check',
                'status': 'completed' if analytics['milestone'].get('is_reached') else 'in_progress',
                'detail': (
                    f"Reached {analytics['milestone']['target']:,} views."
                    if analytics['milestone'].get('is_reached')
                    else f"{analytics['milestone'].get('views_remaining', 0):,} more views needed."
                ),
                'date': None,
            },
        )
    elif campaign_state.get('milestone_views'):
        timeline.insert(
            4,
            {
                'label': 'Milestone Check',
                'status': 'pending',
                'detail': f"Target set to {int(campaign_state.get('milestone_views') or 0):,} views.",
                'date': None,
            },
        )
    return timeline


def build_notification_target(role, event_type, offer_id, metadata=None):
    """Map a notification event to the most relevant frontend page."""
    metadata = metadata or {}
    if role == 'brand':
        if event_type == 'offer_status_updated':
            status = metadata.get('status') or 'all'
            return f"brand-offers.html?filter={status}&offerId={offer_id}"
        if event_type in {'deliverable_submitted', 'deliverable_reviewed', 'milestone_hit_detected'}:
            return f"video-tracker.html?offer_id={offer_id}"
        return "brand-dashboard.html"

    action = 'view'
    if event_type == 'offer_status_updated' and (metadata.get('status') or '') == 'negotiating':
        action = 'counter'
    elif event_type == 'deliverable_reviewed' and metadata.get('payment_status') in {'payment_processing', 'paid'}:
        action = 'view'
    return f"influencer-offers.html?offerId={offer_id}&action={action}"


def build_offer_notification(role, user_id, event, offer, campaign, counterpart):
    """Translate offer lifecycle events into UI notifications."""
    event_type = event.get('event_type')
    metadata = event.get('metadata') or {}
    actor_id = event.get('actor_id')
    offer_id = offer.get('id')
    campaign_name = campaign.get('name') or 'your campaign'
    counterpart_name = (
        counterpart.get('full_name')
        or counterpart.get('username')
        or ('Creator' if role == 'brand' else 'Brand Partner')
    )

    title = None
    message = None

    if role == 'brand':
        if event_type == 'offer_status_updated' and actor_id != user_id:
            status = metadata.get('status') or offer.get('status') or 'pending'
            if status == 'accepted':
                title = 'Offer accepted'
                message = f"{counterpart_name} accepted the offer for {campaign_name}."
            elif status == 'negotiating':
                title = 'Creator negotiating'
                message = f"{counterpart_name} sent a counter or requested changes on {campaign_name}."
            elif status == 'rejected':
                title = 'Offer declined'
                message = f"{counterpart_name} declined the offer for {campaign_name}."
        elif event_type == 'deliverable_submitted':
            title = 'Video submitted'
            message = f"{counterpart_name} submitted a deliverable for {campaign_name}."
        elif event_type == 'milestone_hit_detected':
            target = int(metadata.get('target') or 0)
            title = 'Milestone reached'
            if target > 0:
                message = f"{counterpart_name}'s deliverable for {campaign_name} crossed the {target:,} view milestone."
            else:
                message = f"{counterpart_name}'s deliverable for {campaign_name} reached the campaign milestone."
    else:
        if event_type == 'offer_sent':
            title = 'New campaign offer'
            message = f"{counterpart_name} sent you an offer for {campaign_name}."
        elif event_type == 'offer_status_updated' and actor_id != user_id:
            status = metadata.get('status') or offer.get('status') or 'pending'
            if status == 'accepted':
                title = 'Offer approved'
                message = f"{counterpart_name} confirmed the offer terms for {campaign_name}."
            elif status == 'negotiating':
                title = 'Negotiation update'
                message = f"{counterpart_name} updated the negotiation for {campaign_name}."
            elif status == 'rejected':
                title = 'Offer closed'
                message = f"{counterpart_name} declined or closed the collaboration for {campaign_name}."
        elif event_type == 'deliverable_reviewed' and actor_id != user_id:
            payment_status = metadata.get('payment_status') or ''
            deliverable_status = metadata.get('deliverable_status') or ''
            if payment_status == 'paid':
                title = 'Payment completed'
                message = f"{counterpart_name} marked payment as completed for {campaign_name}."
            elif payment_status == 'payment_processing':
                title = 'Payment released'
                message = f"{counterpart_name} started the payment process for {campaign_name}."
            else:
                title = 'Deliverable reviewed'
                message = f"{counterpart_name} reviewed your deliverable for {campaign_name} ({humanize_status_label(deliverable_status) or 'reviewed'})."
        elif event_type == 'milestone_hit_detected':
            target = int(metadata.get('target') or 0)
            title = 'Milestone reached'
            if target > 0:
                message = f"Your deliverable for {campaign_name} crossed the {target:,} view milestone."
            else:
                message = f"Your deliverable for {campaign_name} reached the campaign milestone."

    if not title or not message:
        return None

    return {
        'id': event.get('id') or f"{offer_id}:{event_type}:{event.get('created_at') or ''}",
        'offer_id': offer_id,
        'campaign_id': campaign.get('id'),
        'kind': event_type,
        'title': title,
        'message': message,
        'created_at': event.get('created_at') or offer.get('created_at'),
        'target_url': build_notification_target(role, event_type, offer_id, metadata),
    }


def synthesize_offer_notifications(role, offer, campaign, counterpart, existing_kinds=None):
    """Build fallback notifications from current offer state when events are missing."""
    existing_kinds = set(existing_kinds or [])
    offer_state = get_offer_runtime_state(offer)
    campaign_state = get_campaign_runtime_state(campaign)
    offer_id = offer.get('id')
    campaign_name = campaign.get('name') or 'your campaign'
    counterpart_name = (
        counterpart.get('full_name')
        or counterpart.get('username')
        or ('Creator' if role == 'brand' else 'Brand Partner')
    )
    notifications = []

    def append_notification(kind, title, message, created_at=None):
        if kind in existing_kinds:
            return
        notifications.append({
            'id': f"snapshot:{role}:{offer_id}:{kind}",
            'offer_id': offer_id,
            'campaign_id': campaign.get('id'),
            'kind': kind,
            'title': title,
            'message': message,
            'created_at': created_at or offer_state.get('deliverable_submitted_at') or offer.get('created_at'),
            'target_url': build_notification_target(role, kind, offer_id, {'status': offer.get('status')}),
        })

    if role == 'brand':
        if offer.get('status') == 'negotiating':
            append_notification(
                'offer_status_updated',
                'Creator negotiating',
                f"{counterpart_name} is negotiating the offer for {campaign_name}.",
            )
        elif offer.get('status') == 'accepted':
            append_notification(
                'offer_status_updated',
                'Offer accepted',
                f"{counterpart_name} accepted the offer for {campaign_name}.",
            )
        elif offer.get('status') == 'rejected':
            append_notification(
                'offer_status_updated',
                'Offer declined',
                f"{counterpart_name} declined the offer for {campaign_name}.",
            )

        if offer_state.get('deliverable_url'):
            append_notification(
                'deliverable_submitted',
                'Video submitted',
                f"{counterpart_name} submitted a deliverable for {campaign_name}.",
                created_at=offer_state.get('deliverable_submitted_at'),
            )

        if offer_state.get('payment_status') in {'payment_processing', 'paid'}:
            append_notification(
                'deliverable_reviewed',
                'Escrow action required' if offer_state.get('payment_status') == 'payment_processing' else 'Payment completed',
                (
                    f"{campaign_name} is ready for payment processing."
                    if offer_state.get('payment_status') == 'payment_processing'
                    else f"Payment for {campaign_name} has been completed."
                ),
                created_at=offer_state.get('payment_released_at'),
            )
    else:
        append_notification(
            'offer_sent',
            'New campaign offer',
            f"{counterpart_name} sent you an offer for {campaign_name}.",
            created_at=offer.get('created_at'),
        )

        if offer.get('status') == 'negotiating':
            append_notification(
                'offer_status_updated',
                'Negotiation update',
                f"{counterpart_name} updated the negotiation for {campaign_name}.",
            )
        elif offer.get('status') == 'accepted':
            append_notification(
                'offer_status_updated',
                'Offer approved',
                f"{counterpart_name} confirmed the offer terms for {campaign_name}.",
            )
        elif offer.get('status') == 'rejected':
            append_notification(
                'offer_status_updated',
                'Offer closed',
                f"{counterpart_name} declined or closed the collaboration for {campaign_name}.",
            )

        if offer_state.get('payment_status') == 'payment_processing':
            append_notification(
                'deliverable_reviewed',
                'Payment released',
                f"{counterpart_name} started payment processing for {campaign_name}.",
                created_at=offer_state.get('payment_released_at'),
            )
        elif offer_state.get('payment_status') == 'paid':
            append_notification(
                'deliverable_reviewed',
                'Payment completed',
                f"{counterpart_name} marked payment as completed for {campaign_name}.",
                created_at=offer_state.get('payment_released_at'),
            )

    if (
        campaign_state.get('milestone_views')
        and offer_state.get('deliverable_status') in {'milestone_hit', 'approved'}
        and 'milestone_hit_detected' not in existing_kinds
    ):
        notifications.append({
            'id': f"snapshot:{role}:{offer_id}:milestone_hit_detected",
            'offer_id': offer_id,
            'campaign_id': campaign.get('id'),
            'kind': 'milestone_hit_detected',
            'title': 'Milestone reached',
            'message': (
                f"{campaign_name} has hit the {int(campaign_state.get('milestone_views') or 0):,} view milestone."
            ),
            'created_at': offer_state.get('deliverable_submitted_at') or offer.get('created_at'),
            'target_url': build_notification_target(role, 'milestone_hit_detected', offer_id, {}),
        })

    return notifications


def safe_db_insert(table_name, payload):
    """Best-effort insert for optional analytics tables."""
    try:
        supabase.table(table_name).insert(payload).execute()
        return True
    except Exception as exc:
        print(f"Optional insert skipped for {table_name}: {exc}")
        return False


def extract_missing_schema_column(exc):
    """Return a missing column name from a Supabase schema-cache error."""
    match = re.search(r"Could not find the '([^']+)' column", str(exc))
    return match.group(1) if match else None


def insert_with_optional_fields(table_name, payload, optional_fields=None):
    """Retry inserts after dropping optional fields missing from the current schema."""
    working_payload = dict(payload)
    dropped_fields = []
    allowed_missing = set(optional_fields or [])

    while True:
        try:
            return supabase.table(table_name).insert(working_payload).execute(), dropped_fields
        except Exception as exc:
            missing_column = extract_missing_schema_column(exc)
            if missing_column and missing_column in working_payload and missing_column in allowed_missing:
                working_payload.pop(missing_column, None)
                dropped_fields.append(missing_column)
                continue
            raise


def update_with_optional_fields(table_name, payload, apply_filters, optional_fields=None):
    """Retry updates after dropping optional fields missing from the current schema."""
    working_payload = dict(payload)
    dropped_fields = []
    allowed_missing = set(optional_fields or [])

    while True:
        if not working_payload:
            return None, dropped_fields
        try:
            query = supabase.table(table_name).update(working_payload)
            return apply_filters(query).execute(), dropped_fields
        except Exception as exc:
            missing_column = extract_missing_schema_column(exc)
            if missing_column and missing_column in working_payload and missing_column in allowed_missing:
                working_payload.pop(missing_column, None)
                dropped_fields.append(missing_column)
                continue
            raise


def log_offer_event(offer_id, actor_id, event_type, metadata=None):
    """Persist offer lifecycle events when the analytics table exists."""
    return safe_db_insert(
        'offer_events',
        {
            'offer_id': offer_id,
            'actor_id': actor_id,
            'event_type': event_type,
            'metadata': metadata or {},
        },
    )


def log_ai_recommendation(feature_name, user_id, entity_type, entity_id, payload):
    """Persist AI recommendation payloads when analytics storage exists."""
    return safe_db_insert(
        'ai_recommendations',
        {
            'feature_name': feature_name,
            'user_id': user_id,
            'entity_type': entity_type,
            'entity_id': entity_id,
            'payload': payload,
        },
    )


def generate_json_with_gemini(prompt, fallback=None):
    """Run a Gemini JSON generation with graceful fallback."""
    model = genai.GenerativeModel('gemini-2.5-flash')
    generation_config = genai.GenerationConfig(response_mime_type="application/json")
    response = None
    try:
        response = model.generate_content(prompt, generation_config=generation_config)
        return json.loads(response.text)
    except Exception as exc:
        print(f"Gemini JSON generation failed: {exc}")
        print(f"Gemini response content (if any): {getattr(response, 'text', 'No text')}")
        return fallback() if callable(fallback) else (fallback or {})


def generate_text_with_gemini(prompt, fallback=''):
    """Run a Gemini text generation with graceful fallback."""
    try:
        model = genai.GenerativeModel('gemini-2.5-flash')
        response = model.generate_content(prompt)
        return (response.text or '').strip() or fallback
    except Exception as exc:
        print(f"Gemini text generation failed: {exc}")
        return fallback


def embed_text_content(text, task_type='retrieval_document'):
    """Get an embedding for text when Gemini embeddings are available."""
    cleaned = (text or '').strip()
    if not cleaned:
        return None

    try:
        response = genai.embed_content(
            model='models/text-embedding-004',
            content=cleaned,
            task_type=task_type,
        )
        if isinstance(response, dict):
            return response.get('embedding')
        return getattr(response, 'embedding', None)
    except Exception as exc:
        print(f"Gemini embedding failed, falling back to local similarity: {exc}")
        return None


def compute_semantic_similarity(campaign, influencer):
    """Blend Gemini embeddings with local semantic similarity for robust ranking."""
    campaign_text = build_campaign_matching_text(campaign)
    creator_text = build_creator_matching_text(influencer)
    local_similarity = local_semantic_similarity(campaign_text, creator_text)

    campaign_embedding = embed_text_content(campaign_text, task_type='retrieval_query')
    creator_embedding = embed_text_content(creator_text, task_type='retrieval_document')
    if campaign_embedding and creator_embedding:
        embedding_similarity = cosine_similarity(campaign_embedding, creator_embedding)
        return round((embedding_similarity * 0.75) + (local_similarity * 0.25), 4), round(local_similarity, 4)
    return round(local_similarity, 4), round(local_similarity, 4)


NICHE_KEYWORDS = {
    'fitness': {'fitness', 'workout', 'gym', 'protein', 'wellness', 'health', 'muscle', 'supplement', 'exercise'},
    'beauty': {'beauty', 'makeup', 'skincare', 'cosmetic', 'glow', 'haircare', 'fragrance'},
    'fashion': {'fashion', 'style', 'apparel', 'outfit', 'streetwear', 'clothing', 'wardrobe', 'wear'},
    'tech': {'tech', 'technology', 'gadget', 'software', 'ai', 'saas', 'app', 'laptop', 'smartphone', 'device'},
    'travel': {'travel', 'trip', 'vacation', 'hotel', 'tourism', 'itinerary', 'flight', 'destination'},
    'food': {'food', 'snack', 'restaurant', 'recipe', 'beverage', 'nutrition', 'cooking', 'meal', 'drink'},
    'lifestyle': {'lifestyle', 'daily', 'routine', 'home', 'family', 'selfcare', 'living', 'vlog'},
    'gaming': {'gaming', 'game', 'stream', 'esports', 'gameplay', 'console', 'pc gaming'},
    'education': {'education', 'learning', 'course', 'student', 'study', 'tutorial', 'class', 'exam'},
    'business': {'business', 'finance', 'startup', 'founder', 'marketing', 'career', 'investing', 'money'},
    'sports': {'sports', 'athlete', 'cricket', 'football', 'running', 'training', 'match', 'league'},
}

NICHE_PRIORITY = {
    'beauty': 1.0,
    'fashion': 0.98,
    'fitness': 0.96,
    'tech': 0.95,
    'food': 0.94,
    'travel': 0.93,
    'gaming': 0.92,
    'education': 0.91,
    'business': 0.9,
    'sports': 0.9,
    'lifestyle': 0.72,
}

RELATED_NICHES = {
    'beauty': {'fashion': 0.84, 'lifestyle': 0.58},
    'fashion': {'beauty': 0.84, 'lifestyle': 0.6},
    'fitness': {'sports': 0.82, 'lifestyle': 0.52},
    'sports': {'fitness': 0.82, 'lifestyle': 0.48},
    'food': {'lifestyle': 0.55},
    'travel': {'lifestyle': 0.52},
    'tech': {'business': 0.5},
    'business': {'tech': 0.5, 'education': 0.46},
}


def score_niche_relevance(text):
    """Return niche relevance scores from free-form campaign or creator text."""
    normalized = normalize_text(text)
    if not normalized:
        return {}

    scores = {}
    for niche, keywords in NICHE_KEYWORDS.items():
        matches = sum(1 for keyword in keywords if keyword in normalized)
        if matches:
            # Weight repeated/stronger niche mentions above a single accidental keyword hit.
            scores[niche] = round(matches / max(len(keywords), 1), 4)
    return scores


def pick_best_niche(niche_scores):
    """Choose the most specific niche instead of defaulting to generic lifestyle too often."""
    if not niche_scores:
        return ''

    adjusted_scores = {
        niche: score + (NICHE_PRIORITY.get(niche, 0.75) * 0.05)
        for niche, score in niche_scores.items()
    }
    best_niche = max(adjusted_scores, key=adjusted_scores.get)

    if best_niche == 'lifestyle':
        lifestyle_score = adjusted_scores.get('lifestyle', 0.0)
        specific_candidates = [
            (niche, score)
            for niche, score in adjusted_scores.items()
            if niche != 'lifestyle' and score >= (lifestyle_score * 0.72)
        ]
        if specific_candidates:
            specific_candidates.sort(key=lambda item: item[1], reverse=True)
            return specific_candidates[0][0]

    return best_niche


def resolve_creator_niche(influencer):
    """Resolve the creator niche strictly from saved database fields."""
    stored_niche = normalize_text(influencer.get('niche') or '')
    if stored_niche in NICHE_KEYWORDS:
        return stored_niche

    profile = influencer.get('profile') or influencer.get('profiles') or {}
    profile_niche = normalize_text(profile.get('niche') or '')
    if profile_niche in NICHE_KEYWORDS:
        return profile_niche
    return ''


def infer_campaign_niches(campaign):
    """Infer the most relevant campaign niches from the campaign text."""
    explicit_niche = normalize_text(
        campaign.get('niche') or get_campaign_runtime_state(campaign).get('niche', '')
    )
    campaign_text = (
        ' '.join(
            filter(
                None,
                [
                    explicit_niche,
                    campaign.get('name'),
                    campaign.get('brief_text'),
                    campaign.get('target_audience'),
                    campaign.get('goals'),
                ],
            )
        )
    )
    niche_scores = score_niche_relevance(campaign_text)
    inferred = [niche for niche, _ in sorted(niche_scores.items(), key=lambda item: item[1], reverse=True)]
    if explicit_niche and explicit_niche in NICHE_KEYWORDS:
        inferred = [explicit_niche] + [niche for niche in inferred if niche != explicit_niche]
    return inferred


def score_niche_alignment(campaign, influencer, campaign_niches=None):
    """Score how strongly a creator niche aligns with the inferred campaign niche."""
    campaign_niches = campaign_niches if campaign_niches is not None else infer_campaign_niches(campaign)
    creator_niche = resolve_creator_niche(influencer)
    creator_text = (
        ' '.join(
            filter(
                None,
                [
                    creator_niche,
                    influencer.get('niche'),
                    influencer.get('bio'),
                    influencer.get('content_description'),
                    influencer.get('audience_interests'),
                    influencer.get('channel_description'),
                    influencer.get('sample_video_transcript'),
                ],
            )
        )
    )
    creator_niche_scores = score_niche_relevance(creator_text)

    if not campaign_niches:
        return 0.5

    top_campaign_niche = campaign_niches[0]
    if creator_niche == top_campaign_niche:
        return 1.0

    related_score = RELATED_NICHES.get(top_campaign_niche, {}).get(creator_niche)
    if related_score is not None:
        return related_score

    if creator_niche and any(niche in creator_niche for niche in campaign_niches):
        return 0.96

    best_alignment = 0.0
    for index, niche in enumerate(campaign_niches):
        if niche in creator_text:
            best_alignment = max(best_alignment, 0.88 if index == 0 else 0.8)
        keyword_score = creator_niche_scores.get(niche, 0.0)
        if keyword_score:
            base_score = 0.72 if index == 0 else 0.62
            best_alignment = max(best_alignment, min(0.9, base_score + (keyword_score * 1.35)))

    return round(best_alignment, 4)


def rank_creators_for_campaign(campaign, influencers):
    """Return ranked creator matches with semantic fit and predictive metrics."""
    campaign_text = build_campaign_matching_text(campaign)
    campaign_niches = infer_campaign_niches(campaign)
    candidate_pool = []
    for influencer in influencers:
        resolved_niche = resolve_creator_niche(influencer)
        enriched_influencer = {**influencer, 'niche': resolved_niche or influencer.get('niche')}
        creator_text = build_creator_matching_text(enriched_influencer)
        niche_alignment = score_niche_alignment(campaign, influencer, campaign_niches=campaign_niches)
        candidate_pool.append(
            (
                niche_alignment,
                local_semantic_similarity(campaign_text, creator_text),
                score_creator_quality(influencer),
                enriched_influencer,
            )
        )

    if campaign_niches:
        exact_niche_pool = [item for item in candidate_pool if item[0] >= 0.9]
        strong_niche_pool = [item for item in candidate_pool if 0.72 <= item[0] < 0.9]
        soft_niche_pool = [item for item in candidate_pool if 0.45 <= item[0] < 0.72]

        if len(exact_niche_pool) >= 4:
            candidate_pool = exact_niche_pool
        elif exact_niche_pool or strong_niche_pool:
            candidate_pool = exact_niche_pool + strong_niche_pool
        elif soft_niche_pool:
            candidate_pool = soft_niche_pool

    candidate_pool.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    ranked = []
    for niche_alignment, _, _, influencer in candidate_pool[:30]:
        semantic_similarity, fallback_similarity = compute_semantic_similarity(campaign, influencer)
        base_match_score = compute_match_score(
            campaign,
            influencer,
            semantic_similarity=semantic_similarity,
        )
        if campaign_niches:
            if niche_alignment >= 0.95:
                match_score = (base_match_score * 0.72) + (niche_alignment * 0.28)
            elif niche_alignment >= 0.72:
                match_score = (base_match_score * 0.56) + (niche_alignment * 0.44)
            elif niche_alignment >= 0.45:
                match_score = (base_match_score * 0.38) + (niche_alignment * 0.62)
            else:
                match_score = (base_match_score * 0.12) + (niche_alignment * 0.88)
        else:
            match_score = (base_match_score * 0.82) + (max(niche_alignment, 0.0) * 0.18)
        match_score = round(min(1.0, max(0.0, match_score)), 4)
        prediction = predict_campaign_performance(
            campaign,
            influencer,
            semantic_similarity=semantic_similarity,
            match_score=base_match_score,
        )
        reasons = summarize_match_reasons(
            campaign,
            influencer,
            semantic_similarity=semantic_similarity,
            prediction=prediction,
        )

        if match_score < 0.28:
            continue

        ranked.append(
            {
                **influencer,
                'niche_alignment': niche_alignment,
                'display_niche': resolve_creator_niche(influencer) or influencer.get('niche') or '',
                'campaign_niches': campaign_niches[:3],
                'semantic_similarity': semantic_similarity,
                'local_similarity': fallback_similarity,
                'base_match_score': base_match_score,
                'match_score': match_score,
                'match_percentage': round(match_score * 100),
                'match_reasons': reasons,
                'prediction': prediction,
            }
        )

    ranked.sort(
        key=lambda item: (
            item.get('niche_alignment', 0),
            item['match_score'],
            item['prediction'].get('predicted_views', 0),
            item['prediction']['value_score'],
        ),
        reverse=True,
    )
    return ranked


def collect_offer_history_stats(offers):
    """Summarize offer outcome patterns for negotiation and evaluation."""
    total = len(offers)
    accepted = sum(1 for offer in offers if offer.get('status') == 'accepted')
    rejected = sum(1 for offer in offers if offer.get('status') == 'rejected')
    negotiating = sum(1 for offer in offers if offer.get('status') == 'negotiating')
    pending = sum(1 for offer in offers if offer.get('status') == 'pending')
    acceptance_rate = round((accepted / total) * 100, 1) if total else 0.0
    return {
        'total_offers': total,
        'accepted': accepted,
        'rejected': rejected,
        'negotiating': negotiating,
        'pending': pending,
        'acceptance_rate': acceptance_rate,
    }

# --- NEW: Gemini Audio Transcription Function ---
#


def parse_yt_duration(duration_str):
    """Converts YouTube's ISO 8601 duration to a readable format."""
    return format_youtube_duration(duration_str)


def transcribe_audio_with_gemini(audio_file_stream, mime_type):
    """
    Uploads audio file stream to Gemini and returns the transcription.
    """
    print(f"--- Uploading audio to Gemini ({mime_type}) ---")
    
    # 1. Upload the file to the Gemini Files API.
    # We pass the file stream directly. Files are auto-deleted after 48h.
    audio_file = genai.upload_file(
        path=audio_file_stream,  # Pass the FileStorage object directly
        display_name="voice-brief-upload",
        mime_type=mime_type
    )
    print(f"Gemini File API: Uploaded file {audio_file.name}")

    # 2. Call the model with the uploaded file and the prompt.
    # We use a model that supports audio understanding.
    model = genai.GenerativeModel('gemini-2.5-flash') # Or your working model
    prompt = "Transcribe this audio. Provide only the raw text of the speech."
    
    response = model.generate_content([prompt, audio_file])
    
    # 3. Clean up the file from Gemini's storage to save space.
    genai.delete_file(audio_file.name)
    print(f"Gemini File API: Deleted file {audio_file.name}")
    
    return response.text

#
# --- NEW: AI Parsing Function (Replaced with Gemini) ---
#
def parse_brief_text_with_ai(document_text):
    """
    Analyzes raw text from a document using the Gemini API
    and extracts structured campaign data.
    """
    
    print("--- Calling Gemini API to parse brief ---")

    # Set up the model with JSON output
    model = genai.GenerativeModel('gemini-2.5-flash')
    generation_config = genai.GenerationConfig(
        response_mime_type="application/json"
    )
    
    prompt = f"""
    You are an expert campaign assistant. Analyze the following campaign brief text
    and extract the key information in a valid JSON format.

    The JSON keys must be: "name", "brief_text", "target_audience", "goals", "budget_range", "platforms".

    - "name": The campaign's title. If no clear title, create a concise one.
    - "brief_text": A concise summary of the campaign goals, deliverables, and key messages.
    - "target_audience": The specific target audience (e.g., "Fitness enthusiasts, 18-35").
    - "goals": The primary campaign goals (e.g., "Brand awareness, Product sales").
    - "budget_range": The budget. If mentioned, match it to one of:
      "₹50,000 - ₹1,00,000", "₹1,00,000 - ₹2,50,000", "₹2,50,000 - ₹5,00,000", "₹5,00,000 - ₹10,00,000", "₹10,00,000+".
      If not mentioned, unclear, or "negotiable", set to an empty string.
    - "platforms": A comma-separated list of platforms (e.g., "instagram, youtube, tiktok").

    Document Text:
    \"\"\"
    {document_text}
    \"\"\"
    """

    response = None  # <--- THIS IS THE FIX. Initialize response to None.

    try:
        response = model.generate_content(
            prompt,
            generation_config=generation_config
        )
        
        json_response = response.text
        print(f"Gemini AI response: {json_response}")
        return json.loads(json_response)
        
    except Exception as e:
        print(f"Gemini API call failed: {e}")
        # This line will now work safely. If response is None, it will print 'No text'.
        print(f"Gemini response content (if any): {getattr(response, 'text', 'No text')}") 
        raise  # Re-raise the exception to be caught by the Flask route
# --- NEW: AI Profile Generation from YouTube Description ---
def generate_profile_from_description(channel_name, description):
    print("--- Asking Gemini to generate profile from YouTube description ---")
    model = genai.GenerativeModel('gemini-2.5-flash')
    generation_config = genai.GenerationConfig(response_mime_type="application/json")
    
    prompt = f"""
    You are an AI profiling expert. Based on this YouTube channel's name and description, infer the best profile details for an influencer marketing platform.

    Channel Name: {channel_name}
    Description: {description}

    Return a valid JSON with exactly these keys:
    - "bio": A short, catchy 2-sentence bio written in third-person.
    - "niche": Choose ONE from: fitness, beauty, fashion, tech, travel, food, lifestyle, gaming, education, business, sports.
    - "audience_age": Infer their main viewer age. Choose ONE: "13-17", "18-24", "25-34", "35-44", "45+".
    - "audience_gender": Infer their main viewer gender. Choose ONE: "male", "female", "mixed".
    - "audience_interests": A short comma-separated list of 3-4 things their audience likes.
    - "content_description": A 1-sentence description of the type of videos they make.
    
    If the description is empty, make your best generic guess based on the channel name, or return defaults like "lifestyle" and "mixed".
    """
    try:
        response = model.generate_content(prompt, generation_config=generation_config)
        return json.loads(response.text)
    except Exception as e:
        print(f"Gemini Profile Generation Failed: {e}")
        return {} # Return empty dict if it fails, so the code doesn't break
    
def extract_video_id(url):
    """Safely extracts the 11-character YouTube Video ID from various URL formats."""
    if "youtu.be" in url:
        return url.split("/")[-1].split("?")[0]
    parsed_url = urlparse.urlparse(url)
    return parse_qs(parsed_url.query).get("v", [None])[0]

# --- NEW: Update Offer Status ---
@app.route('/api/offers/update-status', methods=['POST', 'OPTIONS'])
def update_offer_status():
    """Allows influencers or brands to accept, reject, or update an offer."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    try:
        profile = get_current_profile(user_id)
    except Exception as e:
        return jsonify({"error": f"Failed to load profile: {str(e)}"}), 500

    data = request.json
    offer_id = data.get('offer_id')
    new_status = data.get('status')
    negotiated_amount = data.get('negotiated_amount')
    brand_notes = data.get('brand_notes')
    influencer_quote = data.get('influencer_quote')

    if not offer_id or not new_status:
        return jsonify({"error": "Offer ID and new status are required"}), 400

    if new_status not in {'pending', 'negotiating', 'accepted', 'rejected'}:
        return jsonify({"error": "Invalid offer status"}), 400

    try:
        offer, campaign = get_offer_with_campaign(offer_id)
        if not offer or not campaign:
            return jsonify({"error": "Offer not found"}), 404

        if not can_access_offer(user_id, profile, offer, campaign):
            return jsonify({"error": "Access denied"}), 403

        update_payload = {'status': new_status}
        if negotiated_amount not in (None, ''):
            update_payload['negotiated_amount'] = negotiated_amount
        if influencer_quote not in (None, ''):
            update_payload['influencer_quote'] = influencer_quote
        if brand_notes is not None and profile.get('role') == 'brand':
            update_payload['brand_notes'] = brand_notes

        response = supabase.table('offers').update(update_payload).eq('id', offer_id).execute()
        log_offer_event(
            offer_id,
            user_id,
            'offer_status_updated',
            {
                'status': new_status,
                'negotiated_amount': negotiated_amount,
                'brand_notes_updated': brand_notes is not None and profile.get('role') == 'brand',
                'influencer_quote': influencer_quote,
            },
        )
        
        return jsonify({
            "status": "success",
            "message": f"Offer updated to {new_status}",
            "data": response.data
        })
    except Exception as e:
        print(f"Error updating offer: {e}")
        return jsonify({"error": str(e)}), 500


# --- NEW: AI Smart Reply Generator ---
@app.route('/api/influencer/generate-smart-reply', methods=['POST', 'OPTIONS'])
def generate_smart_reply():
    """Generates professional email replies for influencers negotiating with brands."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    data = request.json
    offer_id = data.get('offer_id')

    try:
        # 1. Fetch the specific offer, campaign, and brand details
        offer_res = supabase.table('offers').select(
            '*, campaigns(name, brief_text, budget_range, brand:profiles!campaigns_brand_id_fkey(full_name))'
        ).eq('id', offer_id).single().execute()
        
        offer = offer_res.data
        if not offer:
            return jsonify({"error": "Offer not found"}), 404
            
        campaign = offer.get('campaigns', {})
        brand_name = campaign.get('brand', {}).get('full_name', 'the brand')
        budget = campaign.get('budget_range', 'Negotiable')
        brief = campaign.get('brief_text', '')

        # 2. Ask Gemini to generate 3 options
        model = genai.GenerativeModel('gemini-2.5-flash')
        generation_config = genai.GenerationConfig(response_mime_type="application/json")
        
        prompt = f"""
        You are an elite Talent Manager for an influencer. The influencer just received a brand deal offer.
        Brand: {brand_name}
        Campaign Name: {campaign.get('name')}
        Budget Offered: {budget}
        Campaign Brief: "{brief}"

        Draft 3 professional email replies for the influencer to choose from:
        1. An enthusiastic ACCEPTANCE of the offer.
        2. A polite COUNTER-OFFER asking for a slightly higher budget, mentioning their high engagement rate.
        3. A professional DECLINE, stating they don't have the bandwidth right now but would love to work together in the future.

        Return valid JSON in this exact format:
        {{
            "accept": "Drafted text here",
            "counter": "Drafted text here",
            "decline": "Drafted text here"
        }}
        """
        
        response = model.generate_content(prompt, generation_config=generation_config)
        
        return jsonify({
            "status": "success",
            "replies": json.loads(response.text)
        })

    except Exception as e:
        print(f"Error generating smart reply: {e}")
        return jsonify({"error": str(e)}), 500

# --- NEW: AI Rate Calculator ---
@app.route('/api/influencer/calculate-rate', methods=['GET'])
def calculate_ai_rate():
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    try:
        # Fetch creator stats from DB
        profile_res = supabase.table('influencer_profiles').select('*').eq('profile_id', user_id).single().execute()
        stats = profile_res.data
        
        if not stats:
            return jsonify({"error": "Profile not found"}), 404

        views = stats.get('total_views', 0)
        videos = stats.get('video_count', 0)
        engagement = stats.get('engagement_rate', 0) or 0
        niche = stats.get('niche', 'general')

        avg_views = views / videos if videos > 0 else 0
        rate_data = calculate_rate_range(views, videos, engagement, niche)
        min_rate = rate_data['min_rate']
        max_rate = rate_data['max_rate']
        recommended_rate = rate_data['recommended_rate']

        # Ask Gemini to write a personalized explanation
        model = genai.GenerativeModel('gemini-2.5-flash')
        recommended_rate_inr = convert_legacy_amount_to_inr(recommended_rate)
        min_rate_inr = convert_legacy_amount_to_inr(min_rate)
        max_rate_inr = convert_legacy_amount_to_inr(max_rate)

        prompt = f"""
        Act as an Influencer Talent Manager. The creator has {avg_views} average views, a {engagement}% engagement rate, and creates {niche} content.
        I have calculated their fair market rate to be {format_inr_amount(recommended_rate_inr)}.
        Write a short, encouraging 3-sentence explanation of WHY they deserve this rate in INR, mentioning their specific stats.
        """
        explanation = model.generate_content(prompt).text.strip()

        return jsonify({
            "status": "success",
            "data": {
                "min_rate": min_rate_inr,
                "max_rate": max_rate_inr,
                "recommended_rate": recommended_rate_inr,
                "explanation": explanation
            }
        })

    except Exception as e:
        print(f"Rate Calc Error: {e}")
        return jsonify({"error": str(e)}), 500


# --- NEW: AI Profile Polish ---
@app.route('/api/influencer/polish-profile', methods=['POST'])
def polish_profile():
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    data = request.json
    current_bio = data.get('bio', '')
    niche = data.get('niche', '')

    try:
        model = genai.GenerativeModel('gemini-2.5-flash')
        generation_config = genai.GenerationConfig(response_mime_type="application/json")
        
        prompt = f"""
        You are a top-tier PR agent for influencers. Rewrite the following creator bio to make it highly attractive to corporate brand sponsors. 
        It must sound professional, dynamic, and highlight their value in the '{niche}' niche.
        Keep it to 2-3 punchy sentences. Don't use emojis.
        
        Original Bio: "{current_bio}"

        Return a valid JSON exactly like this:
        {{ "polished_bio": "The newly written bio goes here" }}
        """
        
        response = model.generate_content(prompt, generation_config=generation_config)
        return jsonify({
            "status": "success",
            "data": json.loads(response.text)
        })

    except Exception as e:
        print(f"Profile Polish Error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/influencer/profile', methods=['POST'])
def update_influencer_profile():
    """Persist influencer profile edits through the backend so niche updates are reliable."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    profile = get_current_profile(user_id)
    if not profile or profile.get('role') != 'influencer':
        return jsonify({"error": "Influencer access required"}), 403

    data = request.json or {}
    full_name = (data.get('full_name') or '').strip()
    niche = normalize_text(data.get('niche') or '').strip()
    if niche and niche not in NICHE_KEYWORDS:
        return jsonify({"error": "Invalid niche selected"}), 400

    languages = data.get('languages') or []
    if isinstance(languages, str):
        languages = [item.strip() for item in languages.split(',') if item.strip()]
    elif isinstance(languages, list):
        languages = [str(item).strip() for item in languages if str(item).strip()]
    else:
        languages = []

    try:
        if full_name:
            supabase.table('profiles').update({
                'full_name': full_name
            }).eq('id', user_id).execute()

        influencer_payload = {
            'profile_id': user_id,
            'niche': niche or None,
            'bio': (data.get('bio') or '').strip() or None,
            'content_description': (data.get('content_description') or '').strip() or None,
            'sample_video_transcript': (data.get('sample_video_transcript') or '').strip() or None,
            'location': (data.get('location') or '').strip() or None,
            'languages': languages,
            'audience_age': (data.get('audience_age') or '').strip() or None,
            'audience_gender': (data.get('audience_gender') or '').strip() or None,
            'audience_interests': (data.get('audience_interests') or '').strip() or None,
            'audience_location': (data.get('audience_location') or '').strip() or None,
            'instagram_handle': (data.get('instagram_handle') or '').strip() or None,
            'tiktok_handle': (data.get('tiktok_handle') or '').strip() or None,
            'twitter_handle': (data.get('twitter_handle') or '').strip() or None,
            'website_url': (data.get('website_url') or '').strip() or None,
            'availability': (data.get('availability') or '').strip() or 'Available',
            'rate_range': normalize_currency_text(data.get('rate_range') or ''),
        }

        optional_profile_fields = {
            'sample_video_transcript',
            'location',
            'languages',
            'audience_age',
            'audience_gender',
            'audience_interests',
            'audience_location',
            'instagram_handle',
            'tiktok_handle',
            'twitter_handle',
            'website_url',
            'availability',
            'rate_range',
        }

        existing_response = supabase.table('influencer_profiles').select('profile_id').eq('profile_id', user_id).execute()
        existing_rows = existing_response.data or []
        if existing_rows:
            response, dropped_fields = update_with_optional_fields(
                'influencer_profiles',
                influencer_payload,
                lambda query: query.eq('profile_id', user_id),
                optional_fields=optional_profile_fields,
            )
        else:
            response, dropped_fields = insert_with_optional_fields(
                'influencer_profiles',
                influencer_payload,
                optional_fields=optional_profile_fields,
            )

        saved_profile = (response.data or [None])[0]
        return jsonify({
            "status": "success",
            "message": "Profile updated successfully",
            "profile": saved_profile,
            "dropped_fields": sorted(dropped_fields),
        })
    except Exception as exc:
        print(f"Error updating influencer profile: {exc}")
        return jsonify({"error": str(exc)}), 500



# Serve frontend files
@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory(app.static_folder, path)


# --- NEW: YouTube Sync & Calculation Route ---
@app.route('/api/influencer/sync-youtube', methods=['POST', 'OPTIONS'])
def sync_youtube_stats():
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
        
    handle = request.json.get('youtube_handle', '').strip()
    if not handle:
        return jsonify({"error": "YouTube handle is required"}), 400

    handle = handle.replace("https://www.youtube.com/", "").replace("/", "")
    if not handle.startswith('@'):
        handle = f"@{handle}"

    try:
        print(f"\n--- Starting YouTube Sync for {handle} ---")
        
        # 1. Fetch Channel Stats & Uploads Playlist ID
        yt_url = f"https://youtube.googleapis.com/youtube/v3/channels?part=snippet,statistics,contentDetails&forHandle={handle}&key={YOUTUBE_API_KEY}"
        yt_res = requests.get(yt_url).json()

        if "error" in yt_res:
            error_message = yt_res['error'].get('message', 'Unknown YouTube Error')
            return jsonify({"error": f"YouTube API Error: {error_message}"}), 400

        if "items" not in yt_res or not yt_res["items"]:
            return jsonify({"error": f"YouTube channel {handle} not found."}), 404

        channel = yt_res["items"][0]
        title = channel["snippet"]["title"]
        description = channel["snippet"]["description"]
        avatar_url = channel["snippet"]["thumbnails"]["high"]["url"]
        
        # --- NEW: Extract native YouTube Data ---
        country = channel["snippet"].get("country", "")
        language = channel["snippet"].get("defaultLanguage", "")
        languages_array = [language] if language else []

        # --- NEW: Smart Regex to extract social links from description ---
        ig_match = re.search(r'(?:instagram\.com/|ig: @?)([a-zA-Z0-9_.]+)', description, re.I)
        instagram_handle = f"@{ig_match.group(1)}" if ig_match else ""

        tw_match = re.search(r'(?:twitter\.com/|x\.com/|twitter: @?)([a-zA-Z0-9_]+)', description, re.I)
        twitter_handle = f"@{tw_match.group(1)}" if tw_match else ""

        tk_match = re.search(r'(?:tiktok\.com/|tiktok: @?)(@?[a-zA-Z0-9_.]+)', description, re.I)
        tiktok_handle = tk_match.group(1) if tk_match else ""
        if tk_match and not tiktok_handle.startswith('@'): 
            tiktok_handle = f"@{tiktok_handle}"

        # Find first valid http link that isn't a social media site
        website_url = ""
        web_matches = re.findall(r'(https?://[^\s]+)', description, re.I)
        for link in web_matches:
            if not any(social in link for social in ['instagram.com', 'twitter.com', 'x.com', 'tiktok.com', 'youtube.com']):
                website_url = link
                break

        stats = channel["statistics"]
        subscribers = int(stats.get("subscriberCount", 0))
        total_views = int(stats.get("viewCount", 0))
        video_count = int(stats.get("videoCount", 0))
        
        uploads_playlist_id = None
        if "contentDetails" in channel and "relatedPlaylists" in channel["contentDetails"]:
            uploads_playlist_id = channel["contentDetails"]["relatedPlaylists"].get("uploads")

        # 2. Fetch last 5 videos to calculate Engagement Rate
        engagement_rate = 0.0
        if uploads_playlist_id:
            playlist_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId={uploads_playlist_id}&maxResults=5&key={YOUTUBE_API_KEY}"
            playlist_res = requests.get(playlist_url).json()
            
            video_ids = [item["contentDetails"]["videoId"] for item in playlist_res.get("items", [])]
            
            if video_ids:
                video_ids_str = ",".join(video_ids)
                videos_url = f"https://youtube.googleapis.com/youtube/v3/videos?part=statistics&id={video_ids_str}&key={YOUTUBE_API_KEY}"
                videos_res = requests.get(videos_url).json()
                
                recent_views = 0
                recent_engagements = 0
                
                for vid in videos_res.get("items", []):
                    v_stats = vid.get("statistics", {})
                    recent_views += int(v_stats.get("viewCount", 0))
                    recent_engagements += int(v_stats.get("likeCount", 0)) + int(v_stats.get("commentCount", 0))
                
                if recent_views > 0:
                    engagement_rate = round((recent_engagements / recent_views) * 100, 2)

        # 3. Use Gemini AI to generate profile details
        ai_profile = generate_profile_from_description(title, description)
        niche = ai_profile.get('niche', 'lifestyle').lower()

        # 4. Calculate Market Rate
        rate_data = calculate_rate_range(total_views, video_count, engagement_rate, niche)
        calculated_rate_range = f"{format_inr_amount(convert_legacy_amount_to_inr(rate_data['min_rate']))} - {format_inr_amount(convert_legacy_amount_to_inr(rate_data['max_rate']))}"

        # 5. Update Database
        supabase.table('profiles').update({'avatar_url': avatar_url, 'full_name': title}).eq('id', user_id).execute()

        influencer_data = {
            'follower_count': subscribers,
            'total_views': total_views,
            'video_count': video_count,
            'engagement_rate': engagement_rate,
            'youtube_channel': handle,
            'platform': 'youtube',
            'channel_description': description,
            'bio': ai_profile.get('bio', 'Content creator on YouTube.'),
            'niche': niche,
            'audience_age': ai_profile.get('audience_age', '18-24'),
            'audience_gender': ai_profile.get('audience_gender', 'mixed'),
            'audience_interests': ai_profile.get('audience_interests', ''),
            'content_description': ai_profile.get('content_description', ''),
            'rate_range': calculated_rate_range,
            
            # --- NEW AUTO-FETCHED FIELDS ---
            'location': country,
            'languages': languages_array,
            'instagram_handle': instagram_handle,
            'twitter_handle': twitter_handle,
            'tiktok_handle': tiktok_handle,
            'website_url': website_url
        }

        supabase.table('influencer_profiles').update(influencer_data).eq('profile_id', user_id).execute()

        return jsonify({"status": "success", "message": "Synced & Generated", "data": influencer_data})

    except Exception as e:
        print(f"❌ CRITICAL ERROR syncing YouTube: {e}")
        return jsonify({"error": str(e)}), 500
        
    # API Routes
@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy", 
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": "ReachIQ API is running!"
    })

@app.route('/api/test-db', methods=['GET'])
def test_db_connection():
    """Test database connection"""
    try:
        # Try to query profiles table
        response = supabase.table('profiles').select('*').limit(1).execute()
        return jsonify({
            "status": "success",
            "message": "Database connection successful",
            "data": response.data
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Database connection failed: {str(e)}"
        }), 500

# User management API
@app.route('/api/users/profile', methods=['GET'])
def get_user_profile():
    """Get user profile - requires authentication"""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
    
    try:
        # Get user profile from Supabase
        response = supabase.table('profiles').select('*').eq('id', user_id).execute()
        
        if not response.data:
            return jsonify({"error": "User not found"}), 404
            
        return jsonify({
            "status": "success",
            "user": response.data[0]
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/campaigns/<campaign_id>/match-influencers', methods=['GET'])
def match_influencers(campaign_id):
    """AI-powered influencer matching for a campaign"""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
        
    try:
        campaign_response = supabase.table('campaigns').select('*').eq('id', campaign_id).eq('brand_id', user_id).execute()
        if not campaign_response.data:
            return jsonify({"error": "Campaign not found or access denied"}), 404
        
        campaign = campaign_response.data[0]
        
        influencers_response = supabase.table('influencer_profiles').select('*, profile:profiles(*)').execute()
        influencers = influencers_response.data

        matched_influencers = rank_creators_for_campaign(campaign, influencers)
        top_matches = matched_influencers[:12]

        for match in top_matches:
            safe_db_insert(
                'ai_match_runs',
                {
                    'campaign_id': campaign_id,
                    'influencer_id': match.get('profile_id'),
                    'semantic_score': match.get('semantic_similarity'),
                    'weighted_score': match.get('match_score'),
                    'predicted_views': match.get('prediction', {}).get('predicted_views'),
                    'predicted_engagement_rate': match.get('prediction', {}).get('predicted_engagement_rate'),
                    'predicted_cpm': match.get('prediction', {}).get('predicted_cpm'),
                    'conversion_likelihood': match.get('prediction', {}).get('conversion_likelihood'),
                    'pricing_assessment': match.get('prediction', {}).get('pricing_fairness'),
                },
            )

        log_ai_recommendation(
            'semantic_creator_matching',
            user_id,
            'campaign',
            campaign_id,
            {
                'campaign_name': campaign.get('name'),
                'top_match_ids': [match.get('profile_id') for match in top_matches],
                'total_matches': len(matched_influencers),
            },
        )
        
        return jsonify({
            "status": "success",
            "campaign": campaign,
            "matched_influencers": top_matches,
            "total_matches": len(matched_influencers)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/brand/offers', methods=['GET'])
def get_brand_offers():
    """Return brand offers with campaign and influencer details."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    try:
        campaigns_response = supabase.table('campaigns').select('*').eq('brand_id', user_id).execute()
        campaigns = campaigns_response.data or []
        campaign_map = {campaign['id']: campaign for campaign in campaigns}

        if not campaign_map:
            return jsonify({"status": "success", "offers": []})

        offer_response = supabase.table('offers').select('*').in_('campaign_id', list(campaign_map.keys())).execute()
        offers = offer_response.data or []

        influencer_ids = list({offer['influencer_id'] for offer in offers if offer.get('influencer_id')})
        influencer_map = {}
        if influencer_ids:
            influencer_response = supabase.table('profiles').select('id, full_name, username').in_('id', influencer_ids).execute()
            influencer_map = {profile['id']: profile for profile in (influencer_response.data or [])}

        serialized_offers = [
            serialize_brand_offer(
                offer,
                campaign_map.get(offer['campaign_id'], {}),
                influencer_map.get(offer.get('influencer_id'))
            )
            for offer in sorted(offers, key=lambda row: row.get('created_at', ''), reverse=True)
        ]

        return jsonify({"status": "success", "offers": serialized_offers})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/influencer/offers', methods=['GET'])
def get_influencer_offers():
    """Return influencer offers with campaign and brand details."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    try:
        offer_response = supabase.table('offers').select('*').eq('influencer_id', user_id).execute()
        offers = offer_response.data or []

        campaign_ids = list({offer['campaign_id'] for offer in offers if offer.get('campaign_id')})
        campaign_map = {}
        if campaign_ids:
            campaign_response = supabase.table('campaigns').select('*').in_('id', campaign_ids).execute()
            campaign_map = {campaign['id']: campaign for campaign in (campaign_response.data or [])}

        brand_ids = list({campaign['brand_id'] for campaign in campaign_map.values() if campaign.get('brand_id')})
        brand_map = {}
        if brand_ids:
            brand_response = supabase.table('profiles').select('id, full_name').in_('id', brand_ids).execute()
            brand_map = {profile['id']: profile for profile in (brand_response.data or [])}

        serialized_offers = [
            serialize_influencer_offer(
                offer,
                campaign_map.get(offer['campaign_id'], {}),
                brand_map.get(campaign_map.get(offer['campaign_id'], {}).get('brand_id'))
            )
            for offer in sorted(offers, key=lambda row: row.get('created_at', ''), reverse=True)
        ]

        return jsonify({"status": "success", "offers": serialized_offers})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/notifications', methods=['GET'])
def get_notifications():
    """Return lifecycle notifications for the authenticated brand or creator."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    profile = get_current_profile(user_id)
    if not profile:
        return jsonify({"error": "Profile not found"}), 404

    try:
        role = profile.get('role')
        if role == 'brand':
            campaigns_response = supabase.table('campaigns').select('*').eq('brand_id', user_id).execute()
            campaigns = campaigns_response.data or []
            campaign_map = {campaign['id']: campaign for campaign in campaigns}
            if not campaign_map:
                return jsonify({"status": "success", "notifications": []})

            offers_response = supabase.table('offers').select('*').in_('campaign_id', list(campaign_map.keys())).execute()
            offers = offers_response.data or []
            counterpart_ids = list({offer.get('influencer_id') for offer in offers if offer.get('influencer_id')})
            counterpart_response = (
                supabase.table('profiles').select('id, full_name, username').in_('id', counterpart_ids).execute()
                if counterpart_ids else None
            )
            counterpart_map = {row['id']: row for row in ((counterpart_response.data or []) if counterpart_response else [])}
        elif role == 'influencer':
            offers_response = supabase.table('offers').select('*').eq('influencer_id', user_id).execute()
            offers = offers_response.data or []
            campaign_ids = list({offer.get('campaign_id') for offer in offers if offer.get('campaign_id')})
            campaign_response = (
                supabase.table('campaigns').select('*').in_('id', campaign_ids).execute()
                if campaign_ids else None
            )
            campaign_map = {row['id']: row for row in ((campaign_response.data or []) if campaign_response else [])}
            counterpart_ids = list({campaign.get('brand_id') for campaign in campaign_map.values() if campaign.get('brand_id')})
            counterpart_response = (
                supabase.table('profiles').select('id, full_name, username').in_('id', counterpart_ids).execute()
                if counterpart_ids else None
            )
            counterpart_map = {row['id']: row for row in ((counterpart_response.data or []) if counterpart_response else [])}
        else:
            return jsonify({"status": "success", "notifications": []})

        if not offers:
            return jsonify({"status": "success", "notifications": []})

        offer_ids = [offer.get('id') for offer in offers if offer.get('id')]
        event_map = {}
        try:
            if offer_ids:
                event_response = supabase.table('offer_events').select('*').in_('offer_id', offer_ids).execute()
                for event in (event_response.data or []):
                    event_map.setdefault(event.get('offer_id'), []).append(event)
        except Exception:
            event_map = {}

        notifications = []
        for offer in offers:
            offer_id = offer.get('id')
            campaign = campaign_map.get(offer.get('campaign_id')) or {}
            counterpart = (
                counterpart_map.get(offer.get('influencer_id'))
                if role == 'brand'
                else counterpart_map.get(campaign.get('brand_id'))
            ) or {}

            events = event_map.get(offer_id)
            if events is None:
                events = get_offer_events(offer_id)

            existing_kinds = []
            for event in events:
                notification = build_offer_notification(role, user_id, event, offer, campaign, counterpart)
                if notification:
                    notifications.append(notification)
                    existing_kinds.append(notification.get('kind'))

            notifications.extend(
                synthesize_offer_notifications(role, offer, campaign, counterpart, existing_kinds=existing_kinds)
            )

        unique_notifications = {}
        for notification in notifications:
            unique_notifications[notification.get('id')] = notification

        ordered_notifications = sorted(unique_notifications.values(), key=lambda item: item.get('created_at') or '', reverse=True)
        return jsonify({"status": "success", "notifications": ordered_notifications[:50]})
    except Exception as exc:
        print(f"Error loading notifications: {exc}")
        return jsonify({"error": "Failed to load notifications"}), 500


def calculate_enhanced_match_score(campaign, influencer_profile, semantic_similarity=None):
    """Enhanced matching algorithm using influencer profile data"""
    return compute_match_score(campaign, influencer_profile, semantic_similarity=semantic_similarity)


# --- NEW: API Route for Parsing VOICE Briefs ---
#
@app.route('/api/campaigns/parse-brief-voice', methods=['POST'])
def parse_campaign_voice():
    """
    Receives an audio file, transcribes it,
    and then parses the text to extract campaign data.
    """
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
    
    if 'brief_audio' not in request.files:
        return jsonify({"error": "No audio file part"}), 400
    
    file = request.files['brief_audio']
    
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    try:
        mime_type = file.mimetype
        print(f"Received voice file with mime_type: {mime_type}")
        
        # --- Step 1: Voice-to-Text ---
        # We pass the file stream object directly to the function
        transcribed_text = transcribe_audio_with_gemini(file.stream, mime_type)
        print(f"Transcribed Text: {transcribed_text}")

        # --- Step 2: Text-to-JSON (Reusing your existing function!) ---
        extracted_data = sanitize_parsed_campaign_data(
            parse_brief_text_with_ai(transcribed_text),
            transcribed_text,
        )
        
        return jsonify({
            "status": "success",
            "message": "Voice brief parsed successfully",
            "brief_data": extracted_data
        })

    except Exception as e:
        print(f"Error parsing voice brief: {e}")
        return jsonify({"error": f"Failed to process voice brief: {str(e)}"}), 500
    


# --- MODIFIED: API Route for Parsing Campaign Documents ---
#
@app.route('/api/campaigns/parse-brief-doc', methods=['POST'])
def parse_campaign_document():
    """
    Parses an uploaded campaign brief document (.pdf, .docx, .txt)
    and returns structured JSON data from the Gemini API.
    """
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
    
    if 'brief_doc' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['brief_doc']
    
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    text = ""
    try:
        if file.filename.endswith('.pdf'):
            doc = fitz.open(stream=file.read(), filetype="pdf")
            for page in doc:
                text += page.get_text()
            doc.close()
        elif file.filename.endswith('.docx'):
            doc = docx.Document(file)
            for para in doc.paragraphs:
                text += para.text + "\n"
        elif file.filename.endswith('.txt'):
            text = file.read().decode('utf-8')
        else:
            return jsonify({"error": "Unsupported file type. Please use .pdf, .docx, or .txt"}), 400

        if not text.strip():
            return jsonify({"error": "Document appears to be empty"}), 400
            
        # --- This is where the *NEW* Gemini Agent does its work ---
        extracted_data = sanitize_parsed_campaign_data(
            parse_brief_text_with_ai(text),
            text,
        )
        
        return jsonify({
            "status": "success",
            "message": "Document parsed successfully",
            "brief_data": extracted_data
        })

    except Exception as e:
        print(f"Error parsing document: {e}")
        return jsonify({"error": f"Failed to process file: {str(e)}"}), 500


@app.route('/api/campaigns/create', methods=['POST'])
def create_campaign():
    """Create a campaign while gracefully handling optional newer schema fields."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    profile = get_current_profile(user_id)
    if not profile or profile.get('role') != 'brand':
        return jsonify({"error": "Brand access required"}), 403

    data = request.json or {}
    campaign_name = (data.get('name') or '').strip()
    brief_text = (data.get('brief_text') or '').strip()
    if not campaign_name or not brief_text:
        return jsonify({"error": "Campaign name and brief are required"}), 400

    campaign_data = {
        'brand_id': user_id,
        'name': campaign_name,
        'brief_text': brief_text,
        'target_audience': (data.get('target_audience') or '').strip() or None,
        'niche': normalize_text(data.get('niche') or '').strip() or None,
        'goals': (data.get('goals') or '').strip() or None,
        'budget_range': normalize_currency_text(data.get('budget_range') or ''),
        'status': (data.get('status') or 'active').strip(),
        'deadline': data.get('deadline') or None,
        'platforms': (data.get('platforms') or '').strip() or None,
        'milestone_views': int(data.get('milestone_views') or 0),
        'timeline_requirements': (data.get('timeline_requirements') or '').strip() or None,
        'currency': 'INR',
    }

    try:
        response, dropped_fields = insert_with_optional_fields(
            'campaigns',
            campaign_data,
            optional_fields={'niche', 'milestone_views', 'timeline_requirements', 'currency'},
        )
        created_campaign = (response.data or [None])[0]
        if not created_campaign:
            return jsonify({"error": "Failed to create campaign"}), 500

        if dropped_fields:
            fallback_goals = embed_metadata(
                created_campaign.get('goals') or campaign_data.get('goals') or '',
                CAMPAIGN_META_PREFIX,
                {
                    'niche': campaign_data.get('niche'),
                    'milestone_views': campaign_data.get('milestone_views'),
                    'timeline_requirements': campaign_data.get('timeline_requirements'),
                    'currency': 'INR',
                },
            )
            supabase.table('campaigns').update({'goals': fallback_goals}).eq('id', created_campaign.get('id')).execute()
            created_campaign['goals'] = fallback_goals
            created_campaign['niche'] = campaign_data.get('niche')
            created_campaign['milestone_views'] = campaign_data.get('milestone_views')
            created_campaign['timeline_requirements'] = campaign_data.get('timeline_requirements')
            created_campaign['currency'] = 'INR'

        return jsonify({
            "status": "success",
            "message": "Campaign created successfully",
            "campaign": created_campaign,
        })
    except Exception as exc:
        print(f"Error creating campaign: {exc}")
        return jsonify({"error": str(exc)}), 500



@app.route('/api/brand/copilot-campaign', methods=['POST'])
def brand_copilot_campaign():
    """Turn a short brand goal into parsed campaign fields for the create form."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    profile = get_current_profile(user_id)
    if not profile or profile.get('role') != 'brand':
        return jsonify({"error": "Brand access required"}), 403

    goal = (request.json or {}).get('goal', '').strip()
    if not goal:
        return jsonify({"error": "A campaign goal is required"}), 400

    def fallback_draft():
        return {
            "name": infer_campaign_name_from_text(goal),
            "brief_text": goal,
            "target_audience": infer_target_audience_from_text(goal),
            "goals": infer_goals_from_text(goal),
            "budget_range": normalize_budget_range_for_form('', goal) or "₹1,00,000 - ₹2,50,000",
            "platforms": "youtube, instagram",
            "outreach_angle": "Lead with audience fit, campaign clarity, and a concise CTA.",
            "budget_reasoning": "This range fits a pilot creator campaign with room for testing and negotiation.",
        }

    draft = generate_json_with_gemini(
        f"""
        You are ReachIQ's campaign parser. Convert the short campaign goal below into structured campaign form fields.

        Goal:
        "{goal}"

        Return valid JSON with exactly these keys:
        - "name"
        - "brief_text"
        - "target_audience"
        - "goals"
        - "budget_range" (must be one of: "₹50,000 - ₹1,00,000", "₹1,00,000 - ₹2,50,000", "₹2,50,000 - ₹5,00,000", "₹5,00,000 - ₹10,00,000", "₹10,00,000+")
        - "platforms" (comma-separated using only instagram, youtube, tiktok, twitter)
        - "outreach_angle"
        - "budget_reasoning"
        
        Rules:
        - Always fill "brief_text" with a useful summary of the campaign.
        - Make "target_audience" specific.
        - Make "goals" a short comma-separated list like "Brand awareness, Product sales".
        - Infer likely platforms if the text implies video creators, reels, shorts, or social launch content.
        - If budget is unclear, return an empty string for "budget_range".
        - Return JSON only.
        """,
        fallback=fallback_draft,
    )

    draft = sanitize_parsed_campaign_data(draft, goal)

    payload = {
        "draft": draft,
    }
    log_ai_recommendation('brand_copilot', user_id, 'campaign_parser', None, payload)
    return jsonify({"status": "success", "data": payload})


def derive_campaign_requirements(campaign, offer=None):
    """Extract lightweight QA requirements from the campaign brief and notes."""
    campaign_state = get_campaign_runtime_state(campaign)
    offer_state = get_offer_runtime_state(offer or {})
    brief_sections = [
        campaign.get('brief_text', ''),
        campaign_state.get('goals', ''),
        campaign_state.get('timeline_requirements', ''),
        offer_state.get('brand_notes', ''),
    ]
    combined_brief = "\n".join(part.strip() for part in brief_sections if str(part).strip())

    required_cta = ''
    cta_match = re.search(r'(use code [^.!?\n]+|visit [^.!?\n]+|link in bio|swipe up|shop now|sign up now)', combined_brief, re.IGNORECASE)
    if cta_match:
        required_cta = cta_match.group(1).strip()

    sentences = [segment.strip(" -•\t") for segment in re.split(r'[\n.!?]+', combined_brief) if segment.strip()]
    talking_points = []
    for sentence in sentences:
        lowered = sentence.lower()
        if len(sentence) < 18:
            continue
        if any(token in lowered for token in ['campaign', 'budget', 'deadline', 'timeline']):
            continue
        if sentence not in talking_points:
            talking_points.append(sentence)
        if len(talking_points) == 4:
            break

    competitors = re.findall(r'(?:avoid|exclude|not\s+mention)\s+([A-Za-z0-9,\s&-]+)', combined_brief, flags=re.IGNORECASE)
    competitor_list = []
    for chunk in competitors:
        competitor_list.extend([name.strip() for name in re.split(r',|/| and ', chunk) if name.strip()])

    return {
        'campaign_brief': combined_brief,
        'required_cta': required_cta,
        'talking_points': talking_points,
        'competitors': competitor_list[:5],
    }


def run_deliverable_review(user_id, video_url='', transcript='', campaign_brief='', required_cta='', talking_points=None, competitors=None, metadata_text='', persist=True):
    """Generate deliverable QA using transcript when available, else video metadata context."""
    talking_points = talking_points or []
    competitors = competitors or []
    source_text = (transcript or metadata_text or campaign_brief or '').strip()
    if not source_text and not campaign_brief:
        raise ValueError("Provide campaign or deliverable context for QA.")

    source_text_lower = source_text.lower()
    cta_pass = required_cta.lower() in source_text_lower if required_cta and source_text else False
    covered_points = [point for point in talking_points if point.lower() in source_text_lower]
    competitor_hits = [name for name in competitors if name.lower() in source_text_lower]
    risk_match = re.search(r'\b(hate|violence|scam|fraud)\b', source_text_lower)

    fallback_review = {
        "summary": "Automated QA generated from campaign requirements and available video context.",
        "overall_score": max(35, min(95, 58 + (15 if cta_pass else 0) + (len(covered_points) * 6) - (len(competitor_hits) * 12))),
        "brand_safety": {
            "level": "medium" if risk_match else "low",
            "notes": "No severe high-risk language detected." if not risk_match else "Potentially risky language detected in the available video context.",
        },
        "cta_compliance": {
            "status": "pass" if cta_pass else "needs_work",
            "evidence": required_cta if cta_pass else ("Required CTA phrase was not found in the available deliverable context." if required_cta else "No explicit CTA requirement was extracted from the campaign."),
        },
        "talking_point_coverage": [
            {"point": point, "status": "covered" if point in covered_points else "missing"}
            for point in talking_points
        ],
        "sentiment": "positive" if re.search(r'\b(love|great|best|recommend)\b', source_text_lower) else "neutral",
        "competitor_mentions": competitor_hits,
        "action_items": [
            "Add or strengthen the CTA line." if required_cta and not cta_pass else "CTA delivery looks aligned with the campaign.",
            "Address missing talking points before approval." if talking_points and len(covered_points) != len(talking_points) else "Talking point coverage looks aligned.",
        ],
    }

    review = generate_json_with_gemini(
        f"""
        You are ReachIQ's deliverable QA assistant. Review the creator deliverable against the brand brief.

        Campaign brief:
        {campaign_brief or "Not provided"}

        Required CTA:
        {required_cta or "Not provided"}

        Required talking points:
        {json.dumps(talking_points)}

        Competitors to flag:
        {json.dumps(competitors)}

        Transcript:
        {transcript or "Not available"}

        Deliverable metadata and context:
        {metadata_text or "Not available"}

        If transcript is unavailable, use the available metadata/context conservatively.

        Return valid JSON with these keys:
        - "summary"
        - "overall_score" (0-100)
        - "brand_safety" {{"level": "low|medium|high", "notes": "..."}}
        - "cta_compliance" {{"status": "pass|needs_work", "evidence": "..."}}
        - "talking_point_coverage" (list of objects with "point" and "status")
        - "sentiment"
        - "competitor_mentions" (list)
        - "action_items" (list of strings)
        """,
        fallback=fallback_review,
    )

    if persist:
        safe_db_insert(
            'deliverable_reviews',
            {
                'brand_id': user_id,
                'video_url': video_url or None,
                'transcript': transcript or None,
                'review_payload': review,
            },
        )
        log_ai_recommendation('deliverable_qa', user_id, 'video', video_url or None, review)

    return review


@app.route('/api/brand/qa-deliverable', methods=['POST'])
def qa_deliverable():
    """Review a creator deliverable for transcript fit, CTA compliance, safety, and competitor mentions."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    profile = get_current_profile(user_id)
    if not profile or profile.get('role') != 'brand':
        return jsonify({"error": "Brand access required"}), 403

    data = request.json or {}
    transcript = (data.get('transcript') or '').strip()
    video_url = (data.get('video_url') or '').strip()
    campaign_brief = (data.get('campaign_brief') or '').strip()
    required_cta = (data.get('required_cta') or '').strip()
    talking_points = [point.strip() for point in (data.get('talking_points') or '').split('\n') if point.strip()]
    competitors = [name.strip() for name in (data.get('competitors') or '').split(',') if name.strip()]
    metadata_text = (data.get('metadata_text') or '').strip()

    review = run_deliverable_review(
        user_id=user_id,
        video_url=video_url,
        transcript=transcript,
        campaign_brief=campaign_brief,
        required_cta=required_cta,
        talking_points=talking_points,
        competitors=competitors,
        metadata_text=metadata_text,
        persist=True,
    )
    return jsonify({"status": "success", "review": review})


@app.route('/api/offers/negotiation-advice', methods=['POST'])
def negotiation_advice():
    """Generate guarded negotiation guidance for brands and creators."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    profile = get_current_profile(user_id)
    data = request.json or {}
    offer_id = data.get('offer_id')
    if not offer_id:
        return jsonify({"error": "Offer ID is required"}), 400

    offer, campaign = get_offer_with_campaign(offer_id)
    if not offer or not campaign:
        return jsonify({"error": "Offer not found"}), 404
    if not can_access_offer(user_id, profile, offer, campaign):
        return jsonify({"error": "Access denied"}), 403
    campaign = {**campaign, **get_campaign_runtime_state(campaign)}

    influencer_profile_res = supabase.table('influencer_profiles').select('*').eq('profile_id', offer.get('influencer_id')).single().execute()
    influencer_profile = influencer_profile_res.data or {}

    history_response = supabase.table('offers').select('*').eq('influencer_id', offer.get('influencer_id')).execute()
    history_stats = collect_offer_history_stats(history_response.data or [])

    creator_rate = parse_budget_value_in_inr(influencer_profile.get('rate_range'))
    campaign_budget = parse_budget_value_in_inr(campaign.get('budget_range'))
    current_offer = (
        normalize_offer_amount(offer.get('negotiated_amount'), campaign)
        or normalize_offer_amount(offer.get('influencer_quote'), campaign)
        or campaign_budget
        or creator_rate
        or 0
    )

    if profile.get('role') == 'brand':
        guardrail_min = round(max(0, min(current_offer, creator_rate or current_offer) * 0.9), 2)
        guardrail_max = round(max(current_offer, campaign_budget or current_offer), 2)
        suggested_amount = round(min(guardrail_max, max(guardrail_min, current_offer * 0.97)), 2)
        stance = 'Hold firm on ROI while showing flexibility on scope and timing.'
    else:
        floor_reference = creator_rate or current_offer or 0
        guardrail_min = round(max(50, floor_reference * 0.9), 2)
        ceiling_reference = max(campaign_budget or floor_reference, floor_reference)
        guardrail_max = round(max(guardrail_min, ceiling_reference * 1.1), 2)
        suggested_amount = round(min(guardrail_max, max(guardrail_min, current_offer or floor_reference or guardrail_min)), 2)
        stance = 'Anchor on your verified engagement and the audience fit you bring.'

    fallback_advice = {
        "recommended_status": "negotiating",
        "suggested_amount": suggested_amount,
        "guardrails": {
            "minimum": guardrail_min,
            "maximum": guardrail_max,
            "approval_required": True,
        },
        "strategy": stance,
        "historical_context": history_stats,
        "draft_message": (
            f"Thanks for the update. Based on campaign scope and creator fit, a fair next step would be {format_inr_amount(suggested_amount)}. "
            "Let us know if that works or if you'd like to adjust deliverables."
        ),
    }

    advice = generate_json_with_gemini(
        f"""
        You are ReachIQ's guarded negotiation assistant.

        Requester role: {profile.get('role')}
        Campaign: {campaign.get('name')}
        Campaign budget range: {campaign.get('budget_range')}
        Creator rate range: {influencer_profile.get('rate_range')}
        Current offer state: {json.dumps(offer)}
        Historical outcomes: {json.dumps(history_stats)}

        Return valid JSON with these keys:
        - "recommended_status"
        - "suggested_amount"
        - "guardrails" with keys "minimum", "maximum", "approval_required"
        - "strategy"
        - "historical_context"
        - "draft_message"

        Keep all suggestions inside the likely fair range and never imply auto-approval.
        """,
        fallback=fallback_advice,
    )

    log_ai_recommendation('negotiation_agent', user_id, 'offer', offer_id, advice)
    return jsonify({"status": "success", "advice": advice})


@app.route('/api/influencer/strategy-coach', methods=['GET'])
def influencer_strategy_coach():
    """Generate creator growth and monetization guidance."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    profile = get_current_profile(user_id)
    if not profile or profile.get('role') != 'influencer':
        return jsonify({"error": "Influencer access required"}), 403

    influencer_res = supabase.table('influencer_profiles').select('*').eq('profile_id', user_id).single().execute()
    influencer = influencer_res.data
    if not influencer:
        return jsonify({"error": "Influencer profile not found"}), 404

    offer_history_res = supabase.table('offers').select('*').eq('influencer_id', user_id).execute()
    history_stats = collect_offer_history_stats(offer_history_res.data or [])

    missing_profile_fields = [
        label for label, key in [
            ('bio depth', 'bio'),
            ('content positioning', 'content_description'),
            ('audience interests', 'audience_interests'),
            ('audience location', 'audience_location'),
            ('social links', 'instagram_handle'),
        ]
        if not influencer.get(key)
    ]

    fallback_strategy = {
        "content_gaps": missing_profile_fields or ["Double down on repeatable series formats for sponsors."],
        "sponsor_fit": [
            influencer.get('niche', 'lifestyle'),
            'consumer brands with strong creator-led education angles',
        ],
        "niche_positioning": (
            f"Position yourself as a {influencer.get('niche', 'creator')} specialist with clear audience value and reliable engagement."
        ),
        "posting_advice": [
            "Publish one repeatable pillar format each week.",
            "Turn your best-performing topics into sponsor-friendly series.",
        ],
        "pricing_improvements": [
            "Lead with engagement and audience fit before price.",
            "Package one core deliverable with an optional add-on to improve close rate.",
        ],
        "history": history_stats,
    }

    strategy = generate_json_with_gemini(
        f"""
        You are ReachIQ's creator-side AI strategy coach.

        Creator profile:
        {json.dumps(influencer)}

        Offer history summary:
        {json.dumps(history_stats)}

        Return valid JSON with these keys:
        - "content_gaps" (list of strings)
        - "sponsor_fit" (list of sponsor categories)
        - "niche_positioning" (string)
        - "posting_advice" (list of strings)
        - "pricing_improvements" (list of strings)
        - "history" (object)
        """,
        fallback=fallback_strategy,
    )

    log_ai_recommendation('creator_strategy_coach', user_id, 'profile', user_id, strategy)
    return jsonify({"status": "success", "strategy": strategy})


@app.route('/api/offers/submit-deliverable', methods=['POST'])
def submit_deliverable():
    """Allow influencers to submit a campaign video link for brand review."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    profile = get_current_profile(user_id)
    if not profile or profile.get('role') != 'influencer':
        return jsonify({"error": "Influencer access required"}), 403

    data = request.json or {}
    offer_id = data.get('offer_id')
    video_url = (data.get('video_url') or '').strip()
    submission_note = (data.get('submission_note') or '').strip()
    if not offer_id or not video_url:
        return jsonify({"error": "Offer ID and video URL are required"}), 400

    offer, campaign = get_offer_with_campaign(offer_id)
    if not offer or not campaign:
        return jsonify({"error": "Offer not found"}), 404
    if offer.get('influencer_id') != user_id:
        return jsonify({"error": "Access denied"}), 403
    if offer.get('status') != 'accepted':
        return jsonify({"error": "Deliverables can only be submitted after offer acceptance"}), 400

    update_payload = {
        'deliverable_url': video_url,
        'deliverable_status': 'submitted',
        'deliverable_submitted_at': datetime.now(timezone.utc).isoformat(),
        'review_notes': submission_note,
    }
    response, dropped_fields = update_with_optional_fields(
        'offers',
        update_payload,
        lambda query: query.eq('id', offer_id),
        optional_fields={'deliverable_url', 'deliverable_status', 'deliverable_submitted_at', 'review_notes'},
    )
    if dropped_fields:
        workflow_state = get_offer_runtime_state(offer)
        fallback_brand_notes = embed_metadata(
            workflow_state.get('brand_notes', ''),
            OFFER_META_PREFIX,
            {
                **workflow_state,
                'deliverable_url': video_url,
                'deliverable_status': 'submitted',
                'deliverable_submitted_at': update_payload['deliverable_submitted_at'],
                'review_notes': submission_note,
            },
        )
        supabase.table('offers').update({'brand_notes': fallback_brand_notes}).eq('id', offer_id).execute()
    log_offer_event(offer_id, user_id, 'deliverable_submitted', {'video_url': video_url, 'note': submission_note})
    maybe_log_milestone_hit_event(
        offer_id,
        user_id,
        {**offer, **update_payload},
        campaign,
    )

    return jsonify({"status": "success", "message": "Deliverable submitted successfully."})


@app.route('/api/offers/update-fulfillment', methods=['POST'])
def update_fulfillment():
    """Allow brands to review deliverables and progress payment states."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    profile = get_current_profile(user_id)
    if not profile or profile.get('role') != 'brand':
        return jsonify({"error": "Brand access required"}), 403

    data = request.json or {}
    offer_id = data.get('offer_id')
    deliverable_status = data.get('deliverable_status')
    payment_status = data.get('payment_status')
    review_notes = data.get('review_notes')

    offer, campaign = get_offer_with_campaign(offer_id)
    if not offer or not campaign:
        return jsonify({"error": "Offer not found"}), 404
    if campaign.get('brand_id') != user_id:
        return jsonify({"error": "Access denied"}), 403

    update_payload = {}
    if deliverable_status:
        update_payload['deliverable_status'] = deliverable_status
    if review_notes is not None:
        update_payload['review_notes'] = review_notes
    if payment_status:
        update_payload['payment_status'] = payment_status
        if payment_status == 'paid':
            update_payload['payment_released_at'] = datetime.now(timezone.utc).isoformat()
            if offer.get('negotiated_amount') not in (None, ''):
                update_payload['payment_amount'] = offer.get('negotiated_amount')

    response, dropped_fields = update_with_optional_fields(
        'offers',
        update_payload,
        lambda query: query.eq('id', offer_id),
        optional_fields={'deliverable_status', 'review_notes', 'payment_status', 'payment_released_at', 'payment_amount'},
    )
    if dropped_fields:
        workflow_state = get_offer_runtime_state(offer)
        fallback_brand_notes = embed_metadata(
            workflow_state.get('brand_notes', ''),
            OFFER_META_PREFIX,
            {
                **workflow_state,
                **update_payload,
            },
        )
        supabase.table('offers').update({'brand_notes': fallback_brand_notes}).eq('id', offer_id).execute()
    log_offer_event(
        offer_id,
        user_id,
        'deliverable_reviewed',
        {
            'deliverable_status': deliverable_status,
            'payment_status': payment_status,
            'review_notes': review_notes,
        },
    )
    return jsonify({"status": "success", "message": "Fulfillment updated successfully."})


@app.route('/api/offers/<offer_id>/analytics', methods=['GET'])
def get_offer_analytics(offer_id):
    """Return analytics and lifecycle context for a submitted deliverable."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    profile = get_current_profile(user_id)
    offer, campaign = get_offer_with_campaign(offer_id)
    if not offer or not campaign:
        return jsonify({"error": "Offer not found"}), 404
    if not can_access_offer(user_id, profile, offer, campaign):
        return jsonify({"error": "Access denied"}), 403
    offer_state = get_offer_runtime_state(offer)
    campaign_state = get_campaign_runtime_state(campaign)
    resolved_offer = {**offer, **offer_state}
    resolved_campaign = {**campaign, **campaign_state}
    if not resolved_offer.get('deliverable_url'):
        return jsonify({"error": "No deliverable submitted for this offer yet."}), 400

    try:
        milestone_target = int(resolved_campaign.get('milestone_views') or 0)
        analytics = analyze_video_payload(resolved_offer.get('deliverable_url'), milestone_target)
        maybe_log_milestone_hit_event(
            offer_id,
            user_id,
            resolved_offer,
            resolved_campaign,
            analytics=analytics,
        )
        events = get_offer_events(offer_id)
        timeline = build_offer_timeline(resolved_offer, resolved_campaign, events, analytics=analytics)
        negotiation_summary = build_negotiation_summary(resolved_offer, resolved_campaign, events)
        qa_requirements = derive_campaign_requirements(resolved_campaign, resolved_offer)
        metadata_text = "\n".join(filter(None, [
            f"Video title: {analytics.get('title', '')}",
            f"Channel: {analytics.get('channel_name', '')}",
            f"Description: {analytics.get('video_meta', {}).get('description', '')}",
            f"Tags: {', '.join(analytics.get('video_meta', {}).get('tags', []))}",
        ]))
        auto_review = run_deliverable_review(
            user_id=user_id,
            video_url=resolved_offer.get('deliverable_url'),
            campaign_brief=qa_requirements.get('campaign_brief', ''),
            required_cta=qa_requirements.get('required_cta', ''),
            talking_points=qa_requirements.get('talking_points', []),
            competitors=qa_requirements.get('competitors', []),
            metadata_text=metadata_text,
            persist=False,
        )
        creator_res = supabase.table('profiles').select('id, full_name, username').eq('id', offer.get('influencer_id')).single().execute()
        creator = creator_res.data or {}

        decision_summary = {
            'milestone_reached': analytics['milestone'].get('is_reached'),
            'payment_recommendation': (
                'Milestone reached. Safe to move to payment processing after final brief review.'
                if analytics['milestone'].get('is_reached')
                else 'Milestone not reached yet. Keep escrow pending or request more delivery time.'
            ),
            'deliverable_status': resolved_offer.get('deliverable_status', 'not_submitted'),
            'payment_status': resolved_offer.get('payment_status', 'escrow_pending'),
        }

        return jsonify({
            "status": "success",
            "data": analytics,
            "review": auto_review,
            "qa_requirements": qa_requirements,
            "offer": {
                "id": offer.get('id'),
                "status": offer.get('status'),
                "deliverable_url": resolved_offer.get('deliverable_url'),
                "deliverable_status": resolved_offer.get('deliverable_status', 'not_submitted'),
                "deliverable_submitted_at": resolved_offer.get('deliverable_submitted_at'),
                "review_notes": resolved_offer.get('review_notes', ''),
                "payment_status": resolved_offer.get('payment_status', 'escrow_pending'),
                "payment_amount": normalize_offer_amount(resolved_offer.get('payment_amount') or offer.get('negotiated_amount'), resolved_campaign),
                "negotiation_summary": negotiation_summary,
                "timeline": timeline,
                "creator": creator,
                "campaign": {
                    "id": campaign.get('id'),
                    "name": campaign.get('name'),
                    "status": campaign.get('status'),
                    "niche": resolved_campaign.get('niche') or '',
                    "milestone_views": resolved_campaign.get('milestone_views') or 0,
                    "timeline_requirements": resolved_campaign.get('timeline_requirements') or '',
                    "budget_range": normalize_currency_text(campaign.get('budget_range')),
                },
                "decision_summary": decision_summary,
            }
        })
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except LookupError as exc:
        return jsonify({"error": str(exc)}), 404
    except Exception as exc:
        print(f"Error loading offer analytics: {exc}")
        return jsonify({"error": "Failed to load analytics."}), 500


@app.route('/api/brand/campaigns', methods=['GET'])
def get_brand_campaigns():
    """Return campaign management data with offer lifecycle summaries."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    try:
        profile = get_current_profile(user_id)
        if not profile or profile.get('role') != 'brand':
            return jsonify({"error": "Brand access required"}), 403

        campaigns_response = supabase.table('campaigns').select('*').eq('brand_id', user_id).order('created_at', desc=True).execute()
        campaigns = campaigns_response.data or []
        campaign_map = {campaign['id']: campaign for campaign in campaigns}

        if not campaigns:
            return jsonify({"status": "success", "campaigns": []})

        offers_response = supabase.table('offers').select('*').in_('campaign_id', list(campaign_map.keys())).execute()
        offers = offers_response.data or []
        influencer_ids = list({offer.get('influencer_id') for offer in offers if offer.get('influencer_id')})
        influencer_map = {}
        if influencer_ids:
            influencer_response = supabase.table('profiles').select('id, full_name, username').in_('id', influencer_ids).execute()
            influencer_map = {profile['id']: profile for profile in (influencer_response.data or [])}

        grouped_offers = {campaign_id: [] for campaign_id in campaign_map.keys()}
        for offer in offers:
            grouped_offers.setdefault(offer['campaign_id'], []).append(offer)

        campaign_payload = []
        for campaign in campaigns:
            campaign_state = get_campaign_runtime_state(campaign)
            related_offers = [
                serialize_brand_offer(offer, campaign, influencer_map.get(offer.get('influencer_id')))
                for offer in sorted(grouped_offers.get(campaign['id'], []), key=lambda row: row.get('created_at', ''), reverse=True)
            ]
            submitted = sum(1 for offer in related_offers if offer.get('deliverable_url'))
            milestone_hit = 0
            for offer in related_offers:
                if offer.get('deliverable_url'):
                    try:
                        analytics = analyze_video_payload(offer['deliverable_url'], int(campaign_state.get('milestone_views') or 0))
                        offer['analytics_preview'] = {
                            'views': analytics['metrics']['views'],
                            'milestone_reached': analytics['milestone']['is_reached'],
                        }
                        if analytics['milestone']['is_reached']:
                            milestone_hit += 1
                    except Exception:
                        offer['analytics_preview'] = None

            campaign_payload.append({
                'id': campaign.get('id'),
                'name': campaign.get('name'),
                'status': campaign.get('status'),
                'brief_text': campaign.get('brief_text'),
                'target_audience': campaign.get('target_audience'),
                'niche': campaign_state.get('niche') or '',
                'budget_range': normalize_currency_text(campaign.get('budget_range')),
                'milestone_views': campaign_state.get('milestone_views') or 0,
                'timeline_requirements': campaign_state.get('timeline_requirements') or '',
                'deadline': campaign.get('deadline'),
                'created_at': campaign.get('created_at'),
                'currency': campaign_state.get('currency', 'INR'),
                'offers': related_offers,
                'submitted_deliverables': submitted,
                'milestone_hits': milestone_hit,
                'can_analyze': campaign.get('status') == 'completed' and submitted > 0,
            })

        return jsonify({"status": "success", "campaigns": campaign_payload})
    except Exception as exc:
        print(f"Error loading brand campaigns: {exc}")
        return jsonify({"error": str(exc)}), 500


# Send offer to influencer
@app.route('/api/offers/send', methods=['POST'])
def send_offer():
    """Send an offer to an influencer"""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
        
    try:
        data = request.json
        campaign_id = data.get('campaign_id')
        influencer_id = data.get('influencer_id')
        brand_notes = data.get('brand_notes', '')
        
        # Verify campaign belongs to the brand
        campaign_response = supabase.table('campaigns').select('*').eq('id', campaign_id).eq('brand_id', user_id).execute()
        if not campaign_response.data:
            return jsonify({"error": "Campaign not found or access denied"}), 404
        
        # Create offer
        offer_data = {
            'campaign_id': campaign_id,
            'influencer_id': influencer_id,
            'status': 'pending',
            'brand_notes': brand_notes,
            'brand_budget_range': campaign_response.data[0].get('budget_range', ''),
            'deliverable_status': 'not_submitted',
            'payment_status': 'escrow_pending',
        }
        
        response, dropped_fields = insert_with_optional_fields(
            'offers',
            offer_data,
            optional_fields={'deliverable_status', 'payment_status'},
        )
        
        if response.data:
            created_offer = response.data[0]
            log_offer_event(
                created_offer.get('id'),
                user_id,
                'offer_sent',
                {
                    'campaign_id': campaign_id,
                    'influencer_id': influencer_id,
                    'brand_notes': brand_notes,
                },
            )
            warning = None
            if dropped_fields:
                warning = (
                    "Your database is missing workflow columns for escrow tracking. "
                    "Apply supabase/migrations/003_campaign_delivery_workflow.sql to enable deliverable and payment status storage."
                )
            return jsonify({
                "status": "success",
                "message": "Offer sent successfully",
                "offer": created_offer,
                "warning": warning,
            })
        else:
            return jsonify({"error": "Failed to create offer"}), 500
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/api/brand/analyze-video', methods=['POST', 'OPTIONS'])
def analyze_video():
    """Analyzes a specific YouTube video for ROI and performance tracking."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
        
    data = request.json
    url = data.get('youtube_url', '')
    milestone_target = int(data.get('milestone') or 0)

    try:
        response_data = analyze_video_payload(url, milestone_target)
        return jsonify({"status": "success", "data": response_data})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except LookupError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        print(f"Error analyzing video: {e}")
        return jsonify({"error": "Failed to connect to YouTube API."}), 500
    
@app.route('/api/brand/dashboard-stats', methods=['GET'])
def get_brand_dashboard_stats():
    """Get dashboard statistics for brand"""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
        
    try:
        campaigns_response = supabase.table('campaigns').select('id, status').eq('brand_id', user_id).execute()
        campaigns = campaigns_response.data or []
        campaign_ids = [campaign['id'] for campaign in campaigns]

        offers = []
        if campaign_ids:
            offers_response = supabase.table('offers').select('status, negotiated_amount, campaign_id').in_('campaign_id', campaign_ids).execute()
            offers = offers_response.data or []

        active_campaigns = sum(1 for campaign in campaigns if campaign.get('status') == 'active')
        pending_offers = sum(1 for offer in offers if offer.get('status') == 'pending')
        completed_deals = sum(1 for offer in offers if offer.get('status') == 'accepted')
        campaign_lookup = {campaign['id']: campaign for campaign in campaigns}
        total_spent = sum(
            (normalize_offer_amount(offer.get('negotiated_amount'), campaign_lookup.get(offer.get('campaign_id'))) or 0)
            for offer in offers if offer.get('status') == 'accepted'
        )

        return jsonify({
            "status": "success",
            "stats": {
                "active_campaigns": active_campaigns,
                "pending_offers": pending_offers,
                "completed_deals": completed_deals,
                "total_spent": total_spent
            }
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
