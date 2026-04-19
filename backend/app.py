from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from supabase import create_client, Client
import os
from datetime import datetime, timezone, timedelta
import threading
import time
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
import uuid
import math
from difflib import SequenceMatcher
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
    score_audience_alignment,
    score_platform_alignment,
    summarize_match_reasons,
    parse_budget_value,
    compute_roi_value,
    engagement_score_from_rate,
    normalize_metric,
    score_follower_scale,
    fallback_creator_rate,
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
MILESTONE_TRACKER_INTERVAL_SECONDS = max(60, int(os.getenv('MILESTONE_TRACKER_INTERVAL_SECONDS', '300')))
MILESTONE_TRACKER_ENABLED = os.getenv('MILESTONE_TRACKER_ENABLED', '1').lower() not in {'0', 'false', 'no'}
TRACKER_OPTIONAL_FIELDS = {
    'tracking_enabled',
    'tracking_started_at',
    'last_tracked_at',
    'last_known_views',
    'milestone_hit_at',
    'milestone_notified',
    'deliverable_status',
    'payment_status',
}
VIDEO_SNAPSHOT_CACHE_TTL_SECONDS = max(30, int(os.getenv('VIDEO_SNAPSHOT_CACHE_TTL_SECONDS', '180')))
YOUTUBE_ANALYTICS_TIMEOUT_SECONDS = max(4, int(os.getenv('YOUTUBE_ANALYTICS_TIMEOUT_SECONDS', '8')))
_video_snapshot_cache = {}
_milestone_tracker_thread = None
_milestone_tracker_lock = threading.Lock()
_milestone_tracker_state = {
    'running': False,
    'started_at': None,
    'last_run_at': None,
    'last_checked_offers': 0,
    'last_hit_count': 0,
    'last_error': None,
}

# --- AI provider configuration ---
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
GEMINI_ENABLED = bool(GEMINI_API_KEY)
if GEMINI_ENABLED:
    genai.configure(api_key=GEMINI_API_KEY)
else:
    print("Warning: GEMINI_API_KEY not found. Gemini-backed features will use fallbacks or return limited functionality.")

OLLAMA_BASE_URL = (os.getenv('OLLAMA_BASE_URL') or 'http://127.0.0.1:11434').rstrip('/')
OLLAMA_MODEL = (os.getenv('OLLAMA_MODEL') or 'qwen3:4b').strip() or 'qwen3:4b'
GROQ_API_KEY = (os.getenv('GROQ_API_KEY') or '').strip()
GROQ_BASE_URL = (os.getenv('GROQ_BASE_URL') or 'https://api.groq.com/openai/v1').rstrip('/')
GROQ_MODEL = (os.getenv('GROQ_MODEL') or 'llama-3.1-8b-instant').strip() or 'llama-3.1-8b-instant'
ASSISTANT_LLM_PROVIDER = (os.getenv('ASSISTANT_LLM_PROVIDER') or 'groq').strip().lower()
OLLAMA_TIMEOUT_SECONDS = max(8, int(os.getenv('OLLAMA_TIMEOUT_SECONDS', '20')))
GROQ_TIMEOUT_SECONDS = max(8, int(os.getenv('GROQ_TIMEOUT_SECONDS', '20')))

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
            'negotiation_flexibility': campaign_state.get('negotiation_flexibility', 0),
            'currency': campaign_state.get('currency', 'INR'),
        },
        'influencer': {
            'id': influencer.get('id') if influencer else None,
            'full_name': influencer.get('full_name') if influencer else 'Unknown Creator',
            'username': influencer.get('username') if influencer else '',
            'avatar_url': influencer.get('avatar_url') if influencer else '',
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
        'brand_avatar_url': brand.get('avatar_url') if brand else '',
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
            'negotiation_flexibility': campaign_state.get('negotiation_flexibility', 0),
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


def resolve_offer_payment_amount(offer, campaign=None):
    """Choose the best available payout amount for an offer in INR."""
    offer_state = get_offer_runtime_state(offer or {})
    for candidate in (
        offer_state.get('payment_amount'),
        offer.get('negotiated_amount') if offer else None,
        offer.get('influencer_quote') if offer else None,
    ):
        normalized = normalize_offer_amount(candidate, campaign)
        if normalized not in (None, ''):
            return round(float(normalized), 2)

    fallback_budget = parse_budget_value_in_inr((campaign or {}).get('budget_range'))
    if fallback_budget not in (None, ''):
        return round(float(fallback_budget), 2)
    return 0.0


def parse_campaign_deadline_value(value):
    """Parse a campaign deadline from ISO or simple human-readable strings."""
    if value in (None, ''):
        return None

    text = str(value).strip()
    for candidate in (
        text,
        text.replace('Z', '+00:00'),
    ):
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            pass

    for pattern in ('%Y-%m-%d', '%d-%m-%Y', '%B %d, %Y', '%b %d, %Y'):
        try:
            return datetime.strptime(text, pattern)
        except ValueError:
            continue
    return None


def get_offer_live_analytics_for_campaign(offer, campaign):
    """Fetch live YouTube analytics for a submitted deliverable, with stored-state fallback."""
    offer_state = get_offer_runtime_state(offer or {})
    deliverable_url = offer_state.get('deliverable_url')
    milestone_target = int(get_campaign_runtime_state(campaign or {}).get('milestone_views') or 0)

    if not deliverable_url:
        return None

    try:
        analytics = analyze_video_payload(deliverable_url, milestone_target, prefer_fresh=True)
        return {
            'views': int(analytics.get('metrics', {}).get('views') or 0),
            'likes': int(analytics.get('metrics', {}).get('likes') or 0),
            'comments': int(analytics.get('metrics', {}).get('comments') or 0),
            'engagement_rate': float(analytics.get('metrics', {}).get('engagement_rate') or 0),
            'milestone_reached': bool(analytics.get('milestone', {}).get('is_reached')),
            'title': analytics.get('title'),
            'source': analytics.get('data_source') or 'youtube_live',
        }
    except Exception as exc:
        print(f"Campaign analytics live fetch failed for offer {offer.get('id')}: {exc}")
        try:
            fallback_views = int(float(offer_state.get('last_known_views') or 0))
        except (TypeError, ValueError):
            fallback_views = 0
        return {
            'views': fallback_views,
            'likes': 0,
            'comments': 0,
            'engagement_rate': 0.0,
            'milestone_reached': (
                offer_state.get('deliverable_status') == 'milestone_hit'
                or offer_state.get('payment_status') in {'awaiting_brand_release', 'payment_processing', 'paid'}
                or bool(offer_state.get('milestone_hit_at'))
            ),
            'title': None,
            'source': 'tracker_cache',
        }


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


def extract_percentage_value(text):
    """Extract a percentage integer like 10 from text such as '10%'."""
    match = re.search(r'(\d+(?:\.\d+)?)\s*%', str(text or ''))
    if not match:
        return 0
    try:
        return int(round(float(match.group(1))))
    except Exception:
        return 0


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


def assistant_extract_labeled_campaign_fields(text):
    """Extract campaign fields from structured multi-line brief text."""
    raw_text = str(text or '')
    if not raw_text.strip():
        return {}

    label_aliases = {
        'name': {'campaign name', 'name'},
        'target_audience': {'our target', 'target audience', 'target'},
        'niche': {'campaign niche', 'niche'},
        'goals_message': {'campaign goals & message', 'campaign goals and message', 'goals & message', 'goals and message', 'campaign goals', 'goals', 'message'},
        'deliverables': {'creator deliverables', 'deliverables'},
        'budget': {'budget'},
        'deadline': {'campaign deadline', 'deadline'},
        'milestone': {'milestone views target', 'milestone target', 'milestone'},
        'negotiation_flexibility': {'negotiation flexibility', 'negotiation flexibility percent', 'flexibility', 'negotiation percent'},
        'platforms': {'preferred platforms', 'platforms'},
        'timeline': {'timeline / requirements', 'timeline requirements', 'requirements', 'timeline'},
    }

    sections = {}
    current_key = None
    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        label_match = re.match(r'^([A-Za-z][A-Za-z0-9 /&\'’()-]{1,60}):\s*(.*)$', line)
        if label_match:
            label = normalize_text(label_match.group(1))
            value = label_match.group(2).strip()
            matched_key = next((key for key, aliases in label_aliases.items() if label in aliases), None)
            if matched_key:
                current_key = matched_key
                sections[current_key] = value
                continue
        if current_key:
            sections[current_key] = (sections.get(current_key, '') + '\n' + line).strip()

    return sections


def assistant_parse_campaign_brief_text(text):
    """Build a deterministic campaign draft from a structured campaign brief."""
    sections = assistant_extract_labeled_campaign_fields(text)
    if not sections:
        return {}

    goals_message = sections.get('goals_message', '')
    deliverables = sections.get('deliverables', '')
    timeline = sections.get('timeline', '')
    combined_summary = '\n'.join(part for part in [goals_message, deliverables, timeline] if part).strip()

    parsed = {
        'name': sections.get('name', '').strip(),
        'brief_text': combined_summary or str(text or '').strip(),
        'target_audience': sections.get('target_audience', '').strip(),
        'niche': normalize_text(sections.get('niche', '')).strip(),
        'goals': infer_goals_from_text(goals_message or str(text or '')),
        'budget_range': normalize_currency_text(sections.get('budget', '')).strip() or normalize_budget_range_for_form(sections.get('budget', ''), source_text=sections.get('budget', '') or str(text or '')),
        'platforms': infer_platforms_from_text(
            '\n'.join(part for part in [sections.get('platforms', ''), deliverables, goals_message] if part),
            preferred=sections.get('platforms', ''),
        ),
        'deadline': sections.get('deadline', '').strip(),
        'milestone_views': assistant_extract_milestone_views(sections.get('milestone', '') or str(text or '')),
        'negotiation_flexibility': extract_percentage_value(sections.get('negotiation_flexibility', '') or str(text or '')),
        'timeline_requirements': timeline,
    }
    return parsed


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
    raw_budget_range = normalize_currency_text(parsed_data.get('budget_range') or '')
    budget_range = raw_budget_range
    if raw_budget_range and raw_budget_range not in BUDGET_RANGE_OPTIONS:
        normalized_budget_text = normalize_text(raw_budget_range)
        has_explicit_inr_range = bool(re.search(r'\d', raw_budget_range)) and (
            'inr' in normalized_budget_text
            or 'rs' in normalized_budget_text
            or len(extract_inr_amounts_from_text(raw_budget_range)) >= 2
        )
        if not has_explicit_inr_range:
            budget_range = normalize_budget_range_for_form(raw_budget_range, source_text=source_text) or raw_budget_range
    negotiation_flexibility = int(parsed_data.get('negotiation_flexibility') or 0)

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
        "negotiation_flexibility": max(0, min(100, negotiation_flexibility)),
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
        'negotiation_flexibility': campaign.get('negotiation_flexibility') if campaign.get('negotiation_flexibility') not in (None, '') else metadata.get('negotiation_flexibility', 0),
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
        'tracking_enabled': offer.get('tracking_enabled') if offer.get('tracking_enabled') is not None else metadata.get('tracking_enabled', False),
        'tracking_started_at': offer.get('tracking_started_at') or metadata.get('tracking_started_at'),
        'last_tracked_at': offer.get('last_tracked_at') or metadata.get('last_tracked_at'),
        'last_known_views': offer.get('last_known_views') if offer.get('last_known_views') not in (None, '') else metadata.get('last_known_views'),
        'milestone_hit_at': offer.get('milestone_hit_at') or metadata.get('milestone_hit_at'),
        'milestone_notified': offer.get('milestone_notified') if offer.get('milestone_notified') is not None else metadata.get('milestone_notified', False),
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


def activate_offer_tracking(offer, actor_id=None):
    """Enable background milestone tracking for a submitted deliverable."""
    now_iso = datetime.now(timezone.utc).isoformat()
    persist_offer_runtime_fields(
        offer,
        {
            'tracking_enabled': True,
            'tracking_started_at': now_iso,
            'last_tracked_at': now_iso,
        },
        optional_fields=TRACKER_OPTIONAL_FIELDS,
    )
    if actor_id:
        log_offer_event(
            offer.get('id'),
            actor_id,
            'milestone_tracking_started',
            {'started_at': now_iso},
        )


def poll_offer_for_milestone(offer, campaign):
    """Check one accepted deliverable and auto-mark milestone release readiness."""
    offer_state = get_offer_runtime_state(offer)
    campaign_state = get_campaign_runtime_state(campaign)
    deliverable_url = offer_state.get('deliverable_url')
    milestone_target = int(campaign_state.get('milestone_views') or 0)
    payment_status = offer_state.get('payment_status') or 'escrow_pending'

    if not deliverable_url or milestone_target <= 0:
        return {'checked': False, 'hit': False}
    if payment_status in {'awaiting_brand_release', 'payment_processing', 'paid'}:
        return {'checked': False, 'hit': False}

    analytics = analyze_video_payload(deliverable_url, milestone_target)
    views = analytics.get('metrics', {}).get('views', 0)
    now_iso = datetime.now(timezone.utc).isoformat()
    update_payload = {
        'tracking_enabled': True,
        'last_tracked_at': now_iso,
        'last_known_views': views,
    }

    milestone_reached = bool(analytics.get('milestone', {}).get('is_reached'))
    if milestone_reached:
        update_payload.update({
            'deliverable_status': 'milestone_hit',
            'payment_status': 'awaiting_brand_release',
            'milestone_hit_at': now_iso,
            'milestone_notified': True,
        })

    persist_offer_runtime_fields(offer, update_payload, optional_fields=TRACKER_OPTIONAL_FIELDS)

    if milestone_reached:
        maybe_log_milestone_hit_event(
            offer.get('id'),
            offer.get('influencer_id') or offer.get('brand_id'),
            {**offer, **update_payload},
            campaign,
            analytics=analytics,
        )
        log_offer_event(
            offer.get('id'),
            offer.get('influencer_id') or offer.get('brand_id'),
            'payment_release_requested',
            {
                'payment_status': 'awaiting_brand_release',
                'target': milestone_target,
                'views': views,
            },
        )

    return {'checked': True, 'hit': milestone_reached, 'views': views}


def run_milestone_tracker_pass():
    """Poll all eligible submitted deliverables for milestone progress."""
    checked = 0
    hits = 0
    try:
        offers_response = supabase.table('offers').select('*').eq('status', 'accepted').execute()
        offers = offers_response.data or []
        campaign_ids = list({offer.get('campaign_id') for offer in offers if offer.get('campaign_id')})
        campaign_map = {}
        if campaign_ids:
            campaign_response = supabase.table('campaigns').select('*').in_('id', campaign_ids).execute()
            campaign_map = {campaign['id']: campaign for campaign in (campaign_response.data or [])}

        for offer in offers:
            campaign = campaign_map.get(offer.get('campaign_id'))
            if not campaign:
                continue

            offer_state = get_offer_runtime_state(offer)
            if not offer_state.get('deliverable_url'):
                continue
            if offer_state.get('payment_status') in {'awaiting_brand_release', 'payment_processing', 'paid'}:
                continue

            checked += 1
            try:
                result = poll_offer_for_milestone(offer, campaign)
                if result.get('hit'):
                    hits += 1
            except Exception as exc:
                print(f"Milestone tracker skipped offer {offer.get('id')}: {exc}")
    finally:
        _milestone_tracker_state['last_run_at'] = datetime.now(timezone.utc).isoformat()
        _milestone_tracker_state['last_checked_offers'] = checked
        _milestone_tracker_state['last_hit_count'] = hits


def _milestone_tracker_loop():
    """Run periodic milestone scans in the background."""
    _milestone_tracker_state['running'] = True
    _milestone_tracker_state['started_at'] = datetime.now(timezone.utc).isoformat()
    while True:
        try:
            run_milestone_tracker_pass()
            _milestone_tracker_state['last_error'] = None
        except Exception as exc:
            _milestone_tracker_state['last_error'] = str(exc)
            print(f"Milestone tracker error: {exc}")
        time.sleep(MILESTONE_TRACKER_INTERVAL_SECONDS)


def start_milestone_tracker():
    """Start the background milestone tracker once per process."""
    global _milestone_tracker_thread
    if not MILESTONE_TRACKER_ENABLED:
        _milestone_tracker_state['running'] = False
        _milestone_tracker_state['last_error'] = 'Tracker disabled by configuration.'
        return False

    with _milestone_tracker_lock:
        if _milestone_tracker_thread and _milestone_tracker_thread.is_alive():
            return False

        _milestone_tracker_thread = threading.Thread(
            target=_milestone_tracker_loop,
            name='reachiq-milestone-tracker',
            daemon=True,
        )
        _milestone_tracker_thread.start()
        return True


def analyze_video_payload(url, milestone_target, prefer_fresh=False, request_timeout_seconds=None):
    """Fetch and calculate video analytics for a YouTube URL."""
    yt_regex = r'(?:youtube\.com\/(?:[^\/]+\/.+\/|(?:v|e(?:mbed)?)\/|.*[?&]v=|shorts\/)|youtu\.be\/)([a-zA-Z0-9_-]{11})'
    match = re.search(yt_regex, url or '')
    if not match:
        raise ValueError("Could not extract a valid 11-character Video ID from the URL.")

    video_id = match.group(1)
    cached_snapshot = _video_snapshot_cache.get(video_id)
    now_ts = time.time()
    timeout_seconds = request_timeout_seconds or YOUTUBE_ANALYTICS_TIMEOUT_SECONDS
    source = 'tracker_cache'

    if (
        not prefer_fresh
        and cached_snapshot
        and (now_ts - cached_snapshot.get('cached_at', 0)) < VIDEO_SNAPSHOT_CACHE_TTL_SECONDS
    ):
        video = cached_snapshot['video']
    else:
        yt_url = f"https://youtube.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails&id={video_id}&key={YOUTUBE_API_KEY}"
        try:
            response = requests.get(yt_url, timeout=timeout_seconds)
            response.raise_for_status()
            res = response.json()
            if not res.get("items"):
                raise LookupError("Video not found or is set to private.")
            video = res["items"][0]
            _video_snapshot_cache[video_id] = {
                'cached_at': now_ts,
                'video': video,
            }
            source = 'youtube_live'
        except (requests.Timeout, requests.RequestException, LookupError) as exc:
            if cached_snapshot:
                print(f"Falling back to cached YouTube analytics for {video_id}: {exc}")
                video = cached_snapshot['video']
                source = 'tracker_cache'
            else:
                raise

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
        },
        "data_source": source,
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


TRACKER_ENGAGEMENT_BENCHMARKS = {
    'beauty': 4.2,
    'fashion': 4.0,
    'lifestyle': 3.5,
    'tech': 3.1,
    'gaming': 3.4,
    'education': 3.0,
    'fitness': 4.1,
    'food': 4.0,
}

TRACKER_CPM_BENCHMARKS = {
    'beauty': 420.0,
    'fashion': 400.0,
    'lifestyle': 360.0,
    'tech': 320.0,
    'gaming': 280.0,
    'education': 260.0,
    'fitness': 390.0,
    'food': 370.0,
}


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default=0):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_ratio(numerator, denominator, default=0.0):
    denominator = _safe_float(denominator, 0.0)
    if denominator <= 0:
        return default
    return _safe_float(numerator, 0.0) / denominator


def _parse_campaign_deadline(value):
    raw = (value or '').strip()
    if not raw:
        return None
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d', '%d/%m/%Y', '%b %d, %Y', '%B %d, %Y'):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw.replace('Z', '+00:00')).date()
    except ValueError:
        return None


def _geo_list_from_text(text):
    if not text:
        return []
    parts = re.split(r',|/|\|| and ', str(text))
    cleaned = []
    for part in parts:
        value = part.strip()
        if value and value.lower() not in {'mixed', 'global', 'worldwide'}:
            cleaned.append(value)
    return cleaned[:5]


def _build_tracker_trend_series(analytics):
    published_date = _parse_campaign_deadline(analytics.get('published_at'))
    today = datetime.now(timezone.utc).date()
    days_live = max(1, ((today - published_date).days + 1) if published_date else 7)
    point_count = 7 if days_live >= 7 else max(4, days_live)
    weights_seed = [0.08, 0.1, 0.11, 0.13, 0.16, 0.19, 0.23]
    weights = weights_seed[-point_count:]
    weight_total = sum(weights) or 1
    normalized_weights = [value / weight_total for value in weights]

    total_views = _safe_int(analytics.get('metrics', {}).get('views'))
    total_engagement = _safe_int(analytics.get('metrics', {}).get('likes')) + _safe_int(analytics.get('metrics', {}).get('comments'))
    total_engagement_rate = _safe_float(analytics.get('metrics', {}).get('engagement_rate'))

    daily_views = []
    daily_engagement_rates = []
    labels = []
    allocated_views = 0
    allocated_engagement = 0
    for index, weight in enumerate(normalized_weights):
        is_last = index == point_count - 1
        point_views = max(0, total_views - allocated_views) if is_last else round(total_views * weight)
        point_engagements = max(0, total_engagement - allocated_engagement) if is_last else round(total_engagement * weight)
        allocated_views += point_views
        allocated_engagement += point_engagements
        if published_date:
            offset = point_count - index - 1
            labels.append((today - timedelta(days=offset)).strftime('%d %b'))
        else:
            labels.append(f'P{index + 1}')
        daily_views.append(point_views)

        base_rate = _safe_ratio(point_engagements * 100, max(point_views, 1), total_engagement_rate)
        trend_bias = 0.94 + ((index + 1) / max(point_count, 1)) * 0.08
        daily_engagement_rates.append(round(min(max(total_engagement_rate * 1.2, 1.0), base_rate * trend_bias), 2))

    return {
        'labels': labels,
        'views': daily_views,
        'engagement_rate': daily_engagement_rates,
        'is_estimated': True,
    }


def _tracker_badge_tone(status):
    status = (status or '').lower()
    if status in {'completed', 'approve payment', 'underpriced', 'high performer', 'work again', 'safe', 'positive', 'overperforming', 'on track'}:
        return 'success'
    if status in {'needs improvement', 'fair', 'medium performer', 'neutral', 'at risk'}:
        return 'warning'
    return 'danger'


def build_offer_decision_dashboard(campaign, offer, creator_profile, analytics, review):
    """Build decision-focused analytics derived from campaign, creator, QA, and YouTube metrics."""
    metrics = analytics.get('metrics', {})
    milestone = analytics.get('milestone', {})
    campaign_niche = normalize_text(campaign.get('niche') or '')

    payment_amount = normalize_offer_amount(
        offer.get('payment_amount') or offer.get('negotiated_amount') or offer.get('influencer_quote'),
        campaign,
    ) or 0
    views = _safe_int(metrics.get('views'))
    likes = _safe_int(metrics.get('likes'))
    comments = _safe_int(metrics.get('comments'))
    engagements = likes + comments
    engagement_rate = _safe_float(metrics.get('engagement_rate'))
    emv = _safe_float(metrics.get('earned_media_value_inr'))
    milestone_target = _safe_int(milestone.get('target') or campaign.get('milestone_views'))
    progress_ratio = _safe_ratio(views, milestone_target, 0.0) if milestone_target else 0.0

    creator_context = {**(creator_profile or {})}
    creator_context.setdefault('platform', creator_profile.get('platform') if creator_profile else offer.get('platform'))
    creator_context.setdefault('engagement_rate', creator_profile.get('engagement_rate') if creator_profile else None)
    creator_context.setdefault('average_views', creator_profile.get('average_views') if creator_profile else None)

    audience_match = score_audience_alignment(campaign, creator_context)
    audience_match_pct = round(audience_match * 100)
    top_geographies = _geo_list_from_text(creator_context.get('audience_location'))
    campaign_geo_targets = _geo_list_from_text(campaign.get('target_audience'))

    avg_views = derive_creator_average_views(creator_context) or views or 1
    expected_views = max(milestone_target, int(avg_views * 0.9)) if milestone_target else int(avg_views * 0.9)
    engagement_benchmark = TRACKER_ENGAGEMENT_BENCHMARKS.get(campaign_niche, 3.5)
    cpm_benchmark = TRACKER_CPM_BENCHMARKS.get(campaign_niche, 360.0)
    cpm = round(_safe_ratio(payment_amount * 1000, views, 0.0), 2) if payment_amount and views else 0.0
    cost_per_engagement = round(_safe_ratio(payment_amount, engagements, 0.0), 2) if payment_amount and engagements else 0.0
    roi_multiple = round(_safe_ratio(emv, payment_amount, 0.0), 2) if payment_amount else 0.0

    talking_points = review.get('talking_point_coverage') or []
    covered_points = [item for item in talking_points if item.get('status') == 'covered']
    missing_points = [item.get('point') for item in talking_points if item.get('status') != 'covered']
    coverage_pct = round((len(covered_points) / len(talking_points)) * 100) if talking_points else 100
    cta_present = (review.get('cta_compliance', {}).get('status') or '').lower() in {'present', 'compliant', 'yes', 'pass'}
    safety_level = (review.get('brand_safety', {}).get('level') or 'unknown').lower()
    sentiment = (review.get('sentiment') or 'neutral').lower()
    competitor_mentions = review.get('competitor_mentions') or []
    qa_score = _safe_float(review.get('overall_score'), 0.0)

    safety_score = {
        'safe': 100,
        'low': 100,
        'moderate': 72,
        'caution': 68,
        'warning': 58,
        'risky': 35,
        'high': 30,
        'unsafe': 20,
    }.get(safety_level, 60)
    cta_score = 100 if cta_present else 45
    brand_alignment_score = round(
        max(
            0.0,
            min(
                100.0,
                (coverage_pct * 0.4)
                + (cta_score * 0.2)
                + (safety_score * 0.2)
                + (qa_score * 0.2)
                - (min(len(competitor_mentions), 3) * 8),
            ),
        ),
        1,
    )

    pricing_label = 'Overpriced'
    if cpm and cpm <= cpm_benchmark * 0.8 and roi_multiple >= 2.5:
        pricing_label = 'Underpriced'
    elif (cpm and cpm <= cpm_benchmark * 1.05) or roi_multiple >= 1.5:
        pricing_label = 'Fair'

    deadline = _parse_campaign_deadline(campaign.get('deadline'))
    days_remaining = (deadline - datetime.now(timezone.utc).date()).days if deadline else None
    milestone_reached = bool(milestone.get('is_reached'))

    performance_is_weak = (
        (expected_views and views < expected_views * 0.85)
        or (engagement_rate and engagement_rate < engagement_benchmark * 0.75)
    )
    if milestone_reached and offer.get('payment_status') == 'paid':
        campaign_status = 'Completed'
    elif milestone_target and views > milestone_target * 1.2:
        campaign_status = 'Overperforming'
    elif days_remaining is not None and days_remaining < 0 and not milestone_reached:
        campaign_status = 'Delayed'
    elif days_remaining is not None and days_remaining <= 5 and progress_ratio < 0.5:
        campaign_status = 'At Risk'
    elif performance_is_weak:
        campaign_status = 'Underperforming'
    else:
        campaign_status = 'On Track'

    if milestone_reached and brand_alignment_score >= 70 and safety_score >= 68 and not competitor_mentions:
        final_recommendation = 'Approve Payment'
    elif brand_alignment_score < 50 or safety_score <= 40 or competitor_mentions:
        final_recommendation = 'Reject'
    else:
        final_recommendation = 'Needs Improvement'

    views_score = min(1.0, _safe_ratio(views, max(milestone_target or 0, expected_views or 1), 0.0))
    engagement_score = min(1.0, _safe_ratio(engagement_rate, max(engagement_benchmark, 0.1), 0.0))
    roi_score = min(1.0, _safe_ratio(roi_multiple, 3.5, 0.0))
    brand_fit_score = min(1.0, brand_alignment_score / 100.0)
    creator_score = round(
        ((views_score * 0.30) + (engagement_score * 0.25) + (roi_score * 0.25) + (brand_fit_score * 0.20)) * 100,
        1,
    )
    if creator_score >= 75:
        creator_label = 'High performer'
    elif creator_score >= 55:
        creator_label = 'Medium performer'
    else:
        creator_label = 'Low performer'

    if creator_score >= 75 and roi_multiple >= 2 and brand_alignment_score >= 72:
        future_recommendation = 'Work again'
    elif creator_score >= 50 or pricing_label == 'Overpriced':
        future_recommendation = 'Negotiate price'
    else:
        future_recommendation = 'Avoid for similar campaigns'

    return {
        'campaign_status': campaign_status,
        'campaign_status_tone': _tracker_badge_tone(campaign_status),
        'roi_multiple': roi_multiple,
        'milestone_progress_pct': round(min(progress_ratio * 100, 100), 1) if milestone_target else 0,
        'milestone_progress_label': f"{views:,} / {milestone_target:,} views" if milestone_target else f"{views:,} views tracked",
        'final_recommendation': final_recommendation,
        'final_recommendation_tone': _tracker_badge_tone(final_recommendation),
        'expected_views': expected_views,
        'engagement_benchmark': engagement_benchmark,
        'cpm_benchmark': cpm_benchmark,
        'cpm': cpm,
        'cost_per_engagement': cost_per_engagement,
        'pricing_label': pricing_label,
        'pricing_tone': _tracker_badge_tone(pricing_label),
        'talking_point_coverage_pct': coverage_pct,
        'cta_present': cta_present,
        'brand_alignment_score': brand_alignment_score,
        'missing_points': missing_points,
        'brand_safety_level': safety_level,
        'brand_safety_tone': _tracker_badge_tone('safe' if safety_score >= 80 else 'warning' if safety_score >= 55 else 'unsafe'),
        'sentiment': sentiment,
        'sentiment_tone': _tracker_badge_tone('positive' if sentiment == 'positive' else 'neutral' if sentiment == 'neutral' else 'negative'),
        'competitor_mentions': competitor_mentions,
        'audience_match_pct': audience_match_pct,
        'top_geographies': top_geographies,
        'campaign_geo_targets': campaign_geo_targets,
        'creator_score': creator_score,
        'creator_label': creator_label,
        'creator_label_tone': _tracker_badge_tone(creator_label),
        'future_recommendation': future_recommendation,
        'future_recommendation_tone': _tracker_badge_tone(future_recommendation),
        'days_remaining': days_remaining,
        'trend_series': _build_tracker_trend_series(analytics),
        'flags': [
            item for item in [
                'Weak CTA' if not cta_present else '',
                'Missing talking points' if missing_points else '',
                'Competitor mentions detected' if competitor_mentions else '',
                'Negative sentiment' if sentiment == 'negative' else '',
                'Brand safety review required' if safety_score < 55 else '',
            ] if item
        ],
        'metrics': {
            'views': views,
            'likes': likes,
            'comments': comments,
            'engagements': engagements,
            'cost': payment_amount,
            'emv': emv,
        },
    }


def build_notification_target(role, event_type, offer_id, metadata=None):
    """Map a notification event to the most relevant frontend page."""
    metadata = metadata or {}
    if role == 'brand':
        if event_type == 'offer_status_updated':
            status = metadata.get('status') or 'all'
            return f"brand-offers.html?filter={status}&offerId={offer_id}"
        if event_type in {'deliverable_submitted', 'deliverable_reviewed', 'milestone_hit_detected', 'payment_release_requested', 'demo_payment_completed', 'payment_release_held'}:
            return f"video-tracker.html?offer={offer_id}"
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
                message = f"{counterpart_name}'s deliverable for {campaign_name} crossed the {target:,} view milestone. Review and release escrow payment."
            else:
                message = f"{counterpart_name}'s deliverable for {campaign_name} reached the campaign milestone. Review and release escrow payment."
        elif event_type == 'payment_release_requested':
            target = int(metadata.get('target') or 0)
            title = 'Escrow ready for release'
            message = (
                f"{campaign_name} hit the {target:,} view milestone. Approve payment release for {counterpart_name}."
                if target > 0
                else f"{campaign_name} is ready for payment release to {counterpart_name}."
            )
        elif event_type == 'demo_payment_completed':
            amount = format_inr_amount(metadata.get('amount') or resolve_offer_payment_amount(offer, campaign))
            target = int(metadata.get('target') or 0)
            views = int(metadata.get('views') or 0)
            title = 'Payment debited'
            milestone_text = f"{target:,} views" if target > 0 else 'the campaign milestone'
            views_text = f" ({views:,} views reached)" if views > 0 else ''
            message = f"{amount} was debited to release escrow for {campaign_name} after {milestone_text} was hit{views_text}."
        elif event_type == 'payment_release_held':
            title = 'Escrow kept on hold'
            message = f"You chose to keep the escrow payment on hold for {campaign_name}."
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
        elif event_type == 'payment_release_requested':
            title = 'Milestone submitted for payout'
            message = f"{campaign_name} is waiting for the brand to approve escrow release."
        elif event_type == 'demo_payment_completed':
            amount = format_inr_amount(metadata.get('amount') or resolve_offer_payment_amount(offer, campaign))
            target = int(metadata.get('target') or 0)
            views = int(metadata.get('views') or 0)
            title = 'Payment credited'
            milestone_text = f"{target:,} views" if target > 0 else 'the campaign milestone'
            views_text = f" ({views:,} views reached)" if views > 0 else ''
            message = f"{amount} was credited for {campaign_name} after {milestone_text} was hit{views_text}."
        elif event_type == 'payment_release_held':
            title = 'Payout on hold'
            message = f"The brand is holding escrow release for {campaign_name} for now."

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
        elif offer_state.get('payment_status') == 'awaiting_brand_release':
            append_notification(
                'payment_release_requested',
                'Escrow ready for release',
                f"{campaign_name} hit its milestone. Review the analytics and release payment to {counterpart_name}.",
                created_at=offer_state.get('milestone_hit_at') or offer_state.get('deliverable_submitted_at'),
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
        elif offer_state.get('payment_status') == 'awaiting_brand_release':
            append_notification(
                'payment_release_requested',
                'Milestone waiting for payout',
                f"{campaign_name} hit the milestone and is waiting for brand approval to release escrow.",
                created_at=offer_state.get('milestone_hit_at') or offer_state.get('deliverable_submitted_at'),
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


def persist_offer_runtime_fields(offer, payload, optional_fields=None):
    """Update offer workflow state and fall back to embedded metadata when needed."""
    response, dropped_fields = update_with_optional_fields(
        'offers',
        payload,
        lambda query: query.eq('id', offer.get('id')),
        optional_fields=optional_fields or TRACKER_OPTIONAL_FIELDS,
    )
    if dropped_fields:
        workflow_state = get_offer_runtime_state(offer)
        fallback_brand_notes = embed_metadata(
            workflow_state.get('brand_notes', ''),
            OFFER_META_PREFIX,
            {
                **workflow_state,
                **payload,
            },
        )
        supabase.table('offers').update({'brand_notes': fallback_brand_notes}).eq('id', offer.get('id')).execute()
    return response, dropped_fields


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


def extract_json_object_from_text(text):
    """Extract the first JSON object from a text response."""
    raw = (text or '').strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        pass

    start = raw.find('{')
    end = raw.rfind('}')
    if start >= 0 and end > start:
        candidate = raw[start:end + 1]
        try:
            return json.loads(candidate)
        except Exception:
            return None
    return None


def generate_with_ollama(prompt, fallback='', expect_json=False, system_prompt=''):
    """Run a local Ollama request with graceful fallback."""
    try:
        messages = []
        if system_prompt:
            messages.append({'role': 'system', 'content': system_prompt})
        messages.append({'role': 'user', 'content': prompt})

        response = requests.post(
            f"{OLLAMA_BASE_URL}/api/chat",
            json={
                'model': OLLAMA_MODEL,
                'messages': messages,
                'stream': False,
                'options': {
                    'temperature': 0.1 if expect_json else 0.3,
                },
            },
            timeout=OLLAMA_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json() or {}
        content = (
            (((payload.get('message') or {}).get('content')) or '')
            if isinstance(payload, dict)
            else ''
        ).strip()
        content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL | re.IGNORECASE).strip()
        if expect_json:
            parsed = extract_json_object_from_text(content)
            if parsed is not None:
                return parsed
            return fallback() if callable(fallback) else (fallback or {})
        return content or fallback
    except Exception as exc:
        print(f"Ollama generation failed: {exc}")
        return fallback() if callable(fallback) else fallback


def generate_with_groq(prompt, fallback='', expect_json=False, system_prompt=''):
    """Run a Groq chat-completions request with graceful fallback."""
    if not GROQ_API_KEY:
        return fallback() if callable(fallback) else fallback
    try:
        messages = []
        if system_prompt:
            messages.append({'role': 'system', 'content': system_prompt})
        messages.append({'role': 'user', 'content': prompt})

        payload = {
            'model': GROQ_MODEL,
            'messages': messages,
            'temperature': 0.1 if expect_json else 0.3,
        }
        if expect_json:
            payload['response_format'] = {'type': 'json_object'}

        response = requests.post(
            f"{GROQ_BASE_URL}/chat/completions",
            headers={
                'Authorization': f'Bearer {GROQ_API_KEY}',
                'Content-Type': 'application/json',
            },
            json=payload,
            timeout=GROQ_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json() or {}
        content = (
            ((((data.get('choices') or [{}])[0]).get('message') or {}).get('content')) or ''
            if isinstance(data, dict)
            else ''
        ).strip()

        if expect_json:
            parsed = extract_json_object_from_text(content)
            if parsed is not None:
                return parsed
            return fallback() if callable(fallback) else (fallback or {})
        return content or (fallback() if callable(fallback) else fallback)
    except Exception as exc:
        print(f"Groq generation failed: {exc}")
        return fallback() if callable(fallback) else fallback


def generate_json_with_gemini(prompt, fallback=None):
    """Run a Gemini JSON generation with graceful fallback."""
    if not GEMINI_ENABLED:
        return fallback() if callable(fallback) else (fallback or {})
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
    if not GEMINI_ENABLED:
        return fallback
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

    if not GEMINI_ENABLED:
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


def compute_semantic_similarity(campaign, influencer, use_embeddings=True):
    """Blend Gemini embeddings with local semantic similarity for robust ranking."""
    campaign_text = build_campaign_matching_text(campaign)
    creator_text = build_creator_matching_text(influencer)
    local_similarity = local_semantic_similarity(campaign_text, creator_text)

    if not use_embeddings:
        return round(local_similarity, 4), round(local_similarity, 4)

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


def derive_creator_average_views(influencer):
    """Estimate a creator's average views from stored totals."""
    average_views = influencer.get('average_views')
    if average_views not in (None, ''):
        try:
            return float(average_views)
        except (TypeError, ValueError):
            return None

    total_views = influencer.get('total_views')
    video_count = influencer.get('video_count')
    try:
        total_views = float(total_views or 0)
        video_count = float(video_count or 0)
    except (TypeError, ValueError):
        return None
    if total_views <= 0 or video_count <= 0:
        return None
    return total_views / max(video_count, 1)


def infer_campaign_niches(campaign):
    """Infer the most relevant campaign niches from the campaign text."""
    explicit_niche = normalize_text(
        campaign.get('campaign_niche')
        or campaign.get('niche')
        or get_campaign_runtime_state(campaign).get('campaign_niche', '')
        or get_campaign_runtime_state(campaign).get('niche', '')
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

    if creator_niche:
        return 0.0

    best_alignment = 0.0
    for index, niche in enumerate(campaign_niches):
        if niche in creator_text:
            best_alignment = max(best_alignment, 0.58 if index == 0 else 0.48)
        keyword_score = creator_niche_scores.get(niche, 0.0)
        if keyword_score:
            base_score = 0.38 if index == 0 else 0.3
            best_alignment = max(best_alignment, min(0.59, base_score + (keyword_score * 0.5)))

    return round(best_alignment, 4)


def rank_creators_for_campaign(campaign, influencers, use_embeddings=True):
    """Return ranked creator matches with semantic fit and predictive metrics."""
    campaign_niches = infer_campaign_niches(campaign)
    primary_campaign_niche = campaign_niches[0] if campaign_niches else ''
    candidate_rows = []
    for influencer in influencers:
        resolved_niche = resolve_creator_niche(influencer)
        enriched_influencer = {**influencer, 'niche': resolved_niche or influencer.get('niche')}
        platform_match_score = score_platform_alignment(campaign.get('platforms'), enriched_influencer.get('platform'))
        if primary_campaign_niche and resolved_niche:
            if resolved_niche == primary_campaign_niche:
                niche_alignment = 1.0
            else:
                related_score = RELATED_NICHES.get(primary_campaign_niche, {}).get(resolved_niche)
                if related_score is None:
                    continue
                niche_alignment = related_score
        else:
            niche_alignment = score_niche_alignment(campaign, influencer, campaign_niches=campaign_niches)

        if niche_alignment < 0.35:
            continue

        if niche_alignment >= 0.85:
            niche_tier = 1
        elif niche_alignment >= 0.60:
            niche_tier = 2
        else:
            niche_tier = 3

        semantic_similarity, fallback_similarity = compute_semantic_similarity(
            campaign,
            enriched_influencer,
            use_embeddings=use_embeddings,
        )
        audience_match_score = score_audience_alignment(campaign, enriched_influencer)
        engagement_score = engagement_score_from_rate(enriched_influencer.get('engagement_rate'))
        average_views = derive_creator_average_views(enriched_influencer) or 0.0
        video_count = max(float(enriched_influencer.get('video_count') or 0), 0.0)
        followers = max(float(enriched_influencer.get('follower_count') or 0), 0.0)
        completion_rate = enriched_influencer.get('completion_rate')
        if completion_rate in (None, ''):
            completion_rate = enriched_influencer.get('past_completion_rate')
        try:
            completion_rate = float(completion_rate)
        except (TypeError, ValueError):
            completion_rate = 0.6
        if completion_rate > 1:
            completion_rate /= 100.0
        completion_rate = min(1.0, max(0.0, completion_rate))

        preliminary_prediction = predict_campaign_performance(
            campaign,
            enriched_influencer,
            semantic_similarity=semantic_similarity,
            match_score=semantic_similarity,
        )
        asking_price = fallback_creator_rate(campaign, enriched_influencer)
        roi_raw = compute_roi_value(
            preliminary_prediction.get('predicted_views') or average_views,
            enriched_influencer.get('rate_range') or asking_price,
            campaign=campaign,
            influencer_profile=enriched_influencer,
        )

        candidate_rows.append(
            {
                'influencer': enriched_influencer,
                'niche_alignment': niche_alignment,
                'niche_tier': niche_tier,
                'semantic_similarity': semantic_similarity,
                'local_similarity': fallback_similarity,
                'audience_match_score': audience_match_score,
                'platform_match_score': platform_match_score,
                'engagement_score': engagement_score,
                'average_views': average_views,
                'video_count': video_count,
                'follower_count': followers,
                'completion_rate': completion_rate,
                'roi_raw': roi_raw,
                'asking_price': asking_price,
            }
        )

    if not candidate_rows:
        return []

    roi_values = [row['roi_raw'] for row in candidate_rows]
    follower_values = [row['follower_count'] for row in candidate_rows]
    avg_view_logs = [math.log1p(max(row['average_views'], 0.0)) for row in candidate_rows]

    roi_min, roi_max = min(roi_values), max(roi_values)
    follower_min, follower_max = min(follower_values), max(follower_values)
    avg_view_min, avg_view_max = min(avg_view_logs), max(avg_view_logs)

    ranked = []
    for row in candidate_rows:
        influencer = row['influencer']
        consistency_score = min(1.0, row['video_count'] / 60.0) if row['video_count'] else 0.35
        avg_views_score = normalize_metric(
            math.log1p(max(row['average_views'], 0.0)),
            avg_view_min,
            avg_view_max,
            default=0.5,
        )
        content_quality_score = round(
            min(
                1.0,
                max(
                    0.0,
                    (consistency_score * 0.30)
                    + (avg_views_score * 0.45)
                    + (row['completion_rate'] * 0.25),
                ),
            ),
            4,
        )
        roi_score = round(normalize_metric(row['roi_raw'], roi_min, roi_max, default=0.5), 4)
        follower_score = round(score_follower_scale(row['follower_count'], follower_min, follower_max), 4)

        enriched_for_scoring = {
            **influencer,
            'roi_score': roi_score,
            'engagement_score': row['engagement_score'],
            'audience_match_score': row['audience_match_score'],
            'follower_score': follower_score,
            'content_quality_score': content_quality_score,
            'platform_match_score': row['platform_match_score'],
            'niche_tier': row['niche_tier'],
            'niche_alignment': row['niche_alignment'],
            'prediction': {
                'pricing_fairness': {
                    'label': 'overpriced' if roi_score < 0.35 else 'fair' if roi_score < 0.7 else 'underpriced'
                }
            },
        }

        final_score = compute_match_score(
            campaign,
            enriched_for_scoring,
            semantic_similarity=row['semantic_similarity'],
        )
        prediction = predict_campaign_performance(
            campaign,
            influencer,
            semantic_similarity=row['semantic_similarity'],
            match_score=final_score,
        )
        reasons = summarize_match_reasons(
            campaign,
            {
                **influencer,
                **enriched_for_scoring,
            },
            semantic_similarity=row['semantic_similarity'],
            prediction=prediction,
        )

        ranked.append(
            {
                **influencer,
                'niche_alignment': row['niche_alignment'],
                'niche_tier': row['niche_tier'],
                'display_niche': resolve_creator_niche(influencer) or influencer.get('niche') or '',
                'campaign_niches': campaign_niches[:3],
                'semantic_similarity': row['semantic_similarity'],
                'local_similarity': row['local_similarity'],
                'base_match_score': final_score,
                'match_score': final_score,
                'match_percentage': round(final_score * 100),
                'match_reasons': reasons,
                'prediction': prediction,
                'roi_score': roi_score,
                'engagement_score': row['engagement_score'],
                'audience_match_score': row['audience_match_score'],
                'platform_match_score': row['platform_match_score'],
                'follower_score': follower_score,
                'content_quality_score': content_quality_score,
            }
        )

    ranked.sort(
        key=lambda item: (
            -item.get('niche_tier', 3),
            item['match_score'],
            item.get('roi_score', 0),
            item.get('engagement_score', 0),
        ),
        reverse=True,
    )
    return ranked


def rank_remaining_creators_for_campaign(campaign, influencers, excluded_creator_ids=None, use_embeddings=True):
    """Rank creators excluded from the strict matcher so they can still be shown after the best-fit set."""
    excluded_creator_ids = {str(item) for item in (excluded_creator_ids or set()) if str(item)}
    campaign_niches = infer_campaign_niches(campaign)
    candidate_rows = []

    for influencer in influencers:
        creator_id = str(influencer.get('profile_id') or influencer.get('id') or '')
        if creator_id and creator_id in excluded_creator_ids:
            continue

        resolved_niche = resolve_creator_niche(influencer)
        enriched_influencer = {**influencer, 'niche': resolved_niche or influencer.get('niche')}
        niche_alignment = score_niche_alignment(campaign, enriched_influencer, campaign_niches=campaign_niches)
        semantic_similarity, fallback_similarity = compute_semantic_similarity(
            campaign,
            enriched_influencer,
            use_embeddings=use_embeddings,
        )
        audience_match_score = score_audience_alignment(campaign, enriched_influencer)
        engagement_score = engagement_score_from_rate(enriched_influencer.get('engagement_rate'))
        platform_match_score = score_platform_alignment(campaign.get('platforms'), enriched_influencer.get('platform'))
        average_views = derive_creator_average_views(enriched_influencer) or 0.0
        video_count = max(float(enriched_influencer.get('video_count') or 0), 0.0)
        followers = max(float(enriched_influencer.get('follower_count') or 0), 0.0)
        completion_rate = enriched_influencer.get('completion_rate')
        if completion_rate in (None, ''):
            completion_rate = enriched_influencer.get('past_completion_rate')
        try:
            completion_rate = float(completion_rate)
        except (TypeError, ValueError):
            completion_rate = 0.6
        if completion_rate > 1:
            completion_rate /= 100.0
        completion_rate = min(1.0, max(0.0, completion_rate))

        preliminary_prediction = predict_campaign_performance(
            campaign,
            enriched_influencer,
            semantic_similarity=semantic_similarity,
            match_score=max(semantic_similarity, niche_alignment),
        )
        asking_price = fallback_creator_rate(campaign, enriched_influencer)
        roi_raw = compute_roi_value(
            preliminary_prediction.get('predicted_views') or average_views,
            enriched_influencer.get('rate_range') or asking_price,
            campaign=campaign,
            influencer_profile=enriched_influencer,
        )

        candidate_rows.append(
            {
                'influencer': enriched_influencer,
                'niche_alignment': niche_alignment,
                'niche_tier': 4,
                'semantic_similarity': semantic_similarity,
                'local_similarity': fallback_similarity,
                'audience_match_score': audience_match_score,
                'platform_match_score': platform_match_score,
                'engagement_score': engagement_score,
                'average_views': average_views,
                'video_count': video_count,
                'follower_count': followers,
                'completion_rate': completion_rate,
                'roi_raw': roi_raw,
                'asking_price': asking_price,
            }
        )

    if not candidate_rows:
        return []

    roi_values = [row['roi_raw'] for row in candidate_rows]
    follower_values = [row['follower_count'] for row in candidate_rows]
    avg_view_logs = [math.log1p(max(row['average_views'], 0.0)) for row in candidate_rows]

    roi_min, roi_max = min(roi_values), max(roi_values)
    follower_min, follower_max = min(follower_values), max(follower_values)
    avg_view_min, avg_view_max = min(avg_view_logs), max(avg_view_logs)

    ranked = []
    for row in candidate_rows:
        influencer = row['influencer']
        consistency_score = min(1.0, row['video_count'] / 60.0) if row['video_count'] else 0.35
        avg_views_score = normalize_metric(
            math.log1p(max(row['average_views'], 0.0)),
            avg_view_min,
            avg_view_max,
            default=0.5,
        )
        content_quality_score = round(
            min(
                1.0,
                max(
                    0.0,
                    (consistency_score * 0.30)
                    + (avg_views_score * 0.45)
                    + (row['completion_rate'] * 0.25),
                ),
            ),
            4,
        )
        roi_score = round(normalize_metric(row['roi_raw'], roi_min, roi_max, default=0.5), 4)
        follower_score = round(score_follower_scale(row['follower_count'], follower_min, follower_max), 4)

        enriched_for_scoring = {
            **influencer,
            'roi_score': roi_score,
            'engagement_score': row['engagement_score'],
            'audience_match_score': row['audience_match_score'],
            'follower_score': follower_score,
            'content_quality_score': content_quality_score,
            'platform_match_score': row['platform_match_score'],
            'niche_tier': row['niche_tier'],
            'niche_alignment': row['niche_alignment'],
            'prediction': {
                'pricing_fairness': {
                    'label': 'overpriced' if roi_score < 0.35 else 'fair' if roi_score < 0.7 else 'underpriced'
                }
            },
        }

        final_score = compute_match_score(
            campaign,
            enriched_for_scoring,
            semantic_similarity=row['semantic_similarity'],
        )
        prediction = predict_campaign_performance(
            campaign,
            influencer,
            semantic_similarity=row['semantic_similarity'],
            match_score=final_score,
        )
        reasons = summarize_match_reasons(
            campaign,
            {
                **influencer,
                **enriched_for_scoring,
            },
            semantic_similarity=row['semantic_similarity'],
            prediction=prediction,
        )

        ranked.append(
            {
                **influencer,
                'niche_alignment': row['niche_alignment'],
                'niche_tier': row['niche_tier'],
                'display_niche': resolve_creator_niche(influencer) or influencer.get('niche') or '',
                'campaign_niches': campaign_niches[:3],
                'semantic_similarity': row['semantic_similarity'],
                'local_similarity': row['local_similarity'],
                'base_match_score': final_score,
                'match_score': final_score,
                'match_percentage': round(final_score * 100),
                'match_reasons': reasons,
                'prediction': prediction,
                'roi_score': roi_score,
                'engagement_score': row['engagement_score'],
                'audience_match_score': row['audience_match_score'],
                'platform_match_score': row['platform_match_score'],
                'follower_score': follower_score,
                'content_quality_score': content_quality_score,
            }
        )

    ranked.sort(
        key=lambda item: (
            item['match_score'],
            item.get('roi_score', 0),
            item.get('engagement_score', 0),
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
def parse_brief_text_with_ai(document_text, provider='gemini'):
    """
    Analyzes raw text from a document/brief using the configured parser provider
    and extracts structured campaign data.
    """
    prompt = f"""
    You are an expert campaign assistant. Analyze the following campaign brief text
    and extract the key information in a valid JSON format.

    Return ONLY valid JSON with exactly these keys:
    "name", "brief_text", "target_audience", "niche", "goals",
    "budget_range", "platforms", "deadline", "milestone_views",
    "timeline_requirements", "negotiation_flexibility".

    Extraction rules:
    - "name": Use the explicit campaign name if provided. Do not add suffixes like "Launch Campaign" unless that is part of the official name in the brief.
    - "brief_text": A concise summary of the campaign goals, deliverables, and key messages.
    - "target_audience": The specific audience from the brief.
    - "niche": Use the explicit niche if provided.
    - "goals": The main campaign goals in a concise comma-separated form.
    - "budget_range": Preserve the exact budget text if given, such as "Rs 6,00,000 - Rs 14,00,000". Do not bucketize or round it.
    - "platforms": A comma-separated lowercase list like "instagram, youtube, tiktok".
    - "deadline": Preserve the exact deadline text if present, such as "May 30, 2026".
    - "milestone_views": Return the integer milestone target only, such as 4500000.
    - "timeline_requirements": Preserve the timeline / requirements text if present.
    - "negotiation_flexibility": Return the integer percentage only, such as 10.

    If any field is missing, return an empty string for text fields and 0 for numeric fields.

    Document Text:
    \"\"\"
    {document_text}
    \"\"\"
    """

    if provider == 'assistant':
        return generate_json_for_assistant(prompt, fallback=lambda: {})

    print("--- Calling Gemini API to parse brief ---")
    return generate_json_with_gemini(prompt, fallback=lambda: {})
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
        "message": "ReachIQ API is running!",
        "milestone_tracker": {
            "enabled": MILESTONE_TRACKER_ENABLED,
            **_milestone_tracker_state,
        },
    })


@app.before_request
def ensure_milestone_tracker_running():
    """Bootstrap the background tracker from the active serving process only."""
    if MILESTONE_TRACKER_ENABLED and not _milestone_tracker_state.get('running'):
        start_milestone_tracker()


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
        campaign = {**campaign, **get_campaign_runtime_state(campaign)}
        
        influencers_response = supabase.table('influencer_profiles').select('*, profile:profiles(*)').execute()
        influencers = influencers_response.data

        matched_influencers = rank_creators_for_campaign(campaign, influencers)
        matched_ids = {
            str(match.get('profile_id') or match.get('id') or '')
            for match in matched_influencers
            if str(match.get('profile_id') or match.get('id') or '')
        }
        remaining_influencers = rank_remaining_creators_for_campaign(
            campaign,
            influencers,
            excluded_creator_ids=matched_ids,
        )
        all_ranked_influencers = matched_influencers + remaining_influencers
        top_matches = all_ranked_influencers[:12]

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
                'total_matches': len(all_ranked_influencers),
            },
        )
        
        return jsonify({
            "status": "success",
            "campaign": campaign,
            "matched_influencers": all_ranked_influencers,
            "total_matches": len(all_ranked_influencers)
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
            influencer_response = supabase.table('profiles').select('id, full_name, username, avatar_url').in_('id', influencer_ids).execute()
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
            brand_response = supabase.table('profiles').select('id, full_name, avatar_url').in_('id', brand_ids).execute()
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


@app.route('/api/assistant/chat', methods=['POST'])
def assistant_chat():
    """Platform assistant for brand and creator users."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    profile = get_current_profile(user_id)
    if not profile:
        return jsonify({"error": "Profile not found"}), 404

    role = profile.get('role')
    if role not in {'brand', 'influencer'}:
        return jsonify({"error": "Assistant is available only for brand and creator accounts."}), 403

    payload = request.json or {}
    message = (payload.get('message') or '').strip()
    history = payload.get('history') or []
    if not message:
        return jsonify({"error": "Message is required"}), 400

    try:
        pending_action = assistant_extract_pending_action(history)
        result = None
        plan = None

        if pending_action and assistant_is_confirmation_reply(message):
            result = assistant_execute_pending_action(user_id, profile, pending_action)
            plan = {'tool': pending_action.get('type') or 'help', 'raw_message': message}
        elif pending_action and assistant_is_rejection_reply(message):
            plan = {'tool': pending_action.get('type') or 'help', 'raw_message': message}
            result = {
                'reply': "Okay, I’ve cancelled that action.",
                'tool': pending_action.get('type') or 'help',
                'data': {'resolved_pending_action_id': pending_action.get('id')},
                'skip_grounded_reply': True,
            }
        elif pending_action:
            edited_result = assistant_try_update_pending_action(message, pending_action)
            if edited_result:
                result = edited_result
                plan = {'tool': pending_action.get('type') or 'help', 'raw_message': message}
            else:
                plan = assistant_extract_plan(role, message, history=history)
                plan['raw_message'] = message
                if plan.get('clarification_needed'):
                    return jsonify({
                        "status": "success",
                        "reply": plan.get('clarification_question') or "I need a bit more detail to answer that accurately.",
                        "tool": plan.get('tool'),
                        "needs_clarification": True,
                    })

                if plan.get('tool') in {'create_campaign', 'send_offer'} and role == 'brand':
                    result = assistant_prepare_brand_action(user_id, profile, plan)
                elif plan.get('tool') == 'submit_deliverable' and role == 'influencer':
                    result = assistant_prepare_creator_action(user_id, profile, plan)
                else:
                    result = assistant_execute_brand_tool(user_id, profile, plan) if role == 'brand' else assistant_execute_creator_tool(user_id, profile, plan)
        else:
            plan = assistant_extract_plan(role, message, history=history)
            plan['raw_message'] = message
            if plan.get('clarification_needed'):
                return jsonify({
                    "status": "success",
                    "reply": plan.get('clarification_question') or "I need a bit more detail to answer that accurately.",
                    "tool": plan.get('tool'),
                    "needs_clarification": True,
                })

            if plan.get('tool') in {'create_campaign', 'send_offer'} and role == 'brand':
                result = assistant_prepare_brand_action(user_id, profile, plan)
            elif plan.get('tool') == 'submit_deliverable' and role == 'influencer':
                result = assistant_prepare_creator_action(user_id, profile, plan)
            else:
                result = assistant_execute_brand_tool(user_id, profile, plan) if role == 'brand' else assistant_execute_creator_tool(user_id, profile, plan)

        final_reply = (
            result.get('reply')
            if result.get('confirmation_required') or result.get('performed_action') or result.get('needs_clarification') or result.get('skip_grounded_reply')
            else assistant_generate_grounded_reply(role, message, plan or {'tool': 'help'}, result)
        )
        return jsonify({
            "status": "success",
            "reply": final_reply,
            "tool": result.get('tool') or plan.get('tool'),
            "data": result.get('data') or {},
            "needs_clarification": bool(result.get('needs_clarification')),
            "confirmation_required": bool(result.get('confirmation_required')),
            "refresh_targets": result.get('refresh_targets') or [],
            "redirect_to": result.get('redirect_to') or '',
            "action_performed": bool(result.get('performed_action')),
        })
    except Exception as exc:
        print(f"Assistant chat failed: {exc}")
        return jsonify({
            "status": "success",
            "reply": "I ran into a platform issue while checking that. Please try again in a moment or rephrase the request.",
            "tool": "help",
            "data": {},
            "needs_clarification": False,
        })


def calculate_enhanced_match_score(campaign, influencer_profile, semantic_similarity=None):
    """Enhanced matching algorithm using influencer profile data"""
    return compute_match_score(campaign, influencer_profile, semantic_similarity=semantic_similarity)


def fetch_notifications_payload(user_id, profile):
    """Return lifecycle notifications for the authenticated brand or creator."""
    role = profile.get('role')
    if role == 'brand':
        campaigns_response = supabase.table('campaigns').select('*').eq('brand_id', user_id).execute()
        campaigns = campaigns_response.data or []
        campaign_map = {campaign['id']: campaign for campaign in campaigns}
        if not campaign_map:
            return []

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
        return []

    if not offers:
        return []

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

    return sorted(unique_notifications.values(), key=lambda item: item.get('created_at') or '', reverse=True)[:50]


def fetch_brand_offers_payload(user_id):
    """Return brand offers with campaign and influencer details."""
    campaigns_response = supabase.table('campaigns').select('*').eq('brand_id', user_id).execute()
    campaigns = campaigns_response.data or []
    campaign_map = {campaign['id']: campaign for campaign in campaigns}

    if not campaign_map:
        return []

    offer_response = supabase.table('offers').select('*').in_('campaign_id', list(campaign_map.keys())).execute()
    offers = offer_response.data or []

    influencer_ids = list({offer['influencer_id'] for offer in offers if offer.get('influencer_id')})
    influencer_map = {}
    if influencer_ids:
        influencer_response = supabase.table('profiles').select('id, full_name, username, avatar_url').in_('id', influencer_ids).execute()
        influencer_map = {profile['id']: profile for profile in (influencer_response.data or [])}

    return [
        serialize_brand_offer(
            offer,
            campaign_map.get(offer['campaign_id'], {}),
            influencer_map.get(offer.get('influencer_id'))
        )
        for offer in sorted(offers, key=lambda row: row.get('created_at', ''), reverse=True)
    ]


def fetch_influencer_offers_payload(user_id):
    """Return influencer offers with campaign and brand details."""
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
        brand_response = supabase.table('profiles').select('id, full_name, avatar_url').in_('id', brand_ids).execute()
        brand_map = {profile['id']: profile for profile in (brand_response.data or [])}

    return [
        serialize_influencer_offer(
            offer,
            campaign_map.get(offer['campaign_id'], {}),
            brand_map.get(campaign_map.get(offer['campaign_id'], {}).get('brand_id'))
        )
        for offer in sorted(offers, key=lambda row: row.get('created_at', ''), reverse=True)
    ]


def fetch_brand_campaigns_payload(user_id):
    """Return campaign management data with offer lifecycle summaries."""
    campaigns_response = supabase.table('campaigns').select('*').eq('brand_id', user_id).order('created_at', desc=True).execute()
    campaigns = campaigns_response.data or []
    campaign_map = {campaign['id']: campaign for campaign in campaigns}

    if not campaigns:
        return []

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
            offer_state = get_offer_runtime_state(offer)
            last_known_views = offer_state.get('last_known_views')
            milestone_reached = (
                offer_state.get('deliverable_status') == 'milestone_hit'
                or offer_state.get('payment_status') in {'awaiting_brand_release', 'payment_processing', 'paid'}
                or bool(offer_state.get('milestone_hit_at'))
            )
            if last_known_views not in (None, ''):
                try:
                    preview_views = int(float(last_known_views))
                except (TypeError, ValueError):
                    preview_views = None
                offer['analytics_preview'] = {
                    'views': preview_views or 0,
                    'milestone_reached': milestone_reached,
                    'last_tracked_at': offer_state.get('last_tracked_at'),
                }
            else:
                offer['analytics_preview'] = {
                    'views': 0,
                    'milestone_reached': milestone_reached,
                    'last_tracked_at': offer_state.get('last_tracked_at'),
                }

            if milestone_reached:
                milestone_hit += 1

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

    return campaign_payload


def build_campaign_draft_from_brief_text(source_text, parser_provider='assistant'):
    """Build a normalized campaign draft from free-form or structured brief text."""
    source_text = (source_text or '').strip()
    structured_fields = assistant_parse_campaign_brief_text(source_text)

    def fallback_draft():
        draft = {
            "name": infer_campaign_name_from_text(source_text),
            "brief_text": source_text,
            "target_audience": infer_target_audience_from_text(source_text),
            "goals": infer_goals_from_text(source_text),
            "budget_range": normalize_budget_range_for_form('', source_text) or "₹1,00,000 - ₹2,50,000",
            "platforms": "youtube, instagram",
            "outreach_angle": "Lead with audience fit, campaign clarity, and a concise CTA.",
            "budget_reasoning": "This range fits a pilot creator campaign with room for testing and negotiation.",
            "negotiation_flexibility": 0,
        }
        for key, value in structured_fields.items():
            if value not in (None, '', []):
                draft[key] = value
        return draft

    ai_draft = parse_brief_text_with_ai(source_text, provider=parser_provider)
    merged_draft = fallback_draft()
    if isinstance(ai_draft, dict):
        for key, value in ai_draft.items():
            if value not in (None, '', []):
                merged_draft[key] = value

    for key in ['name', 'target_audience', 'niche', 'platforms', 'deadline', 'timeline_requirements', 'negotiation_flexibility']:
        if structured_fields.get(key) not in (None, '', []):
            merged_draft[key] = structured_fields.get(key)
    if structured_fields.get('budget_range'):
        merged_draft['budget_range'] = normalize_currency_text(structured_fields.get('budget_range'))
    if structured_fields.get('milestone_views'):
        merged_draft['milestone_views'] = structured_fields.get('milestone_views')

    draft = sanitize_parsed_campaign_data(merged_draft, source_text)
    inferred_niches = infer_campaign_niches(draft)
    if inferred_niches and not normalize_text(draft.get('niche') or ''):
        draft['niche'] = inferred_niches[0]
    draft['milestone_views'] = structured_fields.get('milestone_views') or assistant_extract_milestone_views(source_text)
    draft['timeline_requirements'] = structured_fields.get('timeline_requirements') or assistant_extract_timeline_requirements(source_text)
    draft['negotiation_flexibility'] = int(structured_fields.get('negotiation_flexibility') or draft.get('negotiation_flexibility') or 0)
    if structured_fields.get('deadline'):
        draft['deadline'] = structured_fields.get('deadline')
    return draft


def build_campaign_draft_from_goal(goal):
    """Turn a free-form brand goal into a campaign draft."""
    return build_campaign_draft_from_brief_text(goal, parser_provider='assistant')


def service_create_campaign(user_id, data):
    """Create a campaign while gracefully handling optional newer schema fields."""
    profile = get_current_profile(user_id)
    if not profile or profile.get('role') != 'brand':
        return {"error": "Brand access required"}, 403

    data = data or {}
    campaign_name = (data.get('name') or '').strip()
    brief_text = (data.get('brief_text') or '').strip()
    if not campaign_name or not brief_text:
        return {"error": "Campaign name and brief are required"}, 400

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
        'negotiation_flexibility': int(data.get('negotiation_flexibility') or 0),
        'currency': 'INR',
    }

    try:
        response, dropped_fields = insert_with_optional_fields(
            'campaigns',
            campaign_data,
            optional_fields={'niche', 'milestone_views', 'timeline_requirements', 'negotiation_flexibility', 'currency'},
        )
        created_campaign = (response.data or [None])[0]
        if not created_campaign:
            return {"error": "Failed to create campaign"}, 500

        warning = None
        if dropped_fields:
            fallback_goals = embed_metadata(
                created_campaign.get('goals') or campaign_data.get('goals') or '',
                CAMPAIGN_META_PREFIX,
                {
                    'niche': campaign_data.get('niche'),
                    'milestone_views': campaign_data.get('milestone_views'),
                    'timeline_requirements': campaign_data.get('timeline_requirements'),
                    'negotiation_flexibility': campaign_data.get('negotiation_flexibility'),
                    'currency': 'INR',
                },
            )
            supabase.table('campaigns').update({'goals': fallback_goals}).eq('id', created_campaign.get('id')).execute()
            created_campaign['goals'] = fallback_goals
            created_campaign['niche'] = campaign_data.get('niche')
            created_campaign['milestone_views'] = campaign_data.get('milestone_views')
            created_campaign['timeline_requirements'] = campaign_data.get('timeline_requirements')
            created_campaign['negotiation_flexibility'] = campaign_data.get('negotiation_flexibility')
            created_campaign['currency'] = 'INR'
            warning = "Saved campaign using compatibility mode because some newer campaign columns are missing."

        return {
            "status": "success",
            "message": "Campaign created successfully",
            "campaign": created_campaign,
            "warning": warning,
        }, 200
    except Exception as exc:
        print(f"Error creating campaign: {exc}")
        return {"error": str(exc)}, 500


def service_update_campaign(user_id, campaign_id, data):
    """Update a brand-owned campaign while preserving compatibility metadata fields."""
    profile = get_current_profile(user_id)
    if not profile or profile.get('role') != 'brand':
        return {"error": "Brand access required"}, 403

    campaign_response = (
        supabase.table('campaigns')
        .select('*')
        .eq('id', campaign_id)
        .eq('brand_id', user_id)
        .single()
        .execute()
    )
    existing_campaign = campaign_response.data
    if not existing_campaign:
        return {"error": "Campaign not found or access denied"}, 404

    data = data or {}
    campaign_name = (data.get('name') or '').strip()
    brief_text = (data.get('brief_text') or '').strip()
    if not campaign_name or not brief_text:
        return {"error": "Campaign name and brief are required"}, 400

    campaign_data = {
        'name': campaign_name,
        'brief_text': brief_text,
        'target_audience': (data.get('target_audience') or '').strip() or None,
        'niche': normalize_text(data.get('niche') or '').strip() or None,
        'goals': (data.get('goals') or '').strip() or None,
        'budget_range': normalize_currency_text(data.get('budget_range') or ''),
        'status': (data.get('status') or existing_campaign.get('status') or 'active').strip(),
        'deadline': data.get('deadline') or None,
        'platforms': (data.get('platforms') or '').strip() or None,
        'milestone_views': int(data.get('milestone_views') or 0),
        'timeline_requirements': (data.get('timeline_requirements') or '').strip() or None,
        'negotiation_flexibility': int(data.get('negotiation_flexibility') or 0),
        'currency': 'INR',
    }

    try:
        response, dropped_fields = update_with_optional_fields(
            'campaigns',
            campaign_data,
            lambda query: query.eq('id', campaign_id).eq('brand_id', user_id),
            optional_fields={'niche', 'milestone_views', 'timeline_requirements', 'negotiation_flexibility', 'currency'},
        )
        updated_campaign = (response.data or [None])[0] if response else None
        if not updated_campaign:
            refreshed = (
                supabase.table('campaigns')
                .select('*')
                .eq('id', campaign_id)
                .eq('brand_id', user_id)
                .single()
                .execute()
            )
            updated_campaign = refreshed.data
        if not updated_campaign:
            return {"error": "Failed to update campaign"}, 500

        warning = None
        if dropped_fields:
            fallback_goals = embed_metadata(
                updated_campaign.get('goals') or campaign_data.get('goals') or '',
                CAMPAIGN_META_PREFIX,
                {
                    'niche': campaign_data.get('niche'),
                    'milestone_views': campaign_data.get('milestone_views'),
                    'timeline_requirements': campaign_data.get('timeline_requirements'),
                    'negotiation_flexibility': campaign_data.get('negotiation_flexibility'),
                    'currency': 'INR',
                },
            )
            supabase.table('campaigns').update({'goals': fallback_goals}).eq('id', campaign_id).eq('brand_id', user_id).execute()
            updated_campaign['goals'] = fallback_goals
            updated_campaign['niche'] = campaign_data.get('niche')
            updated_campaign['milestone_views'] = campaign_data.get('milestone_views')
            updated_campaign['timeline_requirements'] = campaign_data.get('timeline_requirements')
            updated_campaign['negotiation_flexibility'] = campaign_data.get('negotiation_flexibility')
            updated_campaign['currency'] = 'INR'
            warning = "Updated campaign using compatibility mode because some newer campaign columns are missing."

        return {
            "status": "success",
            "message": "Campaign updated successfully",
            "campaign": updated_campaign,
            "warning": warning,
        }, 200
    except Exception as exc:
        print(f"Error updating campaign {campaign_id}: {exc}")
        return {"error": str(exc)}, 500


def service_send_offer(user_id, data):
    """Send an offer from a brand to a creator."""
    profile = get_current_profile(user_id)
    if not profile or profile.get('role') != 'brand':
        return {"error": "Brand access required"}, 403

    data = data or {}
    campaign_id = data.get('campaign_id')
    influencer_id = data.get('influencer_id')
    brand_notes = data.get('brand_notes', '')

    if not campaign_id or not influencer_id:
        return {"error": "Campaign ID and creator ID are required"}, 400

    try:
        campaign_response = supabase.table('campaigns').select('*').eq('id', campaign_id).eq('brand_id', user_id).execute()
        if not campaign_response.data:
            return {"error": "Campaign not found or access denied"}, 404

        existing_offer = (
            supabase.table('offers')
            .select('*')
            .eq('campaign_id', campaign_id)
            .eq('influencer_id', influencer_id)
            .limit(1)
            .execute()
        )
        if existing_offer.data:
            return {
                "status": "success",
                "message": "An offer already exists for this creator on the selected campaign.",
                "offer": existing_offer.data[0],
                "already_exists": True,
                "warning": None,
            }, 200

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
            return {
                "status": "success",
                "message": "Offer sent successfully",
                "offer": created_offer,
                "warning": warning,
            }, 200

        return {"error": "Failed to send offer"}, 500
    except Exception as exc:
        print(f"Error sending offer: {exc}")
        return {"error": str(exc)}, 500


def service_submit_deliverable(user_id, data):
    """Submit a creator deliverable link for an accepted offer."""
    profile = get_current_profile(user_id)
    if not profile or profile.get('role') != 'influencer':
        return {"error": "Influencer access required"}, 403

    data = data or {}
    offer_id = data.get('offer_id')
    video_url = (data.get('video_url') or '').strip()
    submission_note = (data.get('submission_note') or '').strip()
    if not offer_id or not video_url:
        return {"error": "Offer ID and video URL are required"}, 400

    try:
        offer, campaign = get_offer_with_campaign(offer_id)
        if not offer or not campaign:
            return {"error": "Offer not found"}, 404
        if offer.get('influencer_id') != user_id:
            return {"error": "Access denied"}, 403
        if offer.get('status') != 'accepted':
            return {"error": "Deliverables can only be submitted after offer acceptance"}, 400

        update_payload = {
            'deliverable_url': video_url,
            'deliverable_status': 'submitted',
            'deliverable_submitted_at': datetime.now(timezone.utc).isoformat(),
            'review_notes': submission_note,
            'tracking_enabled': True,
            'tracking_started_at': datetime.now(timezone.utc).isoformat(),
            'last_tracked_at': datetime.now(timezone.utc).isoformat(),
        }
        persist_offer_runtime_fields(
            offer,
            update_payload,
            optional_fields={
                'deliverable_url',
                'deliverable_status',
                'deliverable_submitted_at',
                'review_notes',
                'tracking_enabled',
                'tracking_started_at',
                'last_tracked_at',
            },
        )
        log_offer_event(offer_id, user_id, 'deliverable_submitted', {'video_url': video_url, 'note': submission_note})
        maybe_log_milestone_hit_event(
            offer_id,
            user_id,
            {**offer, **update_payload},
            campaign,
        )
        activate_offer_tracking({**offer, **update_payload}, actor_id=user_id)

        return {"status": "success", "message": "Deliverable submitted successfully."}, 200
    except Exception as exc:
        print(f"Error submitting deliverable: {exc}")
        return {"error": str(exc)}, 500


def fetch_creator_directory():
    """Return a lightweight searchable creator directory for assistant actions."""
    profiles_response = (
        supabase.table('profiles')
        .select('id, full_name, username, role')
        .eq('role', 'influencer')
        .execute()
    )
    return profiles_response.data or []


def assistant_extract_url(text):
    """Extract the first URL from a user message."""
    match = re.search(r'https?://[^\s)>\]"]+', text or '', re.IGNORECASE)
    if not match:
        return ''
    return match.group(0).rstrip('.,!?')


def assistant_extract_campaign_goal(message):
    """Strip leading command words to keep the campaign goal readable."""
    text = (message or '').strip()
    cleaned = re.sub(r'^(please\s+)?(can you\s+)?(create|make|launch|start|set\s*up|setup)\s+(a\s+|an\s+|my\s+)?campaign\b\s*(for|about)?\s*', '', text, flags=re.IGNORECASE)
    cleaned = cleaned.strip(" :-")
    if not cleaned and re.fullmatch(r'(please\s+)?(can you\s+)?(create|make|launch|start|set\s*up|setup)\s+(a\s+|an\s+|my\s+)?campaign\b[.!?]*', text, flags=re.IGNORECASE):
        return ''
    return cleaned or text


def assistant_campaign_draft_needs_clarification(goal, draft):
    """Decide whether a campaign request is still too generic to confirm."""
    normalized_goal = normalize_text(goal or '')
    draft = draft or {}
    structured_fields = assistant_parse_campaign_brief_text(goal or '')
    if structured_fields:
        structured_hits = sum(
            1
            for key in ['name', 'target_audience', 'budget_range', 'platforms', 'timeline_requirements']
            if structured_fields.get(key)
        )
        if structured_hits >= 3:
            return False

    if normalized_goal in {
        'create campaign', 'create a campaign', 'make campaign', 'make a campaign',
        'launch campaign', 'launch a campaign', 'start campaign', 'start a campaign',
        'set up campaign', 'set up a campaign', 'setup campaign', 'setup a campaign',
    }:
        return True
    if len(normalized_goal.split()) <= 3 and 'campaign' in normalized_goal:
        return True

    name = normalize_text(draft.get('name') or '')
    audience = normalize_text(draft.get('target_audience') or '')
    budget_range = normalize_text(draft.get('budget_range') or '')
    milestone_views = int(draft.get('milestone_views') or 0)
    timeline = normalize_text(draft.get('timeline_requirements') or '')

    if name in {'campaign draft', 'create a campaign campaign'}:
        return True
    if not audience or audience == normalize_text('Relevant audience aligned with the product category'):
        return True
    if not budget_range:
        return True
    if milestone_views <= 0 and not timeline:
        return True
    return False


def assistant_extract_send_offer_entities(message):
    """Extract campaign and creator cues from a send-offer prompt."""
    raw = ' '.join(str(message or '').split())
    if not raw:
        return {}

    patterns = [
        r'send\s+offer\s+of\s+(?P<campaign>.+?)\s+to\s+(?P<creator>.+?)(?:\s+creator)?$',
        r'send\s+(?P<campaign>.+?)\s+campaign\s+offer\s+to\s+(?P<creator>.+?)(?:\s+creator)?$',
        r'send\s+offer\s+to\s+(?P<creator>.+?)\s+for\s+(?P<campaign>.+?)(?:\s+campaign)?$',
        r'send\s+offer\s+to\s+(?P<creator>.+?)(?:\s+creator)?$',
        r'offer\s+(?P<campaign>.+?)\s+to\s+(?P<creator>.+?)(?:\s+creator)?$',
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if not match:
            continue
        campaign = (match.groupdict().get('campaign') or '').strip(' .,:;')
        creator = (match.groupdict().get('creator') or '').strip(' .,:;')
        return {
            'campaign_query': campaign,
            'creator_query': creator,
        }

    campaign_label_match = re.search(r'^(?:campaign\s+name|campaign)\s*:\s*(?P<campaign>.+)$', raw, flags=re.IGNORECASE)
    if campaign_label_match:
        return {'campaign_query': campaign_label_match.group('campaign').strip(' .,:;')}

    creator_label_match = re.search(r'^(?:creator\s+name|creator)\s*:\s*(?P<creator>.+)$', raw, flags=re.IGNORECASE)
    if creator_label_match:
        return {'creator_query': creator_label_match.group('creator').strip(' .,:;')}

    creator_suffix_match = re.search(r'^(?P<creator>[A-Za-z0-9 ._\'’&-]{2,}?)\s+is\s+the\s+creator(?:\s+name)?$', raw, flags=re.IGNORECASE)
    if creator_suffix_match:
        return {'creator_query': creator_suffix_match.group('creator').strip()}
    creator_only_match = re.search(r'^(?:it is |its |this is |creator is |the creator is )?(?P<creator>[A-Za-z0-9 ._\'’&-]{2,})$', raw, flags=re.IGNORECASE)
    if creator_only_match:
        return {'creator_query': creator_only_match.group('creator').strip()}
    return {}


def assistant_extract_recent_assistant_tool(history):
    """Return the last assistant tool used in conversation history."""
    for entry in reversed(history or []):
        if not isinstance(entry, dict):
            continue
        if entry.get('role') != 'assistant':
            continue
        tool = (entry.get('tool') or '').strip()
        if tool and tool not in {'thinking', 'error'}:
            return tool
    return ''


def assistant_extract_recent_assistant_message(history):
    """Return the last assistant message content from conversation history."""
    for entry in reversed(history or []):
        if not isinstance(entry, dict):
            continue
        if entry.get('role') != 'assistant':
            continue
        content = (entry.get('content') or '').strip()
        if content:
            return content
    return ''


def assistant_extract_milestone_views(text):
    """Infer milestone view targets from natural language like 50k views."""
    normalized = normalize_text(text or '')
    patterns = [
        r'(\d+(?:\.\d+)?)\s*(k|m|lakh|crore)\s+views?',
        r'(\d[\d,]*)\s+views?',
        r'(\d[\d,]*)\s+(?:total\s+)?views?',
        r'milestone\s+of\s+(\d+(?:\.\d+)?)\s*(k|m|lakh|crore)?',
        r'(\d+(?:\.\d+)?)\s*(k|m|lakh|crore)\s+milestone',
    ]
    multiplier_map = {
        'k': 1_000,
        'm': 1_000_000,
        'lakh': 100_000,
        'crore': 10_000_000,
        '': 1,
        None: 1,
    }
    for pattern in patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if not match:
            continue
        number = float((match.group(1) or '0').replace(',', ''))
        suffix = match.group(2) if len(match.groups()) > 1 else ''
        return int(number * multiplier_map.get((suffix or '').lower(), 1))
    return 0


def assistant_extract_timeline_requirements(text):
    """Extract lightweight timeline wording from a natural language prompt."""
    source = (text or '').strip()
    match = re.search(r'((?:within|before|by|in)\s+[^.,;\n]+)', source, re.IGNORECASE)
    return match.group(1).strip() if match else ''


def assistant_resolve_creator_profile(creators, creator_query=''):
    """Resolve a creator profile from assistant input."""
    return assistant_pick_match(creator_query, creators, ['full_name', 'username'])


def assistant_is_confirmation_reply(message):
    """Return whether the user confirmed a pending action."""
    text = normalize_text(message or '')
    return text in {
        'yes', 'y', 'yes please', 'confirm', 'confirmed', 'go ahead', 'do it', 'proceed', 'sure', 'okay', 'ok'
    }


def assistant_is_rejection_reply(message):
    """Return whether the user declined a pending action."""
    text = normalize_text(message or '')
    return text in {
        'no', 'n', 'no thanks', 'cancel', 'stop', 'dont', "don't", 'not now', 'hold'
    }

def assistant_nested_text(item, path):
    """Return a nested value as comparable text."""
    value = item
    for part in path.split('.'):
        if not isinstance(value, dict):
            return ''
        value = value.get(part)
    if value in (None, ''):
        return ''
    return str(value)


def assistant_match_score(query, candidate_text):
    """Score how well a text candidate matches a user query."""
    normalized_query = normalize_text(query or '')
    normalized_candidate = normalize_text(candidate_text or '')
    if not normalized_query or not normalized_candidate:
        return 0.0
    if normalized_query == normalized_candidate:
        return 1.0
    score = 0.0
    if normalized_query in normalized_candidate:
        score += 0.88
    query_tokens = [token for token in normalized_query.split() if len(token) > 1]
    candidate_tokens = [token for token in normalized_candidate.split() if len(token) > 1]
    if query_tokens:
        token_hits = sum(1 for token in query_tokens if token in normalized_candidate)
        score += min(0.32, token_hits * 0.08)
    if candidate_tokens and query_tokens:
        candidate_hits = 0
        for candidate_token in candidate_tokens:
            if candidate_token in normalized_query:
                candidate_hits += 1
                continue
            best_similarity = max((SequenceMatcher(None, candidate_token, query_token).ratio() for query_token in query_tokens), default=0.0)
            if best_similarity >= 0.78:
                candidate_hits += 1
        coverage = candidate_hits / max(1, len(candidate_tokens))
        score += coverage * 0.72
    score += SequenceMatcher(None, normalized_query, normalized_candidate).ratio() * 0.35
    return score


def assistant_pick_match(query, items, field_paths):
    """Pick the best fuzzy match from a list of dict-like items."""
    if not items:
        return None


def generate_json_for_assistant(prompt, fallback=None):
    """Run assistant JSON generation using the configured local/provider model."""
    system_prompt = (
        "You are ReachIQ's local assistant planner. "
        "Return valid JSON only, with no markdown fences, commentary, or extra prose."
    )
    if ASSISTANT_LLM_PROVIDER == 'groq':
        return generate_with_groq(prompt, fallback=fallback, expect_json=True, system_prompt=system_prompt)
    if ASSISTANT_LLM_PROVIDER == 'ollama':
        return generate_with_ollama(prompt, fallback=fallback, expect_json=True, system_prompt=system_prompt)
    return generate_json_with_gemini(prompt, fallback=fallback)


def generate_text_for_assistant(prompt, fallback=''):
    """Run assistant text generation using the configured local/provider model."""
    system_prompt = (
        "You are ReachIQ's local platform assistant. "
        "Answer only from the provided ReachIQ context and keep replies concise and helpful."
    )
    if ASSISTANT_LLM_PROVIDER == 'groq':
        return generate_with_groq(prompt, fallback=fallback, expect_json=False, system_prompt=system_prompt)
    if ASSISTANT_LLM_PROVIDER == 'ollama':
        return generate_with_ollama(prompt, fallback=fallback, expect_json=False, system_prompt=system_prompt)
    return generate_text_with_gemini(prompt, fallback=fallback)
    if not query:
        return items[0]

    scored = []
    for item in items:
        haystacks = [assistant_nested_text(item, path) for path in field_paths]
        best_score = max((assistant_match_score(query, haystack) for haystack in haystacks if haystack), default=0.0)
        scored.append((best_score, item))

    scored.sort(key=lambda pair: pair[0], reverse=True)
    best_score, best_item = scored[0]
    return best_item if best_score >= 0.48 else None


def assistant_resolve_brand_campaign(campaigns, campaign_query=''):
    """Resolve a brand campaign by fuzzy name matching."""
    return assistant_pick_match(campaign_query, campaigns, ['name', 'brief_text', 'niche'])


def assistant_resolve_brand_offer(offers, creator_query='', campaign_query=''):
    """Resolve a brand-side offer using creator and/or campaign cues."""
    if not offers:
        return None
    if not creator_query and not campaign_query:
        submitted = [offer for offer in offers if offer.get('deliverable_url')]
        return submitted[0] if submitted else offers[0]

    scored = []
    for offer in offers:
        creator_text = ' '.join([
            assistant_nested_text(offer, 'influencer.full_name'),
            assistant_nested_text(offer, 'influencer.username'),
        ]).strip()
        campaign_text = assistant_nested_text(offer, 'campaigns.name')
        score = 0.0
        if creator_query:
            score += assistant_match_score(creator_query, creator_text) * 0.62
        if campaign_query:
            score += assistant_match_score(campaign_query, campaign_text) * 0.62
        if offer.get('deliverable_url'):
            score += 0.04
        scored.append((score, offer))

    scored.sort(key=lambda pair: pair[0], reverse=True)
    best_score, best_offer = scored[0]
    return best_offer if best_score >= 0.35 else None


def assistant_resolve_creator_offer(offers, campaign_query='', brand_query=''):
    """Resolve a creator-side offer using campaign and/or brand cues."""
    if not offers:
        return None
    if not campaign_query and not brand_query:
        return offers[0]

    scored = []
    for offer in offers:
        score = 0.0
        if campaign_query:
            score += assistant_match_score(campaign_query, assistant_nested_text(offer, 'campaigns.name')) * 0.62
        if brand_query:
            score += assistant_match_score(brand_query, offer.get('brand_name', '')) * 0.62
        scored.append((score, offer))

    scored.sort(key=lambda pair: pair[0], reverse=True)
    best_score, best_offer = scored[0]
    return best_offer if best_score >= 0.35 else None


def assistant_supported_tools(role):
    """Return the supported assistant tools for a role."""
    if role == 'brand':
        return [
            'list_campaigns',
            'list_offers',
            'campaign_details',
            'matching_influencers',
            'create_campaign',
            'send_offer',
            'submission_status',
            'milestone_status',
            'analytics_summary',
            'payment_status',
            'notifications',
            'help',
        ]
    return [
        'list_offers',
        'offer_status',
        'pending_deliverables',
        'submit_deliverable',
        'payment_status',
        'campaign_summary',
        'notifications',
        'help',
    ]


def assistant_help_text(role):
    """Return a compact help message for the platform assistant."""
    if role == 'brand':
        return (
            "I can help with ReachIQ platform tasks for your brand account. Try asking me to list your campaigns or offers, "
            "create a campaign, find top matching influencers for a campaign, send an offer, check submission, milestone, analytics, payment status, or list notifications."
        )
    return (
        "I can help with ReachIQ platform tasks for your creator account. Try asking me to list received offers, show offer status, "
        "list pending deliverables, submit a deliverable link, check payment status, show a campaign summary, or list notifications."
    )


def assistant_plan_fallback(role, message):
    """Fallback intent mapping when Gemini extraction is unavailable."""
    text = normalize_text(message or '')
    tokens = set(text.split())
    is_question = text.startswith(('did ', 'do ', 'have ', 'has ', 'what ', 'which ', 'who ', 'show ', 'list ', 'tell ', 'is ', 'are ', 'can ', 'when ', 'where '))
    plan = {
        'tool': 'help',
        'campaign_query': '',
        'creator_query': '',
        'brand_query': '',
        'video_url': assistant_extract_url(message),
        'limit': 5,
        'clarification_needed': False,
        'clarification_question': '',
    }

    if 'notification' in text:
        plan['tool'] = 'notifications'
        return plan

    if role == 'brand':
        if not is_question and re.search(r'\b(create|make|launch|start|set\s*up|setup)\b.*\bcampaign\b', text):
            plan['tool'] = 'create_campaign'
            structured = assistant_parse_campaign_brief_text(message)
            if structured.get('name'):
                plan['campaign_query'] = structured.get('name')
        elif not is_question and (
            re.search(r'\b(send|make|create|issue)\b.*\boffer\b', text)
            or re.search(r'\boffer\b.*\bto\b', text)
            or re.search(r'\binvite\b.*\bcreator\b', text)
        ):
            plan['tool'] = 'send_offer'
            plan.update({k: v for k, v in assistant_extract_send_offer_entities(message).items() if v})
        elif any(token in text for token in ['match', 'matching', 'influencer', 'creator shortlist', 'top 5']):
            plan['tool'] = 'matching_influencers'
        elif 'analytics' in text or 'performance' in text:
            plan['tool'] = 'analytics_summary'
        elif 'milestone' in text:
            plan['tool'] = 'milestone_status'
        elif 'payment' in text:
            plan['tool'] = 'payment_status'
        elif any(token in text for token in ['submission', 'submitted', 'video link', 'deliverable']):
            plan['tool'] = 'submission_status'
        elif 'offer' in text:
            plan['tool'] = 'list_offers'
        elif 'campaign' in text:
            plan['tool'] = 'list_campaigns' if any(token in text for token in ['list', 'my', 'all']) else 'campaign_details'
        return plan

    if (
        'pending deliverable' in text
        or 'deliverable left' in text
        or 'submission left' in text
        or (('deliverable' in tokens or 'submission' in tokens) and ('pending' in tokens or 'left' in tokens))
    ):
        plan['tool'] = 'pending_deliverables'
    elif not is_question and plan.get('video_url') and re.search(r'\b(submit|upload|send)\b', text) and any(token in text for token in ['deliverable', 'video', 'link', 'url']):
        plan['tool'] = 'submit_deliverable'
    elif 'payment' in text:
        plan['tool'] = 'payment_status'
    elif 'campaign' in text or 'deliverable' in text or 'submission' in text:
        plan['tool'] = 'campaign_summary'
    elif 'offer' in text or 'status' in text:
        plan['tool'] = 'offer_status' if 'status' in text else 'list_offers'
    return plan


def assistant_extract_plan(role, message, history=None):
    """Use Gemini to classify a chat request into one supported assistant tool."""
    allowed_tools = assistant_supported_tools(role)
    history = history or []
    recent_history = history[-6:]
    deterministic_plan = assistant_plan_fallback(role, message)
    deterministic_plan.setdefault('raw_message', message)
    deterministic_plan = assistant_apply_history_context(deterministic_plan, history, role)
    recent_tool = assistant_extract_recent_assistant_tool(history)
    recent_assistant_message = normalize_text(assistant_extract_recent_assistant_message(history))
    if role == 'brand' and recent_tool == 'send_offer':
        asks_for_campaign = any(token in recent_assistant_message for token in ['which campaign', 'campaign name', 'mention the campaign'])
        asks_for_creator = any(token in recent_assistant_message for token in ['creator name', 'need the creator', 'which creator', 'mention the creator'])
        if asks_for_campaign or asks_for_creator:
            deterministic_plan['tool'] = 'send_offer'
            extracted = {k: v for k, v in assistant_extract_send_offer_entities(message).items() if v}
            if not extracted:
                follow_up_value = (message or '').strip()
                if follow_up_value:
                    if asks_for_campaign:
                        extracted['campaign_query'] = follow_up_value
                    elif asks_for_creator:
                        extracted['creator_query'] = follow_up_value
            deterministic_plan.update(extracted)
            deterministic_plan.setdefault('raw_message', message)
            deterministic_plan = assistant_apply_history_context(deterministic_plan, history, role)
    if role == 'brand' and recent_tool == 'create_campaign':
        if any(token in recent_assistant_message for token in ['still need a few details', 'tell me what product or campaign', 'before i create it properly']):
            deterministic_plan['tool'] = 'create_campaign'
            deterministic_plan.setdefault('raw_message', message)
            deterministic_plan = assistant_apply_history_context(deterministic_plan, history, role)
    prompt = f"""
    You are ReachIQ's platform assistant planner.
    The authenticated user role is: {role}.
    You must choose exactly one tool from this allowed list:
    {json.dumps(allowed_tools)}

    Use only platform-related intents. If the message is unrelated or too vague, choose "help".

    Return valid JSON with this shape:
    {{
      "tool": "one of the allowed tools",
      "campaign_query": "campaign name or empty string",
      "creator_query": "creator name or empty string",
      "brand_query": "brand name or empty string",
      "video_url": "a deliverable URL if present else empty string",
      "limit": 5,
      "clarification_needed": false,
      "clarification_question": ""
    }}

    Guidelines:
    - If the user asks to create or launch a campaign choose "create_campaign".
    - If the user asks to send an offer choose "send_offer".
    - If the creator asks to submit or upload a video link choose "submit_deliverable".
    - If the user asks "top 5 matching influencers" choose "matching_influencers".
    - If the user asks about submission or deliverable status choose "submission_status" for brands.
    - If the user asks about milestone progress choose "milestone_status".
    - If the user asks about payment choose "payment_status".
    - If the user asks about analytics or performance choose "analytics_summary".
    - If the user asks which deliverables are left choose "pending_deliverables" for creators.
    - If the user asks for what you can do, choose "help".
    - Extract names into campaign_query / creator_query / brand_query when present.

    Conversation history:
    {json.dumps(recent_history)}

    User message:
    {message}
    """
    plan = generate_json_for_assistant(prompt, fallback=lambda: assistant_plan_fallback(role, message))
    if not isinstance(plan, dict):
        plan = assistant_plan_fallback(role, message)
    plan.setdefault('tool', 'help')
    plan.setdefault('campaign_query', '')
    plan.setdefault('creator_query', '')
    plan.setdefault('brand_query', '')
    plan.setdefault('campaign_id', '')
    plan.setdefault('offer_id', '')
    plan.setdefault('creator_id', '')
    plan.setdefault('video_url', assistant_extract_url(message))
    plan.setdefault('limit', 5)
    plan.setdefault('clarification_needed', False)
    plan.setdefault('clarification_question', '')
    plan.setdefault('raw_message', message)
    if plan['tool'] not in allowed_tools:
        plan = deterministic_plan
        plan['raw_message'] = message
    return assistant_apply_history_context(plan, history, role)


def assistant_format_notification_lines(notifications, limit=5):
    """Format notifications into concise bullet-style lines."""
    lines = []
    for notification in notifications[:limit]:
        lines.append(f"- {notification.get('title')}: {notification.get('message')}")
    return "\n".join(lines)


def assistant_extract_offer_context(offer):
    """Extract conversational context fields from an offer-shaped payload."""
    if not isinstance(offer, dict):
        return {}
    campaign = offer.get('campaigns') or offer.get('campaign') or {}
    influencer = offer.get('influencer') or offer.get('creator') or {}
    context = {
        'campaign_query': campaign.get('name') or '',
        'campaign_id': campaign.get('id') or offer.get('campaign_id') or '',
        'creator_query': influencer.get('full_name') or influencer.get('username') or '',
        'creator_id': influencer.get('id') or offer.get('influencer_id') or '',
        'brand_query': offer.get('brand_name') or '',
        'offer_id': offer.get('id') or '',
    }
    return {key: value for key, value in context.items() if value}


def assistant_extract_context_from_history(history):
    """Resolve the latest campaign/creator context from recent assistant history."""
    resolved = {}
    for entry in reversed(history or []):
        if not isinstance(entry, dict):
            continue
        data = entry.get('data') or {}
        if not isinstance(data, dict):
            continue

        if isinstance(data.get('offer'), dict):
            resolved.update({key: value for key, value in assistant_extract_offer_context(data.get('offer')).items() if key not in resolved})

        pending_action = data.get('pending_action')
        if isinstance(pending_action, dict):
            for key in ['campaign_query', 'campaign_id', 'creator_query', 'creator_id', 'brand_query', 'offer_id']:
                if pending_action.get(key) and key not in resolved:
                    resolved[key] = pending_action.get(key)

        if isinstance(data.get('campaign'), dict):
            campaign = data.get('campaign') or {}
            if campaign.get('name') and 'campaign_query' not in resolved:
                resolved['campaign_query'] = campaign.get('name')
            if campaign.get('id') and 'campaign_id' not in resolved:
                resolved['campaign_id'] = campaign.get('id')

        offers = data.get('offers')
        if isinstance(offers, list) and len(offers) == 1:
            resolved.update({key: value for key, value in assistant_extract_offer_context(offers[0]).items() if key not in resolved})

        matches = data.get('matches')
        if isinstance(matches, list) and len(matches) == 1:
            match = matches[0] or {}
            if match.get('full_name') and 'creator_query' not in resolved:
                resolved['creator_query'] = match.get('full_name')

        if resolved.get('campaign_query') and (resolved.get('creator_query') or resolved.get('brand_query')):
            break
    return resolved


def assistant_apply_history_context(plan, history, role):
    """Fill missing campaign/creator references from previous assistant turns."""
    context = assistant_extract_context_from_history(history)
    if not context:
        return plan

    updated_plan = dict(plan)
    if not updated_plan.get('campaign_query') and context.get('campaign_query'):
        updated_plan['campaign_query'] = context.get('campaign_query')
    if not updated_plan.get('campaign_id') and context.get('campaign_id'):
        updated_plan['campaign_id'] = context.get('campaign_id')
    if not updated_plan.get('offer_id') and context.get('offer_id'):
        updated_plan['offer_id'] = context.get('offer_id')
    if not updated_plan.get('creator_id') and context.get('creator_id'):
        updated_plan['creator_id'] = context.get('creator_id')
    if role == 'brand':
        if not updated_plan.get('creator_query') and context.get('creator_query'):
            updated_plan['creator_query'] = context.get('creator_query')
    else:
        if not updated_plan.get('brand_query') and context.get('brand_query'):
            updated_plan['brand_query'] = context.get('brand_query')
    return updated_plan


def assistant_extract_pending_action(history):
    """Return the latest unresolved pending assistant action from history."""
    resolved_ids = set()
    for entry in reversed(history or []):
        if not isinstance(entry, dict):
            continue
        data = entry.get('data') or {}
        if not isinstance(data, dict):
            continue
        resolved_id = data.get('resolved_pending_action_id')
        if resolved_id:
            resolved_ids.add(str(resolved_id))
        pending_action = data.get('pending_action')
        if isinstance(pending_action, dict):
            action_id = str(pending_action.get('id') or '')
            if action_id and action_id not in resolved_ids:
                return pending_action
    return None


def assistant_format_create_campaign_confirmation(pending_action):
    """Render the campaign confirmation summary from a pending action payload."""
    payload = (pending_action or {}).get('campaign_payload') or {}
    brief_text = (payload.get('brief_text') or '').strip()
    if brief_text and len(brief_text) > 180:
        brief_text = brief_text[:177].rstrip() + '...'
    return (
        f"I’m ready to create the campaign \"{payload.get('name') or 'Campaign Draft'}\".\n"
        f"- Brief: {brief_text or 'Not specified'}\n"
        f"- Niche: {payload.get('niche') or 'Not inferred'}\n"
        f"- Audience: {payload.get('target_audience') or 'Not specified'}\n"
        f"- Goals: {payload.get('goals') or 'Not specified'}\n"
        f"- Budget: {payload.get('budget_range') or 'Negotiable'}\n"
        f"- Milestone: {int(payload.get('milestone_views') or 0):,} views\n"
        f"- Platforms: {payload.get('platforms') or 'Not specified'}\n"
        f"- Deadline: {payload.get('deadline') or 'Not specified'}\n"
        f"- Timeline: {payload.get('timeline_requirements') or 'Not specified'}\n"
        f"- Negotiation flexibility: {int(payload.get('negotiation_flexibility') or 0)}%\n"
        "Reply yes to create it or no to cancel."
    )


def assistant_try_update_pending_action(message, pending_action):
    """Apply simple natural-language edits to an unresolved pending action."""
    pending_action = dict(pending_action or {})
    action_type = pending_action.get('type')
    if action_type != 'create_campaign':
        return None

    payload = dict(pending_action.get('campaign_payload') or {})
    if not payload:
        return None

    text = (message or '').strip()
    normalized = normalize_text(text)
    updated = False

    name_match = re.search(r'(?:change|set|update)\s+(?:the\s+)?(?:campaign\s+)?name\s+(?:to|as)\s+(.+)$', text, flags=re.IGNORECASE)
    if name_match:
        payload['name'] = name_match.group(1).strip(' .')
        updated = True

    brief_match = re.search(r'(?:change|set|update)\s+(?:the\s+)?(?:campaign\s+)?brief\s+(?:to|as)\s+(.+)$', text, flags=re.IGNORECASE)
    if brief_match:
        payload['brief_text'] = brief_match.group(1).strip()
        updated = True

    niche_match = re.search(r'(?:change|set|update)\s+the\s+niche\s+(?:to|as)\s+([A-Za-z &/-]+)$', text, flags=re.IGNORECASE)
    if niche_match:
        payload['niche'] = normalize_text(niche_match.group(1)).strip()
        updated = True

    audience_match = re.search(r'(?:change|set|update)\s+(?:the\s+)?audience\s+(?:to|as)\s+(.+)$', text, flags=re.IGNORECASE)
    if audience_match:
        payload['target_audience'] = audience_match.group(1).strip(' .')
        updated = True

    budget_match = re.search(r'(?:change|set|update)\s+(?:the\s+)?budget\s+(?:to|as)\s+(.+)$', text, flags=re.IGNORECASE)
    if budget_match:
        budget_value = normalize_currency_text(budget_match.group(1)).strip() or normalize_budget_range_for_form(budget_match.group(1), source_text=budget_match.group(1))
        if budget_value:
            payload['budget_range'] = budget_value
            updated = True

    goals_match = re.search(r'(?:change|set|update)\s+(?:the\s+)?(?:campaign\s+)?goals?(?:\s+and\s+message)?\s+(?:to|as)\s+(.+)$', text, flags=re.IGNORECASE)
    if goals_match:
        payload['goals'] = goals_match.group(1).strip()
        updated = True

    milestone_match = re.search(r'(?:change|set|update)\s+(?:the\s+)?milestone(?:\s+views?\s+target)?\s+(?:to|as)\s+(.+)$', text, flags=re.IGNORECASE)
    if milestone_match:
        milestone_value = assistant_extract_milestone_views(milestone_match.group(1))
        if milestone_value > 0:
            payload['milestone_views'] = milestone_value
            updated = True

    platform_match = re.search(r'(?:change|set|update)\s+(?:the\s+)?platforms?\s+(?:to|as)\s+(.+)$', text, flags=re.IGNORECASE)
    if platform_match:
        platform_value = infer_platforms_from_text(platform_match.group(1), preferred=platform_match.group(1))
        if platform_value:
            payload['platforms'] = platform_value
            updated = True

    deadline_match = re.search(r'(?:change|set|update)\s+(?:the\s+)?deadline\s+(?:to|as)\s+(.+)$', text, flags=re.IGNORECASE)
    if deadline_match:
        payload['deadline'] = deadline_match.group(1).strip(' .')
        updated = True

    timeline_match = re.search(r'(?:change|set|update)\s+(?:the\s+)?(?:timeline|requirements|timeline\s*/\s*requirements)\s+(?:to|as)\s+(.+)$', text, flags=re.IGNORECASE)
    if timeline_match:
        payload['timeline_requirements'] = timeline_match.group(1).strip()
        updated = True

    flexibility_match = re.search(r'(?:change|set|update)\s+(?:the\s+)?negotiation(?:\s+flexibility)?\s+(?:to|as)\s+(.+)$', text, flags=re.IGNORECASE)
    if flexibility_match:
        flexibility_value = extract_percentage_value(flexibility_match.group(1))
        if flexibility_value >= 0:
            payload['negotiation_flexibility'] = max(0, min(100, flexibility_value))
            updated = True

    explicit_sections = assistant_extract_labeled_campaign_fields(text)
    if explicit_sections:
        if explicit_sections.get('name'):
            payload['name'] = explicit_sections.get('name', '').strip()
            updated = True
        if explicit_sections.get('target_audience'):
            payload['target_audience'] = explicit_sections.get('target_audience', '').strip()
            updated = True
        if explicit_sections.get('niche'):
            payload['niche'] = normalize_text(explicit_sections.get('niche', '')).strip()
            updated = True
        if explicit_sections.get('goals_message'):
            payload['goals'] = explicit_sections.get('goals_message', '').strip()
            updated = True
        if explicit_sections.get('budget'):
            explicit_budget = normalize_currency_text(explicit_sections.get('budget', '')).strip()
            if explicit_budget:
                payload['budget_range'] = explicit_budget
                updated = True
        if explicit_sections.get('platforms'):
            platform_value = infer_platforms_from_text(explicit_sections.get('platforms', ''), preferred=explicit_sections.get('platforms', ''))
            if platform_value:
                payload['platforms'] = platform_value
                updated = True
        if explicit_sections.get('deadline'):
            payload['deadline'] = explicit_sections.get('deadline', '').strip()
            updated = True
        if explicit_sections.get('timeline'):
            payload['timeline_requirements'] = explicit_sections.get('timeline', '').strip()
            updated = True
        if explicit_sections.get('milestone'):
            milestone_value = assistant_extract_milestone_views(explicit_sections.get('milestone', ''))
            if milestone_value > 0:
                payload['milestone_views'] = milestone_value
                updated = True
        if explicit_sections.get('negotiation_flexibility'):
            flexibility_value = extract_percentage_value(explicit_sections.get('negotiation_flexibility', ''))
            payload['negotiation_flexibility'] = max(0, min(100, flexibility_value))
            updated = True

    brief_label_match = re.match(r'^\s*(?:campaign\s+brief|brief)\s*:\s*(.+)$', text, flags=re.IGNORECASE | re.DOTALL)
    if brief_label_match:
        payload['brief_text'] = brief_label_match.group(1).strip()
        updated = True

    if not updated and normalized and len(explicit_sections) >= 2:
        explicit_brief = assistant_parse_campaign_brief_text(text)
        if explicit_brief:
            for key, value in explicit_brief.items():
                if value not in (None, '', []):
                    payload[key] = value
                    updated = True

    if not updated:
        return None

    pending_action['campaign_payload'] = payload
    pending_action['campaign_query'] = payload.get('name') or pending_action.get('campaign_query') or ''
    return {
        'reply': assistant_format_create_campaign_confirmation(pending_action),
        'tool': 'create_campaign',
        'confirmation_required': True,
        'data': {'pending_action': pending_action, 'draft': payload},
        'skip_grounded_reply': True,
    }


def assistant_trim_context_value(value, max_items=8, depth=0):
    """Trim large nested data before sending it to the LLM."""
    if depth > 3:
        return '...'
    if isinstance(value, list):
        trimmed = [assistant_trim_context_value(item, max_items=max_items, depth=depth + 1) for item in value[:max_items]]
        if len(value) > max_items:
            trimmed.append(f"... and {len(value) - max_items} more")
        return trimmed
    if isinstance(value, dict):
        return {
            key: assistant_trim_context_value(item, max_items=max_items, depth=depth + 1)
            for key, item in value.items()
        }
    return value


def assistant_generate_grounded_reply(role, user_message, plan, result):
    """Use Gemini to turn retrieved platform facts into a more natural reply."""
    tool = result.get('tool') or plan.get('tool') or 'help'
    fallback_reply = result.get('reply') or assistant_help_text(role)
    if tool == 'help' or result.get('needs_clarification'):
        return fallback_reply

    context_payload = assistant_trim_context_value(result.get('data') or {})
    prompt = f"""
    You are ReachIQ's in-product assistant.
    User role: {role}
    Current tool used: {tool}

    The user's question was:
    {user_message}

    You must answer ONLY from the factual platform context below.
    Do not invent campaigns, offers, analytics, milestones, links, or payment states.
    If the context shows no result, say that clearly.
    If the user is asking a yes/no question, start with Yes or No when appropriate.
    Keep the answer natural, helpful, and platform-specific. Avoid sounding robotic.
    Mention exact campaign/creator names when available.
    Keep it concise, but clearer than a rigid status dump.

    Factual fallback answer:
    {fallback_reply}

    Retrieved platform context:
    {json.dumps(context_payload, ensure_ascii=False)}
    """
    return generate_text_for_assistant(prompt, fallback=fallback_reply)


def assistant_find_campaign_by_id(campaigns, campaign_id):
    """Find a campaign in a payload list by id."""
    if not campaign_id:
        return None
    return next((campaign for campaign in campaigns if str(campaign.get('id')) == str(campaign_id)), None)


def assistant_find_offer_by_id(offers, offer_id):
    """Find an offer in a payload list by id."""
    if not offer_id:
        return None
    return next((offer for offer in offers if str(offer.get('id')) == str(offer_id)), None)


def assistant_extract_status_filter(raw_message):
    """Extract a coarse status filter from a natural-language query."""
    text = normalize_text(raw_message or '')
    if 'paid' in text:
        return 'paid'
    if 'processing' in text:
        return 'payment_processing'
    if 'escrow pending' in text or 'escrow' in text or 'pending payment' in text:
        return 'escrow_pending'
    if 'accepted' in text:
        return 'accepted'
    if 'rejected' in text:
        return 'rejected'
    if 'pending' in text:
        return 'pending'
    return ''


def assistant_prepare_brand_action(user_id, profile, plan):
    """Prepare a brand-side write action and return a confirmation prompt."""
    tool = plan.get('tool')
    raw_message = (plan.get('raw_message') or '').strip()

    if tool == 'create_campaign':
        goal = assistant_extract_campaign_goal(raw_message)
        if not goal:
            return {
                'reply': "Tell me what product or campaign you want to launch, and if you can include the niche, target audience, budget, milestone target, and preferred platforms.",
                'tool': tool,
                'needs_clarification': True,
            }
        draft = build_campaign_draft_from_goal(goal)
        if assistant_campaign_draft_needs_clarification(goal, draft):
            return {
                'reply': (
                    "I’ve got the campaign idea, but I still need a few details before I create it properly. "
                    "Tell me the product or campaign name, target audience, budget range, milestone views target, and preferred platforms."
                ),
                'tool': tool,
                'needs_clarification': True,
                'data': {'draft': draft},
            }
        pending_action = {
            'id': str(uuid.uuid4()),
            'type': 'create_campaign',
            'campaign_payload': {
                'name': draft.get('name') or infer_campaign_name_from_text(goal),
                'brief_text': draft.get('brief_text') or goal,
                'target_audience': draft.get('target_audience') or '',
                'niche': draft.get('niche') or '',
                'goals': draft.get('goals') or '',
                'budget_range': draft.get('budget_range') or '',
                'platforms': draft.get('platforms') or '',
                'milestone_views': int(draft.get('milestone_views') or 0),
                'timeline_requirements': draft.get('timeline_requirements') or '',
                'deadline': draft.get('deadline') or '',
                'negotiation_flexibility': int(draft.get('negotiation_flexibility') or 0),
                'status': 'active',
            },
            'campaign_query': draft.get('name') or '',
        }
        summary = assistant_format_create_campaign_confirmation(pending_action)
        return {
            'reply': summary,
            'tool': tool,
            'confirmation_required': True,
            'data': {'pending_action': pending_action, 'draft': draft},
        }

    if tool == 'send_offer':
        campaigns = fetch_brand_campaigns_payload(user_id)
        offers = fetch_brand_offers_payload(user_id)
        creators = fetch_creator_directory()
        extracted_entities = assistant_extract_send_offer_entities(raw_message)
        campaign_query = plan.get('campaign_query') or extracted_entities.get('campaign_query') or ''
        creator_query = plan.get('creator_query') or extracted_entities.get('creator_query') or ''
        campaign = assistant_find_campaign_by_id(campaigns, plan.get('campaign_id')) or assistant_resolve_brand_campaign(campaigns, campaign_query or raw_message)
        if not campaign and 'latest campaign' in normalize_text(raw_message) and campaigns:
            campaign = campaigns[0]
        if not campaign:
            return {
                'reply': "I couldn’t tell which campaign to use for that offer. Mention the campaign name and I’ll line it up.",
                'tool': tool,
                'needs_clarification': True,
            }
        creator = assistant_resolve_creator_profile(creators, creator_query)
        if not creator:
            return {
                'reply': f"I found the campaign {campaign.get('name')}, but I still need the creator name before I send the offer.",
                'tool': tool,
                'needs_clarification': True,
                'data': {'campaign': campaign, 'pending_action': {'campaign_id': campaign.get('id'), 'campaign_query': campaign.get('name'), 'type': 'send_offer'}},
            }
        existing_offer = next(
            (
                item for item in offers
                if assistant_nested_text(item, 'campaigns.id') == str(campaign.get('id'))
                and assistant_nested_text(item, 'influencer.id') == str(creator.get('id'))
            ),
            None,
        )
        if existing_offer:
            return {
                'reply': (
                    f"You already have an offer for {creator.get('full_name') or creator.get('username') or 'that creator'} "
                    f"on {campaign.get('name')}. Current status: {humanize_status_label(existing_offer.get('status'))}."
                ),
                'tool': tool,
                'data': {'offer': existing_offer},
            }

        pending_action = {
            'id': str(uuid.uuid4()),
            'type': 'send_offer',
            'campaign_id': campaign.get('id'),
            'campaign_query': campaign.get('name') or '',
            'creator_id': creator.get('id'),
            'creator_query': creator.get('full_name') or creator.get('username') or '',
            'brand_notes': '',
        }
        return {
            'reply': (
                f"I’m ready to send an offer to {pending_action.get('creator_query') or 'that creator'} "
                f"for {pending_action.get('campaign_query') or 'that campaign'}.\n"
                "Reply yes to send it or no to cancel."
            ),
            'tool': tool,
            'confirmation_required': True,
            'data': {'pending_action': pending_action, 'campaign': campaign, 'creator': creator},
        }

    return None


def assistant_prepare_creator_action(user_id, profile, plan):
    """Prepare a creator-side write action and return a confirmation prompt."""
    tool = plan.get('tool')
    raw_message = (plan.get('raw_message') or '').strip()

    if tool == 'submit_deliverable':
        offers = fetch_influencer_offers_payload(user_id)
        offer = assistant_find_offer_by_id(offers, plan.get('offer_id'))
        if not offer:
            offer = assistant_resolve_creator_offer(offers, campaign_query=plan.get('campaign_query') or raw_message, brand_query=plan.get('brand_query') or raw_message)
        if not offer:
            pending_candidates = [
                item for item in offers
                if item.get('status') == 'accepted' and not get_offer_runtime_state(item).get('deliverable_url')
            ]
            if len(pending_candidates) == 1:
                offer = pending_candidates[0]
        if not offer:
            return {
                'reply': "I couldn’t match that link to one of your accepted campaigns yet. Mention the campaign or brand name and I’ll prepare it.",
                'tool': tool,
                'needs_clarification': True,
            }
        video_url = plan.get('video_url') or assistant_extract_url(raw_message)
        if not video_url:
            return {
                'reply': f"I found {offer.get('campaigns', {}).get('name') or 'that campaign'}, but I still need the video link before I can submit it.",
                'tool': tool,
                'needs_clarification': True,
                'data': {'offer': offer},
            }
        pending_action = {
            'id': str(uuid.uuid4()),
            'type': 'submit_deliverable',
            'offer_id': offer.get('id'),
            'campaign_id': offer.get('campaigns', {}).get('id') or offer.get('campaign_id'),
            'campaign_query': offer.get('campaigns', {}).get('name') or '',
            'brand_query': offer.get('brand_name') or '',
            'video_url': video_url,
            'submission_note': '',
        }
        return {
            'reply': (
                f"I’m ready to submit your deliverable for {pending_action.get('campaign_query') or 'that campaign'} "
                f"to {pending_action.get('brand_query') or 'the brand'}.\n"
                f"Link: {video_url}\n"
                "Reply yes to submit it or no to cancel."
            ),
            'tool': tool,
            'confirmation_required': True,
            'data': {'pending_action': pending_action, 'offer': offer},
        }

    return None


def assistant_execute_pending_action(user_id, profile, pending_action):
    """Execute a previously confirmed assistant action."""
    action_id = str((pending_action or {}).get('id') or '')
    action_type = (pending_action or {}).get('type') or ''
    role = profile.get('role')

    if role == 'brand' and action_type == 'create_campaign':
        payload, status_code = service_create_campaign(user_id, pending_action.get('campaign_payload') or {})
        if status_code >= 400:
            return {
                'reply': payload.get('error') or "I couldn’t create that campaign.",
                'tool': action_type,
                'data': {'resolved_pending_action_id': action_id},
                'skip_grounded_reply': True,
            }
        return {
            'reply': f"Campaign created successfully: {payload.get('campaign', {}).get('name') or 'New campaign'}.",
            'tool': action_type,
            'performed_action': True,
            'refresh_targets': ['campaigns', 'dashboard'],
            'redirect_to': 'brand-campaigns.html',
            'skip_grounded_reply': True,
            'data': {
                'campaign': payload.get('campaign'),
                'warning': payload.get('warning'),
                'resolved_pending_action_id': action_id,
            },
        }

    if role == 'brand' and action_type == 'send_offer':
        payload, status_code = service_send_offer(
            user_id,
            {
                'campaign_id': pending_action.get('campaign_id'),
                'influencer_id': pending_action.get('creator_id'),
                'brand_notes': pending_action.get('brand_notes') or '',
            },
        )
        if status_code >= 400:
            return {
                'reply': payload.get('error') or "I couldn’t send that offer.",
                'tool': action_type,
                'data': {'resolved_pending_action_id': action_id},
                'skip_grounded_reply': True,
            }
        creator_label = pending_action.get('creator_query') or 'that creator'
        campaign_label = pending_action.get('campaign_query') or 'that campaign'
        message = (
            f"Offer sent to {creator_label} for {campaign_label}."
            if not payload.get('already_exists')
            else f"There was already an offer for {creator_label} on {campaign_label}, so I left it as is."
        )
        return {
            'reply': message,
            'tool': action_type,
            'performed_action': True,
            'refresh_targets': ['offers', 'campaigns', 'discovery'],
            'skip_grounded_reply': True,
            'data': {
                'offer': payload.get('offer'),
                'warning': payload.get('warning'),
                'resolved_pending_action_id': action_id,
            },
        }

    if role == 'influencer' and action_type == 'submit_deliverable':
        payload, status_code = service_submit_deliverable(
            user_id,
            {
                'offer_id': pending_action.get('offer_id'),
                'video_url': pending_action.get('video_url'),
                'submission_note': pending_action.get('submission_note') or '',
            },
        )
        if status_code >= 400:
            return {
                'reply': payload.get('error') or "I couldn’t submit that deliverable.",
                'tool': action_type,
                'data': {'resolved_pending_action_id': action_id},
                'skip_grounded_reply': True,
            }
        return {
            'reply': f"Deliverable submitted for {pending_action.get('campaign_query') or 'that campaign'}.",
            'tool': action_type,
            'performed_action': True,
            'refresh_targets': ['offers', 'campaigns', 'analytics'],
            'skip_grounded_reply': True,
            'data': {
                'offer': {
                    'id': pending_action.get('offer_id'),
                    'campaigns': {'id': pending_action.get('campaign_id'), 'name': pending_action.get('campaign_query')},
                    'brand_name': pending_action.get('brand_query'),
                    'deliverable_url': pending_action.get('video_url'),
                },
                'resolved_pending_action_id': action_id,
            },
        }

    return {
        'reply': "I couldn’t complete that action from the current context.",
        'tool': action_type or 'help',
        'data': {'resolved_pending_action_id': action_id} if action_id else {},
        'skip_grounded_reply': True,
    }


def assistant_execute_brand_tool(user_id, profile, plan):
    """Execute a read-only assistant tool for brand users."""
    campaigns = fetch_brand_campaigns_payload(user_id)
    offers = fetch_brand_offers_payload(user_id)
    tool = plan.get('tool')
    campaign_query = (plan.get('campaign_query') or '').strip()
    creator_query = (plan.get('creator_query') or '').strip()
    campaign_id = plan.get('campaign_id')
    offer_id = plan.get('offer_id')
    creator_id = plan.get('creator_id')
    raw_message = (plan.get('raw_message') or '').strip()
    limit = max(1, min(int(plan.get('limit') or 5), 10))

    if tool == 'help':
        return {'reply': assistant_help_text('brand'), 'tool': tool}

    if tool == 'list_campaigns':
        if not campaigns:
            return {'reply': "You do not have any campaigns yet.", 'tool': tool}
        lines = [
            f"- {campaign['name']} ({humanize_status_label(campaign.get('status'))}) | Niche: {campaign.get('niche') or 'Not set'} | Milestone: {(campaign.get('milestone_views') or 0):,} views"
            for campaign in campaigns
        ]
        return {'reply': "Here are all your current campaigns:\n" + "\n".join(lines), 'tool': tool, 'data': {'campaigns': campaigns}}

    if tool == 'list_offers':
        if not offers:
            return {'reply': "You do not have any creator offers yet.", 'tool': tool}
        scoped_campaign = assistant_find_campaign_by_id(campaigns, campaign_id) or (assistant_resolve_brand_campaign(campaigns, campaign_query or raw_message) if (campaign_query or raw_message) else None)
        scoped_creator = assistant_find_offer_by_id(offers, offer_id)
        if not scoped_creator and creator_id:
            scoped_creator = next((offer for offer in offers if assistant_nested_text(offer, 'influencer.id') == str(creator_id)), None)
        if not scoped_creator and creator_query:
            scoped_creator = assistant_resolve_brand_offer(offers, creator_query=creator_query or raw_message, campaign_query=campaign_query or raw_message)
        scoped_offers = offers

        if scoped_campaign:
            scoped_offers = [
                offer for offer in offers
                if assistant_nested_text(offer, 'campaigns.id') == str(scoped_campaign.get('id'))
            ]
            if not scoped_offers:
                return {
                    'reply': f"You have not sent any offers yet for {scoped_campaign.get('name') or 'that campaign'}.",
                    'tool': tool,
                    'data': {'campaign': scoped_campaign, 'offers': []},
                }

            lines = [
                f"- {offer.get('influencer', {}).get('full_name') or 'Creator'} | Offer: {humanize_status_label(offer.get('status'))} | Payment: {humanize_status_label(offer.get('payment_status'))}"
                for offer in scoped_offers[:limit]
            ]
            return {
                'reply': f"Yes, you have sent offers for {scoped_campaign.get('name') or 'that campaign'}:\n" + "\n".join(lines),
                'tool': tool,
                'data': {'campaign': scoped_campaign, 'offers': scoped_offers[:limit]},
            }

        if creator_query and scoped_creator:
            creator_name = scoped_creator.get('influencer', {}).get('full_name') or 'that creator'
            campaign_name = scoped_creator.get('campaigns', {}).get('name') or 'that campaign'
            return {
                'reply': (
                    f"Yes. You sent an offer to {creator_name} for {campaign_name}. "
                    f"Offer status is {humanize_status_label(scoped_creator.get('status'))} and payment status is "
                    f"{humanize_status_label(scoped_creator.get('payment_status'))}."
                ),
                'tool': tool,
                'data': {'offer': scoped_creator},
            }

        lines = [
            f"- {offer.get('influencer', {}).get('full_name') or 'Creator'} for {offer.get('campaigns', {}).get('name') or 'Campaign'} | Offer: {humanize_status_label(offer.get('status'))} | Payment: {humanize_status_label(offer.get('payment_status'))}"
            for offer in offers[:limit]
        ]
        return {'reply': "Here are your latest offers:\n" + "\n".join(lines), 'tool': tool, 'data': {'offers': offers[:limit]}}

    if tool == 'notifications':
        notifications = fetch_notifications_payload(user_id, profile)
        if not notifications:
            return {'reply': "You do not have any notifications right now.", 'tool': tool}
        return {
            'reply': "Here are your latest notifications:\n" + assistant_format_notification_lines(notifications, limit=limit),
            'tool': tool,
            'data': {'notifications': notifications[:limit]},
        }

    campaign = assistant_find_campaign_by_id(campaigns, campaign_id) or assistant_resolve_brand_campaign(campaigns, campaign_query or raw_message)
    if tool in {'campaign_details', 'matching_influencers'} and not campaign:
        return {'reply': "I could not find that campaign. Try mentioning the campaign name more clearly.", 'tool': tool, 'needs_clarification': True}

    offer = assistant_find_offer_by_id(offers, offer_id)
    if not offer and creator_id:
        offer = next((item for item in offers if assistant_nested_text(item, 'influencer.id') == str(creator_id) and (not campaign_id or assistant_nested_text(item, 'campaigns.id') == str(campaign_id))), None)
    if not offer:
        offer = assistant_resolve_brand_offer(offers, creator_query=creator_query or raw_message, campaign_query=campaign_query or raw_message)
    if tool in {'submission_status', 'milestone_status', 'analytics_summary', 'payment_status'} and not offer:
        return {'reply': "I could not match that request to a specific creator or campaign. Mention the campaign or creator name and I’ll check it.", 'tool': tool, 'needs_clarification': True}

    if tool == 'campaign_details':
        return {
            'reply': (
                f"{campaign['name']} is currently {humanize_status_label(campaign.get('status'))}. "
                f"Niche: {campaign.get('niche') or 'Not set'}. Budget: {campaign.get('budget_range') or 'Negotiable'}. "
                f"Milestone target: {(campaign.get('milestone_views') or 0):,} views. "
                f"Submitted videos: {campaign.get('submitted_deliverables', 0)} out of {len(campaign.get('offers') or [])}. "
                f"Timeline requirements: {campaign.get('timeline_requirements') or 'Not added yet'}."
            ),
            'tool': tool,
            'data': {'campaign': campaign},
        }

    if tool == 'matching_influencers':
        source_campaign = supabase.table('campaigns').select('*').eq('id', campaign.get('id')).single().execute().data or {}
        influencers_response = supabase.table('influencer_profiles').select('*, profile:profiles(*)').execute()
        matches = rank_creators_for_campaign(source_campaign, influencers_response.data or [], use_embeddings=False)[:limit]
        if not matches:
            return {'reply': f"I could not find matching creators for {campaign['name']} yet.", 'tool': tool}
        lines = [
            f"- {match.get('full_name') or match.get('display_name') or 'Creator'} | Niche: {match.get('display_niche') or match.get('niche') or 'Not set'} | Match: {int(round((match.get('match_percentage') or 0)))}% | Predicted views: {int(match.get('predicted_views') or 0):,}"
            for match in matches
        ]
        return {
            'reply': f"Top matching creators for {campaign['name']}:\n" + "\n".join(lines),
            'tool': tool,
            'data': {'campaign': campaign, 'matches': matches},
        }

    offer_state = get_offer_runtime_state(offer)
    campaign_source = supabase.table('campaigns').select('*').eq('id', offer.get('campaigns', {}).get('id') or offer.get('campaign_id')).single().execute().data or {}
    campaign_state = get_campaign_runtime_state(campaign_source)

    if tool == 'submission_status':
        scoped_campaign = assistant_find_campaign_by_id(campaigns, campaign_id) or assistant_resolve_brand_campaign(campaigns, campaign_query or raw_message)
        if scoped_campaign and not creator_query:
            scoped_submissions = [
                item for item in offers
                if assistant_nested_text(item, 'campaigns.id') == str(scoped_campaign.get('id'))
                and get_offer_runtime_state(item).get('deliverable_url')
            ]
            if ('else' in normalize_text(raw_message) or 'who all' in normalize_text(raw_message)) and creator_id:
                scoped_submissions = [
                    item for item in scoped_submissions
                    if assistant_nested_text(item, 'influencer.id') != str(creator_id)
                ]
            if not scoped_submissions:
                return {
                    'reply': f"No submitted deliverable link has been received yet for {scoped_campaign.get('name') or 'that campaign'}.",
                    'tool': tool,
                    'data': {'campaign': scoped_campaign, 'offers': []},
                }
            if len(scoped_submissions) == 1:
                only_offer = scoped_submissions[0]
                only_state = get_offer_runtime_state(only_offer)
                return {
                    'reply': (
                        f"Yes. {only_offer.get('influencer', {}).get('full_name') or 'The creator'} has submitted a video for "
                        f"{scoped_campaign.get('name') or 'that campaign'}. Review status: "
                        f"{humanize_status_label(only_state.get('deliverable_status'))}. Link: {only_state.get('deliverable_url')}"
                    ),
                    'tool': tool,
                    'data': {'campaign': scoped_campaign, 'offer': only_offer},
                }
            lines = [
                f"- {item.get('influencer', {}).get('full_name') or 'Creator'} | Review: {humanize_status_label(get_offer_runtime_state(item).get('deliverable_status'))} | Link: {get_offer_runtime_state(item).get('deliverable_url')}"
                for item in scoped_submissions[:limit]
            ]
            return {
                'reply': f"These creators have submitted deliverables for {scoped_campaign.get('name') or 'that campaign'}:\n" + "\n".join(lines),
                'tool': tool,
                'data': {'campaign': scoped_campaign, 'offers': scoped_submissions[:limit]},
            }

        if offer_state.get('deliverable_url'):
            return {
                'reply': (
                    f"Yes. {offer.get('influencer', {}).get('full_name') or 'The creator'} has submitted a video for "
                    f"{offer.get('campaigns', {}).get('name') or 'this campaign'}. "
                    f"Review status: {humanize_status_label(offer_state.get('deliverable_status'))}. "
                    f"Link: {offer_state.get('deliverable_url')}"
                ),
                'tool': tool,
                'data': {'offer': offer},
            }
        return {
            'reply': (
                f"No submission link has been received yet for {offer.get('campaigns', {}).get('name') or 'this campaign'} "
                f"from {offer.get('influencer', {}).get('full_name') or 'that creator'}."
            ),
            'tool': tool,
            'data': {'offer': offer},
        }

    if tool == 'payment_status':
        amount_label = format_inr_amount(offer.get('payment_amount') or offer.get('negotiated_amount') or 0)
        return {
            'reply': (
                f"Payment status for {offer.get('influencer', {}).get('full_name') or 'this creator'} on "
                f"{offer.get('campaigns', {}).get('name') or 'this campaign'} is "
                f"{humanize_status_label(offer_state.get('payment_status'))}. "
                f"Current payment amount: {amount_label or 'Not locked yet'}. "
                f"Review status is {humanize_status_label(offer_state.get('deliverable_status'))}."
            ),
            'tool': tool,
            'data': {'offer': offer},
        }

    analytics = analyze_video_payload(offer_state.get('deliverable_url'), int(campaign_state.get('milestone_views') or 0)) if offer_state.get('deliverable_url') else None
    if not analytics:
        return {'reply': "This offer does not have a submitted deliverable yet, so there are no analytics to summarize.", 'tool': tool, 'data': {'offer': offer}}

    if tool == 'milestone_status':
        milestone_target = int(campaign_state.get('milestone_views') or 0)
        progress_pct = min(100, round(analytics['milestone'].get('progress_percentage') or 0))
        if milestone_target <= 0:
            return {
                'reply': (
                    f"{offer.get('campaigns', {}).get('name') or 'This campaign'} currently has no explicit milestone target saved. "
                    f"The submitted video is at {analytics['metrics']['views']:,} views. "
                    f"Offer review status is {humanize_status_label(offer_state.get('deliverable_status'))} and payment status is "
                    f"{humanize_status_label(offer_state.get('payment_status'))}."
                ),
                'tool': tool,
                'data': {'offer': offer, 'analytics': analytics},
            }
        return {
            'reply': (
                f"{offer.get('campaigns', {}).get('name') or 'This campaign'} is at {analytics['metrics']['views']:,} views "
                f"against a milestone target of {milestone_target:,}. "
                f"Progress: {progress_pct}%. "
                f"Milestone reached: {'Yes' if analytics['milestone'].get('is_reached') else 'No'}. "
                f"Payment status: {humanize_status_label(offer_state.get('payment_status'))}."
            ),
            'tool': tool,
            'data': {'offer': offer, 'analytics': analytics},
        }

    if tool == 'analytics_summary':
        progress_pct = min(100, round(analytics['milestone'].get('progress_percentage') or 0))
        return {
            'reply': (
                f"Analytics for {offer.get('influencer', {}).get('full_name') or 'the creator'} on "
                f"{offer.get('campaigns', {}).get('name') or 'this campaign'}: "
                f"{analytics['metrics']['views']:,} views, {analytics['metrics']['likes']:,} likes, "
                f"{analytics['metrics']['comments']:,} comments, and {analytics['metrics']['engagement_rate']}% engagement. "
                f"Earned media value is {format_inr_amount(analytics['metrics'].get('earned_media_value_inr') or 0, decimals=2)}. "
                f"Milestone progress is {progress_pct}%."
            ),
            'tool': tool,
            'data': {'offer': offer, 'analytics': analytics},
        }

    return {'reply': assistant_help_text('brand'), 'tool': 'help'}


def assistant_execute_creator_tool(user_id, profile, plan):
    """Execute a read-only assistant tool for creator users."""
    offers = fetch_influencer_offers_payload(user_id)
    tool = plan.get('tool')
    campaign_query = (plan.get('campaign_query') or '').strip()
    brand_query = (plan.get('brand_query') or '').strip()
    campaign_id = plan.get('campaign_id')
    offer_id = plan.get('offer_id')
    raw_message = (plan.get('raw_message') or '').strip()
    normalized_message = normalize_text(raw_message)
    limit = max(1, min(int(plan.get('limit') or 5), 10))

    if tool == 'help':
        return {'reply': assistant_help_text('influencer'), 'tool': tool}

    if tool == 'list_offers':
        if not offers:
            return {'reply': "You have not received any campaign offers yet.", 'tool': tool}
        slice_offers = offers if any(token in normalized_message for token in ['all', 'list all']) else offers[:limit]
        lines = [
            f"- {offer.get('campaigns', {}).get('name') or 'Campaign'} from {offer.get('brand_name') or 'Brand'} | Offer: {humanize_status_label(offer.get('status'))} | Payment: {humanize_status_label(offer.get('payment_status'))}"
            for offer in slice_offers
        ]
        return {'reply': "Here are your current offers:\n" + "\n".join(lines), 'tool': tool, 'data': {'offers': slice_offers}}

    if tool == 'notifications':
        notifications = fetch_notifications_payload(user_id, profile)
        if not notifications:
            return {'reply': "You do not have any notifications right now.", 'tool': tool}
        return {
            'reply': "Here are your latest notifications:\n" + assistant_format_notification_lines(notifications, limit=limit),
            'tool': tool,
            'data': {'notifications': notifications[:limit]},
        }

    if tool == 'pending_deliverables':
        pending = [
            offer for offer in offers
            if offer.get('status') == 'accepted' and not get_offer_runtime_state(offer).get('deliverable_url')
        ]
        if not pending:
            return {'reply': "You do not have any pending deliverables right now.", 'tool': tool, 'data': {'offers': []}}
        lines = [
            f"- {offer.get('campaigns', {}).get('name') or 'Campaign'} for {offer.get('brand_name') or 'Brand'} | Payment: {humanize_status_label(get_offer_runtime_state(offer).get('payment_status'))}"
            for offer in pending[:limit]
        ]
        return {'reply': "These accepted campaigns still need your deliverable submission:\n" + "\n".join(lines), 'tool': tool, 'data': {'offers': pending[:limit]}}

    offer = assistant_find_offer_by_id(offers, offer_id)
    if not offer:
        offer = assistant_resolve_creator_offer(offers, campaign_query=campaign_query or raw_message, brand_query=brand_query or raw_message)
    if tool in {'offer_status', 'payment_status', 'campaign_summary'} and not offer:
        return {'reply': "I could not match that to one of your campaigns or brand deals. Mention the campaign or brand name and I’ll narrow it down.", 'tool': tool, 'needs_clarification': True}

    offer_state = get_offer_runtime_state(offer)

    if tool == 'offer_status':
        return {
            'reply': (
                f"{offer.get('campaigns', {}).get('name') or 'This campaign'} from {offer.get('brand_name') or 'the brand'} is "
                f"{humanize_status_label(offer.get('status'))}. "
                f"Deliverable status: {humanize_status_label(offer_state.get('deliverable_status'))}. "
                f"Payment status: {humanize_status_label(offer_state.get('payment_status'))}."
            ),
            'tool': tool,
            'data': {'offer': offer},
        }

    if tool == 'payment_status':
        wants_list = any(token in normalized_message for token in ['list', 'which', 'all', 'show'])
        status_filter = assistant_extract_status_filter(raw_message)
        if (wants_list or not offer_id) and not campaign_query and not brand_query:
            filtered_offers = offers
            if status_filter:
                filtered_offers = [
                    item for item in offers
                    if get_offer_runtime_state(item).get('payment_status') == status_filter
                ]

            if not filtered_offers:
                if status_filter:
                    return {
                        'reply': f"You do not have any campaigns with payment status {humanize_status_label(status_filter)}.",
                        'tool': tool,
                        'data': {'offers': []},
                    }
                return {
                    'reply': "You do not have any campaign payment updates yet.",
                    'tool': tool,
                    'data': {'offers': []},
                }

            lines = [
                f"- {item.get('campaigns', {}).get('name') or 'Campaign'} | Payment: {humanize_status_label(get_offer_runtime_state(item).get('payment_status'))} | Deliverable: {humanize_status_label(get_offer_runtime_state(item).get('deliverable_status'))}"
                for item in filtered_offers[:limit]
            ]
            lead = (
                f"These campaigns have payment status {humanize_status_label(status_filter)}:\n"
                if status_filter else
                "Here are your campaign payment statuses:\n"
            )
            return {
                'reply': lead + "\n".join(lines),
                'tool': tool,
                'data': {'offers': filtered_offers[:limit]},
            }

        amount_label = format_inr_amount(offer.get('payment_amount') or offer.get('negotiated_amount') or 0)
        return {
            'reply': (
                f"Payment status for {offer.get('campaigns', {}).get('name') or 'this campaign'} is "
                f"{humanize_status_label(offer_state.get('payment_status'))}. "
                f"Current amount: {amount_label or 'Not locked yet'}. "
                f"Deliverable status is {humanize_status_label(offer_state.get('deliverable_status'))}."
            ),
            'tool': tool,
            'data': {'offer': offer},
        }

    if tool == 'campaign_summary':
        return {
            'reply': (
                f"{offer.get('campaigns', {}).get('name') or 'This campaign'} from {offer.get('brand_name') or 'the brand'} "
                f"is in {offer.get('campaigns', {}).get('niche') or 'general'} niche with budget {offer.get('campaigns', {}).get('budget_range') or 'Negotiable'}. "
                f"Target audience: {offer.get('campaigns', {}).get('target_audience') or 'Not specified'}. "
                f"Milestone target: {int(offer.get('campaigns', {}).get('milestone_views') or 0):,} views. "
                f"Timeline requirements: {offer.get('campaigns', {}).get('timeline_requirements') or 'Not added yet'}."
            ),
            'tool': tool,
            'data': {'offer': offer},
        }

    return {'reply': assistant_help_text('influencer'), 'tool': 'help'}


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
        extracted_data = build_campaign_draft_from_brief_text(transcribed_text, parser_provider='gemini')
        
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
        extracted_data = build_campaign_draft_from_brief_text(text, parser_provider='gemini')
        
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
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
    payload, status_code = service_create_campaign(user_id, request.json or {})
    return jsonify(payload), status_code



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

    draft = build_campaign_draft_from_goal(goal)

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


def run_deliverable_review(user_id, video_url='', transcript='', campaign_brief='', required_cta='', talking_points=None, competitors=None, metadata_text='', persist=True, use_ai=True):
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

    review = fallback_review
    if use_ai:
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
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
    payload, status_code = service_submit_deliverable(user_id, request.json or {})
    return jsonify(payload), status_code


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


@app.route('/api/offers/demo-payment/hold', methods=['POST'])
def hold_demo_payment():
    """Let the brand explicitly keep escrow on hold after milestone review."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    profile = get_current_profile(user_id)
    if not profile or profile.get('role') != 'brand':
        return jsonify({"error": "Brand access required"}), 403

    data = request.json or {}
    offer_id = data.get('offer_id')
    review_notes = data.get('review_notes')

    offer, campaign = get_offer_with_campaign(offer_id)
    if not offer or not campaign:
        return jsonify({"error": "Offer not found"}), 404
    if campaign.get('brand_id') != user_id:
        return jsonify({"error": "Access denied"}), 403

    update_payload = {
        'payment_status': 'awaiting_brand_release',
    }
    if review_notes is not None:
        update_payload['review_notes'] = review_notes

    persist_offer_runtime_fields(offer, update_payload, optional_fields=TRACKER_OPTIONAL_FIELDS.union({'review_notes'}))
    log_offer_event(
        offer_id,
        user_id,
        'payment_release_held',
        {
            'payment_status': 'awaiting_brand_release',
            'review_notes': review_notes,
        },
    )
    return jsonify({"status": "success", "message": "Escrow payment kept on hold."})


@app.route('/api/offers/demo-payment/release', methods=['POST'])
def release_demo_payment():
    """Simulate escrow release through a fake payment gateway for demos."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    profile = get_current_profile(user_id)
    if not profile or profile.get('role') != 'brand':
        return jsonify({"error": "Brand access required"}), 403

    data = request.json or {}
    offer_id = data.get('offer_id')
    gateway_name = (data.get('gateway_name') or 'Demo Stripe Checkout').strip()

    offer, campaign = get_offer_with_campaign(offer_id)
    if not offer or not campaign:
        return jsonify({"error": "Offer not found"}), 404
    if campaign.get('brand_id') != user_id:
        return jsonify({"error": "Access denied"}), 403

    offer_state = get_offer_runtime_state(offer)
    campaign_state = get_campaign_runtime_state(campaign)
    milestone_target = int(campaign_state.get('milestone_views') or 0)
    if not offer_state.get('deliverable_url'):
        return jsonify({"error": "No submitted deliverable found for this offer."}), 400

    analytics = analyze_video_payload(
        offer_state.get('deliverable_url'),
        milestone_target,
        prefer_fresh=True,
    )
    if milestone_target > 0 and not analytics.get('milestone', {}).get('is_reached'):
        return jsonify({"error": "Milestone has not been reached yet."}), 400

    amount = resolve_offer_payment_amount(offer, campaign)
    update_payload = {
        'deliverable_status': 'milestone_hit',
        'payment_status': 'paid',
        'payment_released_at': datetime.now(timezone.utc).isoformat(),
        'payment_amount': amount,
    }
    persist_offer_runtime_fields(
        offer,
        update_payload,
        optional_fields=TRACKER_OPTIONAL_FIELDS.union({'payment_amount', 'payment_released_at'}),
    )
    log_offer_event(
        offer_id,
        user_id,
        'demo_payment_completed',
        {
            'amount': amount,
            'target': milestone_target,
            'views': analytics.get('metrics', {}).get('views', 0),
            'gateway': gateway_name,
        },
    )
    return jsonify({
        "status": "success",
        "message": "Demo escrow payment released successfully.",
        "payment": {
            "amount": amount,
            "gateway": gateway_name,
            "target": milestone_target,
            "views": analytics.get('metrics', {}).get('views', 0),
        }
    })


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
        analytics = analyze_video_payload(
            resolved_offer.get('deliverable_url'),
            milestone_target,
            prefer_fresh=True,
        )
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
            use_ai=False,
        )
        creator_res = supabase.table('profiles').select('id, full_name, username').eq('id', offer.get('influencer_id')).single().execute()
        creator = creator_res.data or {}
        creator_profile_response = (
            supabase.table('influencer_profiles')
            .select('*')
            .eq('profile_id', offer.get('influencer_id'))
            .limit(1)
            .execute()
        )
        creator_profile = (creator_profile_response.data or [{}])[0] or {}
        decision_dashboard = build_offer_decision_dashboard(
            resolved_campaign,
            resolved_offer,
            creator_profile,
            analytics,
            auto_review,
        )

        decision_summary = {
            'milestone_reached': analytics['milestone'].get('is_reached'),
            'payment_recommendation': (
                'Milestone reached. Decide whether to release the escrow payment now.'
                if resolved_offer.get('payment_status') == 'awaiting_brand_release'
                else (
                    'Demo payment completed. Both brand and creator have been notified of the transfer.'
                    if resolved_offer.get('payment_status') == 'paid'
                    else (
                        'Milestone reached. Safe to move to payment processing after final brief review.'
                        if analytics['milestone'].get('is_reached')
                        else 'Milestone not reached yet. Keep escrow pending or request more delivery time.'
                    )
                )
            ),
            'deliverable_status': resolved_offer.get('deliverable_status', 'not_submitted'),
            'payment_status': resolved_offer.get('payment_status', 'escrow_pending'),
        }

        return jsonify({
            "status": "success",
            "data": analytics,
            "review": auto_review,
            "qa_requirements": qa_requirements,
            "insights": decision_dashboard,
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
                "creator_profile": {
                    "niche": creator_profile.get('niche') or '',
                    "platform": creator_profile.get('platform') or '',
                    "follower_count": _safe_int(creator_profile.get('follower_count')),
                    "engagement_rate": _safe_float(creator_profile.get('engagement_rate')),
                    "average_views": _safe_float(creator_profile.get('average_views')),
                    "audience_age": creator_profile.get('audience_age') or '',
                    "audience_gender": creator_profile.get('audience_gender') or '',
                    "audience_location": creator_profile.get('audience_location') or '',
                    "rate_range": creator_profile.get('rate_range') or '',
                },
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
            influencer_response = supabase.table('profiles').select('id, full_name, username, avatar_url').in_('id', influencer_ids).execute()
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
                    offer_state = get_offer_runtime_state(offer)
                    last_known_views = offer_state.get('last_known_views')
                    milestone_reached = (
                        offer_state.get('deliverable_status') == 'milestone_hit'
                        or offer_state.get('payment_status') in {'awaiting_brand_release', 'payment_processing', 'paid'}
                        or bool(offer_state.get('milestone_hit_at'))
                    )
                    if last_known_views not in (None, ''):
                        try:
                            preview_views = int(float(last_known_views))
                        except (TypeError, ValueError):
                            preview_views = None
                        offer['analytics_preview'] = {
                            'views': preview_views or 0,
                            'milestone_reached': milestone_reached,
                            'last_tracked_at': offer_state.get('last_tracked_at'),
                        }
                    else:
                        offer['analytics_preview'] = {
                            'views': 0,
                            'milestone_reached': milestone_reached,
                            'last_tracked_at': offer_state.get('last_tracked_at'),
                        }

                    if milestone_reached:
                        milestone_hit += 1

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
                'negotiation_flexibility': campaign_state.get('negotiation_flexibility', 0),
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


@app.route('/api/campaigns/<campaign_id>', methods=['GET', 'PUT', 'DELETE'])
def campaign_detail(campaign_id):
    """Fetch, update, or delete a brand-owned campaign."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    try:
        profile = get_current_profile(user_id)
        if not profile or profile.get('role') != 'brand':
            return jsonify({"error": "Brand access required"}), 403

        campaign_response = supabase.table('campaigns').select('*').eq('id', campaign_id).eq('brand_id', user_id).single().execute()
        campaign = campaign_response.data
        if not campaign:
            return jsonify({"error": "Campaign not found or access denied"}), 404

        if request.method == 'GET':
            campaign_state = get_campaign_runtime_state(campaign)
            return jsonify({
                "status": "success",
                "campaign": {
                    **campaign,
                    'niche': campaign_state.get('niche') or '',
                    'milestone_views': campaign_state.get('milestone_views') or 0,
                    'timeline_requirements': campaign_state.get('timeline_requirements') or '',
                    'negotiation_flexibility': campaign_state.get('negotiation_flexibility', 0),
                    'currency': campaign_state.get('currency', 'INR'),
                }
            })

        if request.method == 'PUT':
            payload, status_code = service_update_campaign(user_id, campaign_id, request.json or {})
            return jsonify(payload), status_code

        offers_response = supabase.table('offers').select('id').eq('campaign_id', campaign_id).execute()
        offer_ids = [offer.get('id') for offer in (offers_response.data or []) if offer.get('id')]

        if offer_ids:
            try:
                supabase.table('offer_events').delete().in_('offer_id', offer_ids).execute()
            except Exception as exc:
                print(f"Optional offer_events cleanup skipped for campaign {campaign_id}: {exc}")

            supabase.table('offers').delete().eq('campaign_id', campaign_id).execute()

        try:
            supabase.table('ai_match_runs').delete().eq('campaign_id', campaign_id).execute()
        except Exception as exc:
            print(f"Optional ai_match_runs cleanup skipped for campaign {campaign_id}: {exc}")

        supabase.table('campaigns').delete().eq('id', campaign_id).eq('brand_id', user_id).execute()

        return jsonify({
            "status": "success",
            "message": "Campaign deleted successfully.",
            "campaign_id": campaign_id,
            "campaign_name": campaign.get('name', ''),
        })
    except Exception as exc:
        print(f"Error processing campaign {campaign_id}: {exc}")
        return jsonify({"error": "Failed to process campaign request."}), 500


@app.route('/api/brand/campaigns/<campaign_id>/analytics', methods=['GET'])
def get_brand_campaign_analytics(campaign_id):
    """Return aggregate analytics and chart-ready campaign insights."""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401

    try:
        profile = get_current_profile(user_id)
        if not profile or profile.get('role') != 'brand':
            return jsonify({"error": "Brand access required"}), 403

        campaign_response = (
            supabase.table('campaigns')
            .select('*')
            .eq('id', campaign_id)
            .eq('brand_id', user_id)
            .single()
            .execute()
        )
        campaign = campaign_response.data
        if not campaign:
            return jsonify({"error": "Campaign not found"}), 404

        campaign_state = get_campaign_runtime_state(campaign)
        offers_response = supabase.table('offers').select('*').eq('campaign_id', campaign_id).execute()
        offers = offers_response.data or []

        influencer_ids = [offer.get('influencer_id') for offer in offers if offer.get('influencer_id')]
        influencer_map = {}
        if influencer_ids:
            influencer_response = (
                supabase.table('profiles')
                .select('id, full_name, username')
                .in_('id', list(set(influencer_ids)))
                .execute()
            )
            influencer_map = {row['id']: row for row in (influencer_response.data or [])}

        deadline_dt = parse_campaign_deadline_value(campaign.get('deadline'))
        now = datetime.now()
        days_remaining = None
        overdue = False
        if deadline_dt:
            days_remaining = (deadline_dt.date() - now.date()).days
            overdue = days_remaining < 0

        creator_rows = []
        total_spend = 0.0
        paid_count = 0
        submission_submitted = 0
        submission_pending = 0
        milestone_hit_count = 0
        completed_deliverables = 0
        pending_deliverables = 0
        total_tracked_views = 0

        for offer in offers:
            offer_state = get_offer_runtime_state(offer)
            creator = influencer_map.get(offer.get('influencer_id')) or {}
            creator_name = creator.get('full_name') or creator.get('username') or 'Creator'
            payout_amount = resolve_offer_payment_amount(offer, campaign)
            payment_status = offer_state.get('payment_status') or 'escrow_pending'
            deliverable_status = offer_state.get('deliverable_status') or 'not_submitted'
            has_submission = bool(offer_state.get('deliverable_url'))
            live_analytics = get_offer_live_analytics_for_campaign(offer, campaign) if has_submission else None
            tracked_views = int((live_analytics or {}).get('views') or 0)
            engagement_rate = float((live_analytics or {}).get('engagement_rate') or 0)
            analytics_source = (live_analytics or {}).get('source') or 'none'

            milestone_hit = (
                bool((live_analytics or {}).get('milestone_reached'))
                or deliverable_status == 'milestone_hit'
                or payment_status in {'awaiting_brand_release', 'payment_processing', 'paid'}
                or bool(offer_state.get('milestone_hit_at'))
            )
            completed = (
                deliverable_status in {'reviewed', 'approved', 'milestone_hit'}
                or payment_status == 'paid'
            )
            delayed = bool(overdue and not completed and not has_submission)

            if has_submission:
                submission_submitted += 1
            else:
                submission_pending += 1

            if milestone_hit:
                milestone_hit_count += 1

            if completed:
                completed_deliverables += 1
            else:
                pending_deliverables += 1

            if payment_status == 'paid':
                total_spend += payout_amount
                paid_count += 1
            elif payment_status == 'payment_processing':
                total_spend += payout_amount

            total_tracked_views += tracked_views

            roi_score = (tracked_views / payout_amount) if payout_amount else float(tracked_views)
            creator_rows.append(
                {
                    'offer_id': offer.get('id'),
                    'creator_id': offer.get('influencer_id'),
                    'creator_name': creator_name,
                    'submission_status': 'submitted' if has_submission else 'pending',
                    'deliverable_status': deliverable_status,
                    'milestone_status': 'hit' if milestone_hit else 'pending',
                    'payment_status': payment_status,
                    'payout_amount': round(float(payout_amount), 2),
                    'tracked_views': tracked_views,
                    'engagement_rate': round(engagement_rate, 2),
                    'milestones_achieved': 1 if milestone_hit else 0,
                    'delayed': delayed,
                    'completed': completed,
                    'high_roi': milestone_hit and payout_amount > 0 and roi_score >= 1,
                    'deliverable_url': offer_state.get('deliverable_url'),
                    'analytics_source': analytics_source,
                    'video_title': (live_analytics or {}).get('title'),
                }
            )

        total_creators = len(creator_rows)
        expected_submissions = total_creators
        avg_payout_per_creator = round(total_spend / total_creators, 2) if total_creators else 0.0
        budget_allocated = parse_budget_value_in_inr(campaign.get('budget_range')) or 0.0
        completion_percentage = round((submission_submitted / expected_submissions) * 100, 1) if expected_submissions else 0.0
        cost_per_milestone = round(total_spend / milestone_hit_count, 2) if milestone_hit_count else None
        milestone_target_total = int(campaign_state.get('milestone_views') or 0)
        milestone_progress_percent = round(min(100, (total_tracked_views / milestone_target_total) * 100), 1) if milestone_target_total else 0.0

        top_performer = None
        most_expensive = None
        least_efficient = None
        high_roi_creators = []
        delayed_creators = []
        completed_creators = []

        if creator_rows:
            top_performer = max(creator_rows, key=lambda row: (row['milestones_achieved'], row['tracked_views']))
            most_expensive = max(creator_rows, key=lambda row: row['payout_amount'])
            least_efficient = max(
                creator_rows,
                key=lambda row: (row['payout_amount'] / max(row['tracked_views'], 1)) if row['payout_amount'] else 0,
            )
            high_roi_creators = [row['creator_name'] for row in creator_rows if row['high_roi']]
            delayed_creators = [row['creator_name'] for row in creator_rows if row['delayed']]
            completed_creators = [row['creator_name'] for row in creator_rows if row['completed']]

        analytics_payload = {
            'campaign': {
                'id': campaign.get('id'),
                'name': campaign.get('name'),
                'status': campaign.get('status'),
                'brief_text': campaign.get('brief_text'),
                'budget_range': normalize_currency_text(campaign.get('budget_range')),
                'deadline': campaign.get('deadline'),
                'milestone_views': milestone_target_total,
                'timeline_requirements': campaign_state.get('timeline_requirements') or '',
            },
            'metrics': {
                'total_creators': total_creators,
                'expected_submissions': expected_submissions,
                'submitted_count': submission_submitted,
                'submission_pending_count': submission_pending,
                'milestones_hit_count': milestone_hit_count,
                'pending_deliverables_count': pending_deliverables,
                'completed_deliverables_count': completed_deliverables,
                'budget_allocated': round(float(budget_allocated), 2),
                'actual_spend': round(float(total_spend), 2),
                'avg_payout_per_creator': avg_payout_per_creator,
                'paid_count': sum(1 for row in creator_rows if row['payment_status'] == 'paid'),
                'pending_payment_count': sum(1 for row in creator_rows if row['payment_status'] != 'paid'),
                'completion_percentage': completion_percentage,
                'cost_per_milestone': cost_per_milestone,
                'tracked_views_total': total_tracked_views,
                'milestone_progress_percent': milestone_progress_percent,
            },
            'timeline': {
                'days_remaining': days_remaining,
                'is_overdue': overdue,
                'status': 'overdue' if overdue else ('due_soon' if days_remaining is not None and days_remaining <= 3 else 'on_track'),
            },
            'data_source': {
                'live_youtube_count': sum(1 for row in creator_rows if row.get('analytics_source') == 'youtube_live'),
                'fallback_count': sum(1 for row in creator_rows if row.get('analytics_source') == 'tracker_cache'),
            },
            'creator_metrics': creator_rows,
            'insights': {
                'top_performing_creator': top_performer,
                'most_expensive_creator': most_expensive,
                'least_efficient_creator': least_efficient,
                'high_roi_creators': high_roi_creators,
                'delayed_creators': delayed_creators,
                'completed_creators': completed_creators,
            },
        }

        return jsonify({'status': 'success', 'analytics': analytics_payload})
    except Exception as exc:
        print(f"Error loading campaign analytics: {exc}")
        return jsonify({"error": "Failed to load campaign analytics."}), 500


# Send offer to influencer
@app.route('/api/offers/send', methods=['POST'])
def send_offer():
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
    payload, status_code = service_send_offer(user_id, request.json or {})
    return jsonify(payload), status_code
    
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
        response_data = analyze_video_payload(url, milestone_target, prefer_fresh=True)
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


