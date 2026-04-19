import json
import re
from datetime import datetime

import google.generativeai as genai


CAMPAIGN_PARSE_SCHEMA = {
    "type": "object",
    "properties": {
        "campaign_name": {"type": ["string", "null"]},
        "campaign_brief": {"type": ["string", "null"]},
        "target_audience": {"type": ["string", "null"]},
        "campaign_niche": {"type": ["string", "null"]},
        "campaign_goals": {"type": ["string", "null"]},
        "budget_range": {"type": ["string", "null"]},
        "campaign_deadline": {"type": ["string", "null"]},
        "milestone_views_target": {"type": ["number", "null"]},
        "negotiation_flexibility": {"type": ["number", "null"]},
        "timeline_requirements": {"type": ["string", "null"]},
        "preferred_platforms": {
            "type": ["array", "null"],
            "items": {"type": "string"},
        },
    },
    "required": [
        "campaign_name",
        "campaign_brief",
        "target_audience",
        "campaign_niche",
        "campaign_goals",
        "budget_range",
        "campaign_deadline",
        "milestone_views_target",
        "negotiation_flexibility",
        "timeline_requirements",
        "preferred_platforms",
    ],
}

CAMPAIGN_PARSE_KEYS = list(CAMPAIGN_PARSE_SCHEMA["required"])

PLATFORM_ALIASES = {
    "instagram": "instagram",
    "insta": "instagram",
    "youtube": "youtube",
    "yt": "youtube",
    "tiktok": "tiktok",
    "tik tok": "tiktok",
    "twitter": "twitter",
    "x": "twitter",
}

MONTH_LOOKUP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def _empty_campaign_payload():
    return {key: None for key in CAMPAIGN_PARSE_KEYS}


def _normalize_text(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _parse_compact_number(value):
    if value in (None, "", []):
        return None
    if isinstance(value, (int, float)):
        return int(value)

    text = _normalize_text(value).lower().replace(",", "")
    if not text:
        return None

    percent_match = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
    if percent_match:
        return int(round(float(percent_match.group(1))))

    match = re.search(r"(\d+(?:\.\d+)?)\s*(k|m|lakh|lakhs|lac|lacs|crore|crores)?", text)
    if not match:
        return None

    number = float(match.group(1))
    suffix = (match.group(2) or "").lower()
    multiplier = 1
    if suffix == "k":
        multiplier = 1_000
    elif suffix == "m":
        multiplier = 1_000_000
    elif suffix in {"lakh", "lakhs", "lac", "lacs"}:
        multiplier = 100_000
    elif suffix in {"crore", "crores"}:
        multiplier = 10_000_000
    return int(round(number * multiplier))


def _normalize_date(value):
    text = _normalize_text(value)
    if not text:
        return None

    direct_match = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](\d{2,4})", text)
    if direct_match:
        day = int(direct_match.group(1))
        month = int(direct_match.group(2))
        year = int(direct_match.group(3))
        if year < 100:
            year += 2000
        try:
            return datetime(year, month, day).strftime("%d-%m-%Y")
        except ValueError:
            return None

    month_first = re.search(r"([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s+)(\d{4})", text)
    if month_first:
        month = MONTH_LOOKUP.get(month_first.group(1).lower())
        if month:
            try:
                return datetime(int(month_first.group(3)), month, int(month_first.group(2))).strftime("%d-%m-%Y")
            except ValueError:
                return None

    day_first = re.search(r"(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)(?:,?\s+)(\d{4})", text)
    if day_first:
        month = MONTH_LOOKUP.get(day_first.group(2).lower())
        if month:
            try:
                return datetime(int(day_first.group(3)), month, int(day_first.group(1))).strftime("%d-%m-%Y")
            except ValueError:
                return None
    return None


def _normalize_platforms(value):
    if value in (None, "", []):
        return None

    raw_values = value if isinstance(value, list) else re.split(r"[,/]| and ", str(value), flags=re.IGNORECASE)
    platforms = []
    for raw in raw_values:
        normalized = _normalize_text(raw).lower()
        if not normalized:
            continue
        alias = PLATFORM_ALIASES.get(normalized)
        if alias and alias not in platforms:
            platforms.append(alias)
            continue
        for key, mapped in PLATFORM_ALIASES.items():
            if key in normalized and mapped not in platforms:
                platforms.append(mapped)
    return platforms or None


def _normalize_budget(value):
    text = _normalize_text(value)
    if not text:
        return None

    normalized = text.lower().replace("₹", "rs ").replace("inr", "rs ")
    matches = re.findall(r"(\d+(?:\.\d+)?)\s*(k|m|lakh|lakhs|lac|lacs|crore|crores)?", normalized)
    values = []
    for number, suffix in matches[:2]:
        parsed = _parse_compact_number(f"{number}{suffix}")
        if parsed:
            values.append(parsed)

    if not values:
        return None
    if len(values) == 1:
        return str(values[0])
    return f"{values[0]} - {values[1]}"


def normalize_campaign_parse_result(data):
    normalized = _empty_campaign_payload()
    if not isinstance(data, dict):
        return normalized

    normalized["campaign_name"] = _normalize_text(data.get("campaign_name")) or None
    normalized["campaign_brief"] = _normalize_text(data.get("campaign_brief")) or None
    normalized["target_audience"] = _normalize_text(data.get("target_audience")) or None
    niche = _normalize_text(data.get("campaign_niche")).lower()
    normalized["campaign_niche"] = niche or None
    normalized["campaign_goals"] = _normalize_text(data.get("campaign_goals")) or None
    normalized["budget_range"] = _normalize_budget(data.get("budget_range"))
    normalized["campaign_deadline"] = _normalize_date(data.get("campaign_deadline"))
    normalized["milestone_views_target"] = _parse_compact_number(data.get("milestone_views_target"))
    normalized["negotiation_flexibility"] = _parse_compact_number(data.get("negotiation_flexibility"))
    normalized["timeline_requirements"] = _normalize_text(data.get("timeline_requirements")) or None
    normalized["preferred_platforms"] = _normalize_platforms(data.get("preferred_platforms"))
    return normalized


def has_any_campaign_fields(data):
    if not isinstance(data, dict):
        return False
    return any(value not in (None, "", []) for value in data.values())


def parse_campaign_input(text, model_name="gemini-2.5-flash"):
    prompt = f"""You are a strict campaign data extraction engine.

Return ONLY valid JSON.

Rules:

* Follow schema exactly
* No extra text
* If value missing -> null
* Do NOT guess
* Normalize values (lowercase where applicable)

SCHEMA:
{{
  "campaign_name": string | null,
  "campaign_brief": string | null,
  "target_audience": string | null,
  "campaign_niche": string | null,
  "campaign_goals": string | null,
  "budget_range": string | null,
  "campaign_deadline": string | null,
  "milestone_views_target": number | null,
  "negotiation_flexibility": number | null,
  "timeline_requirements": string | null,
  "preferred_platforms": string[] | null
}}

Special Instructions:

* Extract platforms from: Instagram, YouTube, TikTok, Twitter
* Convert numbers properly (e.g., '50k' -> 50000)
* Convert percentages (e.g., '10%' -> 10)
* Dates -> format dd-mm-yyyy if possible
* Budget -> map to nearest logical range string
* Platforms -> always return array

Input:
{{user_input}}

Input:
{text}"""

    model = genai.GenerativeModel(model_name)
    generation_config = genai.GenerationConfig(
        temperature=0,
        response_mime_type="application/json",
        response_schema=CAMPAIGN_PARSE_SCHEMA,
    )

    response = None
    for attempt in range(2):
        try:
            response = model.generate_content(prompt, generation_config=generation_config)
            parsed = json.loads((response.text or "").strip() or "{}")
            return normalize_campaign_parse_result(parsed)
        except Exception:
            if attempt == 1:
                raise
    return _empty_campaign_payload()
