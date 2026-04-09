import pathlib
import sys


sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from reachiq_logic import (
    build_campaign_matching_text,
    build_creator_matching_text,
    calculate_enhanced_match_score,
    calculate_rate_range,
    local_semantic_similarity,
    parse_yt_duration,
    parse_budget_value,
    predict_campaign_performance,
)


def test_parse_yt_duration_handles_hours_minutes_and_seconds():
    assert parse_yt_duration("PT1H2M10S") == "1:02:10"


def test_parse_yt_duration_handles_minutes_only():
    assert parse_yt_duration("PT7M5S") == "7:05"


def test_calculate_rate_range_applies_minimum_floor():
    rate = calculate_rate_range(total_views=0, video_count=0, engagement_rate=0, niche="lifestyle")
    assert rate == {"min_rate": 40, "recommended_rate": 50, "max_rate": 75}


def test_calculate_rate_range_rewards_premium_niche_and_engagement():
    rate = calculate_rate_range(total_views=500000, video_count=10, engagement_rate=6.2, niche="tech")
    assert rate["recommended_rate"] > 1000
    assert rate["max_rate"] > rate["recommended_rate"] > rate["min_rate"]


def test_match_score_rewards_platform_niche_and_budget_alignment():
    campaign = {
        "brief_text": "Looking for a tech creator to review a new gadget for young professionals",
        "target_audience": "young professional",
        "platforms": "youtube, instagram",
        "budget_range": "$1,000 - $2,500",
    }
    influencer = {
        "platform": "youtube",
        "niche": "tech",
        "bio": "Tech reviewer for young professionals and gadget enthusiasts",
        "follower_count": 120000,
        "engagement_rate": 5.4,
        "rate_range": "$1,000 - $2,500",
    }

    score = calculate_enhanced_match_score(campaign, influencer)
    assert 0.6 <= score <= 1.0


def test_local_semantic_similarity_rewards_shared_context():
    campaign_text = build_campaign_matching_text({
        "name": "Protein Bar Launch",
        "brief_text": "Looking for fitness creators who speak to gym students and healthy snacks",
        "target_audience": "college gym-goers",
        "goals": "awareness and purchase intent",
    })
    creator_text = build_creator_matching_text({
        "niche": "fitness",
        "bio": "Fitness creator sharing gym routines and high-protein snack reviews for students",
        "content_description": "Workout tips and healthy food swaps",
    })

    assert local_semantic_similarity(campaign_text, creator_text) > 0.2


def test_parse_budget_value_handles_ranges_and_plus_values():
    assert parse_budget_value("$1,000 - $2,500") == 1750.0
    assert parse_budget_value("$10,000+") == 10000.0


def test_predict_campaign_performance_returns_forecast_metrics():
    campaign = {
        "brief_text": "Tech launch for young professionals seeking better productivity tools",
        "target_audience": "young professionals",
        "platforms": "youtube",
        "budget_range": "$2,500 - $5,000",
    }
    influencer = {
        "platform": "youtube",
        "niche": "tech",
        "bio": "Tech reviewer for ambitious young professionals",
        "content_description": "Productivity apps and laptop reviews",
        "follower_count": 150000,
        "total_views": 900000,
        "video_count": 12,
        "engagement_rate": 5.2,
        "rate_range": "$2,500 - $5,000",
    }

    prediction = predict_campaign_performance(campaign, influencer)
    assert prediction["predicted_views"] > 0
    assert prediction["predicted_cpm"] > 0
    assert prediction["pricing_fairness"]["label"] in {"underpriced", "fair", "overpriced", "unknown"}
