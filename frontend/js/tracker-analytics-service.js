(function initTrackerAnalyticsService(global) {
  function toneClass(value) {
    const raw = String(value || '').toLowerCase();
    if (raw.includes('approve') || raw.includes('complete') || raw.includes('overperform') || raw.includes('high') || raw.includes('underpriced') || raw.includes('work again') || raw.includes('safe') || raw.includes('positive') || raw.includes('track')) {
      return 'success';
    }
    if (raw.includes('needs') || raw.includes('medium') || raw.includes('fair') || raw.includes('neutral') || raw.includes('risk')) {
      return 'warning';
    }
    return 'danger';
  }

  function safeNumber(value, fallback = 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  function formatCompactInr(value) {
    return global.ReachIQ.formatInr(safeNumber(value), { maximumFractionDigits: 0 });
  }

  function buildTrackerViewModel(payload) {
    const analytics = payload?.data || {};
    const offer = payload?.offer || {};
    const review = payload?.review || {};
    const insights = payload?.insights || {};
    const metrics = analytics.metrics || {};

    return {
      analytics,
      offer,
      review,
      insights,
      summaryCards: [
        {
          label: 'Campaign Status',
          value: insights.campaign_status || 'On Track',
          tone: insights.campaign_status_tone || toneClass(insights.campaign_status),
          note: insights.days_remaining == null ? 'No deadline saved for this campaign yet.' : insights.days_remaining >= 0 ? `${insights.days_remaining} days remaining` : `${Math.abs(insights.days_remaining)} days overdue`,
        },
        {
          label: 'ROI Score',
          value: insights.roi_multiple ? `${insights.roi_multiple.toFixed(2)}x` : 'N/A',
          tone: insights.roi_multiple >= 2 ? 'success' : insights.roi_multiple >= 1 ? 'warning' : 'danger',
          note: `${formatCompactInr(insights.metrics?.emv || metrics.earned_media_value_inr || 0)} earned media value on ${formatCompactInr(insights.metrics?.cost || offer.payment_amount || 0)} cost`,
        },
        {
          label: 'Milestone Progress',
          value: insights.milestone_progress_label || `${safeNumber(metrics.views).toLocaleString()} views`,
          tone: analytics.milestone?.is_reached ? 'success' : 'warning',
          note: `${safeNumber(insights.milestone_progress_pct).toFixed(1)}% of target completed`,
        },
        {
          label: 'Final Recommendation',
          value: insights.final_recommendation || 'Needs Improvement',
          tone: insights.final_recommendation_tone || toneClass(insights.final_recommendation),
          note: insights.final_recommendation === 'Approve Payment'
            ? 'Campaign, QA, and milestone checks are aligned for payout.'
            : insights.final_recommendation === 'Reject'
              ? 'QA or safety issues need attention before release.'
              : 'Review the flagged issues before proceeding.',
        },
      ],
      performance: {
        viewsLabel: `${safeNumber(metrics.views).toLocaleString()} vs ${safeNumber(analytics.milestone?.target).toLocaleString()} target`,
        engagementLabel: `${safeNumber(metrics.engagement_rate).toFixed(2)}% vs ${safeNumber(insights.engagement_benchmark, 3.5).toFixed(2)}% benchmark`,
        cpmLabel: `${safeNumber(insights.cpm).toFixed(2)} vs ${safeNumber(insights.cpm_benchmark, 0).toFixed(2)} industry avg`,
      },
      badges: {
        pricing: { label: insights.pricing_label || 'Fair', tone: insights.pricing_tone || toneClass(insights.pricing_label) },
        performer: { label: insights.creator_label || 'Medium performer', tone: insights.creator_label_tone || toneClass(insights.creator_label) },
        future: { label: insights.future_recommendation || 'Negotiate price', tone: insights.future_recommendation_tone || toneClass(insights.future_recommendation) },
        safety: { label: insights.brand_safety_level || 'unknown', tone: insights.brand_safety_tone || toneClass(insights.brand_safety_level) },
        sentiment: { label: insights.sentiment || 'neutral', tone: insights.sentiment_tone || toneClass(insights.sentiment) },
      },
      flags: insights.flags || [],
      geographies: insights.top_geographies || [],
      chartData: {
        viewsComparison: {
          labels: ['Current Views', 'Milestone Target'],
          datasets: [{
            label: 'Views',
            data: [safeNumber(metrics.views), safeNumber(analytics.milestone?.target)],
            backgroundColor: ['rgba(212,168,83,0.82)', 'rgba(77,217,192,0.25)'],
            borderColor: ['rgba(212,168,83,1)', 'rgba(77,217,192,0.55)'],
            borderWidth: 1,
            borderRadius: 10,
          }],
        },
        engagementVsBenchmark: {
          labels: ['Current', 'Benchmark'],
          datasets: [{
            label: 'Engagement Rate',
            data: [safeNumber(metrics.engagement_rate), safeNumber(insights.engagement_benchmark, 3.5)],
            backgroundColor: ['rgba(77,217,192,0.8)', 'rgba(148,163,184,0.45)'],
            borderColor: ['rgba(77,217,192,1)', 'rgba(148,163,184,0.8)'],
            borderWidth: 1,
            borderRadius: 10,
          }],
        },
        cpmVsIndustry: {
          labels: ['Creator CPM', 'Industry Avg'],
          datasets: [{
            label: 'CPM',
            data: [safeNumber(insights.cpm), safeNumber(insights.cpm_benchmark)],
            backgroundColor: ['rgba(244,114,182,0.78)', 'rgba(148,163,184,0.45)'],
            borderColor: ['rgba(244,114,182,1)', 'rgba(148,163,184,0.8)'],
            borderWidth: 1,
            borderRadius: 10,
          }],
        },
        viewsTrend: {
          labels: insights.trend_series?.labels || [],
          datasets: [{
            label: insights.trend_series?.is_estimated ? 'Estimated Views Trend' : 'Views Trend',
            data: insights.trend_series?.views || [],
            borderColor: 'rgba(212,168,83,1)',
            backgroundColor: 'rgba(212,168,83,0.16)',
            fill: true,
            tension: 0.35,
            pointRadius: 3,
          }],
        },
        engagementTrend: {
          labels: insights.trend_series?.labels || [],
          datasets: [{
            label: insights.trend_series?.is_estimated ? 'Estimated Engagement Trend' : 'Engagement Trend',
            data: insights.trend_series?.engagement_rate || [],
            borderColor: 'rgba(77,217,192,1)',
            backgroundColor: 'rgba(77,217,192,0.12)',
            fill: true,
            tension: 0.35,
            pointRadius: 3,
          }],
        },
      },
    };
  }

  global.ReachIQTrackerAnalytics = {
    buildTrackerViewModel,
    toneClass,
  };
})(window);
