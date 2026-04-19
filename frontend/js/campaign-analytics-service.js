(function initCampaignAnalyticsService(global) {
  const service = {
    async fetchCampaignAnalytics(supabaseClient, campaignId) {
      const {
        data: { session },
      } = await supabaseClient.auth.getSession();
      const response = await fetch(global.ReachIQ.apiUrl(`/brand/campaigns/${campaignId}/analytics`), {
        headers: { Authorization: `Bearer ${session.access_token}` },
      });
      const payload = await global.ReachIQ.parseJsonResponse(response);
      if (!response.ok) throw new Error(payload.error || 'Failed to load campaign analytics.');
      return payload.analytics || {};
    },

    buildChartData(analytics) {
      const creatorMetrics = analytics.creator_metrics || [];
      return {
        payoutBar: {
          labels: creatorMetrics.map(item => item.creator_name),
          datasets: [
            {
              label: 'Payout Used',
              data: creatorMetrics.map(item => item.payout_amount || 0),
              backgroundColor: 'rgba(212,168,83,0.72)',
              borderColor: 'rgba(212,168,83,1)',
              borderWidth: 1,
              borderRadius: 10,
            },
          ],
        },
        milestoneBar: {
          labels: creatorMetrics.map(item => item.creator_name),
          datasets: [
            {
              label: 'Milestones Achieved',
              data: creatorMetrics.map(item => item.milestones_achieved || 0),
              backgroundColor: 'rgba(77,217,192,0.72)',
              borderColor: 'rgba(77,217,192,1)',
              borderWidth: 1,
              borderRadius: 10,
            },
          ],
        },
        escrowPie: {
          labels: ['Paid', 'Pending'],
          datasets: [
            {
              data: [analytics.metrics?.paid_count || 0, analytics.metrics?.pending_payment_count || 0],
              backgroundColor: ['rgba(77,217,192,0.88)', 'rgba(244,195,72,0.88)'],
              borderColor: ['rgba(77,217,192,1)', 'rgba(244,195,72,1)'],
              borderWidth: 1,
            },
          ],
        },
        submissionPie: {
          labels: ['Submitted', 'Pending'],
          datasets: [
            {
              data: [analytics.metrics?.submitted_count || 0, analytics.metrics?.submission_pending_count || 0],
              backgroundColor: ['rgba(96,165,250,0.88)', 'rgba(244,114,182,0.88)'],
              borderColor: ['rgba(96,165,250,1)', 'rgba(244,114,182,1)'],
              borderWidth: 1,
            },
          ],
        },
        budgetBar: {
          labels: ['Allocated', 'Used'],
          datasets: [
            {
              label: 'Budget',
              data: [analytics.metrics?.budget_allocated || 0, analytics.metrics?.actual_spend || 0],
              backgroundColor: ['rgba(148,163,184,0.72)', 'rgba(212,168,83,0.82)'],
              borderColor: ['rgba(148,163,184,1)', 'rgba(212,168,83,1)'],
              borderWidth: 1,
              borderRadius: 10,
            },
          ],
        },
      };
    },
  };

  global.ReachIQCampaignAnalytics = service;
})(window);
