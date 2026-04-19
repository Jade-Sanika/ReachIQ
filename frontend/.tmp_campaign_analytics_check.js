const supabaseClient = window.ReachIQ.createSupabaseClient();
        const campaignId = new URLSearchParams(window.location.search).get('campaign');
        const charts = {};

        function destroyChart(key) {
            if (charts[key]) {
                charts[key].destroy();
                charts[key] = null;
            }
        }

        function createBarChart(key, canvasId, data, labelFormatter = value => value) {
            destroyChart(key);
            charts[key] = new Chart(document.getElementById(canvasId), {
                type: 'bar',
                data,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label(context) {
                                    return `${context.dataset.label}: ${labelFormatter(context.raw)}`;
                                },
                            },
                        },
                    },
                    scales: {
                        x: {
                            ticks: { color: '#d8d3c6' },
                            grid: { color: 'rgba(255,255,255,0.05)' },
                        },
                        y: {
                            beginAtZero: true,
                            ticks: {
                                color: '#d8d3c6',
                                callback: value => labelFormatter(value),
                            },
                            grid: { color: 'rgba(255,255,255,0.05)' },
                        },
                    },
                },
            });
        }

        function createPieChart(key, canvasId, data) {
            destroyChart(key);
            charts[key] = new Chart(document.getElementById(canvasId), {
                type: 'pie',
                data,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            labels: { color: '#f4efe7' },
                        },
                    },
                },
            });
        }

        function formatStatusTone(status) {
            const normalized = String(status || '').toLowerCase();
            if (['paid', 'approved', 'submitted', 'completed', 'hit'].includes(normalized)) return 'success';
            if (['overdue', 'delayed', 'revisions_requested'].includes(normalized)) return 'danger';
            return 'pending';
        }

        function renderMetricCards(analytics) {
            const metrics = analytics.metrics || {};
            document.getElementById('metricCreators').textContent = metrics.total_creators || 0;
            document.getElementById('metricSubmissions').textContent = `${metrics.submitted_count || 0} / ${metrics.expected_submissions || 0}`;
            document.getElementById('metricMilestones').textContent = metrics.milestones_hit_count || 0;
            document.getElementById('metricSpend').textContent = window.ReachIQ.formatInr(metrics.actual_spend || 0);
            document.getElementById('metricAvgPayout').textContent = window.ReachIQ.formatInr(metrics.avg_payout_per_creator || 0);
            document.getElementById('metricCostPerMilestone').textContent = metrics.cost_per_milestone ? window.ReachIQ.formatInr(metrics.cost_per_milestone) : '—';

            const submittedProgress = metrics.expected_submissions ? Math.min(100, ((metrics.submitted_count || 0) / metrics.expected_submissions) * 100) : 0;
            document.getElementById('submissionProgressLabel').textContent = `${metrics.submitted_count || 0} / ${metrics.expected_submissions || 0}`;
            document.getElementById('submissionProgressFill').style.width = `${submittedProgress}%`;

            const trackedViews = metrics.tracked_views_total || 0;
            const milestoneTarget = analytics.campaign?.milestone_views || 0;
            document.getElementById('milestoneProgressLabel').textContent = `${trackedViews.toLocaleString()} / ${(milestoneTarget || 0).toLocaleString()} views`;
            document.getElementById('milestoneProgressFill').style.width = `${Math.max(0, Math.min(100, metrics.milestone_progress_percent || 0))}%`;
            document.getElementById('milestoneProgressMeta').textContent = milestoneTarget
                ? `${metrics.milestone_progress_percent || 0}% of the campaign milestone target has been reached.`
                : 'No milestone target is configured for this campaign yet.';

            const timeline = analytics.timeline || {};
            const timelinePill = document.getElementById('timelineStatusPill');
            timelinePill.className = `timeline-pill ${timeline.is_overdue ? 'danger' : timeline.status === 'due_soon' ? 'pending' : 'success'}`;
            if (timeline.days_remaining === null || timeline.days_remaining === undefined) {
                timelinePill.textContent = 'No deadline configured';
            } else if (timeline.is_overdue) {
                timelinePill.textContent = `${Math.abs(timeline.days_remaining)} day(s) overdue`;
            } else {
                timelinePill.textContent = `${timeline.days_remaining} day(s) remaining`;
            }
        }

        function renderInsights(analytics) {
            const insights = analytics.insights || {};
            const metrics = analytics.metrics || {};

            const top = insights.top_performing_creator;
            document.getElementById('insightTopPerformer').textContent = top ? top.creator_name : 'Not enough data yet';
            document.getElementById('insightTopPerformerText').textContent = top
                ? `${top.creator_name} is leading with ${top.tracked_views.toLocaleString()} tracked views and ${top.milestones_achieved} milestone hit(s).`
                : 'Waiting for creator tracking data.';

            const expensive = insights.most_expensive_creator;
            document.getElementById('insightMostExpensive').textContent = expensive ? expensive.creator_name : 'Not enough data yet';
            document.getElementById('insightMostExpensiveText').textContent = expensive
                ? `${expensive.creator_name} currently has the highest payout at ${window.ReachIQ.formatInr(expensive.payout_amount || 0)}.`
                : 'Waiting for payout data.';

            const inefficient = insights.least_efficient_creator;
            document.getElementById('insightLeastEfficient').textContent = inefficient ? inefficient.creator_name : 'Not enough data yet';
            document.getElementById('insightLeastEfficientText').textContent = inefficient
                ? `${inefficient.creator_name} is the least efficient right now with ${window.ReachIQ.formatInr(inefficient.payout_amount || 0)} spent for ${inefficient.tracked_views.toLocaleString()} tracked views.`
                : 'We’ll flag high-cost, low-performance creators here.';

            const highlightList = document.getElementById('highlightList');
            const chips = [];
            (insights.delayed_creators || []).forEach(name => chips.push(`<span class="highlight-chip danger">⚠ ${name}</span>`));
            (insights.completed_creators || []).forEach(name => chips.push(`<span class="highlight-chip success">✅ ${name}</span>`));
            (insights.high_roi_creators || []).forEach(name => chips.push(`<span class="highlight-chip warning">💰 ${name}</span>`));
            chips.push(`<span class="highlight-chip">Completion ${metrics.completion_percentage || 0}%</span>`);
            highlightList.innerHTML = chips.length ? chips.join('') : '<span class="highlight-chip">No standout signals yet</span>';
        }

        function renderCreatorTable(analytics) {
            const rows = analytics.creator_metrics || [];
            const tbody = document.getElementById('creatorMetricsTableBody');
            if (!rows.length) {
                tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--text-secondary);padding:1.4rem;">No creators linked to this campaign yet.</td></tr>';
                return;
            }

            tbody.innerHTML = rows.map(row => `
                <tr>
                    <td><strong>${row.creator_name}</strong></td>
                    <td><span class="status-pill ${formatStatusTone(row.submission_status)}">${row.submission_status.replace('_', ' ')}</span></td>
                    <td><span class="status-pill ${formatStatusTone(row.milestone_status)}">${row.milestone_status.replace('_', ' ')}</span></td>
                    <td><span class="status-pill ${formatStatusTone(row.payment_status)}">${row.payment_status.replace(/_/g, ' ')}</span></td>
                    <td>${window.ReachIQ.formatInr(row.payout_amount || 0)}</td>
                    <td>${(row.tracked_views || 0).toLocaleString()}</td>
                </tr>
            `).join('');
        }

        function renderCharts(analytics) {
            const data = window.ReachIQCampaignAnalytics.buildChartData(analytics);
            createPieChart('escrow', 'escrowStatusChart', data.escrowPie);
            createPieChart('submission', 'submissionStatusChart', data.submissionPie);
            createBarChart('budget', 'budgetUtilizationChart', data.budgetBar, value => window.ReachIQ.formatInr(value));
            createBarChart('payout', 'creatorPayoutChart', data.payoutBar, value => window.ReachIQ.formatInr(value));
            createBarChart('milestone', 'creatorMilestoneChart', data.milestoneBar, value => Number(value));
        }

        async function init() {
            const auth = await window.ReachIQ.requireRole(supabaseClient, 'brand');
            if (!auth) return;

            if (!campaignId) {
                document.getElementById('analyticsLoading').style.display = 'none';
                document.getElementById('analyticsEmpty').style.display = 'block';
                document.getElementById('analyticsEmpty').innerHTML = '<h3>No campaign selected</h3><p>Open this page from the Manage Campaigns view to analyze a campaign.</p>';
                return;
            }

            try {
                const analytics = await window.ReachIQCampaignAnalytics.fetchCampaignAnalytics(supabaseClient, campaignId);
                document.getElementById('analyticsCampaignTitle').textContent = analytics.campaign?.name || 'Campaign Analytics';
                document.getElementById('analyticsCampaignSubtitle').textContent = analytics.campaign?.brief_text || 'Performance, payout, submission, and milestone insights for this campaign.';
                document.getElementById('analyticsLoading').style.display = 'none';

                if (!(analytics.creator_metrics || []).length) {
                    document.getElementById('analyticsEmpty').style.display = 'block';
                    return;
                }

                document.getElementById('analyticsContent').style.display = 'block';
                renderMetricCards(analytics);
                renderInsights(analytics);
                renderCreatorTable(analytics);
                renderCharts(analytics);
            } catch (error) {
                document.getElementById('analyticsLoading').innerHTML = `<h3>Could not load analytics</h3><p>${error.message}</p>`;
            }
        }

        init();