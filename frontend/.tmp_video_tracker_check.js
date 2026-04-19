
        const supabaseClient = window.ReachIQ.createSupabaseClient();
        const charts = {};
        let lastAnalyzedVideoUrl = '';
        let currentOfferContext = null;
        let trackerLoadingDepth = 0;
        let currentOfferId = null;

        Chart.defaults.font.family = "'DM Sans', sans-serif";
        Chart.defaults.color = '#9ba3b8';

        async function ensureBrandAccess() {
            return await window.ReachIQ.requireRole(supabaseClient, 'brand');
        }

        function currentOfferAmount() {
            return Number(currentOfferContext?.payment_amount || 0) || 0;
        }

        function currentOfferAmountLabel() {
            const amount = currentOfferAmount();
            return amount > 0 ? window.ReachIQ.formatInr(amount) : 'Negotiable';
        }

        function analyticsCacheKey(offerId) {
            return `reachiq_offer_analytics_${offerId}`;
        }

        function offerContextCacheKey(offerId) {
            return `reachiq_offer_context_${offerId}`;
        }

        function showTrackerLoading(title = 'Loading analytics...', message = 'Fetching campaign, milestone, and video performance details.') {
            trackerLoadingDepth += 1;
            document.getElementById('trackerLoadingTitle').textContent = title;
            document.getElementById('trackerLoadingText').textContent = message;
            document.getElementById('trackerLoadingOverlay').style.display = 'flex';
        }

        function hideTrackerLoading() {
            trackerLoadingDepth = Math.max(0, trackerLoadingDepth - 1);
            if (trackerLoadingDepth === 0) {
                document.getElementById('trackerLoadingOverlay').style.display = 'none';
            }
        }

        async function analyzeVideo() {
            const url = document.getElementById('videoUrl').value;
            const milestone = document.getElementById('milestoneTarget').value || 0;
            const errorDiv = document.getElementById('trackerError');
            const btn = document.getElementById('analyzeBtn');
            
            if (!url) { errorDiv.textContent = "Please enter a YouTube URL."; errorDiv.style.display = 'block'; return; }
            errorDiv.style.display = 'none';
            
            btn.innerHTML = '<span class="loading-spinner" style="width:16px;height:16px;border-width:2px;margin:0;display:inline-block;vertical-align:middle;margin-right:6px;"></span> Analyzing...';
            btn.disabled = true;
            
            try {
                const { data: { session } } = await supabaseClient.auth.getSession();
                if (!session) throw new Error("Please log in first.");
                
                const response = await fetch('/api/brand/analyze-video', { 
                    method: 'POST', 
                    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${session.access_token}` }, 
                    body: JSON.stringify({ youtube_url: url, milestone: milestone }) 
                });
                
                const result = await response.json();
                if (result.status === 'success') {
                    lastAnalyzedVideoUrl = url;
                    renderDashboard({ data: result.data });
                }
                else throw new Error(result.error);
                
            } catch (err) { 
                errorDiv.textContent = err.message; 
                errorDiv.style.display = 'block'; 
            } finally { 
                btn.innerHTML = '<span>✦</span> Analyze'; 
                btn.disabled = false; 
            }
        }

        function badgeHtml(label, tone = 'warning') {
            return `<span class="insight-badge ${tone}">${label}</span>`;
        }

        function renderDecisionSummary(model) {
            const container = document.getElementById('decisionSummaryGrid');
            const cards = model.summaryCards || [];
            container.innerHTML = cards.map(card => `
                <div class="decision-card ${card.tone || 'warning'}">
                    <p class="decision-label">${card.label}</p>
                    <h3>${card.value}</h3>
                    <p>${card.note || ''}</p>
                </div>
            `).join('');
            container.style.display = cards.length ? 'grid' : 'none';
        }

        function renderValueAnalysis(model) {
            const insights = model.insights || {};
            const container = document.getElementById('valueAnalysisGrid');
            container.innerHTML = `
                <div class="decision-card ${model.badges.pricing.tone}">
                    <p class="decision-label">Creator Value Analysis</p>
                    <h3>${model.badges.pricing.label}</h3>
                    <p>CPM: ${Number(insights.cpm || 0).toFixed(2)} • Cost / Engagement: ${window.ReachIQ.formatInr(insights.cost_per_engagement || 0, { maximumFractionDigits: 2 })}</p>
                </div>
                <div class="decision-card ${Number(insights.roi_multiple || 0) >= 2 ? 'success' : Number(insights.roi_multiple || 0) >= 1 ? 'warning' : 'danger'}">
                    <p class="decision-label">ROI Multiple</p>
                    <h3>${insights.roi_multiple ? `${Number(insights.roi_multiple).toFixed(2)}x` : 'N/A'}</h3>
                    <p>Earned media value versus payout cost for this creator.</p>
                </div>
                <div class="decision-card ${model.badges.performer.tone}">
                    <p class="decision-label">Creator Performance Score</p>
                    <h3>${Number(insights.creator_score || 0).toFixed(1)} / 100</h3>
                    <p>${model.badges.performer.label}</p>
                </div>
                <div class="decision-card ${model.badges.future.tone}">
                    <p class="decision-label">Future Recommendation</p>
                    <h3>${model.badges.future.label}</h3>
                    <p>Based on ROI, engagement, and QA fit for similar campaigns.</p>
                </div>
            `;
            container.style.display = 'grid';
        }

        function renderDecisionPanels(model) {
            const insights = model.insights || {};
            document.getElementById('decisionPanelsGrid').style.display = 'grid';

            document.getElementById('qaInsightChips').innerHTML = [
                badgeHtml(`Coverage ${Number(insights.talking_point_coverage_pct || 0).toFixed(0)}%`, Number(insights.talking_point_coverage_pct || 0) >= 75 ? 'success' : Number(insights.talking_point_coverage_pct || 0) >= 50 ? 'warning' : 'danger'),
                badgeHtml(`CTA ${insights.cta_present ? 'Present' : 'Missing'}`, insights.cta_present ? 'success' : 'danger'),
                badgeHtml(`Alignment ${Number(insights.brand_alignment_score || 0).toFixed(0)}/100`, Number(insights.brand_alignment_score || 0) >= 70 ? 'success' : Number(insights.brand_alignment_score || 0) >= 50 ? 'warning' : 'danger'),
            ].join('');
            document.getElementById('qaMissingList').innerHTML = (insights.missing_points || []).length
                ? insights.missing_points.map(item => `<li>Missing talking point: ${item}</li>`).join('')
                : '<li>Talking points are well covered and CTA checks look healthy.</li>';

            document.getElementById('riskFlagChips').innerHTML = [
                badgeHtml(`Brand Safety: ${insights.brand_safety_level || 'unknown'}`, model.badges.safety.tone),
                badgeHtml(`Sentiment: ${insights.sentiment || 'neutral'}`, model.badges.sentiment.tone),
                badgeHtml(`Competitors: ${(insights.competitor_mentions || []).length}`, (insights.competitor_mentions || []).length ? 'danger' : 'success'),
            ].join('');
            document.getElementById('riskFlagList').innerHTML = (model.flags || []).length
                ? model.flags.map(flag => `<li>${flag}</li>`).join('')
                : '<li>No major risk flags detected in the current deliverable review.</li>';

            document.getElementById('audienceInsightChips').innerHTML = [
                badgeHtml(`Audience Match ${Number(insights.audience_match_pct || 0).toFixed(0)}%`, Number(insights.audience_match_pct || 0) >= 70 ? 'success' : Number(insights.audience_match_pct || 0) >= 50 ? 'warning' : 'danger'),
                badgeHtml(`Target Geos ${Math.max((insights.campaign_geo_targets || []).length, 1)}`, 'warning'),
            ].join('');
            document.getElementById('audienceGeoList').innerHTML = (insights.top_geographies || []).length
                ? insights.top_geographies.map(item => `<li>Top geography: ${item}</li>`).join('')
                : '<li>No creator geography data is saved yet. Audience match is based on the available profile signals.</li>';

            document.getElementById('creatorDecisionChips').innerHTML = [
                badgeHtml(model.badges.performer.label, model.badges.performer.tone),
                badgeHtml(model.badges.pricing.label, model.badges.pricing.tone),
                badgeHtml(model.badges.future.label, model.badges.future.tone),
            ].join('');
            document.getElementById('creatorDecisionList').innerHTML = `
                <li>Final recommendation: ${insights.final_recommendation || 'Needs Improvement'}.</li>
                <li>ROI score: ${insights.roi_multiple ? `${Number(insights.roi_multiple).toFixed(2)}x` : 'N/A'} with CPM ${Number(insights.cpm || 0).toFixed(2)}.</li>
                <li>Creator score: ${Number(insights.creator_score || 0).toFixed(1)} / 100.</li>
            `;
        }

        function renderDashboard(payload) {
            const data = payload?.data || payload || {};
            const model = window.ReachIQTrackerAnalytics.buildTrackerViewModel(payload?.data ? payload : { data });
            document.getElementById('dashboardSection').style.display = 'block';
            document.getElementById('dashboardSection').scrollIntoView({ behavior: 'smooth', block: 'start' });
            const sourceBadge = document.getElementById('analyticsSourceBadge');
            const usingFallback = data?.data_source === 'tracker_cache';
            sourceBadge.textContent = usingFallback ? '● CACHED FALLBACK' : '● LIVE DATA';
            sourceBadge.style.color = usingFallback ? 'var(--gold)' : 'var(--teal)';
            renderDecisionSummary(model);
            
            // Basic Info
            document.getElementById('vidThumbnail').src = data.thumbnail;
            document.getElementById('vidTitle').textContent = data.title;
            document.getElementById('vidChannel').textContent = data.channel_name;
            document.getElementById('vidDate').textContent = data.published_at;
            document.getElementById('vidDuration').textContent = data.video_meta.duration;

            // Tags
            const tagsContainer = document.getElementById('vidTags');
            if (data.video_meta.tags && data.video_meta.tags.length > 0) {
                tagsContainer.innerHTML = data.video_meta.tags.map(t => `<span class="tag-pill">#${t}</span>`).join('');
            } else {
                tagsContainer.innerHTML = '';
            }

            // KPIs
            document.getElementById('kpiViews').textContent = data.metrics.views.toLocaleString();
            document.getElementById('kpiLikes').textContent = data.metrics.likes.toLocaleString();
            document.getElementById('kpiComments').textContent = data.metrics.comments.toLocaleString();
            document.getElementById('kpiEngagement').textContent = data.metrics.engagement_rate + '%';
            document.getElementById('kpiLikeRatio').textContent = data.metrics.like_to_view_ratio + '%';
            document.getElementById('kpiEmv').textContent = window.ReachIQ.formatInr(data.metrics.earned_media_value_inr || 0, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            
            // Milestone
            const pbFill = document.getElementById('progressBar');
            const badge = document.getElementById('milestoneBadge');
            const msText = document.getElementById('milestoneText');
            const pct = Math.min(data.milestone.progress_percentage, 100);
            
            pbFill.style.width = pct + '%';
            document.getElementById('progressPct').textContent = Math.round(pct) + '%';
            
            if (data.milestone.target === 0) { 
                badge.textContent = "No Target"; badge.className = "milestone-badge-pending"; msText.textContent = "Enter a view target to track milestone progress."; 
            } else if (data.milestone.is_reached) { 
                badge.textContent = "✅ Reached"; badge.className = "milestone-badge-success"; pbFill.style.background = "linear-gradient(90deg, #4dd9c0, #2ec9ad)"; msText.textContent = `Surpassed ${data.milestone.target.toLocaleString()} views — ready for next payout.`; 
            } else { 
                badge.textContent = "⏳ In Progress"; badge.className = "milestone-badge-pending"; msText.textContent = `Needs ${data.milestone.views_remaining.toLocaleString()} more views to reach the target.`; 
            }
            
            renderValueAnalysis(model);
            renderDecisionPanels(model);
            drawCharts(data, model);
        }

        function drawCharts(data, model) {
            Object.values(charts).forEach(c => c && c.destroy());
            const gridColor = 'rgba(248,244,239,0.06)';
            const tickColor = '#5c6480';
            const chartData = model.chartData;

            // Bar: Views vs Target
            charts.bar = new Chart(document.getElementById('milestoneBarChart').getContext('2d'), {
                type: 'bar',
                data: chartData.viewsComparison,
                options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { grid: { color: gridColor }, ticks: { color: tickColor } }, y: { grid: { color: gridColor }, ticks: { color: tickColor } } } }
            });

            // Engagement vs benchmark
            charts.pie = new Chart(document.getElementById('engagementPieChart').getContext('2d'), {
                type: 'bar',
                data: chartData.engagementVsBenchmark,
                options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { grid: { color: gridColor }, ticks: { color: tickColor } }, y: { grid: { color: gridColor }, ticks: { color: tickColor }, beginAtZero: true } } }
            });

            // CPM vs industry
            charts.radar = new Chart(document.getElementById('benchmarkRadarChart').getContext('2d'), {
                type: 'bar',
                data: chartData.cpmVsIndustry,
                options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { grid: { color: gridColor }, ticks: { color: tickColor } }, y: { grid: { color: gridColor }, ticks: { color: tickColor }, beginAtZero: true } } }
            });

            // Views trend
            charts.demo = new Chart(document.getElementById('demographicsBarChart').getContext('2d'), {
                type: 'line',
                data: chartData.viewsTrend,
                options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { color: '#9ba3b8' } } }, scales: { x: { grid: { color: gridColor }, ticks: { color: tickColor } }, y: { grid: { color: gridColor }, ticks: { color: tickColor }, beginAtZero: true } } }
            });

            // Engagement trend
            charts.geo = new Chart(document.getElementById('geoDoughnutChart').getContext('2d'), {
                type: 'line',
                data: chartData.engagementTrend,
                options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { color: '#9ba3b8' } } }, scales: { x: { grid: { color: gridColor }, ticks: { color: tickColor } }, y: { grid: { color: gridColor }, ticks: { color: tickColor }, beginAtZero: true } } }
            });
        }

        function renderQaPlaceholder(message = 'Open analytics from a campaign submission to see the automatic QA review.') {
            document.getElementById('qaResults').style.display = 'none';
            document.getElementById('qaResults').innerHTML = '';
            document.getElementById('qaEmptyState').textContent = message;
            document.getElementById('qaEmptyState').style.display = 'block';
        }

        function renderQaResults(review, requirements = null) {
            const container = document.getElementById('qaResults');
            const talkingPoints = review.talking_point_coverage || [];
            const actionItems = review.action_items || [];
            const competitors = review.competitor_mentions || [];
            const requirementNotes = [];
            if (requirements?.required_cta) requirementNotes.push(`CTA: ${requirements.required_cta}`);
            if ((requirements?.talking_points || []).length) requirementNotes.push(`Talking points: ${requirements.talking_points.length}`);
            if ((requirements?.competitors || []).length) requirementNotes.push(`Competitors flagged: ${requirements.competitors.join(', ')}`);

            container.innerHTML = `
                <div class="qa-summary-grid">
                    <div class="qa-summary-card">
                        <p class="qa-label">Overall Score</p>
                        <p class="qa-value">${review.overall_score || 0}/100</p>
                        <p class="qa-text">${review.summary || ''}</p>
                    </div>
                    <div class="qa-summary-card">
                        <p class="qa-label">Brand Safety</p>
                        <p class="qa-value" style="text-transform:capitalize;">${review.brand_safety?.level || 'unknown'}</p>
                        <p class="qa-text">${review.brand_safety?.notes || ''}</p>
                    </div>
                    <div class="qa-summary-card">
                        <p class="qa-label">CTA Compliance</p>
                        <p class="qa-value" style="text-transform:capitalize;">${review.cta_compliance?.status || 'unknown'}</p>
                        <p class="qa-text">${review.cta_compliance?.evidence || ''}</p>
                    </div>
                    <div class="qa-summary-card">
                        <p class="qa-label">Campaign Checks Used</p>
                        <p class="qa-text">${requirementNotes.join(' • ') || 'Campaign brief and milestone target were used for the automatic review.'}</p>
                    </div>
                </div>
                <div class="qa-detail-grid">
                    <div class="qa-detail-card">
                        <h3>Talking Point Coverage</h3>
                        <div class="qa-chip-list">
                            ${talkingPoints.map(item => `<span class="qa-chip ${item.status === 'covered' ? 'covered' : 'missing'}">${item.point}: ${item.status}</span>`).join('') || '<span class="qa-chip">No talking points provided</span>'}
                        </div>
                    </div>
                    <div class="qa-detail-card">
                        <h3>Sentiment & Competitors</h3>
                        <p class="qa-text"><strong>Sentiment:</strong> ${review.sentiment || 'unknown'}</p>
                        <p class="qa-text"><strong>Competitor Mentions:</strong> ${competitors.length ? competitors.join(', ') : 'None flagged'}</p>
                    </div>
                    <div class="qa-detail-card">
                        <h3>Action Items</h3>
                        <ul class="qa-actions">
                            ${actionItems.map(item => `<li>${item}</li>`).join('') || '<li>No action items.</li>'}
                        </ul>
                    </div>
                </div>`;
            container.style.display = 'block';
            document.getElementById('qaEmptyState').style.display = 'none';
        }

        function renderOfferContext(offer) {
            currentOfferContext = offer;
            document.getElementById('offerContextPanel').style.display = 'block';
            document.getElementById('offerContextTitle').textContent = `${offer.creator?.full_name || 'Creator'} • ${offer.campaign?.name || 'Campaign'}`;
            document.getElementById('offerContextSummary').innerHTML = `
                <div class="qa-summary-card">
                    <p class="qa-label">Negotiation</p>
                    <p class="qa-text">${offer.negotiation_summary?.headline || 'No summary available.'}</p>
                </div>
                <div class="qa-summary-card">
                    <p class="qa-label">Escrow Status</p>
                    <p class="qa-value" style="font-size:1rem;text-transform:capitalize;">${(offer.payment_status || 'escrow_pending').replace(/_/g, ' ')}</p>
                    <p class="qa-text">${offer.decision_summary?.payment_recommendation || ''}</p>
                </div>
                <div class="qa-summary-card">
                    <p class="qa-label">Milestone Target</p>
                    <p class="qa-value" style="font-size:1rem;">${(offer.campaign?.milestone_views || 0).toLocaleString()} views</p>
                    <p class="qa-text">${offer.campaign?.timeline_requirements || 'No timeline requirements saved.'}</p>
                </div>`;

            document.getElementById('offerTimeline').innerHTML = (offer.timeline || []).map(item => `
                <div class="timeline-item ${item.status}">
                    <strong>${item.label}</strong>
                    <span>${item.detail || ''}</span>
                </div>
            `).join('');

            const actionRow = document.getElementById('paymentActionRow');
            if (offer.payment_status === 'awaiting_brand_release' && offer.decision_summary?.milestone_reached) {
                actionRow.innerHTML = `
                    <div class="payment-decision-card">
                        <div>
                            <p class="payment-decision-label">Escrow Decision</p>
                            <h3>Milestone hit. Release ${currentOfferAmountLabel()} to ${offer.creator?.full_name || 'the creator'}?</h3>
                            <p>${offer.decision_summary?.payment_recommendation || 'Review the analytics, then choose whether to release the demo escrow payment now.'}</p>
                        </div>
                        <div class="payment-decision-actions">
                            <button class="btn-secondary" onclick="holdEscrowRelease()">No, Keep On Hold</button>
                            <button class="btn-primary" onclick="openHostedPaymentPage()">Yes, Process Payment</button>
                        </div>
                    </div>
                `;
            } else if (offer.payment_status === 'paid') {
                actionRow.innerHTML = `
                    <div class="payment-decision-card paid">
                        <div>
                            <p class="payment-decision-label">Escrow Completed</p>
                            <h3>${currentOfferAmountLabel()} released successfully</h3>
                            <p>${offer.decision_summary?.payment_recommendation || 'The fake escrow payment has been completed and notifications were sent to both parties.'}</p>
                        </div>
                    </div>
                `;
            } else {
                actionRow.innerHTML = `
                    <button class="btn-secondary" onclick="updatePaymentState('payment_processing')">Start Payment</button>
                    <button class="btn-primary" onclick="updatePaymentState('paid')">Mark Paid</button>
                `;
            }
        }

        async function loadOfferAnalytics(offerId) {
            showTrackerLoading('Loading analytics...', 'Pulling the latest campaign, milestone, and YouTube performance data for this deliverable.');
            try {
                const { data: { session } } = await supabaseClient.auth.getSession();
                const response = await fetch(`/api/offers/${offerId}/analytics`, {
                    headers: { 'Authorization': `Bearer ${session.access_token}` }
                });
                const payload = await response.json();
                if (!response.ok) throw new Error(payload.error || 'Failed to load offer analytics');

                lastAnalyzedVideoUrl = payload.offer?.deliverable_url || '';
                if (lastAnalyzedVideoUrl) {
                    document.getElementById('videoUrl').value = lastAnalyzedVideoUrl;
                }
                if (payload.offer?.campaign?.milestone_views) {
                    document.getElementById('milestoneTarget').value = payload.offer.campaign.milestone_views;
                }

                try {
                    window.sessionStorage.setItem(analyticsCacheKey(offerId), JSON.stringify(payload));
                } catch (error) {
                    console.warn('Could not cache offer analytics:', error);
                }

                renderOfferContext(payload.offer);
                renderDashboard(payload);
                if (payload.review) {
                    renderQaResults(payload.review, payload.qa_requirements || {});
                } else {
                    renderQaPlaceholder('Automatic QA was not available for this deliverable.');
                }
            } finally {
                hideTrackerLoading();
            }
        }

        function hydrateCachedOfferContext(offerId) {
            try {
                const raw = window.sessionStorage.getItem(offerContextCacheKey(offerId));
                if (!raw) return;
                const cached = JSON.parse(raw);
                if (cached?.campaigns?.milestone_views) {
                    document.getElementById('milestoneTarget').value = cached.campaigns.milestone_views;
                }
                if (cached?.deliverable_url) {
                    document.getElementById('videoUrl').value = cached.deliverable_url;
                    lastAnalyzedVideoUrl = cached.deliverable_url;
                }
                renderOfferContext({
                    ...cached,
                    creator: cached.influencer || cached.creator,
                    campaign: cached.campaign || cached.campaigns,
                });
            } catch (error) {
                console.warn('Could not hydrate cached offer context:', error);
            }
        }

        function hydrateCachedAnalytics(offerId) {
            try {
                const raw = window.sessionStorage.getItem(analyticsCacheKey(offerId));
                if (!raw) return false;
                const cached = JSON.parse(raw);
                if (!cached?.offer || !cached?.data) return false;

                lastAnalyzedVideoUrl = cached.offer?.deliverable_url || '';
                if (lastAnalyzedVideoUrl) {
                    document.getElementById('videoUrl').value = lastAnalyzedVideoUrl;
                }
                if (cached.offer?.campaign?.milestone_views) {
                    document.getElementById('milestoneTarget').value = cached.offer.campaign.milestone_views;
                }
                renderOfferContext(cached.offer);
                renderDashboard(cached);
                if (cached.review) {
                    renderQaResults(cached.review, cached.qa_requirements || {});
                }
                return true;
            } catch (error) {
                console.warn('Could not hydrate cached analytics:', error);
                return false;
            }
        }

        async function updatePaymentState(paymentStatus) {
            if (!currentOfferContext?.id) return;

            const { data: { session } } = await supabaseClient.auth.getSession();
            const response = await fetch('/api/offers/update-fulfillment', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${session.access_token}`
                },
                body: JSON.stringify({
                    offer_id: currentOfferContext.id,
                    deliverable_status: paymentStatus === 'paid' ? 'milestone_hit' : currentOfferContext.deliverable_status,
                    payment_status: paymentStatus,
                    review_notes: currentOfferContext.review_notes || 'Updated from analytics dashboard'
                })
            });
            const payload = await response.json();
            if (!response.ok) throw new Error(payload.error || 'Failed to update payment status');
            await loadOfferAnalytics(currentOfferContext.id);
        }

        function openHostedPaymentPage() {
            if (!currentOfferContext?.id) return;
            try {
                window.sessionStorage.setItem(`reachiq_checkout_offer_${currentOfferContext.id}`, JSON.stringify({
                    id: currentOfferContext.id,
                    payment_amount: currentOfferContext.payment_amount,
                    creator_name: currentOfferContext.creator?.full_name || 'Creator',
                    campaign_name: currentOfferContext.campaign?.name || 'Campaign',
                    milestone_views: currentOfferContext.campaign?.milestone_views || 0,
                }));
            } catch (error) {
                console.warn('Could not cache checkout context:', error);
            }
            window.location.href = `demo-payment.html?offer=${currentOfferContext.id}`;
        }

        async function holdEscrowRelease() {
            if (!currentOfferContext?.id) return;
            try {
                const { data: { session } } = await supabaseClient.auth.getSession();
                const response = await fetch('/api/offers/demo-payment/hold', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${session.access_token}`
                    },
                    body: JSON.stringify({
                        offer_id: currentOfferContext.id,
                        review_notes: currentOfferContext.review_notes || 'Brand kept escrow release on hold after milestone review.'
                    })
                });
                const payload = await response.json();
                if (!response.ok) throw new Error(payload.error || 'Failed to keep payment on hold');
                await loadOfferAnalytics(currentOfferContext.id);
                alert('Escrow payment kept on hold.');
            } catch (error) {
                alert(error.message);
            }
        }

        async function logout() { await supabaseClient.auth.signOut(); window.location.href = '/'; }
        document.addEventListener('DOMContentLoaded', async () => {
            await ensureBrandAccess();
            renderQaPlaceholder();
            const params = new URLSearchParams(window.location.search);
            const offerId = params.get('offer');
            if (offerId) {
                currentOfferId = offerId;
                hydrateCachedOfferContext(offerId);
                hydrateCachedAnalytics(offerId);
                try {
                    await loadOfferAnalytics(offerId);
                } catch (error) {
                    document.getElementById('trackerError').textContent = error.message;
                    document.getElementById('trackerError').style.display = 'block';
                }
            }
        });
    