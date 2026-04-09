function formatCampaignBudget(offer) {
    if (offer.negotiated_amount) return window.ReachIQ.formatInr(offer.negotiated_amount);
    return offer.campaigns?.budget_range || offer.brand_budget_range || 'Negotiable';
}

function formatBrandName(offer) {
    return offer.brand_name || 'Brand Partner';
}

window.ReachIQInfluencer = {
    formatCampaignBudget,
    formatBrandName
};
