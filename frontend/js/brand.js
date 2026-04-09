function formatOfferAmount(offer) {
    if (offer.negotiated_amount) return window.ReachIQ.formatInr(offer.negotiated_amount);
    if (offer.influencer_quote) return window.ReachIQ.formatInr(offer.influencer_quote);
    return offer.campaigns?.budget_range || offer.brand_budget_range || 'Negotiable';
}

function initialsFromName(name = '') {
    return name
        .split(' ')
        .filter(Boolean)
        .map(part => part[0])
        .join('')
        .slice(0, 2)
        .toUpperCase();
}

window.ReachIQBrand = {
    formatOfferAmount,
    initialsFromName
};
