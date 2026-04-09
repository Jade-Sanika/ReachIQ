(function initReachIQNotifications(global) {
  const BRAND_PAGES = new Set([
    'brand-dashboard.html',
    'brand-campaigns.html',
    'brand-offers.html',
    'brand-notifications.html',
    'create-campaign.html',
    'creator-discovery.html',
    'video-tracker.html',
  ]);

  function getCurrentPage() {
    const parts = global.location.pathname.split('/');
    return parts[parts.length - 1] || 'index.html';
  }

  function getRoleFromPage() {
    const page = getCurrentPage();
    if (BRAND_PAGES.has(page)) return 'brand';
    if (page.startsWith('influencer-')) return 'influencer';
    return null;
  }

  function getNotificationsPage(role) {
    return role === 'brand' ? 'brand-notifications.html' : 'influencer-notifications.html';
  }

  function formatDate(value) {
    if (!value) return 'Just now';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return 'Just now';
    return date.toLocaleString('en-IN', {
      day: 'numeric',
      month: 'short',
      hour: 'numeric',
      minute: '2-digit',
    });
  }

  function getReadKey(role, userId) {
    return `reachiq_notifications_read_${role}_${userId}`;
  }

  function readSeenIds(role, userId) {
    try {
      const raw = global.localStorage.getItem(getReadKey(role, userId));
      const parsed = JSON.parse(raw || '[]');
      return new Set(Array.isArray(parsed) ? parsed : []);
    } catch (error) {
      return new Set();
    }
  }

  function writeSeenIds(role, userId, ids) {
    global.localStorage.setItem(getReadKey(role, userId), JSON.stringify(Array.from(ids)));
  }

  function markNotificationRead(role, userId, notificationId) {
    const seen = readSeenIds(role, userId);
    seen.add(notificationId);
    writeSeenIds(role, userId, seen);
  }

  function injectBell(role, unreadCount) {
    const navLinks = document.querySelector('.nav-links');
    if (!navLinks || navLinks.querySelector('.nav-notification-link')) return;

    const bellLink = document.createElement('a');
    const page = getCurrentPage();
    const notificationsPage = getNotificationsPage(role);
    bellLink.href = notificationsPage;
    bellLink.className = `nav-notification-link${page === notificationsPage ? ' active' : ''}`;
    bellLink.setAttribute('aria-label', 'Notifications');
    bellLink.innerHTML = `
      <span class="nav-bell-icon">&#128276;</span>
      <span class="nav-notification-badge ${unreadCount ? '' : 'is-hidden'}">${unreadCount || ''}</span>
    `;

    const logoutButton = navLinks.querySelector('.btn-logout');
    if (logoutButton) navLinks.insertBefore(bellLink, logoutButton);
    else navLinks.appendChild(bellLink);
  }

  function renderNotificationsPage(role, userId, notifications) {
    const list = document.getElementById('notificationsList');
    if (!list) return;

    const seen = readSeenIds(role, userId);
    if (!notifications.length) {
      list.innerHTML = `
        <div class="empty-state">
          <h3>No notifications yet</h3>
          <p>Offer, negotiation, milestone, and payment updates will appear here.</p>
        </div>
      `;
      return;
    }

    list.innerHTML = notifications.map((notification) => {
      const unread = !seen.has(notification.id);
      return `
        <button
          type="button"
          class="notification-item ${unread ? 'is-unread' : 'is-read'}"
          data-id="${notification.id}"
          data-target="${notification.target_url || ''}"
        >
          <div class="notification-topline">
            <span class="notification-title">${notification.title || 'Notification'}</span>
            <span class="notification-date">${formatDate(notification.created_at)}</span>
          </div>
          <p class="notification-message">${notification.message || ''}</p>
          <span class="notification-pill">${(notification.kind || 'update').replace(/_/g, ' ')}</span>
        </button>
      `;
    }).join('');

    list.querySelectorAll('.notification-item').forEach((item) => {
      item.addEventListener('click', () => {
        const notificationId = item.dataset.id;
        const target = item.dataset.target || getNotificationsPage(role);
        markNotificationRead(role, userId, notificationId);
        global.location.href = target;
      });
    });
  }

  async function loadNotifications(role, session) {
    const response = await fetch(global.ReachIQ.apiUrl('/notifications'), {
      headers: {
        Authorization: `Bearer ${session.access_token}`,
      },
    });
    const payload = await global.ReachIQ.parseJsonResponse(response);
    if (!response.ok) throw new Error(payload.error || 'Failed to load notifications');
    return payload.notifications || [];
  }

  async function init() {
    const role = getRoleFromPage();
    if (!role || !global.ReachIQ || !global.supabase) return;

    try {
      const client = global.ReachIQ.createSupabaseClient();
      const [{ data: { user } }, { data: { session } }] = await Promise.all([
        client.auth.getUser(),
        client.auth.getSession(),
      ]);
      if (!user || !session) return;

      const notifications = await loadNotifications(role, session);
      const seen = readSeenIds(role, user.id);
      const unreadCount = notifications.filter((notification) => !seen.has(notification.id)).length;

      injectBell(role, unreadCount);
      renderNotificationsPage(role, user.id, notifications);
    } catch (error) {
      console.error('Notifications unavailable:', error);
      injectBell(role, 0);
    }
  }

  global.ReachIQNotifications = {
    init,
    markNotificationRead,
  };

  document.addEventListener('DOMContentLoaded', init);
})(window);
