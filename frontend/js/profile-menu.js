(function initReachIQProfileMenu(global) {
  function pageName() {
    const parts = global.location.pathname.split('/');
    return parts[parts.length - 1] || 'index.html';
  }

  function initialsFromName(name = '') {
    const bits = String(name).trim().split(/\s+/).filter(Boolean);
    if (!bits.length) return 'U';
    return bits.slice(0, 2).map((part) => part[0]).join('').toUpperCase();
  }

  function buildMenuLinks(role) {
    if (role === 'brand') {
      return [
        { href: 'brand-dashboard.html', icon: '&#128200;', label: 'Dashboard' },
        { href: 'brand-profile.html', icon: '&#128100;', label: 'Brand Profile' },
        { href: 'brand-notifications.html', icon: '&#128276;', label: 'Notifications' },
        { href: 'brand-campaigns.html', icon: '&#128221;', label: 'Campaigns' },
      ];
    }

    return [
      { href: 'influencer-dashboard.html', icon: '&#128200;', label: 'Dashboard' },
      { href: 'influencer-profile.html', icon: '&#128100;', label: 'My Profile' },
      { href: 'influencer-notifications.html', icon: '&#128276;', label: 'Notifications' },
    ];
  }

  function buildProfileMenuMarkup(profile, user) {
    const name = profile?.full_name || user?.user_metadata?.full_name || 'ReachIQ User';
    const email = user?.email || '';
    const role = profile?.role || 'member';
    const activePage = pageName();
    const links = buildMenuLinks(role)
      .filter((link) => link.href !== activePage)
      .map((link) => `
        <a class="nav-profile-link" href="${link.href}">
          <span><span>${link.icon}</span><span>${link.label}</span></span>
          <span>&rsaquo;</span>
        </a>
      `)
      .join('');

    return `
      <div class="nav-profile-header">
        <div class="nav-profile-name">${name}</div>
        <div class="nav-profile-email">${email}</div>
        <div class="nav-profile-role">${role}</div>
      </div>
      <div class="nav-profile-links">
        ${links}
      </div>
      <button type="button" class="nav-profile-signout" data-profile-signout="true">
        <span><span>&#10162;</span><span>Sign Out</span></span>
      </button>
    `;
  }

  async function performLogout(client) {
    try {
      await client.auth.signOut();
    } catch (error) {
      console.error('Profile menu logout failed:', error);
    }
    global.location.href = '/';
  }

  async function init() {
    if (!global.ReachIQ || !global.supabase) return;

    const navLinks = document.querySelector('.nav-links');
    const logoutButton = navLinks?.querySelector('.btn-logout');
    if (!navLinks || !logoutButton || navLinks.querySelector('.nav-profile')) return;

    const client = global.ReachIQ.createSupabaseClient();
    const [{ data: { user } }, { data: { session } }] = await Promise.all([
      client.auth.getUser(),
      client.auth.getSession(),
    ]);
    if (!user || !session) return;

    let profile = null;
    try {
      const response = await client.from('profiles').select('full_name, role, avatar_url').eq('id', user.id).single();
      profile = response.data || null;
    } catch (error) {
      profile = null;
    }

    const displayName = profile?.full_name || user?.user_metadata?.full_name || user?.email;
    const avatarMarkup = profile?.avatar_url
      ? `<img src="${profile.avatar_url}" alt="${displayName || 'Profile'}" class="nav-profile-avatar">`
      : initialsFromName(displayName);

    const wrapper = document.createElement('div');
    wrapper.className = 'nav-profile';
    wrapper.innerHTML = `
      <button type="button" class="nav-profile-trigger" aria-label="Open profile menu">
        ${avatarMarkup}
      </button>
      <div class="nav-profile-menu" aria-hidden="true">
        ${buildProfileMenuMarkup(profile, user)}
      </div>
    `;

    const trigger = wrapper.querySelector('.nav-profile-trigger');
    const menu = wrapper.querySelector('.nav-profile-menu');
    const signout = wrapper.querySelector('[data-profile-signout="true"]');

    function closeMenu() {
      menu.classList.remove('is-open');
      trigger.classList.remove('is-open');
      menu.setAttribute('aria-hidden', 'true');
    }

    function toggleMenu() {
      const nextOpen = !menu.classList.contains('is-open');
      if (!nextOpen) {
        closeMenu();
        return;
      }
      menu.classList.add('is-open');
      trigger.classList.add('is-open');
      menu.setAttribute('aria-hidden', 'false');
    }

    trigger.addEventListener('click', (event) => {
      event.stopPropagation();
      toggleMenu();
    });

    signout.addEventListener('click', async (event) => {
      event.preventDefault();
      closeMenu();
      await performLogout(client);
    });

    document.addEventListener('click', (event) => {
      if (!wrapper.contains(event.target)) closeMenu();
    });

    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') closeMenu();
    });

    logoutButton.replaceWith(wrapper);
  }

  document.addEventListener('DOMContentLoaded', init);
})(window);
