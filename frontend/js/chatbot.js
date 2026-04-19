(function initReachIQAssistant(global) {
  const BRAND_PAGES = new Set([
    'brand-dashboard.html',
    'brand-campaigns.html',
    'brand-offers.html',
    'brand-notifications.html',
    'create-campaign.html',
    'creator-discovery.html',
    'video-tracker.html',
  ]);

  const CREATOR_PAGES = new Set([
    'influencer-dashboard.html',
    'influencer-notifications.html',
    'influencer-offers.html',
    'influencer-profile.html',
    'influencer-profile-polish.html',
    'influencer-rate-calculator.html',
  ]);

  const state = {
    role: null,
    user: null,
    session: null,
    client: null,
    messages: [],
    sending: false,
  };

  function pageName() {
    const parts = global.location.pathname.split('/');
    return parts[parts.length - 1] || 'index.html';
  }

  function detectRole() {
    const page = pageName();
    if (BRAND_PAGES.has(page)) return 'brand';
    if (CREATOR_PAGES.has(page)) return 'influencer';
    return null;
  }

  function historyKey(role, userId) {
    return `reachiq_assistant_history_${role}_${userId}`;
  }

  function starterMessages(role) {
    return [
      {
        role: 'assistant',
        content: role === 'brand'
          ? 'I can help with your ReachIQ brand workspace. Ask me about campaigns, offers, matching creators, submissions, milestones, analytics, payments, notifications, or ask me to create campaigns and send offers with confirmation.'
          : 'I can help with your ReachIQ creator workspace. Ask me about received offers, campaign summaries, pending deliverables, payment status, notifications, or ask me to submit a deliverable link with confirmation.',
        tool: 'help',
      },
    ];
  }

  function refreshTargetsForPage(page) {
    const map = {
      'brand-dashboard.html': ['dashboard', 'campaigns', 'offers'],
      'brand-campaigns.html': ['campaigns', 'offers'],
      'brand-offers.html': ['offers', 'campaigns'],
      'brand-notifications.html': ['notifications'],
      'create-campaign.html': ['campaigns', 'dashboard'],
      'creator-discovery.html': ['discovery', 'offers', 'campaigns'],
      'video-tracker.html': ['analytics', 'campaigns', 'offers'],
      'influencer-dashboard.html': ['dashboard', 'offers', 'campaigns'],
      'influencer-notifications.html': ['notifications'],
      'influencer-offers.html': ['offers', 'campaigns', 'analytics'],
      'influencer-profile.html': ['profile'],
      'influencer-profile-polish.html': ['profile'],
      'influencer-rate-calculator.html': ['profile'],
    };
    return map[page] || [];
  }

  function loadHistory() {
    try {
      const raw = global.sessionStorage.getItem(historyKey(state.role, state.user.id));
      const parsed = JSON.parse(raw || '[]');
      if (Array.isArray(parsed) && parsed.length) return parsed;
    } catch (error) {
      console.warn('Could not load assistant history:', error);
    }
    return starterMessages(state.role);
  }

  function saveHistory() {
    try {
      global.sessionStorage.setItem(historyKey(state.role, state.user.id), JSON.stringify(state.messages));
    } catch (error) {
      console.warn('Could not save assistant history:', error);
    }
  }

  function clearHistory() {
    if (!state.role || !state.user) return;
    try {
      global.sessionStorage.removeItem(historyKey(state.role, state.user.id));
    } catch (error) {
      console.warn('Could not clear assistant history:', error);
    }
    state.messages = starterMessages(state.role);
    renderMessages();
  }

  function escapeHtml(value = '') {
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function formatMessageContent(text = '') {
    return escapeHtml(text).replace(/\n/g, '<br>');
  }

  function renderMessages() {
    const list = document.getElementById('assistantMessages');
    if (!list) return;

    list.innerHTML = state.messages.map((message) => `
      <div class="assistant-message assistant-message-${message.role}">
        <div class="assistant-bubble">
          ${message.tool ? `<div class="assistant-tool-pill">${escapeHtml(String(message.tool).replace(/_/g, ' '))}</div>` : ''}
          <div class="assistant-message-text">${formatMessageContent(message.content)}</div>
        </div>
      </div>
    `).join('');

    list.scrollTop = list.scrollHeight;
  }

  function injectAssistantUI() {
    if (document.getElementById('assistantToggle')) return;

    const wrapper = document.createElement('div');
    wrapper.className = 'assistant-root';
    wrapper.innerHTML = `
      <button type="button" id="assistantToggle" class="assistant-toggle" aria-label="Open ReachIQ assistant">
        <span class="assistant-toggle-ring"></span>
        <span class="assistant-toggle-icon">&#10022;</span>
      </button>
      <aside id="assistantDrawer" class="assistant-drawer" aria-hidden="true">
        <div class="assistant-drawer-header">
          <div>
            <p class="assistant-eyebrow">ReachIQ Assistant</p>
            <h3>${state.role === 'brand' ? 'Brand Workspace Copilot' : 'Creator Workspace Copilot'}</h3>
          </div>
          <button type="button" id="assistantClose" class="assistant-close" aria-label="Close assistant">&times;</button>
        </div>
        <div class="assistant-drawer-subtitle">Platform-aware support for ReachIQ only. I can answer workspace questions and, with confirmation, trigger supported platform actions for your role.</div>
        <div id="assistantMessages" class="assistant-messages"></div>
        <form id="assistantForm" class="assistant-form">
          <textarea id="assistantInput" class="assistant-input" rows="3" placeholder="${state.role === 'brand' ? 'Ask about campaigns, offers, analytics, or creators...' : 'Ask about offers, deliverables, payments, or campaign details...'}"></textarea>
          <div class="assistant-form-footer">
            <div id="assistantStatus" class="assistant-status">Ready</div>
            <button type="submit" id="assistantSend" class="btn-primary">Send</button>
          </div>
        </form>
      </aside>
    `;
    document.body.appendChild(wrapper);

    document.getElementById('assistantToggle').addEventListener('click', toggleDrawer);
    document.getElementById('assistantClose').addEventListener('click', closeDrawer);
    document.getElementById('assistantForm').addEventListener('submit', (event) => {
      event.preventDefault();
      sendCurrentInput();
    });

    const input = document.getElementById('assistantInput');
    input.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' && !event.shiftKey) {
        event.preventDefault();
        sendCurrentInput();
      }
    });
  }

  function openDrawer() {
    const drawer = document.getElementById('assistantDrawer');
    if (!drawer) return;
    drawer.classList.add('is-open');
    drawer.setAttribute('aria-hidden', 'false');
  }

  function closeDrawer() {
    const drawer = document.getElementById('assistantDrawer');
    if (!drawer) return;
    drawer.classList.remove('is-open');
    drawer.setAttribute('aria-hidden', 'true');
    clearHistory();
    setStatus('Ready', false);
    const input = document.getElementById('assistantInput');
    if (input) input.value = '';
  }

  function toggleDrawer() {
    const drawer = document.getElementById('assistantDrawer');
    if (!drawer) return;
    if (drawer.classList.contains('is-open')) closeDrawer();
    else openDrawer();
  }

  function setStatus(text, isBusy = false) {
    const status = document.getElementById('assistantStatus');
    const send = document.getElementById('assistantSend');
    const input = document.getElementById('assistantInput');
    if (status) status.textContent = text;
    if (send) {
      send.disabled = isBusy;
      send.innerHTML = isBusy
        ? '<span class="loading-spinner assistant-inline-spinner"></span> Working...'
        : 'Send';
    }
    if (input) input.disabled = isBusy;
  }

  function appendMessage(role, content, tool = '', data = null) {
    state.messages.push({ role, content, tool, data });
    saveHistory();
    renderMessages();
  }

  function replaceLastAssistantMessage(content, tool = '', data = null) {
    let index = -1;
    for (let cursor = state.messages.length - 1; cursor >= 0; cursor -= 1) {
      if (state.messages[cursor].role === 'assistant' && state.messages[cursor].isTyping) {
        index = cursor;
        break;
      }
    }
    if (index >= 0) {
      state.messages[index] = { role: 'assistant', content, tool, data };
    } else {
      state.messages.push({ role: 'assistant', content, tool, data });
    }
    saveHistory();
    renderMessages();
  }

  function shouldRefreshCurrentPage(targets) {
    if (!Array.isArray(targets) || !targets.length) return false;
    const currentTargets = new Set(refreshTargetsForPage(pageName()));
    return targets.some((target) => currentTargets.has(target));
  }

  function applyAssistantActionEffects(payload) {
    if (!payload) return;
    try {
      document.dispatchEvent(new CustomEvent('reachiq:assistant-action', { detail: payload }));
    } catch (error) {
      console.warn('Could not dispatch assistant action event:', error);
    }

    if (payload.redirect_to) {
      setStatus('Updating workspace...', true);
      global.setTimeout(() => {
        global.location.href = payload.redirect_to;
      }, 700);
      return;
    }

    if (payload.action_performed && shouldRefreshCurrentPage(payload.refresh_targets || [])) {
      setStatus('Refreshing workspace...', true);
      global.setTimeout(() => {
        global.location.reload();
      }, 700);
    }
  }

  async function sendCurrentInput() {
    if (state.sending) return;
    const input = document.getElementById('assistantInput');
    if (!input) return;
    const message = input.value.trim();
    if (!message) return;

    appendMessage('user', message);
    input.value = '';
    openDrawer();

    state.sending = true;
    state.messages.push({ role: 'assistant', content: 'Checking ReachIQ...', tool: 'thinking', isTyping: true });
    renderMessages();
    setStatus('Checking your workspace...', true);

    try {
      const response = await fetch(global.ReachIQ.apiUrl('/assistant/chat'), {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${state.session.access_token}`,
        },
        body: JSON.stringify({
          message,
          page_context: pageName(),
          history: state.messages
            .filter((entry) => !entry.isTyping)
            .map((entry) => ({ role: entry.role, content: entry.content, tool: entry.tool || '', data: entry.data || {} })),
        }),
      });
      const payload = await global.ReachIQ.parseJsonResponse(response);
      if (!response.ok) throw new Error(payload.error || 'Assistant request failed');

      replaceLastAssistantMessage(payload.reply || 'I could not find anything for that request.', payload.tool || '', payload.data || {});
      setStatus(
        payload.confirmation_required
          ? 'Awaiting confirmation'
          : (payload.needs_clarification ? 'Need more detail' : 'Ready'),
        false,
      );
      applyAssistantActionEffects(payload);
    } catch (error) {
      replaceLastAssistantMessage(error.message || 'Assistant request failed.', 'error');
      setStatus('Error', false);
    } finally {
      state.sending = false;
    }
  }

  async function init() {
    state.role = detectRole();
    if (!state.role || !global.ReachIQ || !global.supabase) return;

    state.client = global.ReachIQ.createSupabaseClient();
    const [{ data: { user } }, { data: { session } }] = await Promise.all([
      state.client.auth.getUser(),
      state.client.auth.getSession(),
    ]);
    if (!user || !session) return;

    let profile = null;
    try {
      const response = await state.client.from('profiles').select('role').eq('id', user.id).single();
      profile = response.data || null;
    } catch (error) {
      profile = null;
    }

    if (!profile || profile.role !== state.role) return;

    state.user = user;
    state.session = session;
    state.messages = loadHistory();

    injectAssistantUI();
    renderMessages();
  }

  document.addEventListener('DOMContentLoaded', init);
})(window);
