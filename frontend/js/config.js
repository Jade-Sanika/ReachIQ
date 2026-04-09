(function initReachIQConfig(global) {
  function createTabScopedStorage() {
    const fallbackMemory = new Map();
    const hasSessionStorage = (() => {
      try {
        const probeKey = '__reachiq_storage_probe__';
        global.sessionStorage.setItem(probeKey, '1');
        global.sessionStorage.removeItem(probeKey);
        return true;
      } catch (error) {
        return false;
      }
    })();

    if (hasSessionStorage) {
      return {
        getItem(key) {
          return global.sessionStorage.getItem(key);
        },
        setItem(key, value) {
          global.sessionStorage.setItem(key, value);
        },
        removeItem(key) {
          global.sessionStorage.removeItem(key);
        },
      };
    }

    return {
      getItem(key) {
        return fallbackMemory.has(key) ? fallbackMemory.get(key) : null;
      },
      setItem(key, value) {
        fallbackMemory.set(key, value);
      },
      removeItem(key) {
        fallbackMemory.delete(key);
      },
    };
  }

  const config = {
    supabaseUrl: 'https://rbqphouxghbiynlqanmp.supabase.co',
    supabaseAnonKey: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJicXBob3V4Z2hiaXlubHFhbm1wIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjA2NTAzNzIsImV4cCI6MjA3NjIyNjM3Mn0.VWSTqtXNy-twpJWk1VJqWRLfomKex8maiSf6wVuFlLM',
    apiBaseUrl: (() => {
      const { protocol, hostname, port } = global.location;
      const isLocalFile = protocol === 'file:';
      const isLocalhost = hostname === '127.0.0.1' || hostname === 'localhost' || hostname === '';
      return isLocalFile || (isLocalhost && port && port !== '5000')
        ? 'http://127.0.0.1:5000/api'
        : '/api';
    })(),
    legacyUsdToInr: 83,
    createSupabaseClient() {
      return global.supabase.createClient(this.supabaseUrl, this.supabaseAnonKey, {
        auth: {
          persistSession: true,
          autoRefreshToken: true,
          detectSessionInUrl: true,
          storageKey: 'reachiq-auth-session',
          storage: createTabScopedStorage(),
        },
      });
    },
    apiUrl(path = '') {
      if (!path) return this.apiBaseUrl;
      return `${this.apiBaseUrl}${path.startsWith('/') ? path : `/${path}`}`;
    },
    formatInr(value, options = {}) {
      const amount = Number(value || 0);
      return new Intl.NumberFormat('en-IN', {
        style: 'currency',
        currency: 'INR',
        maximumFractionDigits: options.maximumFractionDigits ?? 0,
        minimumFractionDigits: options.minimumFractionDigits ?? 0,
      }).format(amount);
    },
    displayMoney(value) {
      if (value === null || value === undefined || value === '') return 'Negotiable';
      if (typeof value === 'number') return this.formatInr(value);

      const text = String(value);
      if (text.includes('₹')) return text;
      if (!text.includes('$')) return text;

      const numbers = Array.from(text.matchAll(/(\d[\d,]*)/g)).map((match) => Number(match[1].replace(/,/g, '')) * this.legacyUsdToInr);
      if (!numbers.length) return text.replace(/\$/g, '₹');
      if (text.includes('+')) return `${this.formatInr(numbers[0])}+`;
      if (numbers.length === 1) return this.formatInr(numbers[0]);
      return `${this.formatInr(numbers[0])} - ${this.formatInr(numbers[1])}`;
    },
    async parseJsonResponse(response) {
      const raw = await response.text();
      if (!raw) return {};
      try {
        return JSON.parse(raw);
      } catch (error) {
        if (raw.trim().startsWith('<')) {
          throw new Error('The API returned an HTML page instead of JSON. Make sure the Flask backend is running on http://127.0.0.1:5000.');
        }
        throw new Error(raw);
      }
    },
    async requireRole(client, expectedRole) {
      const { data: { user } } = await client.auth.getUser();
      if (!user) {
        global.location.href = '/';
        return null;
      }

      const { data: profile, error } = await client
        .from('profiles')
        .select('role')
        .eq('id', user.id)
        .single();

      if (error || !profile) {
        global.location.href = '/';
        return null;
      }

      if (profile.role !== expectedRole) {
        global.location.href = profile.role === 'brand' ? '/brand-dashboard.html' : '/influencer-dashboard.html';
        return null;
      }

      return { user, profile };
    }
  };

  global.ReachIQ = config;
})(window);
