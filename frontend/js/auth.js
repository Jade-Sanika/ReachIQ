import { supabaseClient } from './supabase.js';
import { showMessage, redirectTo, apiCall } from './utils.js';

// Authentication state management
let currentUser = null;
let currentProfile = null;

// Check authentication status
async function checkAuth() {
    try {
        const { data: { user }, error } = await supabaseClient.auth.getUser();
        
        if (error) throw error;
        
        if (!user) {
            // Not logged in - stay on current page if it's index.html
            if (!window.location.pathname.endsWith('/') && 
                !window.location.pathname.includes('index.html')) {
                redirectTo('/');
            }
            return { user: null, profile: null };
        }
        
        // Get user profile from our API
        const token = await getToken();
        const profileData = await apiCall('/users/profile', {
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });
        
        currentUser = user;
        currentProfile = profileData.user;
        
        // Redirect based on role
        handleRoleBasedRedirect(profileData.user.role);
        
        return { user, profile: profileData.user };
        
    } catch (error) {
        console.error('Auth check error:', error);
        // If API call fails, try to get profile directly from Supabase
        return await fallbackAuthCheck();
    }
}

async function fallbackAuthCheck() {
    try {
        const { data: { user } } = await supabaseClient.auth.getUser();
        
        if (!user) {
            redirectTo('/');
            return { user: null, profile: null };
        }
        
        // Get profile directly from Supabase
        const { data: profile, error } = await supabaseClient
            .from('profiles')
            .select('*')
            .eq('id', user.id)
            .single();
            
        if (error) throw error;
        
        currentUser = user;
        currentProfile = profile;
        
        handleRoleBasedRedirect(profile.role);
        
        return { user, profile };
        
    } catch (error) {
        console.error('Fallback auth check failed:', error);
        redirectTo('/');
        return { user: null, profile: null };
    }
}

async function getToken() {
    const { data: { session } } = await supabaseClient.auth.getSession();
    return session?.access_token;
}

function handleRoleBasedRedirect(role) {
    const currentPath = window.location.pathname;
    
    // If user is on homepage, redirect to dashboard
    if (currentPath.endsWith('/') || currentPath.includes('index.html')) {
        if (role === 'brand') {
            redirectTo('/brand-dashboard.html');
        } else if (role === 'influencer') {
            redirectTo('/influencer-dashboard.html');
        }
    }
}

// Sign up function
async function signUp(email, password, fullName, role) {
    try {
        showMessage('Creating your account...', 'success');
        
        // Create auth user
        const { data: authData, error: authError } = await supabaseClient.auth.signUp({
            email,
            password,
        });
        
        if (authError) throw authError;
        
        // Create profile
        const { error: profileError } = await supabaseClient
            .from('profiles')
            .insert([
                {
                    id: authData.user.id,
                    username: email.split('@')[0],
                    full_name: fullName,
                    role: role
                }
            ]);
        
        if (profileError) throw profileError;
        
        // If influencer, create influencer profile
        if (role === 'influencer') {
            await supabaseClient
                .from('influencer_profiles')
                .insert([
                    {
                        profile_id: authData.user.id,
                        follower_count: 0,
                        platform: 'instagram',
                        niche: 'lifestyle',
                        bio: 'New creator on ReachIQ'
                    }
                ]);
        }
        
        showMessage('Account created successfully! Please check your email for verification.', 'success');
        
        // Auto login after signup
        setTimeout(async () => {
            await login(email, password);
        }, 2000);
        
    } catch (error) {
        console.error('Signup error:', error);
        showMessage(error.message, 'error');
    }
}

// Login function
async function login(email, password) {
    try {
        showMessage('Logging in...', 'success');
        
        const { data, error } = await supabaseClient.auth.signInWithPassword({
            email,
            password,
        });
        
        if (error) throw error;
        
        showMessage('Login successful!', 'success');
        
        // checkAuth will handle the redirect
        setTimeout(() => checkAuth(), 1000);
        
    } catch (error) {
        console.error('Login error:', error);
        showMessage(error.message, 'error');
    }
}

// Logout function
async function logout() {
    try {
        const { error } = await supabaseClient.auth.signOut();
        if (error) throw error;
        
        currentUser = null;
        currentProfile = null;
        
        redirectTo('/');
        
    } catch (error) {
        console.error('Logout error:', error);
        showMessage(error.message, 'error');
    }
}

// Get current user
function getCurrentUser() {
    return { user: currentUser, profile: currentProfile };
}

// Initialize auth on page load
document.addEventListener('DOMContentLoaded', function() {
    // Only check auth if not on login page
    if (!window.location.pathname.endsWith('/') && 
        !window.location.pathname.includes('index.html')) {
        checkAuth();
    }
});

export { checkAuth, signUp, login, logout, getCurrentUser };
