// Supabase configuration
const SUPABASE_URL = 'https://rbqphouxghbiynlqanmp.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJicXBob3V4Z2hiaXlubHFhbm1wIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjA2NTAzNzIsImV4cCI6MjA3NjIyNjM3Mn0.VWSTqtXNy-twpJWk1VJqWRLfomKex8maiSf6wVuFlLM';

console.log('Initializing Supabase with URL:', SUPABASE_URL);

// Initialize Supabase client
const supabase = supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

console.log('Supabase initialized successfully');

// Make it globally available
window.supabase = supabase;