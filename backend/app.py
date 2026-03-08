from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from supabase import create_client, Client
import os
from dotenv import load_dotenv
import fitz  # PyMuPDF
import docx  # python-docx
import json
import google.generativeai as genai
import mimetypes
import requests

# Load environment variables
load_dotenv()

app = Flask(__name__, static_folder='../frontend', static_url_path='')
CORS(app)

# Get Supabase credentials from environment
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_KEY')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY') # <-- NEW: YouTube Key


if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise ValueError("Supabase credentials not found in environment variables")

print(f"Supabase URL: {SUPABASE_URL}")
print(f"Supabase Service Key: {SUPABASE_SERVICE_KEY}")
print(f"Supabase Key: {SUPABASE_KEY}")

# Initialize Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# --- NEW: Configure Gemini API Key ---
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in environment variables")
genai.configure(api_key=GEMINI_API_KEY)

def get_current_user():
    """Get current user from Supabase session"""
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return None
    
    token = auth_header.replace('Bearer ', '')
    
    try:
        # Use Supabase client to get the user from token
        response = supabase.auth.get_user(token)
        if response.user:
            return response.user.id
    except Exception as e:
        print(f"Error getting user from token: {e}")
    
    return None

# --- NEW: Gemini Audio Transcription Function ---
#
def transcribe_audio_with_gemini(audio_file_stream, mime_type):
    """
    Uploads audio file stream to Gemini and returns the transcription.
    """
    print(f"--- Uploading audio to Gemini ({mime_type}) ---")
    
    # 1. Upload the file to the Gemini Files API.
    # We pass the file stream directly. Files are auto-deleted after 48h.
    audio_file = genai.upload_file(
        path=audio_file_stream,  # Pass the FileStorage object directly
        display_name="voice-brief-upload",
        mime_type=mime_type
    )
    print(f"Gemini File API: Uploaded file {audio_file.name}")

    # 2. Call the model with the uploaded file and the prompt.
    # We use a model that supports audio understanding.
    model = genai.GenerativeModel('gemini-2.5-flash') # Or your working model
    prompt = "Transcribe this audio. Provide only the raw text of the speech."
    
    response = model.generate_content([prompt, audio_file])
    
    # 3. Clean up the file from Gemini's storage to save space.
    genai.delete_file(audio_file.name)
    print(f"Gemini File API: Deleted file {audio_file.name}")
    
    return response.text

#
# --- NEW: AI Parsing Function (Replaced with Gemini) ---
#
def parse_brief_text_with_ai(document_text):
    """
    Analyzes raw text from a document using the Gemini API
    and extracts structured campaign data.
    """
    
    print("--- Calling Gemini API to parse brief ---")

    # Set up the model with JSON output
    model = genai.GenerativeModel('gemini-2.5-flash')
    generation_config = genai.GenerationConfig(
        response_mime_type="application/json"
    )
    
    prompt = f"""
    You are an expert campaign assistant. Analyze the following campaign brief text
    and extract the key information in a valid JSON format.

    The JSON keys must be: "name", "brief_text", "target_audience", "goals", "budget_range", "platforms".

    - "name": The campaign's title. If no clear title, create a concise one.
    - "brief_text": A concise summary of the campaign goals, deliverables, and key messages.
    - "target_audience": The specific target audience (e.g., "Fitness enthusiasts, 18-35").
    - "goals": The primary campaign goals (e.g., "Brand awareness, Product sales").
    - "budget_range": The budget. If mentioned, match it to one of:
      "$500 - $1,000", "$1,000 - $2,500", "$2,500 - $5,000", "$5,000 - $10,000", "$10,000+".
      If not mentioned, unclear, or "negotiable", set to an empty string.
    - "platforms": A comma-separated list of platforms (e.g., "instagram, youtube, tiktok").

    Document Text:
    \"\"\"
    {document_text}
    \"\"\"
    """

    response = None  # <--- THIS IS THE FIX. Initialize response to None.

    try:
        response = model.generate_content(
            prompt,
            generation_config=generation_config
        )
        
        json_response = response.text
        print(f"Gemini AI response: {json_response}")
        return json.loads(json_response)
        
    except Exception as e:
        print(f"Gemini API call failed: {e}")
        # This line will now work safely. If response is None, it will print 'No text'.
        print(f"Gemini response content (if any): {getattr(response, 'text', 'No text')}") 
        raise  # Re-raise the exception to be caught by the Flask route
# --- NEW: AI Profile Generation from YouTube Description ---
def generate_profile_from_description(channel_name, description):
    print("--- Asking Gemini to generate profile from YouTube description ---")
    model = genai.GenerativeModel('gemini-2.5-flash')
    generation_config = genai.GenerationConfig(response_mime_type="application/json")
    
    prompt = f"""
    You are an AI profiling expert. Based on this YouTube channel's name and description, infer the best profile details for an influencer marketing platform.

    Channel Name: {channel_name}
    Description: {description}

    Return a valid JSON with exactly these keys:
    - "bio": A short, catchy 2-sentence bio written in third-person.
    - "niche": Choose ONE from: fitness, beauty, fashion, tech, travel, food, lifestyle, gaming, education, business, sports.
    - "audience_age": Infer their main viewer age. Choose ONE: "13-17", "18-24", "25-34", "35-44", "45+".
    - "audience_gender": Infer their main viewer gender. Choose ONE: "male", "female", "mixed".
    - "audience_interests": A short comma-separated list of 3-4 things their audience likes.
    - "content_description": A 1-sentence description of the type of videos they make.
    
    If the description is empty, make your best generic guess based on the channel name, or return defaults like "lifestyle" and "mixed".
    """
    try:
        response = model.generate_content(prompt, generation_config=generation_config)
        return json.loads(response.text)
    except Exception as e:
        print(f"Gemini Profile Generation Failed: {e}")
        return {} # Return empty dict if it fails, so the code doesn't break


# Serve frontend files
@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory(app.static_folder, path)


# --- NEW: YouTube Sync & Calculation Route ---
@app.route('/api/influencer/sync-youtube', methods=['POST', 'OPTIONS'])
def sync_youtube_stats():
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
        
    handle = request.json.get('youtube_handle', '').strip()
    if not handle:
        return jsonify({"error": "YouTube handle is required"}), 400

    handle = handle.replace("https://www.youtube.com/", "").replace("/", "")
    if not handle.startswith('@'):
        handle = f"@{handle}"

    try:
        print(f"\n--- Starting YouTube Sync for {handle} ---")
        
        # 1. Fetch Channel Stats & Uploads Playlist ID
        yt_url = f"https://youtube.googleapis.com/youtube/v3/channels?part=snippet,statistics,contentDetails&forHandle={handle}&key={YOUTUBE_API_KEY}"
        yt_res = requests.get(yt_url).json()

        # 🚨 NEW: Catch the exact YouTube API error and print it!
        if "error" in yt_res:
            error_message = yt_res['error'].get('message', 'Unknown YouTube Error')
            print(f"❌ YOUTUBE API ERROR: {error_message}")
            return jsonify({"error": f"YouTube API Error: {error_message}"}), 400

        if "items" not in yt_res or not yt_res["items"]:
            print(f"❌ Channel {handle} not found on YouTube.")
            return jsonify({"error": f"YouTube channel {handle} not found."}), 404

        print("✅ Channel found! Extracting stats...")
        channel = yt_res["items"][0]
        title = channel["snippet"]["title"]
        description = channel["snippet"]["description"]
        avatar_url = channel["snippet"]["thumbnails"]["high"]["url"]
        
        stats = channel["statistics"]
        subscribers = int(stats.get("subscriberCount", 0))
        total_views = int(stats.get("viewCount", 0))
        video_count = int(stats.get("videoCount", 0))
        
        # Try to get the uploads playlist, default to None if the channel has no videos
        uploads_playlist_id = None
        if "contentDetails" in channel and "relatedPlaylists" in channel["contentDetails"]:
            uploads_playlist_id = channel["contentDetails"]["relatedPlaylists"].get("uploads")

        # 2. Fetch last 5 videos to calculate Engagement Rate
        engagement_rate = 0.0
        if uploads_playlist_id:
            print("🎬 Fetching recent videos for engagement rate...")
            playlist_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId={uploads_playlist_id}&maxResults=5&key={YOUTUBE_API_KEY}"
            playlist_res = requests.get(playlist_url).json()
            
            video_ids = [item["contentDetails"]["videoId"] for item in playlist_res.get("items", [])]
            
            if video_ids:
                video_ids_str = ",".join(video_ids)
                videos_url = f"https://youtube.googleapis.com/youtube/v3/videos?part=statistics&id={video_ids_str}&key={YOUTUBE_API_KEY}"
                videos_res = requests.get(videos_url).json()
                
                recent_views = 0
                recent_engagements = 0
                
                for vid in videos_res.get("items", []):
                    v_stats = vid.get("statistics", {})
                    recent_views += int(v_stats.get("viewCount", 0))
                    recent_engagements += int(v_stats.get("likeCount", 0)) + int(v_stats.get("commentCount", 0))
                
                if recent_views > 0:
                    engagement_rate = round((recent_engagements / recent_views) * 100, 2)

        # 3. Use Gemini AI to generate profile details
        print("🤖 Asking Gemini AI to build the profile...")
        ai_profile = generate_profile_from_description(title, description)

        # 4. Update Database
        print("💾 Saving all data to Supabase...")
        supabase.table('profiles').update({'avatar_url': avatar_url, 'full_name': title}).eq('id', user_id).execute()

        influencer_data = {
            'follower_count': subscribers,
            'total_views': total_views,
            'video_count': video_count,
            'engagement_rate': engagement_rate,
            'youtube_channel': handle,
            'platform': 'youtube',
            'channel_description': description,
            'bio': ai_profile.get('bio', 'Content creator on YouTube.'),
            'niche': ai_profile.get('niche', 'lifestyle'),
            'audience_age': ai_profile.get('audience_age', '18-24'),
            'audience_gender': ai_profile.get('audience_gender', 'mixed'),
            'audience_interests': ai_profile.get('audience_interests', ''),
            'content_description': ai_profile.get('content_description', '')
        }

        supabase.table('influencer_profiles').update(influencer_data).eq('profile_id', user_id).execute()

        print("🎉 Sync Complete!")
        return jsonify({"status": "success", "message": "Synced & Generated", "data": influencer_data})

    except Exception as e:
        print(f"❌ CRITICAL ERROR syncing YouTube: {e}")
        return jsonify({"error": str(e)}), 500
    
    # API Routes
@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy", 
        "timestamp": "2025-10-18T08:33:14Z",
        "message": "ReachIQ API is running!"
    })

@app.route('/api/test-db', methods=['GET'])
def test_db_connection():
    """Test database connection"""
    try:
        # Try to query profiles table
        response = supabase.table('profiles').select('*').limit(1).execute()
        return jsonify({
            "status": "success",
            "message": "Database connection successful",
            "data": response.data
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Database connection failed: {str(e)}"
        }), 500

# User management API
@app.route('/api/users/profile', methods=['GET'])
def get_user_profile():
    """Get user profile - requires authentication"""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
    
    try:
        # Get user profile from Supabase
        response = supabase.table('profiles').select('*').eq('id', user_id).execute()
        
        if not response.data:
            return jsonify({"error": "User not found"}), 404
            
        return jsonify({
            "status": "success",
            "user": response.data[0]
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# AI Matching Algorithm - TEMPORARY FIX: Allow without auth for testing
@app.route('/api/campaigns/<campaign_id>/match-influencers', methods=['GET'])
def match_influencers(campaign_id):
    """AI-powered influencer matching for a campaign"""
    # TEMPORARY: Allow without authentication for testing
    # user_id = get_current_user()
    # if not user_id:
    #     return jsonify({"error": "Authentication required"}), 401
        
    try:
        # Get campaign details - remove brand_id check temporarily
        campaign_response = supabase.table('campaigns').select('*').eq('id', campaign_id).execute()
        if not campaign_response.data:
            return jsonify({"error": "Campaign not found"}), 404
        
        campaign = campaign_response.data[0]
        
        # Get all influencers
        influencers_response = supabase.table('influencer_profiles').select('*, profile:profiles(*)').execute()
        influencers = influencers_response.data
        
        # Calculate match scores for each influencer
        matched_influencers = []
        for influencer in influencers:
            score = calculate_enhanced_match_score(campaign, influencer)
            
            if score > 0.3:  # Only show influencers with >30% match
                matched_influencers.append({
                    **influencer,
                    'match_score': score,
                    'match_percentage': round(score * 100)
                })
        
        # Sort by match score (highest first)
        matched_influencers.sort(key=lambda x: x['match_score'], reverse=True)
        
        return jsonify({
            "status": "success",
            "campaign": campaign,
            "matched_influencers": matched_influencers[:12],  # Top 12 matches
            "total_matches": len(matched_influencers)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def calculate_enhanced_match_score(campaign, influencer_profile):
    """Enhanced matching algorithm using influencer profile data"""
    score = 0
    max_score = 0
    
    campaign_text = (campaign.get('brief_text') or '').lower()
    campaign_audience = (campaign.get('target_audience') or '').lower()
    campaign_platforms = (campaign.get('platforms') or '').lower()
    
    # 1. Platform Match (20% weight)
    if campaign_platforms and influencer_profile.get('platform'):
        campaign_platforms_list = [p.strip().lower() for p in campaign_platforms.split(',')] if campaign_platforms else []
        influencer_platform = influencer_profile['platform'].lower()
        if influencer_platform in campaign_platforms_list:
            score += 0.2
        max_score += 0.2
    
    # 2. Niche Match (25% weight)
    if campaign_text and influencer_profile.get('niche'):
        influencer_niche = influencer_profile['niche'].lower()
        
        niche_keywords = {
            'fitness': ['fitness', 'workout', 'exercise', 'gym', 'health', 'wellness', 'nutrition'],
            'beauty': ['beauty', 'makeup', 'skincare', 'cosmetic', 'glam', 'selfcare'],
            'fashion': ['fashion', 'style', 'clothing', 'outfit', 'trend', 'wear'],
            'tech': ['tech', 'technology', 'gadget', 'electronic', 'innovation', 'digital'],
            'travel': ['travel', 'vacation', 'tour', 'adventure', 'explore', 'destination'],
            'food': ['food', 'cooking', 'recipe', 'restaurant', 'culinary', 'dish'],
            'lifestyle': ['lifestyle', 'life', 'daily', 'routine', 'home', 'family'],
            'gaming': ['gaming', 'game', 'esports', 'stream', 'console', 'pc']
        }
        
        if influencer_niche in niche_keywords:
            for keyword in niche_keywords[influencer_niche]:
                if keyword in campaign_text:
                    score += 0.25
                    break
        max_score += 0.25
    
    # 3. Audience Match (20% weight)
    if campaign_audience and influencer_profile.get('bio'):
        audience_indicators = ['18-25', '25-35', '35-45', '45+', 'teen', 'young', 'adult', 'professional']
        influencer_bio = (influencer_profile.get('bio') or '').lower()
        
        for indicator in audience_indicators:
            if indicator in campaign_audience and indicator in influencer_bio:
                score += 0.2
                break
        max_score += 0.2
    
    # 4. Follower Count & Engagement (25% weight)
    follower_count = influencer_profile.get('follower_count', 0)
    engagement_rate = influencer_profile.get('engagement_rate', 0)
    
    # Score based on follower count tier
    if follower_count >= 100000:  # Macro-influencer
        score += 0.15
    elif follower_count >= 10000:  # Micro-influencer
        score += 0.1
    elif follower_count >= 1000:   # Nano-influencer
        score += 0.05
    
    # Score based on engagement rate
    if engagement_rate and engagement_rate >= 5.0:    # Excellent engagement
        score += 0.1
    elif engagement_rate and engagement_rate >= 3.0:  # Good engagement
        score += 0.05
        
    max_score += 0.25
    
    # 5. Budget Match (10% weight)
    if campaign.get('budget_range') and influencer_profile.get('rate_range'):
        campaign_budget = campaign['budget_range'].lower()
        influencer_rate = influencer_profile['rate_range'].lower()
        
        if campaign_budget == influencer_rate:
            score += 0.1
        max_score += 0.1
    
    # Normalize score
    if max_score > 0:
        final_score = score / max_score
        return min(final_score, 1.0)
    return 0


# --- NEW: API Route for Parsing VOICE Briefs ---
#
@app.route('/api/campaigns/parse-brief-voice', methods=['POST'])
def parse_campaign_voice():
    """
    Receives an audio file, transcribes it,
    and then parses the text to extract campaign data.
    """
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
    
    if 'brief_audio' not in request.files:
        return jsonify({"error": "No audio file part"}), 400
    
    file = request.files['brief_audio']
    
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    try:
        mime_type = file.mimetype
        print(f"Received voice file with mime_type: {mime_type}")
        
        # --- Step 1: Voice-to-Text ---
        # We pass the file stream object directly to the function
        transcribed_text = transcribe_audio_with_gemini(file.stream, mime_type)
        print(f"Transcribed Text: {transcribed_text}")

        # --- Step 2: Text-to-JSON (Reusing your existing function!) ---
        extracted_data = parse_brief_text_with_ai(transcribed_text)
        
        return jsonify({
            "status": "success",
            "message": "Voice brief parsed successfully",
            "brief_data": extracted_data
        })

    except Exception as e:
        print(f"Error parsing voice brief: {e}")
        return jsonify({"error": f"Failed to process voice brief: {str(e)}"}), 500
    


# --- MODIFIED: API Route for Parsing Campaign Documents ---
#
@app.route('/api/campaigns/parse-brief-doc', methods=['POST'])
def parse_campaign_document():
    """
    Parses an uploaded campaign brief document (.pdf, .docx, .txt)
    and returns structured JSON data from the Gemini API.
    """
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
    
    if 'brief_doc' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['brief_doc']
    
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    text = ""
    try:
        if file.filename.endswith('.pdf'):
            doc = fitz.open(stream=file.read(), filetype="pdf")
            for page in doc:
                text += page.get_text()
            doc.close()
        elif file.filename.endswith('.docx'):
            doc = docx.Document(file)
            for para in doc.paragraphs:
                text += para.text + "\n"
        elif file.filename.endswith('.txt'):
            text = file.read().decode('utf-8')
        else:
            return jsonify({"error": "Unsupported file type. Please use .pdf, .docx, or .txt"}), 400

        if not text.strip():
            return jsonify({"error": "Document appears to be empty"}), 400
            
        # --- This is where the *NEW* Gemini Agent does its work ---
        extracted_data = parse_brief_text_with_ai(text)
        
        return jsonify({
            "status": "success",
            "message": "Document parsed successfully",
            "brief_data": extracted_data
        })

    except Exception as e:
        print(f"Error parsing document: {e}")
        return jsonify({"error": f"Failed to process file: {str(e)}"}), 500



# Send offer to influencer
@app.route('/api/offers/send', methods=['POST'])
def send_offer():
    """Send an offer to an influencer"""
    user_id = get_current_user()
    if not user_id:
        return jsonify({"error": "Authentication required"}), 401
        
    try:
        data = request.json
        campaign_id = data.get('campaign_id')
        influencer_id = data.get('influencer_id')
        brand_notes = data.get('brand_notes', '')
        
        # Verify campaign belongs to the brand
        campaign_response = supabase.table('campaigns').select('*').eq('id', campaign_id).eq('brand_id', user_id).execute()
        if not campaign_response.data:
            return jsonify({"error": "Campaign not found or access denied"}), 404
        
        # Create offer
        offer_data = {
            'campaign_id': campaign_id,
            'influencer_id': influencer_id,
            'status': 'pending',
            'brand_notes': brand_notes,
            'brand_budget_range': campaign_response.data[0].get('budget_range', '')
        }
        
        response = supabase.table('offers').insert(offer_data).execute()
        
        if response.data:
            return jsonify({
                "status": "success",
                "message": "Offer sent successfully",
                "offer": response.data[0]
            })
        else:
            return jsonify({"error": "Failed to create offer"}), 500
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# Get brand dashboard stats - TEMPORARY FIX: Return mock data
@app.route('/api/brand/dashboard-stats', methods=['GET'])
def get_brand_dashboard_stats():
    """Get dashboard statistics for brand"""
    # TEMPORARY: Return mock data without authentication
    # user_id = get_current_user()
    # if not user_id:
    #     return jsonify({"error": "Authentication required"}), 401
        
    try:
        # Return mock data for testing
        return jsonify({
            "status": "success",
            "stats": {
                "active_campaigns": 3,
                "pending_offers": 5,
                "completed_deals": 12,
                "total_spent": 4500
            }
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)