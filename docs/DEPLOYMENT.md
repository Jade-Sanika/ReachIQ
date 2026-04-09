# Deployment Guide

## Environment
- Copy `backend/.env.example` to `backend/.env`
- Fill in `SUPABASE_URL`, `SUPABASE_KEY`, `SUPABASE_SERVICE_KEY`, `GEMINI_API_KEY`, and `YOUTUBE_API_KEY`

## Database
- Apply the baseline schema in `supabase/migrations/001_initial_schema.sql`
- Apply the AI feature tables in `supabase/migrations/002_ai_features.sql`
- Apply the campaign delivery workflow changes in `supabase/migrations/003_campaign_delivery_workflow.sql`
- Confirm Supabase Auth is enabled for email/password
- Verify RLS policies before exposing the project publicly

## Backend
- Create a virtual environment inside `backend`
- Install dependencies with `pip install -r requirements.txt`
- Run from the repo root with `python run.py`

## Frontend
- Frontend assets are served by Flask from the `frontend` folder
- Shared browser configuration now lives in `frontend/js/config.js`

## Validation Checklist
- `GET /api/health` returns a live timestamp
- Brand login can create campaigns and load dashboard stats
- Brand discovery loads authenticated matches for `?campaign=<id>`
- Brand and creator offer pages can update statuses without placeholder alerts
- Creator can submit a deliverable link from the offers page
- Brand can review submissions in `brand-campaigns.html` and open offer-specific analytics in `video-tracker.html?offer=<id>`
- Milestone and payment status updates persist through the fulfillment flow
- Creator rate calculator and profile polish work with valid Gemini credentials

## Production Checklist
- Disable Flask debug mode
- Rotate any keys that were previously logged or hardcoded in unsafe places
- Put the Flask app behind a production WSGI server and reverse proxy
- Add monitoring, request logging, and API error alerts
- Restrict CORS to trusted origins
