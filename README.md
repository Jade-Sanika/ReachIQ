## ReachIQ

ReachIQ is an AI-assisted influencer marketing platform for brands and creators. Brands can create campaigns, discover creators, send offers, and track ROI. Creators can sync their YouTube profile, review offers, calculate rates, and polish their bios with AI.

## Stack
- Frontend: static multi-page HTML/CSS/JS
- Backend: Flask
- Database/Auth: Supabase
- AI: Gemini for existing platform AI features, Ollama for the local chatbot assistant
- External data: YouTube Data API

## Core Features
- Brand and creator authentication
- AI-assisted campaign brief parsing from documents and voice
- YouTube-based creator profile sync
- AI-assisted creator matching and campaign scoring
- Offer management for both brands and creators
- AI smart reply generation
- AI profile polish
- AI-assisted creator rate calculation
- Video tracker for YouTube deliverables
- Campaign delivery workflow with milestones, creator video submission, analytics review, negotiation summaries, and escrow-style payment tracking

## Project Structure
- `frontend/`: pages, styles, and shared browser config
- `backend/`: Flask API, business logic, requirements, tests
- `supabase/migrations/`: baseline database schema and RLS policies
- `docs/`: deployment notes
- `Campaigns/`: sample campaign brief files

## Local Setup
1. Copy `backend/.env.example` to `backend/.env`
2. Create a virtual environment inside `backend`
3. Install dependencies with `pip install -r requirements.txt`
4. Apply `supabase/migrations/001_initial_schema.sql`
5. Apply `supabase/migrations/002_ai_features.sql`
6. Apply `supabase/migrations/003_campaign_delivery_workflow.sql`
7. Run the app from the repo root with `python run.py`

## Chatbot LLM Setup
The ReachIQ chatbot assistant is configured separately from the rest of the platform AI features.

Recommended chatbot setup in `backend/.env`:
- `ASSISTANT_LLM_PROVIDER=groq`
- `GROQ_API_KEY=your-groq-api-key`
- `GROQ_BASE_URL=https://api.groq.com/openai/v1`
- `GROQ_MODEL=llama-3.1-8b-instant`
- `GROQ_TIMEOUT_SECONDS=20`

Current chatbot model:
- `llama-3.1-8b-instant` via Groq

Important:
- The chatbot uses Groq when `ASSISTANT_LLM_PROVIDER=groq`.
- Other existing AI features like some document/voice/profile/review flows still rely on Gemini unless we migrate them separately.
- If Groq is unavailable or times out, chatbot requests fall back to deterministic backend behavior where possible.

Optional local alternative:
- `ASSISTANT_LLM_PROVIDER=ollama`
- `OLLAMA_BASE_URL=http://127.0.0.1:11434`
- `OLLAMA_MODEL=qwen3:4b`
- `OLLAMA_TIMEOUT_SECONDS=20`

## Testing
- Run backend logic tests with `pytest backend/tests`

## Deployment Readiness Improvements Included
- Removed sensitive Supabase service-key logging
- Restored authenticated campaign matching
- Replaced mocked health timestamp and brand dashboard stats with live values
- Added dedicated offer APIs to avoid brittle browser-side joins and RLS workarounds
- Replaced dead-end offer placeholder flows with working manage/detail flows
- Centralized frontend Supabase config in `frontend/js/config.js`
- Added missing backend dependencies
- Added baseline schema, env template, tests, and deployment docs

## Delivery Workflow
- Brands can set campaign milestone targets and timeline requirements during campaign creation
- Accepted creators can submit YouTube deliverable links directly from their offers page
- Brands can review each submission from Manage Campaigns, open offer-specific analytics, and decide whether milestones were met
- Negotiation summaries are shown on both brand and creator views
- Payment state now supports an escrow-style progression from `escrow_pending` to `payment_processing` to `paid`

## Notes
- The current matching engine is still heuristic rather than model-based
- The video tracker still contains demo audience-prediction charts and should not be presented as real ML output in production
