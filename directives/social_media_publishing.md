# Social Media Publishing System — Directive

> **Module**: Social Media Manager for Autodirecto CRM  
> **Status**: IN PROGRESS — Phases 1-4 built, Phase 5 partial (AI features)  
> **Date**: 2026-03-15 (updated)  
> **Owner**: SimplyAPI CRM (`templates/index.html` + `routes/social_routes.py`)

---

## 1. Goal

Build a full **Social Media Publishing & Analytics** module inside the CRM that allows users to:

1. **Compose posts** — select multiple photos (inventory + overlay), write captions, add hashtags
2. **Publish directly** to Instagram (feed, carousel, reels, stories) and Facebook Page
3. **Schedule posts** — visual calendar with drag-and-drop, auto-publish at scheduled time
4. **Track performance** — likes, comments, reach, impressions, engagement per post
5. **Dashboard analytics** — account-level stats, best posting times, growth trends

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      CRM FRONTEND (Alpine.js)                    │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────────────┐  │
│  │ Composer  │ │ Calendar │ │ Post List│ │  Analytics Dashboard│  │
│  │  - Photos │ │  - Month │ │  - Feed  │ │  - Reach/Impress.  │  │
│  │  - Caption│ │  - Week  │ │  - Status│ │  - Engagement      │  │
│  │  - Music  │ │  - Drag  │ │  - Stats │ │  - Growth          │  │
│  │  - Tags   │ │  - Drop  │ │  - Edit  │ │  - Best times      │  │
│  └─────┬─────┘ └────┬─────┘ └────┬─────┘ └──────────┬─────────┘  │
│        └─────────────┴───────────┴───────────────────┘            │
│                              │ API calls                          │
└──────────────────────────────┼────────────────────────────────────┘
                               │
┌──────────────────────────────┼────────────────────────────────────┐
│                     SimplyAPI BACKEND (Flask)                      │
│  ┌──────────────────┐ ┌─────────────────┐ ┌────────────────────┐  │
│  │  /api/social/*   │ │  Token Manager  │ │  Scheduler Worker  │  │
│  │  - compose       │ │  - refresh      │ │  - cron check      │  │
│  │  - publish       │ │  - long-lived   │ │  - auto-publish    │  │
│  │  - schedule      │ │  - validate     │ │  - retry on fail   │  │
│  │  - insights      │ │                 │ │                    │  │
│  └────────┬─────────┘ └────────┬────────┘ └─────────┬──────────┘  │
│           └────────────────────┴────────────────────┘              │
│                              │                                    │
└──────────────────────────────┼────────────────────────────────────┘
                               │
              ┌────────────────┴─────────────────┐
              │       Meta Graph API v25.0        │
              │  ┌────────┐  ┌─────────────────┐  │
              │  │IG API  │  │ FB Pages API    │  │
              │  │-publish │  │ - post          │  │
              │  │-insights│  │ - photos        │  │
              │  │-media   │  │ - scheduled     │  │
              │  └────────┘  └─────────────────┘  │
              └───────────────────────────────────┘
```

---

## 3. Meta API Integration Details

### 3.1 Required Permissions

| Permission | Purpose |
|---|---|
| `pages_manage_posts` | Publish posts to FB Page |
| `pages_read_engagement` | Read post insights on FB |
| `instagram_basic` | Read IG account info |
| `instagram_content_publish` | Publish to IG (feed, carousel, reels, stories) |
| `instagram_manage_insights` | Read IG post & account insights |
| `pages_read_user_content` | Read page posts |
| `publish_video` | Publish video/reels |

### 3.2 Instagram Publishing Flow

#### Single Image Post
```
1. POST /{ig-user-id}/media
   → image_url (must be public HTTPS URL)
   → caption (text + hashtags)
   → Returns: {id: "CONTAINER_ID"}

2. Wait → GET /{CONTAINER_ID}?fields=status_code
   → Poll until status = "FINISHED"

3. POST /{ig-user-id}/media_publish
   → creation_id = CONTAINER_ID
   → Returns: {id: "MEDIA_ID"}
```

#### Carousel Post (up to 10 images/videos)
```
1. For each image:
   POST /{ig-user-id}/media
   → image_url, is_carousel_item=true
   → Collect CONTAINER_IDs

2. POST /{ig-user-id}/media
   → media_type=CAROUSEL
   → children=CONTAINER_1,CONTAINER_2,...
   → caption="..."
   → Returns: {id: "CAROUSEL_CONTAINER_ID"}

3. POST /{ig-user-id}/media_publish
   → creation_id = CAROUSEL_CONTAINER_ID
```

#### Reels
```
1. POST /{ig-user-id}/media
   → media_type=REELS
   → video_url (public URL)
   → caption, audio_name (for music)
   → Returns: {id: "CONTAINER_ID"}

2. Upload video to rupload.facebook.com if using resumable upload

3. Poll status → POST media_publish
```

#### Stories
```
Same as single image but media_type=STORIES
```

### 3.3 Facebook Page Publishing Flow

#### Text + Link Post
```
POST /{page-id}/feed
→ message="text"
→ link="url" (optional)
→ published=true (or false + scheduled_publish_time for scheduling)
```

#### Photo Post
```
POST /{page-id}/photos
→ url="public_image_url"
→ caption="text"
```

#### Scheduled Post
```
POST /{page-id}/feed
→ message="text"
→ published=false
→ scheduled_publish_time=UNIX_TIMESTAMP
(must be 10 min to 30 days from now)
```

### 3.4 Insights / Analytics

#### Per-Post Instagram Insights
```
GET /{media-id}/insights?metric=engagement,impressions,reach,saved,shares
→ Returns lifetime values for each metric
```

#### Account-Level Instagram Insights
```
GET /{ig-user-id}/insights
→ metric=impressions,reach,profile_views,follower_count
→ period=day
→ since/until for date range (up to 90 days)
```

#### Facebook Page Insights
```
GET /{page-id}/insights
→ metric=page_impressions,page_engaged_users,page_post_engagements
→ period=day
```

### 3.5 Token Management

**Long-lived tokens last 60 days.** Must be refreshed before expiry.

```
1. Short-lived user token (from login) → lasts ~1 hour
2. Exchange for long-lived user token:
   GET /oauth/access_token
   → grant_type=fb_exchange_token
   → fb_exchange_token={short_lived_token}
   → Returns: 60-day token

3. Get Page Access Token (never expires if user token is long-lived):
   GET /{user-id}/accounts
   → Returns page tokens for each page

4. Store in crm_settings table:
   - meta_page_access_token
   - meta_ig_user_id
   - meta_fb_page_id
   - meta_token_expires_at
```

**Auto-refresh strategy**: Background job checks `meta_token_expires_at` daily. If < 7 days remaining, refresh token automatically. Alert user if manual re-auth needed.

---

## 4. Database Schema

### 4.1 New Supabase Tables

```sql
-- Social media posts (both scheduled and published)
CREATE TABLE social_posts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    consignacion_id UUID REFERENCES consignaciones(id),
    
    -- Content
    caption TEXT,
    hashtags TEXT,           -- stored separately for analytics
    post_type VARCHAR(20),   -- 'image', 'carousel', 'reel', 'story'
    
    -- Media (array of image/video URLs from Supabase Storage)
    media_urls JSONB DEFAULT '[]',    -- [{url, type: 'image'|'video', is_overlay: bool}]
    
    -- Platforms (which platforms to publish to)
    publish_instagram BOOLEAN DEFAULT true,
    publish_facebook BOOLEAN DEFAULT true,
    
    -- Scheduling
    status VARCHAR(20) DEFAULT 'draft', -- draft, scheduled, publishing, published, failed
    scheduled_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    
    -- Meta API IDs (after publishing)
    ig_media_id VARCHAR(100),
    ig_container_id VARCHAR(100),
    fb_post_id VARCHAR(100),
    
    -- Insights (updated periodically)
    ig_impressions INTEGER DEFAULT 0,
    ig_reach INTEGER DEFAULT 0,
    ig_engagement INTEGER DEFAULT 0,    -- likes + comments
    ig_saves INTEGER DEFAULT 0,
    ig_shares INTEGER DEFAULT 0,
    ig_likes INTEGER DEFAULT 0,
    ig_comments INTEGER DEFAULT 0,
    
    fb_impressions INTEGER DEFAULT 0,
    fb_reach INTEGER DEFAULT 0,
    fb_engagement INTEGER DEFAULT 0,
    fb_likes INTEGER DEFAULT 0,
    fb_comments INTEGER DEFAULT 0,
    fb_shares INTEGER DEFAULT 0,
    
    -- Error tracking
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Social media account settings (extends crm_settings)
-- Keys to add to crm_settings:
--   meta_page_access_token   → long-lived page token
--   meta_ig_user_id          → Instagram Business Account ID
--   meta_fb_page_id          → Facebook Page ID
--   meta_token_expires_at    → token expiry timestamp
--   meta_app_id              → Meta App ID
--   meta_app_secret          → Meta App Secret
--   social_default_hashtags  → default hashtags for all posts
--   social_auto_schedule     → true/false for AI-suggested scheduling
```

### 4.2 Migration File

Create: `supabase/migrations/setup_social_posts.sql`

---

## 5. Backend API Endpoints (app.py)

### 5.1 Token & Account Setup

| Method | Route | Description |
|---|---|---|
| `POST` | `/api/social/connect` | Save Meta tokens + account IDs to crm_settings |
| `GET` | `/api/social/status` | Check connection status, token validity, account info |
| `POST` | `/api/social/refresh-token` | Refresh long-lived token |
| `POST` | `/api/social/disconnect` | Remove stored tokens |

### 5.2 Post Management

| Method | Route | Description |
|---|---|---|
| `GET` | `/api/social/posts` | List all posts (filterable by status, date range) |
| `POST` | `/api/social/posts` | Create new post (draft or scheduled) |
| `GET` | `/api/social/posts/<id>` | Get single post with insights |
| `PUT` | `/api/social/posts/<id>` | Update draft/scheduled post |
| `DELETE` | `/api/social/posts/<id>` | Delete draft/scheduled post |

### 5.3 Publishing

| Method | Route | Description |
|---|---|---|
| `POST` | `/api/social/posts/<id>/publish` | Publish immediately to selected platforms |
| `POST` | `/api/social/posts/<id>/schedule` | Schedule for future date |
| `POST` | `/api/social/upload-media` | Upload image/video to Supabase, return public URL |
| `GET` | `/api/social/posts/<id>/status` | Check publishing status (polls Meta API) |

### 5.4 Analytics & Insights

| Method | Route | Description |
|---|---|---|
| `GET` | `/api/social/insights/account` | Account-level IG + FB insights |
| `GET` | `/api/social/insights/posts` | All posts with their metrics |
| `POST` | `/api/social/insights/refresh` | Force-refresh insights from Meta API |
| `GET` | `/api/social/insights/best-times` | Analyze best posting times from data |

### 5.5 Calendar

| Method | Route | Description |
|---|---|---|
| `GET` | `/api/social/calendar?month=3&year=2026` | Posts for calendar view |
| `PUT` | `/api/social/posts/<id>/reschedule` | Move post to new date/time (drag-drop) |

---

## 6. Execution Scripts

### 6.1 `execution/social_publish.py`
Deterministic script that handles the actual Meta API calls:
- `publish_to_instagram(post_data, token, ig_user_id)` → handles single/carousel/reel
- `publish_to_facebook(post_data, token, page_id)` → handles photo/text posts
- `check_container_status(container_id, token)` → polls until FINISHED
- `upload_media_to_supabase(file_data)` → stores in `social-media/` bucket, returns public URL

### 6.2 `execution/social_insights.py`
Fetches and stores insights:
- `fetch_ig_media_insights(media_id, token)` → returns dict of metrics
- `fetch_ig_account_insights(ig_user_id, token, since, until)` → account metrics
- `fetch_fb_post_insights(post_id, token)` → FB post metrics
- `calculate_best_times(posts_data)` → analyze engagement by hour/day

### 6.3 `execution/social_scheduler.py`
Background worker for scheduled posts:
- Runs every minute (via APScheduler or cron)
- Queries `social_posts WHERE status='scheduled' AND scheduled_at <= NOW()`
- Calls `social_publish.py` for each due post
- Updates status to `published` or `failed` with error message
- Retries up to 3 times with exponential backoff

### 6.4 `execution/social_token_manager.py`
Token lifecycle management:
- `check_token_validity(token)` → bool
- `refresh_long_lived_token(token, app_id, app_secret)` → new token
- `get_page_token(user_token, page_id)` → page access token
- Should run daily as background job

---

## 7. Frontend Views (CRM Alpine.js)

### 7.1 New CRM View: "Redes Sociales" (Social Media)

Add to sidebar navigation. Internal view name: `social`.

#### Sub-tabs within the view:

```
┌──────────────────────────────────────────────────────────────────┐
│  [📝 Composer]  [📅 Calendario]  [📊 Analytics]  [⚙️ Conexión]  │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  (Content of active sub-tab)                                     │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 7.2 Composer Tab

```
┌─────────────────────────────────┬──────────────────────────────┐
│  MEDIA SELECTOR (left)          │  POST DETAILS (right)        │
│                                 │                              │
│  ┌─ Vehicle Picker ──────────┐  │  Caption:                    │
│  │ Search: [__________]      │  │  ┌───────────────────────┐   │
│  │ ☐ Citroen C2 2010         │  │  │ Write your caption... │   │
│  │ ☐ Mercedes C200 2022      │  │  │ #autodirecto #autos   │   │
│  │ ☐ BMW X3 2021             │  │  │ #ventadeautos #chile  │   │
│  └───────────────────────────┘  │  └───────────────────────┘   │
│                                 │                              │
│  ┌─ Photos ──────────────────┐  │  Hashtags: [auto-suggest]    │
│  │ [img1] [img2] [overlay]   │  │                              │
│  │ [img3] [img4] [img5]      │  │  Platforms:                  │
│  │                           │  │  ☑ Instagram  ☑ Facebook     │
│  │  Click to select (max 10) │  │                              │
│  │  ★ = overlay image first  │  │  Post Type:                  │
│  └───────────────────────────┘  │  ○ Feed  ○ Carousel          │
│                                 │  ○ Reel  ○ Story             │
│  Selected: 4/10 photos          │                              │
│  [↕ Drag to reorder]           │  Schedule:                    │
│                                 │  ○ Post Now                  │
│                                 │  ○ Schedule: [date] [time]   │
│                                 │                              │
│                                 │  ┌────────────────────────┐  │
│                                 │  │  📱 PREVIEW (phone)    │  │
│                                 │  │  ┌──────────────┐      │  │
│                                 │  │  │              │      │  │
│                                 │  │  │  (IG post    │      │  │
│                                 │  │  │   preview)   │      │  │
│                                 │  │  │              │      │  │
│                                 │  │  └──────────────┘      │  │
│                                 │  └────────────────────────┘  │
│                                 │                              │
│                                 │  [💾 Save Draft] [🚀 Post]  │
└─────────────────────────────────┴──────────────────────────────┘
```

**Features:**
- Vehicle picker loads from consignaciones inventory
- Photos include inventory photos + overlay images generated in Instagram Overlay Pro
- Drag-to-reorder selected photos
- Caption with emoji picker, character count (2200 max for IG)
- Auto-suggested hashtags based on vehicle type (#suv, #sedan, etc.)
- Phone mockup preview showing how post will look on Instagram
- AI caption generator (optional): "Generate caption" button that uses OpenAI to write engaging post text

### 7.3 Calendar Tab

```
┌──────────────────────────────────────────────────────────────────┐
│  ◀  Marzo 2026  ▶        [Mes] [Semana] [Lista]                 │
├──────┬──────┬──────┬──────┬──────┬──────┬──────┤                 │
│ Lun  │ Mar  │ Mié  │ Jue  │ Vie  │ Sáb  │ Dom  │                │
├──────┼──────┼──────┼──────┼──────┼──────┼──────┤                 │
│      │      │      │      │      │  1   │  2   │                 │
│      │      │      │      │      │      │      │                 │
├──────┼──────┼──────┼──────┼──────┼──────┼──────┤                 │
│  3   │  4   │  5   │  6   │  7   │  8   │  9   │                 │
│      │ 🟢   │      │ 🟢   │      │ 🟡   │      │                 │
│      │10:00 │      │14:00 │      │Pend. │      │                 │
│      │BMW X3│      │C200  │      │      │      │                 │
├──────┼──────┼──────┼──────┼──────┼──────┼──────┤                 │
│ ...  │      │      │      │      │      │      │                 │
└──────┴──────┴──────┴──────┴──────┴──────┴──────┘                 │
                                                                   │
│  🟢 Published   🟡 Scheduled   🔴 Failed   ⚪ Draft              │
│                                                                  │
│  Drag posts to reschedule • Click to edit/view details           │
└──────────────────────────────────────────────────────────────────┘
```

**Features:**
- Month/week/list view toggle
- Color-coded by status (published, scheduled, failed, draft)
- Drag-and-drop to reschedule (updates `scheduled_at` via API)
- Click a post to open details/edit
- "Suggested slots" — AI recommends best times based on past engagement data
- Visual indicator of posting frequency (helps maintain consistency)

### 7.4 Analytics Dashboard Tab

```
┌──────────────────────────────────────────────────────────────────┐
│  📊 Analytics          Period: [Últimos 30 días ▼]               │
│                                                                  │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐            │
│  │ 12,450   │ │ 8,230    │ │ 423      │ │ 3.4%     │            │
│  │Impresion.│ │ Alcance  │ │Engagement│ │ Tasa Eng.│            │
│  │ ▲ +12%   │ │ ▲ +8%    │ │ ▲ +15%   │ │ ▲ +0.3% │            │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘            │
│                                                                  │
│  ┌─ Engagement Over Time ────────────────────────────────────┐   │
│  │  📈 (line chart: impressions, reach, engagement by day)   │   │
│  └───────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌─ Best Posting Times ──────┐ ┌─ Top Posts ─────────────────┐   │
│  │  Heatmap by hour/day      │ │  1. Mercedes C200 - 234 ❤️  │   │
│  │  🟢🟢🟡⚪⚪🟡🟢          │ │  2. BMW X3 - 198 ❤️         │   │
│  │  Best: Mar 10:00, Vie 14  │ │  3. Citroen C2 - 156 ❤️    │   │
│  └───────────────────────────┘ └─────────────────────────────┘   │
│                                                                  │
│  ┌─ Recent Posts Performance ────────────────────────────────┐   │
│  │  Post          │ Platform │ Reach │ Likes │ Comments │ 📊 │   │
│  │  Mercedes C200 │ IG + FB  │ 1,230 │  89   │    12    │ ▶  │   │
│  │  BMW X3 2021   │ IG       │   980 │  67   │     8    │ ▶  │   │
│  │  ...           │          │       │       │          │    │   │
│  └───────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

**Features:**
- Summary cards with period-over-period comparison (▲ +12%)
- Line chart for engagement trends over time (use Chart.js or lightweight canvas chart)
- Heatmap showing best posting times (hour × day of week)
- Top performing posts ranked by engagement
- Table of all posts with sortable metrics
- Export analytics to CSV

### 7.5 Connection/Settings Tab

```
┌──────────────────────────────────────────────────────────────────┐
│  ⚙️ Conexión Redes Sociales                                     │
│                                                                  │
│  ┌─ Instagram ───────────────────────────────────────────────┐   │
│  │  ✅ Connected: @autodirecto.cl                            │   │
│  │  Account ID: 178414058223...                              │   │
│  │  Token expires: 2026-05-14 (60 days)                      │   │
│  │  [🔄 Refresh Token]  [❌ Disconnect]                      │   │
│  └───────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌─ Facebook Page ───────────────────────────────────────────┐   │
│  │  ✅ Connected: Autodirecto Chile                          │   │
│  │  Page ID: 1234567890...                                   │   │
│  │  [❌ Disconnect]                                          │   │
│  └───────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌─ Default Settings ────────────────────────────────────────┐   │
│  │  Default Hashtags: #autodirecto #autosenchile #venta      │   │
│  │  Default caption template: [editable]                     │   │
│  │  Auto-schedule: ☐ Let AI suggest posting times            │   │
│  │  Insights refresh: Every [6] hours                        │   │
│  └───────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌─ Connect New Account ─────────────────────────────────────┐   │
│  │  To connect, you need:                                    │   │
│  │  1. A Meta Business App (developers.facebook.com)         │   │
│  │  2. Instagram Business/Creator account                    │   │
│  │  3. FB Page connected to your IG account                  │   │
│  │                                                           │   │
│  │  Meta App ID: [______________]                            │   │
│  │  App Secret:  [______________]                            │   │
│  │  Page Access Token: [__________________________]          │   │
│  │  IG User ID: [______________]                             │   │
│  │  FB Page ID: [______________]                             │   │
│  │                                                           │   │
│  │  [🔗 Connect & Verify]                                    │   │
│  └───────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

---

## 8. Implementation Phases

### Phase 1: Foundation (Priority: HIGH) ✅ DONE
**Estimated: 1–2 sessions**

1. ✅ **Database**: Create `social_posts` table in Supabase (`setup_social_posts.sql`)
2. ✅ **Backend**: Meta API integration
   - Publishing logic inline in `routes/social_routes.py` (not separate execution scripts)
   - Token validation via `/api/social/connect`, `/api/social/refresh-token`
   - API endpoints: `/api/social/connect`, `/api/social/status`, `/api/social/disconnect`, `/api/social/posts`
3. ✅ **Frontend**: Connection settings tab
   - Save Meta credentials to `crm_settings`
   - Test connection button
   - Status display

**Deliverable**: ✅ Can connect Meta account and verify it works.

### Phase 2: Composer & Publishing (Priority: HIGH) ✅ DONE
**Estimated: 2–3 sessions**

1. ✅ **Frontend**: Post Composer UI
   - Vehicle/photo selector (reuse existing consignaciones data)
   - Caption editor with character count
   - Platform selector (IG, FB, both)
   - Post type selector (feed, carousel, reel, story) ← NEW
   - Phone preview mockup
2. ✅ **Backend**: Publishing pipeline
   - Upload selected photos to Supabase `social-media/` bucket
   - Create post record in `social_posts` ← NEW
   - Call Meta API to publish
   - Track status (polling container status)
3. ⬜ **Integration with Instagram Overlay Pro**: "Publish" button in overlay editor that pre-fills composer with overlay image + vehicle data

**Deliverable**: ✅ Can compose and publish posts to IG + FB from CRM.

### Phase 3: Calendar & Scheduling (Priority: MEDIUM) ✅ DONE
**Estimated: 1–2 sessions**

1. ✅ **Frontend**: Calendar view (month grid)
   - Render posts by `scheduled_at` date
   - Color-code by status (published/scheduled/failed/draft)
   - Click to view details
   - ⬜ Drag-and-drop to reschedule (backend ready, frontend UX pending)
2. ✅ **Backend**: Scheduler worker
   - `POST /api/social/check-scheduled` — checks for due posts
   - Retry logic with retry_count tracking
   - ⬜ Auto-polling (APScheduler/cron not yet configured — manual trigger for now)
3. ✅ **Facebook scheduled posts**: Use native `scheduled_publish_time` parameter
4. ✅ **Instagram scheduled posts**: Store in DB, scheduler publishes at time

**Deliverable**: ✅ Can schedule posts for future dates. Auto-publish via `/check-scheduled`.

### Phase 4: Analytics & Insights (Priority: MEDIUM) ⚠️ PARTIAL
**Estimated: 1–2 sessions**

1. ✅ **Backend**: Insights fetching
   - `POST /api/social/insights/refresh` — force refresh from Meta API ← NEW
   - `GET /api/social/insights/best-times` — engagement analysis ← NEW
   - Store metrics in `social_posts` table
   - Account-level metrics via `/api/social/insights/account`
2. ⚠️ **Frontend**: Analytics dashboard (partial)
   - ✅ Summary cards (followers, media count, impressions, reach)
   - ✅ Recent post performance list
   - ⬜ Line chart (engagement over time) — needs Chart.js CDN
   - ⬜ Best posting times heatmap — backend ready, frontend pending
   - ⬜ Top posts ranking view

**Deliverable**: ⚠️ Basic analytics dashboard. Charts and heatmap still needed.

### Phase 5: AI Enhancements (Priority: LOW) ⚠️ PARTIAL
**Estimated: 1 session**

1. ✅ **AI Caption Generator**: Template-based generation from vehicle specs
2. ⬜ **AI Hashtag Suggestions**: Analyze top-performing hashtags from past posts
3. ⬜ **AI Scheduling**: Recommend best times based on historical engagement data
4. ⬜ **AI Post Ideas**: Suggest content themes (e.g., "Feature Friday", "New Arrival", etc.)

**Deliverable**: ⚠️ Basic caption generation. Full AI assistant pending.

---

## 9. Key Constraints & Gotchas

### Instagram API Constraints
- **Images must be JPEG** — convert PNG overlays to JPEG before publishing
- **Images must be on public HTTPS URL** — upload to Supabase Storage first, get public URL
- **Rate limit**: 100 published posts per 24h (carousels count as 1)
- **Carousel max**: 10 items
- **Caption max**: 2,200 characters
- **Hashtag max**: 30 per post
- **Container expires**: Must publish within 24h of creating container
- **No music via API**: Instagram doesn't support adding music through the API (Reels audio must be in the video file itself)
- **No filters via API**: Must apply any filters client-side before upload

### Facebook API Constraints
- **Scheduled posts**: Must be 10 min to 30 days in future
- **Page token needed**: User tokens won't work for page posts
- **Only app-created posts can be updated/deleted** via API

### Token Strategy
- Store `meta_page_access_token` in `crm_settings` (encrypted if possible)
- Page tokens derived from long-lived user tokens **don't expire** — but if user changes password or revokes app, they break
- Always validate token before publishing: `GET /me?access_token=TOKEN`
- Display clear warnings in CRM when token is expiring/invalid

### Image Pipeline
```
Overlay canvas → canvas.toBlob('image/jpeg', 0.95) 
    → POST /api/social/upload-media (FormData)
    → Supabase Storage: social-media/{post_id}/{filename}.jpg
    → Public URL for Meta API
```

---

## 10. Setup Checklist (For User)

Before implementation can begin, the user needs to:

- [ ] **Create Meta Business App** at [developers.facebook.com](https://developers.facebook.com)
  - App type: "Business"
  - Add products: "Instagram Graph API", "Facebook Login for Business"
- [ ] **Connect Instagram Business Account** to a Facebook Page
  - IG account must be Business or Creator (not Personal)
  - Must be connected to a FB Page in IG settings
- [ ] **Generate Page Access Token** with required permissions
  - Use Graph API Explorer or App Dashboard
  - Exchange for long-lived token
- [ ] **Get IDs**:
  - Instagram Business Account ID (from `/me/accounts` → `instagram_business_account`)
  - Facebook Page ID (from `/me/accounts`)
- [ ] **Complete Page Publishing Authorization** if required by Facebook
- [ ] **Share credentials** (App ID, App Secret, Page Token, IG User ID, FB Page ID)

---

## 11. File Structure (After Implementation)

```
SimplyAPI/
├── execution/
│   ├── social_publish.py          # Meta API publish logic
│   ├── social_insights.py         # Fetch & store insights
│   ├── social_scheduler.py        # Background scheduler worker  
│   └── social_token_manager.py    # Token validation & refresh
├── directives/
│   └── social_media_publishing.md # THIS FILE
├── app.py                         # + ~15 new /api/social/* routes
├── templates/
│   └── index.html                 # + Social Media view (~800 lines)
└── supabase/
    └── migrations/
        └── setup_social_posts.sql # New table
```

---

## 12. Dependencies

- **No new Python packages needed** — `requests` (already in requirements) handles all Meta API calls
- **Frontend**: Chart.js via CDN for analytics charts (single `<script>` tag)
- **Supabase Storage**: New bucket `social-media` for uploaded post images

---

## 13. Risk Mitigation

| Risk | Mitigation |
|---|---|
| Token expires, posts fail | Auto-refresh + CRM alert banner when < 7 days remaining |
| Meta API rate limit hit | Track usage, queue posts, respect 100/24h limit |
| Image upload fails | Retry 3x with backoff, show error in CRM with "retry" button |
| Scheduled post misses time | Scheduler runs every 60s, max delay = 60s. Retry on fail. |
| Meta App Review required | For own accounts, "Standard Access" works without review |
| User revokes Meta app permissions | Detect 401 errors, show "Reconnect" prompt in CRM |

---

*Last updated: 2026-03-15*
