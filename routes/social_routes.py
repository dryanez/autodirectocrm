"""
social_routes.py — Social Media Publishing Blueprint for Autodirecto CRM.

Handles:
  - Connection status / token verification
  - Post creation, scheduling, publishing (IG + FB)
  - Media upload (images to Supabase → public URL for Meta API)
  - Insights fetching (per-post + account-level)
  - Calendar data

All Meta Graph API calls go through this module.
Zero lines added to app.py beyond the 2-line registration.
"""

import hashlib
import hmac
import io
import json
import os
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import requests
from flask import Blueprint, jsonify, request

# ─── Blueprint ───────────────────────────────────────────────────────────────
social_bp = Blueprint('social', __name__, url_prefix='/api/social')

# ─── Config (per-tenant via company_settings, env fallback) ──────────────────
def _cfg(key, default=''):
    return os.environ.get(key, default)

GRAPH_API = 'https://graph.facebook.com/v25.0'

# Webhook verify token — must match what you enter in Meta dashboard
WEBHOOK_VERIFY_TOKEN = os.environ.get('META_WEBHOOK_VERIFY_TOKEN', 'autodirecto_social_2026')


def _tenant_settings():
    """Load per-company settings for the current request user."""
    try:
        from app import _get_current_user, _get_company_id, _get_company_settings
        user = _get_current_user()
        cid = _get_company_id(user)
        return _get_company_settings(cid)
    except Exception:
        return {}


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _meta_token():
    """Return the best available Meta token — per-tenant first, env fallback."""
    cs = _tenant_settings()
    return cs.get('meta_system_user_token') or cs.get('meta_fb_page_access_token') or _cfg('META_SYSTEM_USER_TOKEN') or _cfg('META_FB_PAGE_ACCESS_TOKEN')

def _ig_user_id():
    cs = _tenant_settings()
    return cs.get('meta_ig_user_id') or _cfg('META_IG_USER_ID')

def _fb_page_id():
    cs = _tenant_settings()
    return cs.get('meta_fb_page_id') or _cfg('META_FB_PAGE_ID')

def _graph_get(endpoint, params=None, token=None):
    """GET request to Meta Graph API. Token as access_token param."""
    tok = token or _meta_token()
    if not tok:
        return {'error': {'message': 'No Meta token configured'}}
    p = dict(params or {})
    p['access_token'] = tok
    try:
        url = f'{GRAPH_API}/{endpoint}'
        print(f'[social] GET {url}')
        r = requests.get(url, params=p, timeout=30)
        data = r.json()
        if 'error' in data:
            print(f'[social] ❌ GET error: {data["error"]}')
        return data
    except Exception as e:
        print(f'[social] ❌ GET exception: {e}')
        return {'error': {'message': str(e)}}

def _graph_post(endpoint, data=None, token=None):
    """POST request to Meta Graph API. Form-encoded with access_token (NOT JSON)."""
    tok = token or _meta_token()
    if not tok:
        return {'error': {'message': 'No Meta token configured'}}
    payload = dict(data or {})
    payload['access_token'] = tok
    try:
        url = f'{GRAPH_API}/{endpoint}'
        print(f'[social] POST {url} keys={list(payload.keys())}')
        r = requests.post(url, data=payload, timeout=60)
        result = r.json()
        if 'error' in result:
            print(f'[social] ❌ POST error: {result["error"]}')
        else:
            print(f'[social] ✅ POST ok: {list(result.keys())}')
        return result
    except Exception as e:
        print(f'[social] ❌ POST exception: {e}')
        return {'error': {'message': str(e)}}

def _get_db():
    """Import db module lazily to avoid circular imports."""
    try:
        from db import get_db
        return get_db()
    except Exception:
        return None


# ─── Supabase REST helpers (for social_posts table) ──────────────────────────

def _supa_url():
    return os.environ.get('SUPABASE_URL', '').strip()

def _supa_key():
    return (os.environ.get('SUPABASE_SERVICE_ROLE_KEY')
            or os.environ.get('SUPABASE_ANON_KEY', '')).strip()

def _supa_headers(prefer_return=True):
    h = {
        'apikey': _supa_key(),
        'Authorization': f'Bearer {_supa_key()}',
        'Content-Type': 'application/json',
    }
    if prefer_return:
        h['Prefer'] = 'return=representation'
    return h

def _supa_rest(table):
    return f'{_supa_url()}/rest/v1/{table}'

def _supa_select(table, params=None, order=None, limit=None):
    """GET rows from Supabase table with optional filters."""
    p = dict(params or {})
    p.setdefault('select', '*')
    if order:
        p['order'] = order
    if limit:
        p['limit'] = limit
    try:
        r = requests.get(_supa_rest(table), params=p,
                         headers=_supa_headers(False), timeout=15)
        return r.json() if r.status_code in (200, 206) else []
    except Exception as e:
        print(f'[social] supa GET {table} error: {e}')
        return []

def _supa_insert(table, record):
    """INSERT a row into Supabase table. Returns the created row or dict with error."""
    try:
        r = requests.post(_supa_rest(table), json=record,
                          headers=_supa_headers(), timeout=15)
        if r.status_code in (200, 201):
            data = r.json()
            return data[0] if isinstance(data, list) else data
        err = r.text[:300]
        print(f'[social] supa INSERT {table} {r.status_code}: {err}', flush=True)
        return {'_error': True, 'status': r.status_code, 'detail': err}
    except Exception as e:
        print(f'[social] supa INSERT {table} error: {e}', flush=True)
        return {'_error': True, 'detail': str(e)}

def _supa_update(table, record, filters):
    """UPDATE row(s) in Supabase table. filters = dict of eq filters."""
    try:
        params = {k: f'eq.{v}' for k, v in filters.items()}
        r = requests.patch(_supa_rest(table), json=record,
                           params=params, headers=_supa_headers(), timeout=15)
        if r.status_code in (200, 204):
            data = r.json() if r.text else []
            return data[0] if isinstance(data, list) and data else data
        print(f'[social] supa UPDATE {table} {r.status_code}: {r.text[:200]}')
        return None
    except Exception as e:
        print(f'[social] supa UPDATE {table} error: {e}')
        return None

def _supa_delete(table, filters):
    """DELETE row(s) from Supabase table."""
    try:
        params = {k: f'eq.{v}' for k, v in filters.items()}
        r = requests.delete(_supa_rest(table), params=params,
                            headers=_supa_headers(False), timeout=15)
        return r.status_code in (200, 204)
    except Exception as e:
        print(f'[social] supa DELETE {table} error: {e}')
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# 1. CONNECTION & STATUS
# ═══════════════════════════════════════════════════════════════════════════════

@social_bp.route('/status', methods=['GET'])
def social_status():
    """Check connection status: token validity, account info."""
    tok = _meta_token()
    if not tok:
        return jsonify({'connected': False, 'error': 'No token configured'}), 200

    # Validate token
    result = _graph_get('me', {'fields': 'id,name'})
    if 'error' in result:
        return jsonify({'connected': False, 'error': result['error'].get('message', 'Invalid token')}), 200

    # Get FB Page info
    fb_page_id = _fb_page_id()
    fb_info = None
    if fb_page_id:
        fb_info = _graph_get(fb_page_id, {'fields': 'id,name,category,followers_count'})
        if 'error' in fb_info:
            fb_info = None

    # Get IG account info
    ig_user_id = _ig_user_id()
    ig_info = None
    if ig_user_id:
        ig_info = _graph_get(ig_user_id, {'fields': 'id,name,username,profile_picture_url,followers_count,media_count'})
        if 'error' in ig_info:
            ig_info = None

    return jsonify({
        'connected': True,
        'token_user': result,
        'facebook': fb_info,
        'instagram': ig_info,
    })


@social_bp.route('/connect', methods=['POST'])
def social_connect():
    """Save Meta tokens + account IDs to crm_settings (via Supabase)."""
    data = request.json or {}
    required = ['page_access_token', 'ig_user_id', 'fb_page_id']
    for key in required:
        if not data.get(key):
            return jsonify({'ok': False, 'error': f'Missing {key}'}), 400

    settings_map = {
        'meta_page_access_token': data['page_access_token'],
        'meta_ig_user_id': data['ig_user_id'],
        'meta_fb_page_id': data['fb_page_id'],
        'meta_app_id': data.get('app_id', ''),
        'meta_app_secret': data.get('app_secret', ''),
    }

    # Validate token before saving
    test = _graph_get('me', {'fields': 'id,name'}, token=data['page_access_token'])
    if 'error' in test:
        return jsonify({'ok': False, 'error': 'Token validation failed: ' + test['error'].get('message', '')}), 400

    # Save each setting to crm_settings
    db = _get_db()
    if db:
        for k, v in settings_map.items():
            try:
                db.execute("DELETE FROM crm_settings WHERE key=?", (k,))
                db.execute("INSERT INTO crm_settings (key, value) VALUES (?, ?)", (k, v))
            except Exception:
                pass
        db.commit()

    return jsonify({'ok': True, 'message': 'Connected successfully'})


@social_bp.route('/disconnect', methods=['POST'])
def social_disconnect():
    """Remove stored Meta tokens from crm_settings."""
    keys = ['meta_page_access_token', 'meta_ig_user_id', 'meta_fb_page_id',
            'meta_app_id', 'meta_app_secret', 'meta_token_expires_at']
    db = _get_db()
    if db:
        for k in keys:
            try:
                db.execute("DELETE FROM crm_settings WHERE key=?", (k,))
            except Exception:
                pass
        db.commit()
    return jsonify({'ok': True, 'message': 'Disconnected'})


@social_bp.route('/refresh-token', methods=['POST'])
def social_refresh_token():
    """Exchange current token for a new long-lived token."""
    app_id = _cfg('META_APP_ID')
    app_secret = _cfg('META_APP_SECRET')
    current_token = _meta_token()

    if not all([app_id, app_secret, current_token]):
        return jsonify({'ok': False, 'error': 'Missing app_id, app_secret, or current token'}), 400

    try:
        r = requests.get(f'{GRAPH_API}/oauth/access_token', params={
            'grant_type': 'fb_exchange_token',
            'client_id': app_id,
            'client_secret': app_secret,
            'fb_exchange_token': current_token,
        }, timeout=15)
        data = r.json()
        if 'access_token' in data:
            new_token = data['access_token']
            expires_in = data.get('expires_in', 5184000)  # ~60 days
            expires_at = (datetime.utcnow() + timedelta(seconds=expires_in)).isoformat()

            # Save new token
            db = _get_db()
            if db:
                try:
                    db.execute("DELETE FROM crm_settings WHERE key='meta_page_access_token'")
                    db.execute("INSERT INTO crm_settings (key, value) VALUES ('meta_page_access_token', ?)", (new_token,))
                    db.execute("DELETE FROM crm_settings WHERE key='meta_token_expires_at'")
                    db.execute("INSERT INTO crm_settings (key, value) VALUES ('meta_token_expires_at', ?)", (expires_at,))
                    db.commit()
                except Exception:
                    pass

            return jsonify({
                'ok': True,
                'expires_in': expires_in,
                'expires_at': expires_at,
                'token_preview': new_token[:20] + '...',
            })
        else:
            return jsonify({'ok': False, 'error': data.get('error', {}).get('message', 'Token refresh failed')}), 400
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# 2. PUBLISH TO INSTAGRAM
# ═══════════════════════════════════════════════════════════════════════════════

@social_bp.route('/publish/instagram', methods=['POST'])
def publish_instagram():
    """
    Publish to Instagram.
    Body JSON:
      - image_urls: [str] — public HTTPS URLs (max 10 for carousel)
      - caption: str
      - post_type: 'image' | 'carousel' | 'reel' | 'story'
      - video_url: str (for reel/story video)
    """
    data = request.json or {}
    ig_id = _ig_user_id()
    if not ig_id:
        return jsonify({'ok': False, 'error': 'Instagram account not configured'}), 400

    post_type = data.get('post_type', 'image')
    caption = data.get('caption', '')
    image_urls = data.get('image_urls', [])
    video_url = data.get('video_url', '')

    try:
        if post_type == 'image' and len(image_urls) == 1:
            # ── Single image post ──
            container = _graph_post(f'{ig_id}/media', {
                'image_url': image_urls[0],
                'caption': caption,
            })
            if 'error' in container:
                return jsonify({'ok': False, 'error': container['error'].get('message', 'Container creation failed')}), 400

            container_id = container.get('id')
            if not container_id:
                return jsonify({'ok': False, 'error': 'No container ID returned'}), 400

            # Poll status
            _wait_for_container(container_id)

            # Publish
            result = _graph_post(f'{ig_id}/media_publish', {'creation_id': container_id})
            if 'error' in result:
                return jsonify({'ok': False, 'error': result['error'].get('message', 'Publish failed')}), 400

            return jsonify({'ok': True, 'ig_media_id': result.get('id'), 'type': 'image'})

        elif post_type == 'carousel' or (post_type == 'image' and len(image_urls) > 1):
            # ── Carousel (2–10 images) ──
            child_ids = []
            for url in image_urls[:10]:
                child = _graph_post(f'{ig_id}/media', {
                    'image_url': url,
                    'is_carousel_item': 'true',
                })
                if 'error' in child:
                    return jsonify({'ok': False, 'error': f'Failed creating carousel item: {child["error"].get("message")}'}), 400
                child_ids.append(child['id'])

            # Create carousel container
            carousel = _graph_post(f'{ig_id}/media', {
                'media_type': 'CAROUSEL',
                'children': ','.join(child_ids),
                'caption': caption,
            })
            if 'error' in carousel:
                return jsonify({'ok': False, 'error': carousel['error'].get('message', 'Carousel container failed')}), 400

            _wait_for_container(carousel['id'])

            result = _graph_post(f'{ig_id}/media_publish', {'creation_id': carousel['id']})
            if 'error' in result:
                return jsonify({'ok': False, 'error': result['error'].get('message', 'Publish failed')}), 400

            return jsonify({'ok': True, 'ig_media_id': result.get('id'), 'type': 'carousel'})

        elif post_type == 'reel':
            # ── Reel ──
            container = _graph_post(f'{ig_id}/media', {
                'media_type': 'REELS',
                'video_url': video_url or image_urls[0] if image_urls else '',
                'caption': caption,
            })
            if 'error' in container:
                return jsonify({'ok': False, 'error': container['error'].get('message', 'Reel container failed')}), 400

            _wait_for_container(container['id'], max_wait=120)

            result = _graph_post(f'{ig_id}/media_publish', {'creation_id': container['id']})
            if 'error' in result:
                return jsonify({'ok': False, 'error': result['error'].get('message', 'Reel publish failed')}), 400

            return jsonify({'ok': True, 'ig_media_id': result.get('id'), 'type': 'reel'})

        elif post_type == 'story':
            # ── Story ──
            payload = {'media_type': 'STORIES'}
            if video_url:
                payload['video_url'] = video_url
            elif image_urls:
                payload['image_url'] = image_urls[0]
            else:
                return jsonify({'ok': False, 'error': 'No media URL for story'}), 400

            container = _graph_post(f'{ig_id}/media', payload)
            if 'error' in container:
                return jsonify({'ok': False, 'error': container['error'].get('message', 'Story container failed')}), 400

            _wait_for_container(container['id'])

            result = _graph_post(f'{ig_id}/media_publish', {'creation_id': container['id']})
            if 'error' in result:
                return jsonify({'ok': False, 'error': result['error'].get('message', 'Story publish failed')}), 400

            return jsonify({'ok': True, 'ig_media_id': result.get('id'), 'type': 'story'})

        else:
            return jsonify({'ok': False, 'error': f'Unknown post_type: {post_type}'}), 400

    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


def _wait_for_container(container_id, max_wait=60):
    """Poll container status until FINISHED or timeout."""
    start = time.time()
    while time.time() - start < max_wait:
        status = _graph_get(container_id, {'fields': 'status_code'})
        code = status.get('status_code', '')
        if code == 'FINISHED':
            return True
        if code == 'ERROR':
            raise Exception(f'Container {container_id} failed: {status}')
        if code == 'EXPIRED':
            raise Exception(f'Container {container_id} expired')
        time.sleep(3)
    return False  # Timeout — try publishing anyway


# ═══════════════════════════════════════════════════════════════════════════════
# 3. PUBLISH TO FACEBOOK PAGE
# ═══════════════════════════════════════════════════════════════════════════════

@social_bp.route('/publish/facebook', methods=['POST'])
def publish_facebook():
    """
    Publish to Facebook Page.
    Body JSON:
      - message: str (caption)
      - image_urls: [str] — public HTTPS URLs (optional)
      - link: str (optional)
      - scheduled_time: int (unix timestamp, optional — 10min to 30 days)
    """
    data = request.json or {}
    page_id = _fb_page_id()
    if not page_id:
        return jsonify({'ok': False, 'error': 'Facebook Page not configured'}), 400

    message = data.get('message', data.get('caption', ''))
    image_urls = data.get('image_urls', [])
    link = data.get('link', '')
    scheduled_time = data.get('scheduled_time')

    try:
        if image_urls:
            # ── Photo post ──
            # FB photos API: 'url' for image, 'message' for text (NOT 'caption')
            payload = {
                'url': image_urls[0],
                'message': message,
            }
            if scheduled_time:
                payload['published'] = 'false'
                payload['scheduled_publish_time'] = str(scheduled_time)

            result = _graph_post(f'{page_id}/photos', payload)
            if 'error' in result:
                return jsonify({'ok': False, 'error': result['error'].get('message', 'FB photo publish failed')}), 400

            return jsonify({
                'ok': True,
                'fb_post_id': result.get('post_id', result.get('id')),
                'fb_photo_id': result.get('id'),
                'type': 'photo'
            })
        else:
            # ── Text/link post ──
            payload = {'message': message}
            if link:
                payload['link'] = link
            if scheduled_time:
                payload['published'] = 'false'
                payload['scheduled_publish_time'] = str(scheduled_time)

            result = _graph_post(f'{page_id}/feed', payload)
            if 'error' in result:
                return jsonify({'ok': False, 'error': result['error'].get('message', 'FB post failed')}), 400

            return jsonify({'ok': True, 'fb_post_id': result.get('id'), 'type': 'text'})

    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# 4. UPLOAD MEDIA (image → Supabase Storage → public URL)
# ═══════════════════════════════════════════════════════════════════════════════

@social_bp.route('/upload-media', methods=['POST'])
def upload_media():
    """
    Upload an image (from form-data or base64) to Supabase Storage.
    Returns the public URL that Meta API can fetch.

    Accepts:
      - file: multipart file upload
      - OR base64: JSON body with {base64: "data:image/jpeg;base64,...", filename: "..."}
    """
    sb_url = os.environ.get('SUPABASE_URL', '')
    sb_key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')
    if not sb_url or not sb_key:
        return jsonify({'ok': False, 'error': 'Supabase not configured'}), 500

    bucket = 'vehicle-photos'  # Reuse existing bucket
    folder = 'social-media'

    try:
        if request.content_type and 'multipart' in request.content_type:
            # File upload
            f = request.files.get('file')
            if not f:
                return jsonify({'ok': False, 'error': 'No file uploaded'}), 400
            file_data = f.read()
            filename = f.filename or f'social_{uuid.uuid4().hex[:8]}.jpg'
            content_type = f.content_type or 'image/jpeg'
        else:
            # Base64 upload
            data = request.json or {}
            b64 = data.get('base64', '')
            if not b64:
                return jsonify({'ok': False, 'error': 'No base64 data'}), 400

            import base64 as b64_mod
            # Strip data URI prefix
            if ',' in b64:
                header, b64_data = b64.split(',', 1)
            else:
                b64_data = b64
                header = ''
            file_data = b64_mod.b64decode(b64_data)
            filename = data.get('filename', f'social_{uuid.uuid4().hex[:8]}.jpg')
            content_type = 'image/jpeg'
            if 'png' in header:
                content_type = 'image/png'

        # Upload to Supabase Storage
        path = f'{folder}/{uuid.uuid4().hex[:8]}_{filename}'
        upload_url = f'{sb_url}/storage/v1/object/{bucket}/{path}'
        headers = {
            'Authorization': f'Bearer {sb_key}',
            'apikey': sb_key,
            'Content-Type': content_type,
            'x-upsert': 'true',
        }
        r = requests.post(upload_url, data=file_data, headers=headers, timeout=30)

        if r.status_code not in (200, 201):
            return jsonify({'ok': False, 'error': f'Upload failed: {r.status_code} {r.text}'}), 500

        # Construct public URL
        public_url = f'{sb_url}/storage/v1/object/public/{bucket}/{path}'
        return jsonify({'ok': True, 'url': public_url, 'path': path})

    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# 5. POST MANAGEMENT (CRUD on social_posts table)
# ═══════════════════════════════════════════════════════════════════════════════

@social_bp.route('/posts', methods=['GET'])
def list_posts():
    """List social posts. Filters: ?status=, ?month=, &year="""
    params = {'select': '*'}
    status = request.args.get('status')
    if status:
        params['status'] = f'eq.{status}'

    month = request.args.get('month')
    year = request.args.get('year')
    if month and year:
        start = f'{year}-{int(month):02d}-01T00:00:00'
        if int(month) == 12:
            end = f'{int(year)+1}-01-01T00:00:00'
        else:
            end = f'{year}-{int(month)+1:02d}-01T00:00:00'
        params['or'] = (
            f'(scheduled_at.gte.{start},scheduled_at.lt.{end}),'
            f'(published_at.gte.{start},published_at.lt.{end}),'
            f'(and(scheduled_at.is.null,created_at.gte.{start},created_at.lt.{end}))'
        )

    rows = _supa_select('social_posts', params, order='created_at.desc', limit=200)
    return jsonify({'ok': True, 'posts': rows})


@social_bp.route('/posts', methods=['POST'])
def create_post():
    """Create a new social post (draft or scheduled)."""
    data = request.json or {}
    record = {
        'caption': data.get('caption', ''),
        'hashtags': data.get('hashtags', ''),
        'post_type': data.get('post_type', 'image'),
        'media_urls': data.get('media_urls', []),
        'publish_instagram': data.get('publish_instagram', True),
        'publish_facebook': data.get('publish_facebook', True),
        'status': data.get('status', 'draft'),
    }
    if data.get('consignacion_id'):
        record['consignacion_id'] = data['consignacion_id']
    if data.get('scheduled_at'):
        record['scheduled_at'] = data['scheduled_at']
        record['status'] = 'scheduled'

    row = _supa_insert('social_posts', record)
    if isinstance(row, dict) and row.get('_error'):
        return jsonify({'ok': False, 'error': f"Failed to create post: {row.get('detail', 'Unknown')}"}), 500
    if row:
        return jsonify({'ok': True, 'post': row})
    return jsonify({'ok': False, 'error': 'Failed to create post'}), 500


@social_bp.route('/posts/<post_id>', methods=['GET'])
def get_post(post_id):
    """Get a single social post by ID."""
    rows = _supa_select('social_posts', {'id': f'eq.{post_id}'})
    if not rows:
        return jsonify({'ok': False, 'error': 'Post not found'}), 404
    return jsonify({'ok': True, 'post': rows[0]})


@social_bp.route('/posts/<post_id>', methods=['PUT'])
def update_post(post_id):
    """Update a draft/scheduled post."""
    data = request.json or {}
    allowed = ['caption', 'hashtags', 'post_type', 'media_urls',
               'publish_instagram', 'publish_facebook', 'scheduled_at', 'status']
    updates = {}
    for k in allowed:
        if k in data:
            if k == 'media_urls':
                updates[k] = json.dumps(data[k]) if isinstance(data[k], list) else data[k]
            else:
                updates[k] = data[k]

    if not updates:
        return jsonify({'ok': False, 'error': 'Nothing to update'}), 400

    result = _supa_update('social_posts', updates, {'id': post_id})
    if result:
        return jsonify({'ok': True, 'post': result})
    return jsonify({'ok': False, 'error': 'Update failed'}), 500


@social_bp.route('/posts/<post_id>', methods=['DELETE'])
def delete_post(post_id):
    """Delete a draft/scheduled post."""
    # Only allow deleting draft/scheduled posts
    rows = _supa_select('social_posts', {'id': f'eq.{post_id}'})
    if rows and rows[0].get('status') in ('published', 'publishing'):
        return jsonify({'ok': False, 'error': 'Cannot delete a published post'}), 400

    ok = _supa_delete('social_posts', {'id': post_id})
    return jsonify({'ok': ok})


@social_bp.route('/posts/<post_id>/publish', methods=['POST'])
def publish_post(post_id):
    """
    Publish a saved post immediately.
    Reads post from social_posts, calls IG/FB APIs, updates status.
    """
    rows = _supa_select('social_posts', {'id': f'eq.{post_id}'})
    if not rows:
        return jsonify({'ok': False, 'error': 'Post not found'}), 404

    post = rows[0]
    if post.get('status') == 'published':
        return jsonify({'ok': False, 'error': 'Already published'}), 400

    # Mark as publishing
    _supa_update('social_posts', {'status': 'publishing'}, {'id': post_id})

    media_urls = post.get('media_urls', [])
    if isinstance(media_urls, str):
        media_urls = json.loads(media_urls)
    caption = post.get('caption', '')
    post_type = post.get('post_type', 'image')
    results = {'ig': None, 'fb': None}
    errors = []

    try:
        # Instagram
        if post.get('publish_instagram') and media_urls:
            ig_id = _ig_user_id()
            if ig_id:
                image_urls = [m['url'] if isinstance(m, dict) else m for m in media_urls]
                if post_type == 'carousel' or len(image_urls) > 1:
                    # Carousel
                    child_ids = []
                    for url in image_urls[:10]:
                        child = _graph_post(f'{ig_id}/media', {
                            'image_url': url,
                            'is_carousel_item': 'true',
                        })
                        if 'error' in child:
                            errors.append(f'IG carousel item: {child["error"].get("message")}')
                            break
                        child_ids.append(child['id'])

                    if child_ids and not errors:
                        carousel = _graph_post(f'{ig_id}/media', {
                            'media_type': 'CAROUSEL',
                            'children': ','.join(child_ids),
                            'caption': caption,
                        })
                        if 'error' not in carousel:
                            _wait_for_container(carousel['id'])
                            r = _graph_post(f'{ig_id}/media_publish', {'creation_id': carousel['id']})
                            results['ig'] = r.get('id')
                        else:
                            errors.append(f'IG carousel: {carousel["error"].get("message")}')
                else:
                    # Single image
                    container = _graph_post(f'{ig_id}/media', {
                        'image_url': image_urls[0],
                        'caption': caption,
                    })
                    if 'error' not in container:
                        _wait_for_container(container['id'])
                        r = _graph_post(f'{ig_id}/media_publish', {'creation_id': container['id']})
                        results['ig'] = r.get('id')
                    else:
                        errors.append(f'IG: {container["error"].get("message")}')

        # Facebook
        if post.get('publish_facebook'):
            page_id = _fb_page_id()
            if page_id:
                image_urls = [m['url'] if isinstance(m, dict) else m for m in media_urls]
                if image_urls:
                    r = _graph_post(f'{page_id}/photos', {
                        'url': image_urls[0],
                        'message': caption,
                    })
                else:
                    r = _graph_post(f'{page_id}/feed', {'message': caption})
                if 'error' not in r:
                    results['fb'] = r.get('post_id', r.get('id'))
                else:
                    errors.append(f'FB: {r["error"].get("message")}')

        # Update post record
        update = {'updated_at': datetime.utcnow().isoformat()}
        if results['ig'] or results['fb']:
            update['status'] = 'published'
            update['published_at'] = datetime.utcnow().isoformat()
            if results['ig']:
                update['ig_media_id'] = results['ig']
            if results['fb']:
                update['fb_post_id'] = results['fb']
        else:
            update['status'] = 'failed'
            update['error_message'] = '; '.join(errors)
            update['retry_count'] = (post.get('retry_count') or 0) + 1

        _supa_update('social_posts', update, {'id': post_id})

        return jsonify({
            'ok': bool(results['ig'] or results['fb']),
            'ig_media_id': results['ig'],
            'fb_post_id': results['fb'],
            'errors': errors,
        })

    except Exception as e:
        _supa_update('social_posts', {
            'status': 'failed',
            'error_message': str(e),
            'retry_count': (post.get('retry_count') or 0) + 1,
        }, {'id': post_id})
        return jsonify({'ok': False, 'error': str(e)}), 500


@social_bp.route('/posts/<post_id>/schedule', methods=['POST'])
def schedule_post(post_id):
    """Schedule a post for future publishing."""
    data = request.json or {}
    scheduled_at = data.get('scheduled_at')
    if not scheduled_at:
        return jsonify({'ok': False, 'error': 'scheduled_at is required'}), 400

    result = _supa_update('social_posts', {
        'status': 'scheduled',
        'scheduled_at': scheduled_at,
    }, {'id': post_id})
    if result:
        return jsonify({'ok': True, 'post': result})
    return jsonify({'ok': False, 'error': 'Schedule failed'}), 500


@social_bp.route('/posts/<post_id>/reschedule', methods=['PUT'])
def reschedule_post(post_id):
    """Reschedule a post (drag-and-drop from calendar)."""
    data = request.json or {}
    scheduled_at = data.get('scheduled_at')
    if not scheduled_at:
        return jsonify({'ok': False, 'error': 'scheduled_at is required'}), 400

    result = _supa_update('social_posts', {
        'scheduled_at': scheduled_at,
    }, {'id': post_id})
    if result:
        return jsonify({'ok': True, 'post': result})
    return jsonify({'ok': False, 'error': 'Reschedule failed'}), 500


@social_bp.route('/calendar', methods=['GET'])
def calendar_posts():
    """Get posts for calendar view. ?month=3&year=2026"""
    month = request.args.get('month', datetime.utcnow().month)
    year = request.args.get('year', datetime.utcnow().year)
    month, year = int(month), int(year)

    start = f'{year}-{month:02d}-01T00:00:00'
    if month == 12:
        end = f'{year+1}-01-01T00:00:00'
    else:
        end = f'{year}-{month+1:02d}-01T00:00:00'

    # Get scheduled + published posts in range
    params = {
        'select': 'id,caption,post_type,status,scheduled_at,published_at,media_urls,'
                  'publish_instagram,publish_facebook,ig_likes,ig_comments,consignacion_id',
        'or': (
            f'(scheduled_at.gte.{start},scheduled_at.lt.{end}),'
            f'(published_at.gte.{start},published_at.lt.{end})'
        ),
    }
    rows = _supa_select('social_posts', params, order='scheduled_at.asc.nullsfirst')
    return jsonify({'ok': True, 'posts': rows, 'month': month, 'year': year})


@social_bp.route('/check-scheduled', methods=['POST'])
def check_scheduled():
    """
    Called periodically (by cron or frontend) to publish any due scheduled posts.
    Finds posts WHERE status='scheduled' AND scheduled_at <= NOW() and publishes them.
    """
    now = datetime.utcnow().isoformat()
    params = {
        'status': 'eq.scheduled',
        'scheduled_at': f'lte.{now}',
    }
    due_posts = _supa_select('social_posts', params, limit=10)
    published = []
    failed = []

    for post in due_posts:
        post_id = post['id']
        try:
            # Reuse the publish endpoint logic
            _supa_update('social_posts', {'status': 'publishing'}, {'id': post_id})

            media_urls = post.get('media_urls', [])
            if isinstance(media_urls, str):
                media_urls = json.loads(media_urls)
            caption = post.get('caption', '')
            image_urls = [m['url'] if isinstance(m, dict) else m for m in media_urls]
            ig_result = fb_result = None

            if post.get('publish_instagram') and image_urls:
                ig_id = _ig_user_id()
                if ig_id:
                    if len(image_urls) > 1:
                        child_ids = []
                        for url in image_urls[:10]:
                            child = _graph_post(f'{ig_id}/media', {
                                'image_url': url, 'is_carousel_item': 'true',
                            })
                            if 'error' not in child:
                                child_ids.append(child['id'])
                        if child_ids:
                            car = _graph_post(f'{ig_id}/media', {
                                'media_type': 'CAROUSEL',
                                'children': ','.join(child_ids),
                                'caption': caption,
                            })
                            if 'error' not in car:
                                _wait_for_container(car['id'])
                                r = _graph_post(f'{ig_id}/media_publish', {'creation_id': car['id']})
                                ig_result = r.get('id')
                    else:
                        c = _graph_post(f'{ig_id}/media', {
                            'image_url': image_urls[0], 'caption': caption,
                        })
                        if 'error' not in c:
                            _wait_for_container(c['id'])
                            r = _graph_post(f'{ig_id}/media_publish', {'creation_id': c['id']})
                            ig_result = r.get('id')

            if post.get('publish_facebook'):
                page_id = _fb_page_id()
                if page_id:
                    if image_urls:
                        r = _graph_post(f'{page_id}/photos', {
                            'url': image_urls[0], 'message': caption,
                        })
                    else:
                        r = _graph_post(f'{page_id}/feed', {'message': caption})
                    if 'error' not in r:
                        fb_result = r.get('post_id', r.get('id'))

            update = {'published_at': datetime.utcnow().isoformat()}
            if ig_result or fb_result:
                update['status'] = 'published'
                if ig_result:
                    update['ig_media_id'] = ig_result
                if fb_result:
                    update['fb_post_id'] = fb_result
                published.append(post_id)
            else:
                update['status'] = 'failed'
                update['retry_count'] = (post.get('retry_count') or 0) + 1
                failed.append(post_id)

            _supa_update('social_posts', update, {'id': post_id})

        except Exception as e:
            _supa_update('social_posts', {
                'status': 'failed',
                'error_message': str(e),
                'retry_count': (post.get('retry_count') or 0) + 1,
            }, {'id': post_id})
            failed.append(post_id)

    return jsonify({
        'ok': True,
        'checked': len(due_posts),
        'published': published,
        'failed': failed,
    })


@social_bp.route('/insights/refresh', methods=['POST'])
def insights_refresh():
    """Force-refresh insights for all published posts from Meta API."""
    posts = _supa_select('social_posts', {
        'status': 'eq.published',
        'select': 'id,ig_media_id,fb_post_id',
    }, limit=100)

    updated = 0
    for post in posts:
        updates = {}
        # IG insights
        ig_id = post.get('ig_media_id')
        if ig_id:
            data = _graph_get(f'{ig_id}/insights', {
                'metric': 'impressions,reach,saved,shares'
            })
            if 'data' in data:
                for m in data['data']:
                    name = m.get('name', '')
                    val = m.get('values', [{}])[0].get('value', 0)
                    if name == 'impressions':
                        updates['ig_impressions'] = val
                    elif name == 'reach':
                        updates['ig_reach'] = val
                    elif name == 'saved':
                        updates['ig_saves'] = val
                    elif name == 'shares':
                        updates['ig_shares'] = val

            # Also get like/comment counts from media object
            media_data = _graph_get(ig_id, {'fields': 'like_count,comments_count'})
            if 'like_count' in media_data:
                updates['ig_likes'] = media_data['like_count']
                updates['ig_comments'] = media_data.get('comments_count', 0)
                updates['ig_engagement'] = media_data['like_count'] + media_data.get('comments_count', 0)

        if updates:
            _supa_update('social_posts', updates, {'id': post['id']})
            updated += 1

    return jsonify({'ok': True, 'updated': updated, 'total': len(posts)})


@social_bp.route('/insights/best-times', methods=['GET'])
def insights_best_times():
    """Analyze best posting times from published posts."""
    posts = _supa_select('social_posts', {
        'status': 'eq.published',
        'select': 'published_at,ig_engagement,ig_impressions,ig_reach',
    }, limit=500)

    # Build hour-of-day × day-of-week engagement matrix
    heatmap = {}  # {day_of_week: {hour: {engagement, count}}}
    for p in posts:
        ts = p.get('published_at')
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
        except Exception:
            continue
        dow = dt.strftime('%A')  # Monday, Tuesday, etc.
        hour = dt.hour
        key = f'{dow}_{hour}'
        if key not in heatmap:
            heatmap[key] = {'day': dow, 'hour': hour, 'total_engagement': 0, 'count': 0}
        heatmap[key]['total_engagement'] += (p.get('ig_engagement') or 0)
        heatmap[key]['count'] += 1

    # Calculate averages and find best times
    for k in heatmap:
        c = heatmap[k]['count']
        heatmap[k]['avg_engagement'] = round(heatmap[k]['total_engagement'] / c, 1) if c > 0 else 0

    best = sorted(heatmap.values(), key=lambda x: x['avg_engagement'], reverse=True)[:10]

    return jsonify({'ok': True, 'best_times': best, 'heatmap': list(heatmap.values())})


# ═══════════════════════════════════════════════════════════════════════════════
# 6. INSIGHTS
# ═══════════════════════════════════════════════════════════════════════════════

@social_bp.route('/insights/account', methods=['GET'])
def insights_account():
    """Get account-level Instagram insights."""
    ig_id = _ig_user_id()
    if not ig_id:
        return jsonify({'ok': False, 'error': 'IG not configured'}), 400

    period = request.args.get('period', 'day')
    metrics = request.args.get('metrics', 'impressions,reach,profile_views')

    result = _graph_get(f'{ig_id}/insights', {
        'metric': metrics,
        'period': period,
    })
    if 'error' in result:
        return jsonify({'ok': False, 'error': result['error'].get('message', 'Insights fetch failed')}), 400

    return jsonify({'ok': True, 'data': result.get('data', [])})


@social_bp.route('/insights/media/<media_id>', methods=['GET'])
def insights_media(media_id):
    """Get insights for a specific IG media post."""
    metrics = request.args.get('metrics', 'impressions,reach,engagement,saved,shares')
    result = _graph_get(f'{media_id}/insights', {'metric': metrics})
    if 'error' in result:
        return jsonify({'ok': False, 'error': result['error'].get('message', 'Media insights failed')}), 400

    return jsonify({'ok': True, 'data': result.get('data', [])})


@social_bp.route('/insights/recent', methods=['GET'])
def insights_recent():
    """Get recent IG posts with basic metrics."""
    ig_id = _ig_user_id()
    if not ig_id:
        return jsonify({'ok': False, 'error': 'IG not configured'}), 400

    limit = request.args.get('limit', 25)
    result = _graph_get(f'{ig_id}/media', {
        'fields': 'id,caption,media_type,media_url,thumbnail_url,timestamp,like_count,comments_count,permalink',
        'limit': limit,
    })
    if 'error' in result:
        return jsonify({'ok': False, 'error': result['error'].get('message', 'Recent posts failed')}), 400

    return jsonify({'ok': True, 'posts': result.get('data', [])})


@social_bp.route('/rate-limit', methods=['GET'])
def rate_limit():
    """Check IG publishing rate limit."""
    ig_id = _ig_user_id()
    if not ig_id:
        return jsonify({'ok': False, 'error': 'IG not configured'}), 400

    result = _graph_get(f'{ig_id}/content_publishing_limit')
    if 'error' in result:
        return jsonify({'ok': False, 'error': result['error'].get('message')}), 400

    data = result.get('data', [{}])
    usage = data[0].get('quota_usage', 0) if data else 0
    return jsonify({'ok': True, 'quota_usage': usage, 'quota_total': 100})


# ═══════════════════════════════════════════════════════════════════════════════
# 6. INSTAGRAM / FACEBOOK WEBHOOKS
# ═══════════════════════════════════════════════════════════════════════════════

@social_bp.route('/webhook', methods=['GET'])
def webhook_verify():
    """
    Meta webhook verification (GET).
    Meta sends: hub.mode=subscribe, hub.verify_token=<your token>, hub.challenge=<string>
    Return the challenge if token matches.

    In Meta dashboard set:
      Callback URL: https://autodirectocrm.vercel.app/api/social/webhook
      Verify token: autodirecto_social_2026
    """
    mode = request.args.get('hub.mode', '')
    token = request.args.get('hub.verify_token', '')
    challenge = request.args.get('hub.challenge', '')

    print(f'[social] webhook verify: mode={mode} token_match={token == WEBHOOK_VERIFY_TOKEN}')

    if mode == 'subscribe' and token == WEBHOOK_VERIFY_TOKEN:
        print(f'[social] ✅ webhook verified!')
        return challenge, 200
    else:
        print(f'[social] ❌ webhook verification failed')
        return 'Forbidden', 403


@social_bp.route('/webhook', methods=['POST'])
def webhook_receive():
    """Receive Instagram/Facebook webhook events (comments, mentions, etc)."""
    # Verify signature
    app_secret = _cfg('META_APP_SECRET')
    if app_secret:
        sig_header = request.headers.get('X-Hub-Signature-256', '')
        if sig_header:
            expected = 'sha256=' + hmac.new(
                app_secret.encode(), request.data, hashlib.sha256
            ).hexdigest()
            if not hmac.compare_digest(sig_header, expected):
                print('[social] ❌ webhook signature mismatch')
                return 'Bad signature', 403

    payload = request.json or {}
    obj = payload.get('object', '')
    entries = payload.get('entry', [])
    print(f'[social] 📩 webhook: object={obj}, entries={len(entries)}')

    for entry in entries:
        changes = entry.get('changes', [])
        for change in changes:
            field = change.get('field', '')
            value = change.get('value', {})
            print(f'[social]   → {field}: {json.dumps(value)[:200]}')

    return 'OK', 200


# ═══════════════════════════════════════════════════════════════════════════════
# 7. AI ASSISTANT (Gemini Flash — low cost)
# ═══════════════════════════════════════════════════════════════════════════════

GEMINI_API = 'https://generativelanguage.googleapis.com/v1beta/models'
GEMINI_MODEL = 'gemini-2.0-flash'

def _gemini_key():
    return os.environ.get('GOOGLE_API_KEY', '').strip()

def _gemini_generate(prompt, max_tokens=512, temperature=0.8):
    """Call Gemini Flash. Returns text or None."""
    key = _gemini_key()
    if not key:
        return None
    try:
        r = requests.post(
            f'{GEMINI_API}/{GEMINI_MODEL}:generateContent?key={key}',
            json={
                'contents': [{'parts': [{'text': prompt}]}],
                'generationConfig': {
                    'maxOutputTokens': max_tokens,
                    'temperature': temperature,
                },
            },
            timeout=20,
        )
        if r.status_code == 200:
            data = r.json()
            candidates = data.get('candidates', [])
            if candidates:
                parts = candidates[0].get('content', {}).get('parts', [])
                return parts[0].get('text', '') if parts else None
        print(f'[social-ai] Gemini {r.status_code}: {r.text[:200]}', flush=True)
        return None
    except Exception as e:
        print(f'[social-ai] Gemini error: {e}', flush=True)
        return None


@social_bp.route('/ai/caption', methods=['POST'])
def ai_generate_caption():
    """
    Generate an engaging Instagram/Facebook caption using Gemini Flash.
    Body: { vehicle: { brand, model, year, mileage, fuel_type, transmission, selling_price, color }, tone?: string, language?: string }
    """
    data = request.json or {}
    v = data.get('vehicle', {})
    tone = data.get('tone', 'profesional pero cercano')
    lang = data.get('language', 'español chileno')

    parts = ' '.join(filter(None, [v.get('brand'), v.get('model'), str(v.get('year', ''))]))
    price = f"${int(v['selling_price']):,}".replace(',', '.') if v.get('selling_price') else ''
    km = f"{int(v['mileage']):,} km".replace(',', '.') if v.get('mileage') else ''

    prompt = f"""Eres un community manager experto para Autodirecto.cl, una consignadora de autos en Chile.

Genera UN caption para Instagram/Facebook para este vehículo:
- Vehículo: {parts or 'auto en consignación'}
- Precio: {price or 'consultar'}
- Kilometraje: {km or 'N/A'}
- Combustible: {v.get('fuel_type', 'N/A')}
- Transmisión: {v.get('transmission', 'N/A')}
- Color: {v.get('color', 'N/A')}

Reglas:
- Tono: {tone}
- Idioma: {lang}
- Incluye emojis relevantes (🚗💰✨ etc)
- Máximo 280 caracteres para el texto principal
- Agrega un call-to-action (DM, WhatsApp, etc)
- NO incluyas hashtags (se agregan por separado)
- NO uses markdown ni asteriscos
- Sé creativo, no repitas la misma estructura siempre

Responde SOLO con el caption, nada más."""

    text = _gemini_generate(prompt, max_tokens=300, temperature=0.9)
    if text:
        return jsonify({'ok': True, 'caption': text.strip()})

    # Fallback: template-based
    templates = [
        f"🔥 ¡Llegó un {parts}!\n\n{'💰 ' + price if price else ''}{'📋 ' + km if km else ''}\n\n✨ Consignación premium\n📍 Autodirecto — Tu auto, sin complicaciones\n\n¿Interesado? Escríbenos por DM 💬",
        f"🚗 {parts} disponible ahora\n\n{'Precio: ' + price if price else ''}{'  ·  ' + km if km else ''}\n\n📸 Agenda tu visita\n🤝 Financiamiento disponible",
        f"⭐ {parts}\n{'💲 ' + price if price else ''}\n{'📊 ' + km if km else ''}\n\n🏷️ Consignación Premium Autodirecto\n📲 Consulta por WhatsApp o DM",
    ]
    import random
    return jsonify({'ok': True, 'caption': random.choice(templates), 'source': 'template'})


@social_bp.route('/ai/hashtags', methods=['POST'])
def ai_suggest_hashtags():
    """
    Suggest relevant hashtags using Gemini Flash.
    Body: { caption: str, vehicle?: { brand, model }, count?: int }
    """
    data = request.json or {}
    caption = data.get('caption', '')
    v = data.get('vehicle', {})
    count = min(data.get('count', 15), 30)
    brand = (v.get('brand') or '').lower()

    prompt = f"""Eres un experto en social media para una consignadora de autos en Chile (Autodirecto.cl).

Genera exactamente {count} hashtags para esta publicación de Instagram:

Caption: "{caption[:300]}"
Marca: {brand or 'auto'}

Reglas:
- Mezcla hashtags populares (#autos #chile) con de nicho (#autosusados #consignacion)
- Incluye la marca del auto como hashtag si la hay
- Incluye #autodirecto siempre
- Solo hashtags en español
- Sin explicaciones, solo los hashtags separados por espacio
- NO uses # duplicados

Responde SOLO con los hashtags, nada más."""

    text = _gemini_generate(prompt, max_tokens=200, temperature=0.7)
    if text:
        # Parse and clean hashtags
        tags = [t.strip() for t in text.replace('\n', ' ').split() if t.strip().startswith('#')]
        tags = list(dict.fromkeys(tags))[:count]  # deduplicate, limit
        return jsonify({'ok': True, 'hashtags': tags, 'text': ' '.join(tags)})

    # Fallback
    base = ['#autodirecto', '#autos', '#chile', '#ventadeautos', '#autosusados',
            '#consignacion', '#seminuevo', '#automotriz', '#autoschile']
    if brand:
        base.insert(1, f'#{brand}')
    return jsonify({'ok': True, 'hashtags': base[:count], 'text': ' '.join(base[:count]), 'source': 'template'})


@social_bp.route('/ai/best-time', methods=['POST'])
def ai_suggest_best_time():
    """
    Suggest best posting time based on engagement data + Gemini analysis.
    Body: { day_of_week?: str, posts_data?: array }
    """
    data = request.json or {}
    day = data.get('day_of_week', '')
    posts_data = data.get('posts_data', [])

    # Build context from real post data if available
    context = ''
    if posts_data:
        context = 'Datos de engagement de posts anteriores:\n'
        for p in posts_data[:20]:
            ts = p.get('published_at', p.get('scheduled_at', ''))
            eng = p.get('ig_engagement', 0) + p.get('fb_engagement', 0)
            context += f"  - Publicado: {ts}, Engagement: {eng}\n"

    prompt = f"""Eres un experto en social media para una concesionaria de autos en Chile.

{context}

¿Cuál es el mejor horario para publicar en Instagram/Facebook para una concesionaria de autos en Chile{' el día ' + day if day else ''}?

Responde en este formato JSON exacto (sin markdown):
{{"hour": 10, "minute": 0, "reason": "Razón breve en español"}}

Considera:
- Horarios laborales en Chile (CLT, UTC-3/-4)
- Comportamiento de compradores de autos
- Engagement máximo en redes"""

    text = _gemini_generate(prompt, max_tokens=150, temperature=0.3)
    if text:
        try:
            # Extract JSON from response
            import re
            match = re.search(r'\{[^}]+\}', text)
            if match:
                result = json.loads(match.group())
                return jsonify({'ok': True, **result})
        except Exception:
            pass

    # Sensible defaults for Chilean car dealership
    defaults = {
        'lunes': {'hour': 10, 'minute': 0, 'reason': 'Inicio de semana, gente buscando autos'},
        'martes': {'hour': 12, 'minute': 30, 'reason': 'Hora de almuerzo, buen scroll'},
        'miércoles': {'hour': 11, 'minute': 0, 'reason': 'Mitad de semana, engagement alto'},
        'jueves': {'hour': 18, 'minute': 0, 'reason': 'Fin de jornada, planificando fin de semana'},
        'viernes': {'hour': 17, 'minute': 0, 'reason': 'Viernes, gente planificando visitas'},
        'sábado': {'hour': 10, 'minute': 30, 'reason': 'Sábado mañana, compradores activos'},
        'domingo': {'hour': 11, 'minute': 0, 'reason': 'Domingo relajado, buen engagement'},
    }
    d = defaults.get(day.lower(), {'hour': 10, 'minute': 30, 'reason': 'Horario óptimo general para Chile'})
    return jsonify({'ok': True, **d, 'source': 'default'})


@social_bp.route('/ai/post-ideas', methods=['POST'])
def ai_post_ideas():
    """
    Generate content ideas/themes for social media posts.
    Body: { vehicles?: array, theme?: str, count?: int }
    """
    data = request.json or {}
    vehicles = data.get('vehicles', [])
    theme = data.get('theme', '')
    count = min(data.get('count', 5), 10)

    # Build vehicle inventory context
    inv_ctx = ''
    if vehicles:
        inv_ctx = 'Vehículos disponibles actualmente:\n'
        for v in vehicles[:10]:
            inv_ctx += f"  - {v.get('brand','')} {v.get('model','')} {v.get('year','')}, {v.get('selling_price','')}\n"

    today = datetime.now()
    day_name = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo'][today.weekday()]

    prompt = f"""Eres el community manager de Autodirecto.cl, una consignadora de autos premium en Chile.

Hoy es {day_name} {today.strftime('%d/%m/%Y')}.

{inv_ctx}

{f'Tema solicitado: {theme}' if theme else ''}

Genera exactamente {count} ideas de contenido para Instagram/Facebook.

Para cada idea, responde en este formato (una idea por línea):
TIPO | TÍTULO | DESCRIPCIÓN BREVE

Tipos válidos: Destacado, Nuevo Ingreso, Tip, Comparativa, Testimonio, Promoción, Detrás de Escena, Dato Curioso, Pregunta, Tendencia

Ejemplo:
Destacado | Feature Friday 🏆 | Destaca el auto más premium del inventario con fotos profesionales
Nuevo Ingreso | ¡Recién llegado! 🆕 | Anuncia un nuevo vehículo con datos clave y CTA

Reglas:
- Ideas variadas y creativas
- Relevantes para concesionaria de autos en Chile
- Incluye emojis en los títulos
- Responde SOLO las ideas, sin introducción ni cierre"""

    text = _gemini_generate(prompt, max_tokens=500, temperature=0.9)
    if text:
        ideas = []
        for line in text.strip().split('\n'):
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 3:
                ideas.append({
                    'type': parts[0],
                    'title': parts[1],
                    'description': parts[2],
                })
            elif len(parts) == 2:
                ideas.append({'type': 'Idea', 'title': parts[0], 'description': parts[1]})
        if ideas:
            return jsonify({'ok': True, 'ideas': ideas[:count]})

    # Fallback ideas
    fallback = [
        {'type': 'Destacado', 'title': 'Feature Friday 🏆', 'description': 'Destaca el auto más premium de la semana'},
        {'type': 'Nuevo Ingreso', 'title': '¡Recién llegado! 🆕', 'description': 'Anuncia un nuevo vehículo en consignación'},
        {'type': 'Tip', 'title': 'Tip del día 💡', 'description': 'Consejos para comprar auto usado de forma segura'},
        {'type': 'Comparativa', 'title': 'VS Battle ⚔️', 'description': 'Compara dos autos populares del inventario'},
        {'type': 'Pregunta', 'title': '¿Cuál prefieres? 🤔', 'description': 'Encuesta entre dos vehículos para generar engagement'},
    ]
    return jsonify({'ok': True, 'ideas': fallback[:count], 'source': 'template'})


# ═══════════════════════════════════════════════════════════════════════════════
# 8. DEBUG
# ═══════════════════════════════════════════════════════════════════════════════

@social_bp.route('/debug/test-token', methods=['GET'])
def debug_test_token():
    """Quick test — verify token works for IG + FB."""
    tok = _meta_token()
    if not tok:
        return jsonify({'error': 'No token', 'env': {
            'META_SYSTEM_USER_TOKEN': bool(_cfg('META_SYSTEM_USER_TOKEN')),
            'META_FB_PAGE_ACCESS_TOKEN': bool(_cfg('META_FB_PAGE_ACCESS_TOKEN')),
            'META_IG_USER_ID': _cfg('META_IG_USER_ID'),
            'META_FB_PAGE_ID': _cfg('META_FB_PAGE_ID'),
        }})

    me = _graph_get('me', {'fields': 'id,name'})
    ig = _graph_get(_ig_user_id(), {'fields': 'id,username'}) if _ig_user_id() else {'skip': True}
    fb = _graph_get(_fb_page_id(), {'fields': 'id,name'}) if _fb_page_id() else {'skip': True}

    return jsonify({
        'token_preview': tok[:20] + '...',
        'me': me,
        'instagram': ig,
        'facebook': fb,
    })
