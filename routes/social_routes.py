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

# ─── Config (from .env) ─────────────────────────────────────────────────────
def _cfg(key, default=''):
    return os.environ.get(key, default)

GRAPH_API = 'https://graph.facebook.com/v25.0'

# Webhook verify token — must match what you enter in Meta dashboard
WEBHOOK_VERIFY_TOKEN = os.environ.get('META_WEBHOOK_VERIFY_TOKEN', 'autodirecto_social_2026')


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _meta_token():
    """Return the best available Meta token. Prefers system user token."""
    return _cfg('META_SYSTEM_USER_TOKEN') or _cfg('META_FB_PAGE_ACCESS_TOKEN')

def _ig_user_id():
    return _cfg('META_IG_USER_ID')

def _fb_page_id():
    return _cfg('META_FB_PAGE_ID')

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
      Callback URL: https://autodirectocrm-production.up.railway.app/api/social/webhook
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
# 7. DEBUG
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
