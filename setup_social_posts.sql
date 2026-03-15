-- ═══════════════════════════════════════════════════════════════════════════════
-- Social Media Posts Table
-- Migration: 20260315_social_posts.sql
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS social_posts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    consignacion_id UUID,

    -- Content
    caption TEXT,
    hashtags TEXT,
    post_type VARCHAR(20) DEFAULT 'image',   -- image, carousel, reel, story

    -- Media (array of image/video URLs from Supabase Storage)
    media_urls JSONB DEFAULT '[]'::jsonb,

    -- Platforms
    publish_instagram BOOLEAN DEFAULT true,
    publish_facebook BOOLEAN DEFAULT true,

    -- Scheduling & status
    status VARCHAR(20) DEFAULT 'draft',       -- draft, scheduled, publishing, published, failed
    scheduled_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,

    -- Meta API IDs (after publishing)
    ig_media_id VARCHAR(100),
    ig_container_id VARCHAR(100),
    fb_post_id VARCHAR(100),

    -- IG Insights (updated periodically)
    ig_impressions INTEGER DEFAULT 0,
    ig_reach INTEGER DEFAULT 0,
    ig_engagement INTEGER DEFAULT 0,
    ig_saves INTEGER DEFAULT 0,
    ig_shares INTEGER DEFAULT 0,
    ig_likes INTEGER DEFAULT 0,
    ig_comments INTEGER DEFAULT 0,

    -- FB Insights
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

-- Index for calendar queries
CREATE INDEX IF NOT EXISTS idx_social_posts_scheduled ON social_posts (scheduled_at)
    WHERE scheduled_at IS NOT NULL;

-- Index for status filtering
CREATE INDEX IF NOT EXISTS idx_social_posts_status ON social_posts (status);

-- Index for consignacion lookup
CREATE INDEX IF NOT EXISTS idx_social_posts_consignacion ON social_posts (consignacion_id)
    WHERE consignacion_id IS NOT NULL;

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION update_social_posts_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_social_posts_updated_at ON social_posts;
CREATE TRIGGER trg_social_posts_updated_at
    BEFORE UPDATE ON social_posts
    FOR EACH ROW
    EXECUTE FUNCTION update_social_posts_updated_at();
