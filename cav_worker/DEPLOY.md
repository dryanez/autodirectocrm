# Deploy CAV Worker on Railway — Step by Step

## Step 1 — Create Railway account
Go to https://railway.app and sign up (free, no credit card needed to start)

## Step 2 — New Project
1. Click **"New Project"**
2. Select **"Deploy from GitHub repo"**
3. Authorize Railway to access your GitHub
4. Select **dryanez/autodirectocrm**

## Step 3 — THIS IS THE KEY PART: Set Root Directory
After selecting the repo, Railway shows you a settings screen.

Look for **"Root Directory"** field and type:
```
cav_worker
```

That tells Railway: "only look inside the cav_worker/ folder, treat it as the whole app"

If you don't see it during setup, you can set it after:
- Go to your service → **Settings** tab → **Source** section → **Root Directory** → type `cav_worker`
- Then click **"Redeploy"**

## Step 4 — Add Environment Variables
In Railway: Service → **Variables** tab → click **"New Variable"**

Add these one by one:

| Variable Name        | Value                        |
|----------------------|------------------------------|
| `TWOCAPTCHA_API_KEY` | your 2captcha.com API key    |
| `CAV_SECRET`         | cav-autodirecto-2026         |

Get your 2captcha key at: https://2captcha.com/enterpage → "API Key" in the top menu.

## Step 5 — Deploy
Railway will automatically build the Dockerfile and deploy.
Build takes ~3-4 minutes (installing Chromium is slow).

## Step 6 — Get your URL
After deploy: Service → **Settings** → **Networking** → click **"Generate Domain"**

You'll get something like:
```
https://cav-worker-production-abc123.up.railway.app
```

## Step 7 — Connect to your CRM on Vercel
Go to https://vercel.com → your autodirectocrm project → **Settings** → **Environment Variables**

Add:
| Variable Name    | Value                                              |
|------------------|----------------------------------------------------|
| `CAV_WORKER_URL` | https://cav-worker-production-abc123.up.railway.app |
| `CAV_SECRET`     | cav-autodirecto-2026                               |

Then **Redeploy** your Vercel app (Deployments → ⋯ → Redeploy).

## Step 8 — Test it
```bash
curl -X POST https://cav-worker-production-abc123.up.railway.app/cav \
  -H "Content-Type: application/json" \
  -d '{"plate": "XXXX12", "secret": "cav-autodirecto-2026"}'
```

Should return JSON with owner name, status, annotations.

---

## Cost on Railway
- Hobby plan: **$5/month** — covers the CAV worker completely
- The worker sleeps when not in use (wakes up in ~2s on first request)
