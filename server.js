const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');

const app = express();
app.use(cors());
app.use(express.json());

const SHOPIFY_API_KEY    = process.env.SHOPIFY_API_KEY    || 'c1542c4ed17151e558edc3f37ceb9fd2';
const SHOPIFY_API_SECRET = process.env.SHOPIFY_API_SECRET || '';
const SCOPES             = 'read_orders,write_orders,read_returns,write_returns';
const BACKEND_URL        = process.env.BACKEND_URL        || 'https://returns-backend.onrender.com';

// Token persists if set via environment variable (survives restarts)
let ACCESS_TOKEN = process.env.ACCESS_TOKEN || '';
let SHOP_DOMAIN  = process.env.SHOP_DOMAIN  || '';

app.get('/', (req, res) => {
  res.json({
    status: 'Returns Manager Backend running',
    connected: !!ACCESS_TOKEN,
    shop: SHOP_DOMAIN || 'not connected'
  });
});

app.get('/auth', (req, res) => {
  const shop = req.query.shop || SHOP_DOMAIN;
  if (!shop) return res.status(400).send('Missing ?shop=your-store.myshopify.com');
  const redirectUri = `${BACKEND_URL}/auth/callback`;
  const installUrl = `https://${shop}/admin/oauth/authorize?client_id=${SHOPIFY_API_KEY}&scope=${SCOPES}&redirect_uri=${redirectUri}&state=state123`;
  res.redirect(installUrl);
});

app.get('/auth/callback', async (req, res) => {
  const { shop, code } = req.query;
  if (!shop || !code) return res.status(400).send('Missing shop or code');
  try {
    const tokenRes = await fetch(`https://${shop}/admin/oauth/access_token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ client_id: SHOPIFY_API_KEY, client_secret: SHOPIFY_API_SECRET, code })
    });
    const tokenData = await tokenRes.json();
    if (tokenData.access_token) {
      ACCESS_TOKEN = tokenData.access_token;
      SHOP_DOMAIN  = shop;
      console.log(`CONNECTED: ${shop} | TOKEN: ${tokenData.access_token}`);
      // Show token on page so user can save it as env var
      res.send(`
        <!DOCTYPE html>
        <html>
        <head><title>Connected!</title></head>
        <body style="font-family:sans-serif;background:#0f0f11;color:#f0f0f5;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0">
          <div style="background:#17171b;border:1px solid #2a2a33;border-radius:16px;padding:36px;max-width:560px;width:95%">
            <div style="font-size:48px;text-align:center;margin-bottom:16px">🎉</div>
            <h2 style="color:#7effa0;text-align:center;margin-bottom:8px">Successfully Connected!</h2>
            <p style="color:#7a7a90;text-align:center;margin-bottom:28px">Store: <strong style="color:#f0f0f5">${shop}</strong></p>
            
            <div style="background:#1e1e24;border:1px solid #2a2a33;border-radius:10px;padding:16px;margin-bottom:16px">
              <p style="color:#ffca5e;font-size:12px;font-weight:600;letter-spacing:1px;text-transform:uppercase;margin-bottom:8px">⚠️ Save This Token — Copy it Now!</p>
              <p style="color:#7a7a90;font-size:12px;margin-bottom:10px">Add this as ACCESS_TOKEN in Render environment variables to keep your connection permanent.</p>
              <div style="background:#0f0f11;border:1px solid #2a2a33;border-radius:8px;padding:12px;word-break:break-all;font-family:monospace;font-size:13px;color:#5ee8ff;user-select:all">${tokenData.access_token}</div>
            </div>

            <div style="background:#1e1e24;border:1px solid #2a2a33;border-radius:10px;padding:16px">
              <p style="color:#7a7a90;font-size:12px;font-weight:600;letter-spacing:1px;text-transform:uppercase;margin-bottom:10px">Steps to make this permanent:</p>
              <ol style="color:#7a7a90;font-size:13px;line-height:2;padding-left:18px">
                <li>Copy the token above</li>
                <li>Go to <strong style="color:#f0f0f5">render.com</strong> → Your service → <strong style="color:#f0f0f5">Environment</strong></li>
                <li>Add: <code style="color:#7effa0">ACCESS_TOKEN</code> = (paste token)</li>
                <li>Click <strong style="color:#f0f0f5">Save Changes</strong> → Render redeploys</li>
                <li>Close this tab, refresh your Returns Manager</li>
              </ol>
            </div>
          </div>
        </body>
        </html>
      `);
    } else {
      res.status(400).send('OAuth failed: ' + JSON.stringify(tokenData));
    }
  } catch (err) {
    res.status(500).send('Error: ' + err.message);
  }
});

async function shopifyAPI(endpoint) {
  if (!ACCESS_TOKEN || !SHOP_DOMAIN) throw new Error('Not authenticated. Visit /auth first.');
  const url = `https://${SHOP_DOMAIN}/admin/api/2026-01/${endpoint}`;
  const res = await fetch(url, {
    headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN, 'Content-Type': 'application/json' }
  });
  return res.json();
}

app.get('/api/orders', async (req, res) => {
  try {
    const data = await shopifyAPI('orders.json?status=any&limit=50&fields=id,order_number,email,created_at,financial_status,fulfillment_status,total_price,currency,customer,line_items');
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/returns', async (req, res) => {
  try {
    const data = await shopifyAPI('returns.json');
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/orders/:id', async (req, res) => {
  try {
    const data = await shopifyAPI(`orders/${req.params.id}.json`);
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/status', (req, res) => {
  res.json({ connected: !!ACCESS_TOKEN, shop: SHOP_DOMAIN || null });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Backend running on port ${PORT} | Connected: ${!!ACCESS_TOKEN}`));
