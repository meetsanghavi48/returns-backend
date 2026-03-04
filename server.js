const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');
const crypto = require('crypto');

const app = express();
app.use(cors());
app.use(express.json());

const SHOPIFY_API_KEY    = process.env.SHOPIFY_API_KEY    || 'c1542c4ed17151e558edc3f37ceb9fd2';
const SHOPIFY_API_SECRET = process.env.SHOPIFY_API_SECRET || '';
const SHOP               = process.env.SHOP_DOMAIN        || '';
const SCOPES             = 'read_orders,write_orders,read_returns,write_returns';
const BACKEND_URL        = process.env.BACKEND_URL        || 'https://your-render-app.onrender.com';

let ACCESS_TOKEN = process.env.ACCESS_TOKEN || '';
let SHOP_DOMAIN  = SHOP;

app.get('/', (req, res) => {
  res.json({
    status: 'Returns Manager Backend running',
    connected: !!ACCESS_TOKEN,
    shop: SHOP_DOMAIN || 'not connected yet'
  });
});

app.get('/auth', (req, res) => {
  const shop = req.query.shop || SHOP;
  if (!shop) return res.status(400).send('Missing ?shop=your-store.myshopify.com');
  const redirectUri = `${BACKEND_URL}/auth/callback`;
  const installUrl = `https://${shop}/admin/oauth/authorize?client_id=${SHOPIFY_API_KEY}&scope=${SCOPES}&redirect_uri=${redirectUri}&state=randomstate123`;
  console.log('Starting OAuth for:', shop);
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
      console.log('Connected to:', shop);
      res.send(`<html><body style="font-family:sans-serif;background:#0f0f11;color:#f0f0f5;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;flex-direction:column;gap:16px"><div style="font-size:48px">🎉</div><h2 style="color:#7effa0">Successfully Connected!</h2><p>Store <strong>${shop}</strong> is now connected.</p><p style="color:#7a7a90">Close this tab and refresh your Returns Manager.</p></body></html>`);
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
  const res = await fetch(url, { headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN, 'Content-Type': 'application/json' } });
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
app.listen(PORT, () => console.log(`Backend running on port ${PORT}`));
