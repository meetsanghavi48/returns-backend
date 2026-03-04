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

let ACCESS_TOKEN = process.env.ACCESS_TOKEN || '';
let SHOP_DOMAIN  = process.env.SHOP_DOMAIN  || '';

app.get('/', (req, res) => res.json({ status: 'running', connected: !!ACCESS_TOKEN, shop: SHOP_DOMAIN }));

app.get('/auth', (req, res) => {
  const shop = req.query.shop || SHOP_DOMAIN;
  if (!shop) return res.status(400).send('Missing shop');
  res.redirect(`https://${shop}/admin/oauth/authorize?client_id=${SHOPIFY_API_KEY}&scope=${SCOPES}&redirect_uri=${BACKEND_URL}/auth/callback&state=state123`);
});

app.get('/auth/callback', async (req, res) => {
  const { shop, code } = req.query;
  if (!shop || !code) return res.status(400).send('Missing params');
  try {
    const r = await fetch(`https://${shop}/admin/oauth/access_token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ client_id: SHOPIFY_API_KEY, client_secret: SHOPIFY_API_SECRET, code })
    });
    const d = await r.json();
    if (d.access_token) {
      ACCESS_TOKEN = d.access_token;
      SHOP_DOMAIN  = shop;
      console.log('TOKEN:' + d.access_token);
      res.send('<h2 style="font-family:sans-serif;color:green">Connected! Close this tab.</h2>');
    } else {
      res.status(400).send(JSON.stringify(d));
    }
  } catch(e) { res.status(500).send(e.message); }
});

async function shopifyAPI(endpoint) {
  if (!ACCESS_TOKEN || !SHOP_DOMAIN) throw new Error('Not authenticated');
  const r = await fetch(`https://${SHOP_DOMAIN}/admin/api/2026-01/${endpoint}`, {
    headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN }
  });
  const text = await r.text();
  try { return JSON.parse(text); }
  catch(e) { return { error: text }; }
}

// Orders - no fields filter to avoid protected data issues
app.get('/api/orders', async (req, res) => {
  try {
    const data = await shopifyAPI('orders.json?status=any&limit=50&fields=id,order_number,created_at,financial_status,fulfillment_status,total_price,currency,line_items');
    res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/orders/:id', async (req, res) => {
  try {
    const data = await shopifyAPI(`orders/${req.params.id}.json?fields=id,order_number,created_at,financial_status,fulfillment_status,total_price,currency,line_items`);
    res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/status', (req, res) => {
  res.json({ connected: !!ACCESS_TOKEN, shop: SHOP_DOMAIN || null });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Running on ${PORT} | Connected: ${!!ACCESS_TOKEN}`));
