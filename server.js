const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');

const app = express();
app.use(cors());
app.use(express.json());

const SHOPIFY_API_KEY    = process.env.SHOPIFY_API_KEY    || 'c1542c4ed17151e558edc3f37ceb9fd2';
const SHOPIFY_API_SECRET = process.env.SHOPIFY_API_SECRET || '';
const BACKEND_URL        = process.env.BACKEND_URL        || 'https://returns-backend.onrender.com';
// Request offline access token (shpat_ prefix, never expires)
const SCOPES = 'read_orders,write_orders,read_returns,write_returns';

let ACCESS_TOKEN = process.env.ACCESS_TOKEN || '';
let SHOP_DOMAIN  = process.env.SHOP_DOMAIN  || '';

app.get('/', (req, res) => res.json({
  status: 'Returns Manager Backend',
  connected: !!ACCESS_TOKEN,
  shop: SHOP_DOMAIN || 'not connected',
  token_type: ACCESS_TOKEN.startsWith('shpat_') ? 'offline (good)' : ACCESS_TOKEN.startsWith('shpua_') ? 'online (needs fix)' : 'none'
}));

// OAuth - request OFFLINE token by not including grant_options[]=per-user
app.get('/auth', (req, res) => {
  const shop = req.query.shop || SHOP_DOMAIN;
  if (!shop) return res.status(400).send('Missing ?shop=');
  const url = `https://${shop}/admin/oauth/authorize?client_id=${SHOPIFY_API_KEY}&scope=${SCOPES}&redirect_uri=${BACKEND_URL}/auth/callback&state=state123`;
  res.redirect(url);
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
      console.log(`SHOP: ${shop} | TOKEN: ${d.access_token} | TYPE: ${d.access_token.substring(0,6)}`);
      res.send(`<html><body style="font-family:sans-serif;padding:40px;background:#0f0f11;color:#f0f0f5">
        <h2 style="color:#7effa0">Connected!</h2>
        <p>Shop: <b>${shop}</b></p>
        <p>Token type: <b style="color:${d.access_token.startsWith('shpat_')?'#7effa0':'#ffca5e'}">${d.access_token.startsWith('shpat_')?'Offline (permanent)':'Online (temporary)'}</b></p>
        <p style="background:#1e1e24;padding:12px;border-radius:8px;font-family:monospace;word-break:break-all;color:#5ee8ff">${d.access_token}</p>
        <p style="color:#ffca5e">Copy token above → Save as ACCESS_TOKEN in Render environment</p>
      </body></html>`);
    } else {
      res.status(400).send(JSON.stringify(d));
    }
  } catch(e) { res.status(500).send(e.message); }
});

// GraphQL API - avoids protected customer data REST restriction
async function graphql(query) {
  if (!ACCESS_TOKEN || !SHOP_DOMAIN) throw new Error('Not authenticated');
  const r = await fetch(`https://${SHOP_DOMAIN}/admin/api/2026-01/graphql.json`, {
    method: 'POST',
    headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query })
  });
  return r.json();
}

// GET /api/orders - uses GraphQL, no protected data issues
app.get('/api/orders', async (req, res) => {
  try {
    const result = await graphql(`{
      orders(first: 50, reverse: true) {
        edges {
          node {
            id
            name
            createdAt
            displayFinancialStatus
            displayFulfillmentStatus
            totalPriceSet { shopMoney { amount currencyCode } }
            lineItems(first: 10) {
              edges {
                node {
                  title
                  quantity
                  variant { price }
                }
              }
            }
          }
        }
      }
    }`);

    if (result.errors) {
      return res.status(400).json({ error: result.errors[0].message });
    }

    // Normalize to same shape as before so frontend works unchanged
    const orders = result.data.orders.edges.map(({ node: o }) => ({
      id: o.id.replace('gid://shopify/Order/', ''),
      order_number: o.name.replace('#', ''),
      created_at: o.createdAt,
      financial_status: o.displayFinancialStatus.toLowerCase(),
      fulfillment_status: o.displayFulfillmentStatus.toLowerCase(),
      total_price: o.totalPriceSet.shopMoney.amount,
      currency: o.totalPriceSet.shopMoney.currencyCode,
      email: '',
      customer: null,
      line_items: o.lineItems.edges.map(({ node: li }) => ({
        title: li.title,
        quantity: li.quantity,
        price: li.variant?.price || '0'
      }))
    }));

    res.json({ orders });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/status', (req, res) => {
  res.json({ connected: !!ACCESS_TOKEN, shop: SHOP_DOMAIN || null });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Running on ${PORT} | Connected: ${!!ACCESS_TOKEN}`));
