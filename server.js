const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');

const app = express();
app.use(cors({
  origin: '*',
  methods: ['GET','POST','PUT','DELETE','OPTIONS'],
  allowedHeaders: ['Content-Type','Authorization','X-Shopify-Access-Token']
}));
app.use(express.json());

// Handle preflight requests
app.options('*', cors());

const SHOPIFY_API_KEY    = process.env.SHOPIFY_API_KEY    || 'c1542c4ed17151e558edc3f37ceb9fd2';
const SHOPIFY_API_SECRET = process.env.SHOPIFY_API_SECRET || '';
const BACKEND_URL        = process.env.BACKEND_URL        || 'https://returns-backend.onrender.com';
const SCOPES             = 'read_orders,write_orders,read_returns,write_returns';

let ACCESS_TOKEN = process.env.ACCESS_TOKEN || '';
let SHOP_DOMAIN  = process.env.SHOP_DOMAIN  || '';

// ── SHOPIFY REST API HELPER ──
async function shopifyREST(method, endpoint, body) {
  if (!ACCESS_TOKEN || !SHOP_DOMAIN) throw new Error('Not authenticated');
  const opts = {
    method: method || 'GET',
    headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN, 'Content-Type': 'application/json' }
  };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(`https://${SHOP_DOMAIN}/admin/api/2026-01/${endpoint}`, opts);
  const text = await r.text();
  try { return JSON.parse(text); } catch(e) { return { error: text }; }
}

// ── SHOPIFY GRAPHQL HELPER ──
async function graphql(query) {
  if (!ACCESS_TOKEN || !SHOP_DOMAIN) throw new Error('Not authenticated');
  const r = await fetch(`https://${SHOP_DOMAIN}/admin/api/2026-01/graphql.json`, {
    method: 'POST',
    headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query })
  });
  return r.json();
}

// ── HEALTH ──
// Allow embedding in Shopify Admin iframe
app.use((req, res, next) => {
  res.setHeader('Content-Security-Policy', "frame-ancestors 'self' https://*.myshopify.com https://admin.shopify.com");
  res.setHeader('Access-Control-Allow-Origin', '*');
  next();
});

app.get('/', (req, res) => res.json({
  status: 'Returns Manager Backend',
  connected: !!ACCESS_TOKEN,
  shop: SHOP_DOMAIN || 'not connected'
}));

// ── OAUTH ──
app.get('/auth', (req, res) => {
  const shop = req.query.shop || SHOP_DOMAIN;
  if (!shop) return res.status(400).send('Missing ?shop=');
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
      console.log(`TOKEN: ${d.access_token}`);
      res.send(`<html><body style="font-family:sans-serif;padding:40px;background:#0f0f11;color:#f0f0f5"><h2 style="color:#7effa0">Connected!</h2><p>${d.access_token}</p></body></html>`);
    } else res.status(400).send(JSON.stringify(d));
  } catch(e) { res.status(500).send(e.message); }
});

// ── STATUS ──
app.get('/api/status', (req, res) => {
  res.json({ connected: !!ACCESS_TOKEN, shop: SHOP_DOMAIN || null });
});

// ── GET ORDERS (merchant dashboard) ──
app.get('/api/orders', async (req, res) => {
  try {
    const result = await graphql(`{
      orders(first: 50, reverse: true) {
        edges {
          node {
            id
            name
            createdAt
            tags
            note
            displayFinancialStatus
            displayFulfillmentStatus
            totalPriceSet { shopMoney { amount currencyCode } }
            lineItems(first: 10) {
              edges { node { title quantity } }
            }
          }
        }
      }
    }`);
    if (result.errors) return res.status(400).json({ error: result.errors[0].message });
    const orders = result.data.orders.edges.map(({ node: o }) => ({
      id: o.id.replace('gid://shopify/Order/', ''),
      gid: o.id,
      order_number: o.name.replace('#', ''),
      created_at: o.createdAt,
      financial_status: o.displayFinancialStatus.toLowerCase(),
      fulfillment_status: o.displayFulfillmentStatus.toLowerCase(),
      total_price: o.totalPriceSet.shopMoney.amount,
      currency: o.totalPriceSet.shopMoney.currencyCode,
      tags: o.tags,
      note: o.note || '',
      email: '',
      customer: null,
      line_items: o.lineItems.edges.map(({ node: li }) => ({
        title: li.title, quantity: li.quantity, price: '0'
      })),
      // Return status from tags
      return_status: o.tags.includes('return-approved') ? 'approved'
        : o.tags.includes('return-rejected') ? 'rejected'
        : o.tags.includes('return-requested') ? 'pending'
        : null
    }));
    res.json({ orders });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── LOOKUP ORDER BY ORDER NUMBER (customer portal) ──
app.get('/api/lookup', async (req, res) => {
  const { order_number } = req.query;
  if (!order_number) return res.status(400).json({ error: 'Missing order_number' });
  try {
    const data = await shopifyREST('GET', `orders.json?name=%23${order_number}&status=any&fields=id,order_number,created_at,financial_status,fulfillment_status,total_price,currency,line_items,tags,note`);
    if (data.orders && data.orders.length > 0) {
      const o = data.orders[0];
      // Check if already has a return
      const hasReturn = o.tags && (o.tags.includes('return-requested') || o.tags.includes('return-approved') || o.tags.includes('return-rejected'));
      const returnStatus = o.tags && o.tags.includes('return-approved') ? 'approved'
        : o.tags && o.tags.includes('return-rejected') ? 'rejected'
        : o.tags && o.tags.includes('return-requested') ? 'pending' : null;
      res.json({
        found: true,
        order: {
          id: o.id,
          order_number: o.order_number,
          created_at: o.created_at,
          financial_status: o.financial_status,
          fulfillment_status: o.fulfillment_status,
          total_price: o.total_price,
          currency: o.currency,
          line_items: o.line_items,
          has_return: hasReturn,
          return_status: returnStatus,
          note: o.note
        }
      });
    } else {
      res.json({ found: false });
    }
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── SUBMIT RETURN REQUEST (customer portal) ──
app.post('/api/returns/request', async (req, res) => {
  const { order_id, order_number, items, reason, refund_method, customer_note } = req.body;
  if (!order_id || !reason) return res.status(400).json({ error: 'Missing required fields' });
  try {
    // Build note with return details
    const returnNote = `RETURN REQUEST #${order_number}
Reason: ${reason}
Refund Method: ${refund_method || 'Original payment'}
Items: ${items.map(i => `${i.title} (x${i.quantity})`).join(', ')}
Customer Note: ${customer_note || 'None'}
Submitted: ${new Date().toISOString()}`;

    // Add tag and note to Shopify order
    const updateData = await shopifyREST('PUT', `orders/${order_id}.json`, {
      order: {
        id: order_id,
        tags: 'return-requested',
        note: returnNote
      }
    });

    if (updateData.order) {
      console.log(`Return requested for order #${order_number}`);
      res.json({ success: true, message: 'Return request submitted successfully' });
    } else {
      res.status(400).json({ error: 'Failed to update order', details: updateData });
    }
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── APPROVE / REJECT RETURN (merchant dashboard) ──
app.post('/api/returns/:order_id/approve', async (req, res) => {
  const { order_id } = req.params;
  try {
    await shopifyREST('PUT', `orders/${order_id}.json`, {
      order: { id: order_id, tags: 'return-approved' }
    });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/returns/:order_id/reject', async (req, res) => {
  const { order_id } = req.params;
  try {
    await shopifyREST('PUT', `orders/${order_id}.json`, {
      order: { id: order_id, tags: 'return-rejected' }
    });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Running on ${PORT} | Connected: ${!!ACCESS_TOKEN}`));
