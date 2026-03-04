const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');

const app = express();
app.use(cors({ origin: '*', methods: ['GET','POST','PUT','DELETE','OPTIONS'], allowedHeaders: ['Content-Type','Authorization','X-Shopify-Access-Token'] }));
app.use(express.json());
app.options('*', cors());

const SHOPIFY_API_KEY    = process.env.SHOPIFY_API_KEY    || 'c1542c4ed17151e558edc3f37ceb9fd2';
const SHOPIFY_API_SECRET = process.env.SHOPIFY_API_SECRET || '';
const BACKEND_URL        = process.env.BACKEND_URL        || 'https://returns-backend.onrender.com';
const SCOPES             = 'read_orders,write_orders,read_returns,write_returns,read_products';

let ACCESS_TOKEN = process.env.ACCESS_TOKEN || '';
let SHOP_DOMAIN  = process.env.SHOP_DOMAIN  || '';

async function shopifyREST(method, endpoint, body) {
  if (!ACCESS_TOKEN || !SHOP_DOMAIN) throw new Error('Not authenticated');
  const opts = { method: method||'GET', headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN, 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(`https://${SHOP_DOMAIN}/admin/api/2026-01/${endpoint}`, opts);
  const text = await r.text();
  try { return JSON.parse(text); } catch(e) { return { error: text }; }
}

async function graphql(query) {
  if (!ACCESS_TOKEN || !SHOP_DOMAIN) throw new Error('Not authenticated');
  const r = await fetch(`https://${SHOP_DOMAIN}/admin/api/2026-01/graphql.json`, {
    method: 'POST',
    headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query })
  });
  return r.json();
}

app.use((req, res, next) => {
  res.setHeader('Content-Security-Policy', "frame-ancestors 'self' https://*.myshopify.com https://admin.shopify.com");
  res.setHeader('Access-Control-Allow-Origin', '*');
  next();
});

app.get('/', (req, res) => res.json({ status: 'Returns Manager Backend', connected: !!ACCESS_TOKEN, shop: SHOP_DOMAIN||'not connected' }));

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
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ client_id: SHOPIFY_API_KEY, client_secret: SHOPIFY_API_SECRET, code })
    });
    const d = await r.json();
    if (d.access_token) {
      ACCESS_TOKEN = d.access_token; SHOP_DOMAIN = shop;
      console.log(`TOKEN: ${d.access_token}`);
      res.send(`<html><body style="font-family:sans-serif;padding:40px;background:#0f0f11;color:#f0f0f5"><h2 style="color:#7effa0">Connected!</h2><p>${d.access_token}</p></body></html>`);
    } else res.status(400).send(JSON.stringify(d));
  } catch(e) { res.status(500).send(e.message); }
});

app.get('/api/status', (req, res) => res.json({ connected: !!ACCESS_TOKEN, shop: SHOP_DOMAIN||null }));

// ── GET ORDERS (merchant dashboard) ──
app.get('/api/orders', async (req, res) => {
  try {
    const result = await graphql(`{
      orders(first: 50, reverse: true) {
        edges { node {
          id name createdAt tags note
          displayFinancialStatus displayFulfillmentStatus
          totalPriceSet { shopMoney { amount currencyCode } }
          lineItems(first: 10) { edges { node { title quantity } } }
        }}
      }
    }`);
    if (result.errors) return res.status(400).json({ error: result.errors[0].message });
    const orders = result.data.orders.edges.map(({ node: o }) => ({
      id: o.id.replace('gid://shopify/Order/', ''),
      gid: o.id,
      order_number: o.name.replace('#',''),
      created_at: o.createdAt,
      financial_status: o.displayFinancialStatus.toLowerCase(),
      fulfillment_status: o.displayFulfillmentStatus.toLowerCase(),
      total_price: o.totalPriceSet.shopMoney.amount,
      currency: o.totalPriceSet.shopMoney.currencyCode,
      tags: o.tags, note: o.note||'', email: '', customer: null,
      line_items: o.lineItems.edges.map(({ node: li }) => ({ title: li.title, quantity: li.quantity, price: '0' })),
      request_type: (o.tags.includes('exchange-requested')||o.tags.includes('exchange-approved')||o.tags.includes('exchange-rejected')) ? 'exchange' : 'return',
      return_status: (o.tags.includes('return-approved')||o.tags.includes('exchange-approved')) ? 'approved'
        : (o.tags.includes('return-rejected')||o.tags.includes('exchange-rejected')) ? 'rejected'
        : o.tags.includes('return-requested') ? 'pending'
        : o.tags.includes('exchange-requested') ? 'exchange-pending' : null
    }));
    res.json({ orders });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── LOOKUP ORDER (customer portal) ──
app.get('/api/lookup', async (req, res) => {
  const { order_number, contact } = req.query;
  if (!order_number) return res.status(400).json({ error: 'Missing order_number' });
  try {
    const data = await shopifyREST('GET',
      `orders.json?name=%23${order_number}&status=any&fields=id,order_number,created_at,financial_status,fulfillment_status,total_price,currency,line_items,tags,note,email,phone,customer,shipping_address,billing_address`
    );
    if (!data.orders?.length) return res.json({ found: false });
    const o = data.orders[0];

    // Optional contact verification - skip if Shopify has no contact data
    if (contact && contact.trim()) {
      const inp = contact.toLowerCase().trim();
      const inpDigits = inp.replace(/\D/g,'');
      const oEmail = (o.email||o.contact_email||'').toLowerCase();
      const oPhone = (o.phone||o.shipping_address?.phone||o.billing_address?.phone||'').replace(/\D/g,'');
      // Only verify if Shopify actually returned contact info
      if (oEmail || oPhone) {
        const emailOk = oEmail && oEmail.includes(inp);
        const phoneOk = oPhone && inpDigits.length >= 6 && oPhone.includes(inpDigits.slice(-8));
        if (!emailOk && !phoneOk) {
          return res.json({ found: false, mismatch: true });
        }
      }
      // If no contact info from Shopify, skip verification and proceed
    }

    // Fetch product images & variant data
    const productIds = [...new Set((o.line_items||[]).map(li=>li.product_id).filter(Boolean))].join(',');
    let productData = {};
    if (productIds) {
      const prods = await shopifyREST('GET', `products.json?ids=${productIds}&fields=id,images,options,variants&limit=20`);
      (prods.products||[]).forEach(p => {
        productData[p.id] = { image: p.images?.[0]?.src||null, options: p.options||[], variants: p.variants||[] };
      });
    }

    // Deadlines (8 days from order)
    const orderDate = new Date(o.created_at);
    const deadline = new Date(orderDate.getTime() + 8*24*60*60*1000).toISOString();

    // Address
    const addr = o.shipping_address || o.billing_address || null;

    // Return status
    const tags = o.tags || '';
    const hasReturn = tags.includes('return-requested')||tags.includes('return-approved')||tags.includes('return-rejected')||tags.includes('exchange-requested')||tags.includes('exchange-approved')||tags.includes('exchange-rejected')||tags.includes('mixed-requested');
    const returnStatus = tags.includes('return-approved')||tags.includes('exchange-approved') ? 'approved'
      : tags.includes('return-rejected')||tags.includes('exchange-rejected') ? 'rejected'
      : tags.includes('return-requested') ? 'pending'
      : tags.includes('exchange-requested') ? 'exchange-pending'
      : tags.includes('mixed-requested') ? 'pending' : null;
    const requestType = tags.includes('exchange-requested')||tags.includes('exchange-approved')||tags.includes('exchange-rejected') ? 'exchange'
      : tags.includes('mixed-requested') ? 'mixed' : 'return';

    res.json({
      found: true,
      order: {
        id: o.id, order_number: o.order_number, created_at: o.created_at,
        financial_status: o.financial_status, fulfillment_status: o.fulfillment_status,
        total_price: o.total_price, currency: o.currency,
        has_return: hasReturn, return_status: returnStatus, request_type: requestType,
        note: o.note, return_deadline: deadline, exchange_deadline: deadline,
        address: addr,
        line_items: (o.line_items||[]).map(li => ({
          id: li.id, title: li.title, variant_title: li.variant_title,
          variant_
