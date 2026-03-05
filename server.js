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
      orders(first: 250, reverse: true) {
        edges { node {
          id name createdAt tags note
          displayFinancialStatus displayFulfillmentStatus
          totalPriceSet { shopMoney { amount currencyCode } }
          lineItems(first: 20) { edges { node { 
            title quantity originalUnitPriceSet { shopMoney { amount } }
            variant { image { url } }
          } } }
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
      customer_name: 'Guest',
      customer_email: '',
      line_items: o.lineItems.edges.map(({ node: li }) => ({ 
        title: li.title, quantity: li.quantity,
        price: li.originalUnitPriceSet?.shopMoney?.amount || '0',
        image_url: li.variant?.image?.url || null
      })),
      request_type: (o.tags.some(t=>['exchange-requested','exchange-approved','exchange-rejected'].includes(t))) ? 'exchange'
        : (o.tags.some(t=>['mixed-requested','mixed-approved','mixed-rejected'].includes(t))) ? 'mixed' : 'return',
      return_status: o.tags.some(t=>['return-approved','exchange-approved','mixed-approved'].includes(t)) ? 'approved'
        : o.tags.some(t=>['return-rejected','exchange-rejected','mixed-rejected'].includes(t)) ? 'rejected'
        : o.tags.includes('return-requested') ? 'pending'
        : o.tags.includes('exchange-requested') ? 'exchange-pending'
        : o.tags.includes('mixed-requested') ? 'pending' : null
    }));
    const withRequests = orders.filter(o=>o.return_status);
    console.log(`Orders: ${orders.length} total, ${withRequests.length} with return/exchange requests`);
    if(withRequests.length) console.log('Requests:', withRequests.map(o=>`#${o.order_number}(${o.return_status})`).join(', '));
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
    console.log(`Order #${o.order_number} tags: "${tags}"`);
    const returnTags = ['return-requested','return-approved','return-rejected','exchange-requested','exchange-approved','exchange-rejected','mixed-requested'];
    const hasReturn = returnTags.some(t => tags.split(',').map(s=>s.trim()).includes(t));
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
          variant_id: li.variant_id, product_id: li.product_id,
          quantity: li.quantity, price: li.price,
          image_url: productData[li.product_id]?.image || null,
        }))
      }
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── FEATURED PRODUCTS (upsell) — MUST be before /:product_id ──
app.get('/api/products/featured', async (req, res) => {
  try {
    const data = await shopifyREST('GET', 'products.json?fields=id,title,images,variants,options&limit=8');
    res.json({ products: (data.products||[]).map(p => ({
      id: p.id, title: p.title,
      image: p.images?.[0]?.src||null,
      price: p.variants?.[0]?.price||'0',
      compare_at_price: p.variants?.[0]?.compare_at_price||null,
    }))});
  } catch(e) { res.status(500).json({ products: [] }); }
});

// ── SEARCH PRODUCTS — MUST be before /:product_id ──
app.get('/api/products/search', async (req, res) => {
  const { q } = req.query;
  if (!q) return res.json({ products: [] });
  try {
    const data = await shopifyREST('GET', `products.json?title=${encodeURIComponent(q)}&fields=id,title,images,variants,options&limit=8`);
    res.json({ products: (data.products||[]).map(p => ({
      id: p.id, title: p.title,
      image: p.images?.[0]?.src||null,
      price: p.variants?.[0]?.price||'0',
      options: p.options||[],
      variants: (p.variants||[]).map(v=>({ id: v.id, title: v.title, option1: v.option1, option2: v.option2, option3: v.option3, available: v.inventory_quantity > 0 }))
    }))});
  } catch(e) { res.status(500).json({ products: [] }); }
});

// ── GET PRODUCT VARIANTS ──
app.get('/api/products/:product_id', async (req, res) => {
  try {
    const data = await shopifyREST('GET', `products/${req.params.product_id}.json?fields=id,title,images,options,variants`);
    const p = data.product;
    if (!p) return res.status(404).json({ error: 'Not found' });
    res.json({
      id: p.id, title: p.title,
      image: p.images?.[0]?.src||null,
      options: p.options||[],
      variants: (p.variants||[]).map(v=>({ id: v.id, title: v.title, option1: v.option1, option2: v.option2, option3: v.option3, price: v.price, available: (v.inventory_quantity||1) > 0 }))
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── SUBMIT REQUEST ──
app.post('/api/returns/request', async (req, res) => {
  const { order_id, order_number, items, refund_method, customer_note, address } = req.body;
  if (!order_id || !items?.length) return res.status(400).json({ error: 'Missing required fields' });
  try {
    const returns = items.filter(i=>i.action==='return');
    const exchanges = items.filter(i=>i.action==='exchange');
    const hasBoth = returns.length>0 && exchanges.length>0;
    const hasExchange = exchanges.length>0;
    const hasReturn = returns.length>0;

    const itemLines = items.map(i => {
      let line = `[${i.action.toUpperCase()}] ${i.title} x${i.qty||1}`;
      if (i.reason) line += ` | Reason: ${i.reason}`;
      if (i.action==='exchange') {
        if (i.exchange_variant_title) line += ` | Exchange for: ${i.exchange_variant_title}`;
        if (i.exchange_product_title) line += ` | New Product: ${i.exchange_product_title}`;
      }
      return line;
    }).join('\n');

    const tag = hasBoth ? 'mixed-requested' : hasExchange ? 'exchange-requested' : 'return-requested';

    const note = `${hasBoth?'MIXED':''}${hasReturn&&!hasBoth?'RETURN':''}${hasExchange&&!hasBoth?'EXCHANGE':''} REQUEST #${order_number}
Items:
${itemLines}
${hasReturn?`Refund Method: ${refund_method||'Store Credit'}`:''}
Customer Note: ${customer_note||'None'}
${address?`Pickup: ${address.address1}, ${address.city}`:''}
Submitted: ${new Date().toISOString()}`;

    const updateData = await shopifyREST('PUT', `orders/${order_id}.json`, {
      order: { id: order_id, tags: tag, note }
    });

    if (updateData.order) {
      res.json({ success: true, type: hasBoth?'mixed':hasExchange?'exchange':'return' });
    } else {
      res.status(400).json({ error: 'Failed to update order', details: updateData });
    }
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── APPROVE / REJECT ──
app.post('/api/returns/:order_id/approve', async (req, res) => {
  const { order_id } = req.params;
  const { type } = req.body;
  try {
    const approvedTag = type==='exchange'?'exchange-approved':type==='mixed'?'mixed-approved':'return-approved';
    const removeTag   = type==='exchange'?'exchange-requested':type==='mixed'?'mixed-requested':'return-requested';
    // Fetch current tags, swap requested → approved
    const orderData = await shopifyREST('GET', `orders/${order_id}.json?fields=tags`);
    const existingTags = (orderData?.order?.tags||'').split(',').map(t=>t.trim()).filter(t=>t && t!==removeTag);
    existingTags.push(approvedTag);
    await shopifyREST('PUT', `orders/${order_id}.json`, { order: { id: order_id, tags: existingTags.join(', ') } });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/returns/:order_id/reject', async (req, res) => {
  const { order_id } = req.params;
  const { type } = req.body;
  try {
    const rejectedTag = type==='exchange'?'exchange-rejected':type==='mixed'?'mixed-rejected':'return-rejected';
    const removeTag   = type==='exchange'?'exchange-requested':type==='mixed'?'mixed-requested':'return-requested';
    const orderData = await shopifyREST('GET', `orders/${order_id}.json?fields=tags`);
    const existingTags = (orderData?.order?.tags||'').split(',').map(t=>t.trim()).filter(t=>t && t!==removeTag);
    existingTags.push(rejectedTag);
    await shopifyREST('PUT', `orders/${order_id}.json`, { order: { id: order_id, tags: existingTags.join(', ') } });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Running on ${PORT} | Connected: ${!!ACCESS_TOKEN}`));

// ═══════════════════════════════════════════════════════════
// ── DELHIVERY CONFIG ──
// ═══════════════════════════════════════════════════════════
let DELHIVERY_TOKEN    = process.env.DELHIVERY_TOKEN    || '';
let DELHIVERY_WAREHOUSE= process.env.DELHIVERY_WAREHOUSE|| ''; // exact warehouse name in Delhivery
let DELHIVERY_MODE     = process.env.DELHIVERY_MODE     || 'staging'; // 'staging' or 'production'

function delhiveryBase() {
  return DELHIVERY_MODE === 'production'
    ? 'https://track.delhivery.com'
    : 'https://staging-express.delhivery.com';
}

async function delhiveryAPI(method, path, body, isForm) {
  if (!DELHIVERY_TOKEN) throw new Error('Delhivery token not configured');
  const url = delhiveryBase() + path;
  const headers = { 'Authorization': `Token ${DELHIVERY_TOKEN}` };
  const opts = { method: method || 'GET', headers };
  if (body) {
    if (isForm) {
      headers['Content-Type'] = 'application/x-www-form-urlencoded';
      opts.body = `format=json&data=${encodeURIComponent(JSON.stringify(body))}`;
    } else {
      headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(body);
    }
  }
  const r = await fetch(url, opts);
  const text = await r.text();
  try { return JSON.parse(text); } catch(e) { return { raw: text }; }
}

// ── SAVE DELHIVERY CONFIG ──
app.post('/api/delhivery/config', (req, res) => {
  const { token, warehouse, mode } = req.body;
  if (token) DELHIVERY_TOKEN = token;
  if (warehouse) DELHIVERY_WAREHOUSE = warehouse;
  if (mode) DELHIVERY_MODE = mode;
  res.json({ success: true, configured: !!DELHIVERY_TOKEN, warehouse: DELHIVERY_WAREHOUSE, mode: DELHIVERY_MODE });
});

app.get('/api/delhivery/config', (req, res) => {
  res.json({ configured: !!DELHIVERY_TOKEN, warehouse: DELHIVERY_WAREHOUSE, mode: DELHIVERY_MODE });
});

// ── PINCODE SERVICEABILITY ──
app.get('/api/delhivery/serviceability/:pincode', async (req, res) => {
  try {
    const data = await delhiveryAPI('GET', `/c/api/pin-codes/json/?filter_codes=${req.params.pincode}`);
    const pin = data?.delivery_codes?.[0];
    res.json({
      serviceable: !!pin,
      cod: pin?.['cod']?.toLowerCase() === 'y',
      prepaid: pin?.['pre-paid']?.toLowerCase() === 'y',
      pickup: pin?.pickup?.toLowerCase() === 'y',
      pincode: req.params.pincode
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── FETCH WAYBILL ──
app.get('/api/delhivery/waybill', async (req, res) => {
  try {
    const clientName = req.query.client || '';
    const data = await delhiveryAPI('GET', `/waybill/api/bulk/json/?cl=${encodeURIComponent(clientName)}&count=1`);
    res.json({ waybill: data?.waybill_list?.[0] || null, raw: data });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── TRACK SHIPMENT ──
app.get('/api/delhivery/track/:waybill', async (req, res) => {
  try {
    const data = await delhiveryAPI('GET', `/api/v1/packages/?waybill=${req.params.waybill}`);
    const pkg = data?.ShipmentData?.[0]?.Shipment;
    if (!pkg) return res.json({ found: false, raw: data });
    res.json({
      found: true,
      waybill: pkg.AWB,
      status: pkg.Status?.Status,
      status_detail: pkg.Status?.Instructions,
      status_date: pkg.Status?.StatusDateTime,
      expected_date: pkg.ExpectedDeliveryDate,
      origin: pkg.Origin,
      destination: pkg.Destination,
      scans: (pkg.Scans || []).map(s => ({
        status: s.ScanDetail?.Scan,
        detail: s.ScanDetail?.Instructions,
        location: s.ScanDetail?.ScannedLocation,
        date: s.ScanDetail?.ScanDateTime
      }))
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── CREATE REVERSE PICKUP (RVP) in Delhivery ──
app.post('/api/delhivery/create-pickup', async (req, res) => {
  const {
    order_id, order_number,
    customer_name, customer_phone, customer_address, customer_city,
    customer_state, customer_pincode,
    products_desc, total_amount, quantity, weight,
    return_address  // optional override
  } = req.body;

  if (!DELHIVERY_TOKEN) return res.status(400).json({ error: 'Delhivery not configured. Add token first.' });
  if (!customer_pincode) return res.status(400).json({ error: 'Customer pincode required' });

  const rvpOrderId = `RVP-${order_number}-${Date.now().toString().slice(-6)}`;
  const payload = {
    pickup_location: { name: DELHIVERY_WAREHOUSE },
    shipments: [{
      name: customer_name || 'Customer',
      add: customer_address || '',
      pin: String(customer_pincode),
      city: customer_city || '',
      state: customer_state || '',
      country: 'India',
      phone: String(customer_phone || '').replace(/\D/g, '').slice(-10),
      order: rvpOrderId,
      payment_mode: 'Pickup',
      products_desc: products_desc || 'Return Shipment',
      hsn_code: '62034200',
      cod_amount: '0',
      order_date: new Date().toISOString().split('T')[0],
      total_amount: String(total_amount || '0'),
      seller_name: DELHIVERY_WAREHOUSE,
      seller_inv: `INV-${order_number}`,
      quantity: parseInt(quantity) || 1,
      weight: parseFloat(weight) || 0.5,
      shipment_length: 15,
      shipment_width: 12,
      shipment_height: 10,
      // Return to warehouse (delivery destination for RVP)
      ...(return_address ? {
        return_name: return_address.name,
        return_add: return_address.address,
        return_pin: String(return_address.pincode),
        return_city: return_address.city,
        return_state: return_address.state,
        return_phone: String(return_address.phone || ''),
        return_country: 'India'
      } : {})
    }]
  };

  try {
    const data = await delhiveryAPI('POST', '/api/cmu/create.json', payload, true);
    const pkg = data?.packages?.[0];
    const waybill = pkg?.waybill || data?.waybill;
    const success = !!(waybill || pkg?.status === 'Success' || data?.success);

    // Save AWB to Shopify order note
    if (success && waybill) {
      const currentOrder = await shopifyREST('GET', `orders/${order_id}.json?fields=note`);
      const existingNote = currentOrder?.order?.note || '';
      const newNote = existingNote + `\n\nDELHIVERY RVP\nAWB: ${waybill}\nRVP Order: ${rvpOrderId}\nCreated: ${new Date().toISOString()}`;
      await shopifyREST('PUT', `orders/${order_id}.json`, {
        order: { id: order_id, note: newNote, tags: 'pickup-scheduled' }
      });
    }

    res.json({ success, waybill, rvp_order_id: rvpOrderId, raw: data });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── CREATE SHOPIFY REFUND ──
app.post('/api/shopify/refund/:order_id', async (req, res) => {
  const { order_id } = req.params;
  const { line_items, refund_method, note } = req.body;
  // refund_method: 'store_credit' | 'original'
  try {
    // 1. Calculate refund
    const calcPayload = {
      refund: {
        shipping: { full_refund: false },
        refund_line_items: (line_items || []).map(li => ({
          line_item_id: li.id,
          quantity: li.quantity || 1,
          restock_type: 'return'
        }))
      }
    };
    const calc = await shopifyREST('POST', `orders/${order_id}/refunds/calculate.json`, calcPayload);
    const transactions = calc?.refund?.transactions || [];

    // 2. Create the actual refund
    const refundPayload = {
      refund: {
        notify: true,
        note: note || 'Return approved via Returns Portal',
        shipping: { full_refund: false },
        refund_line_items: (line_items || []).map(li => ({
          line_item_id: li.id,
          quantity: li.quantity || 1,
          restock_type: 'return'
        })),
        transactions: refund_method === 'store_credit'
          ? [] // No transaction = store credit (manual)
          : transactions.map(t => ({
              parent_id: t.parent_id,
              amount: t.amount,
              kind: 'refund',
              gateway: t.gateway
            }))
      }
    };

    const result = await shopifyREST('POST', `orders/${order_id}/refunds.json`, refundPayload);
    if (result?.refund?.id) {
      // Tag the order
      await shopifyREST('PUT', `orders/${order_id}.json`, {
        order: { id: order_id, tags: 'return-refunded' }
      });
      res.json({ success: true, refund_id: result.refund.id, amount: result.refund.transactions?.[0]?.amount });
    } else {
      res.status(400).json({ error: 'Refund failed', details: result });
    }
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── CREATE SHOPIFY EXCHANGE ORDER ──
app.post('/api/shopify/exchange/:order_id', async (req, res) => {
  const { order_id } = req.params;
  const { exchange_items, customer_address, order_number } = req.body;
  // exchange_items: [{variant_id, quantity, price, title}]
  try {
    // Get original order for customer/address details
    const orig = await shopifyREST('GET', `orders/${order_id}.json?fields=id,email,shipping_address,billing_address,customer`);
    const o = orig?.order;
    if (!o) return res.status(400).json({ error: 'Original order not found' });

    const addr = customer_address || o.shipping_address || o.billing_address;

    // Build draft order for exchange
    const draftPayload = {
      draft_order: {
        line_items: (exchange_items || []).map(item => ({
          variant_id: item.variant_id,
          quantity: item.quantity || 1,
          price: item.price || '0',
          title: item.title || 'Exchange Item',
          applied_discount: {
            description: 'Exchange discount',
            value_type: 'percentage',
            value: '100',
            amount: item.price || '0',
            title: 'Exchange'
          }
        })),
        customer: o.customer ? { id: o.customer.id } : undefined,
        shipping_address: addr,
        billing_address: o.billing_address || addr,
        email: o.email,
        note: `Exchange order for #${order_number || order_id}`,
        tags: 'exchange-order',
        send_invoice: false
      }
    };

    // Create draft order
    const draft = await shopifyREST('POST', 'draft_orders.json', draftPayload);
    const draftId = draft?.draft_order?.id;
    if (!draftId) return res.status(400).json({ error: 'Failed to create draft order', details: draft });

    // Complete the draft order (creates actual order)
    const completed = await shopifyREST('PUT', `draft_orders/${draftId}/complete.json`);
    const newOrderId = completed?.draft_order?.order_id;

    // Tag original order
    await shopifyREST('PUT', `orders/${order_id}.json`, {
      order: { id: order_id, tags: 'exchange-fulfilled' }
    });

    res.json({
      success: true,
      new_order_id: newOrderId,
      draft_order_id: draftId,
      new_order_name: completed?.draft_order?.name
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── GET ORDER DETAILS FOR DELHIVERY (full address + line items) ──
app.get('/api/orders/:order_id/details', async (req, res) => {
  try {
    const data = await shopifyREST('GET',
      `orders/${req.params.order_id}.json?fields=id,order_number,shipping_address,billing_address,line_items,note,tags,total_price`
    );
    const o = data?.order;
    if (!o) return res.status(404).json({ error: 'Not found' });
    res.json({
      id: o.id,
      order_number: o.order_number,
      address: o.shipping_address || o.billing_address,
      line_items: (o.line_items || []).map(li => ({
        id: li.id, title: li.title, variant_id: li.variant_id,
        quantity: li.quantity, price: li.price
      })),
      total_price: o.total_price,
      note: o.note,
      tags: o.tags
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});
