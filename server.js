const express = require('express');
const cors    = require('cors');
const fetch   = require('node-fetch');
const crypto  = require('crypto');

const app = express();
app.use(cors({ origin:'*', methods:['GET','POST','PUT','DELETE','OPTIONS'], allowedHeaders:['Content-Type','Authorization','X-Shopify-Access-Token'] }));
app.use(express.json({ limit:'10mb' }));
app.options('*', cors());
app.use((req,res,next)=>{ res.setHeader('Content-Security-Policy',"frame-ancestors 'self' https://*.myshopify.com https://admin.shopify.com"); res.setHeader('Access-Control-Allow-Origin','*'); next(); });

// ── SHOPIFY CONFIG ──
const SHOPIFY_API_KEY    = process.env.SHOPIFY_API_KEY    || 'c1542c4ed17151e558edc3f37ceb9fd2';
const SHOPIFY_API_SECRET = process.env.SHOPIFY_API_SECRET || '';
const BACKEND_URL        = process.env.BACKEND_URL        || 'https://returns-backend.onrender.com';
const SCOPES             = 'read_orders,write_orders,read_returns,write_returns,read_products,write_products,read_draft_orders,write_draft_orders,read_gift_cards,write_gift_cards,read_inventory,write_inventory';
let ACCESS_TOKEN = process.env.ACCESS_TOKEN || '';
let SHOP_DOMAIN  = process.env.SHOP_DOMAIN  || '';

// ── DELHIVERY CONFIG ──
let DELHIVERY_TOKEN    = process.env.DELHIVERY_TOKEN    || '';
let DELHIVERY_WAREHOUSE= process.env.DELHIVERY_WAREHOUSE|| '';
let DELHIVERY_MODE     = process.env.DELHIVERY_MODE     || 'staging';

// ── IN-MEMORY STORES (persist via Shopify metafields for production) ──
// Rules engine rules
let RULES = [
  { id:'rule_1', name:'Keep It Rule', enabled:true, priority:1,
    conditions:[{ field:'product_price', op:'lt', value:'15' },{ field:'shipping_cost', op:'gt', value:'8' }],
    action:'keep_it', action_params:{ message:'Please keep or donate this item. Full refund issued!' } },
  { id:'rule_2', name:'Auto-Approve Size Issues', enabled:true, priority:2,
    conditions:[{ field:'return_reason', op:'contains', value:'size' }],
    action:'auto_approve', action_params:{} },
  { id:'rule_3', name:'Flag Serial Returners', enabled:true, priority:3,
    conditions:[{ field:'customer_return_count', op:'gte', value:'3' }],
    action:'flag_review', action_params:{ note:'Serial returner flagged for manual review' } },
  { id:'rule_4', name:'Instant Credit for Low-Value', enabled:false, priority:4,
    conditions:[{ field:'order_value', op:'lt', value:'50' },{ field:'return_reason', op:'eq', value:'Size Issue' }],
    action:'auto_approve', action_params:{} }
];

// Audit trail
let AUDIT_LOG = [];

// In-memory return requests (mirrors Shopify tags + enriched data)
let RETURN_REQUESTS = {};

// Analytics counters
let ANALYTICS = {
  total_requests: 0, approved: 0, rejected: 0, auto_approved: 0,
  refunded_amount: 0, store_credits_issued: 0, exchanges_created: 0,
  reasons: {}, products_returned: {}
};

// Warehouse config
let WAREHOUSE_CONFIG = {
  name: process.env.WAREHOUSE_NAME || '',
  address: process.env.WAREHOUSE_ADDRESS || '',
  city: process.env.WAREHOUSE_CITY || '',
  state: process.env.WAREHOUSE_STATE || '',
  pincode: process.env.WAREHOUSE_PINCODE || '',
  phone: process.env.WAREHOUSE_PHONE || '',
  email: process.env.WAREHOUSE_EMAIL || ''
};

// Return window (days)
let RETURN_WINDOW_DAYS = parseInt(process.env.RETURN_WINDOW_DAYS) || 30;
let STORE_CREDIT_BONUS = parseFloat(process.env.STORE_CREDIT_BONUS) || 10; // % bonus for store credit
let RESTOCKING_FEE_PCT = parseFloat(process.env.RESTOCKING_FEE_PCT) || 0;

// ── HELPERS ──
function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2,7); }

function auditLog(order_id, action, actor, details) {
  const entry = { id: uid(), timestamp: new Date().toISOString(), order_id: String(order_id), action, actor: actor||'system', details: details||'' };
  AUDIT_LOG.unshift(entry);
  if (AUDIT_LOG.length > 2000) AUDIT_LOG = AUDIT_LOG.slice(0, 2000);
  console.log(`[AUDIT] #${order_id} | ${action} | ${actor} | ${details}`);
  return entry;
}

async function shopifyREST(method, endpoint, body) {
  if (!ACCESS_TOKEN || !SHOP_DOMAIN) throw new Error('Not authenticated');
  const opts = { method: method||'GET', headers:{ 'X-Shopify-Access-Token': ACCESS_TOKEN, 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(`https://${SHOP_DOMAIN}/admin/api/2024-01/${endpoint}`, opts);
  const text = await r.text();
  try { return JSON.parse(text); } catch(e) { return { error: text }; }
}

async function graphql(query, variables) {
  if (!ACCESS_TOKEN || !SHOP_DOMAIN) throw new Error('Not authenticated');
  const r = await fetch(`https://${SHOP_DOMAIN}/admin/api/2024-01/graphql.json`, {
    method:'POST',
    headers:{ 'X-Shopify-Access-Token': ACCESS_TOKEN, 'Content-Type':'application/json' },
    body: JSON.stringify({ query, variables })
  });
  return r.json();
}

// Update Shopify tags preserving existing ones
async function updateOrderTags(order_id, addTags, removeTags=[]) {
  const orderData = await shopifyREST('GET', `orders/${order_id}.json?fields=tags`);
  let tags = (orderData?.order?.tags||'').split(',').map(t=>t.trim()).filter(Boolean);
  tags = tags.filter(t => !removeTags.includes(t));
  addTags.forEach(t => { if (!tags.includes(t)) tags.push(t); });
  await shopifyREST('PUT', `orders/${order_id}.json`, { order:{ id:order_id, tags:tags.join(', ') } });
  return tags;
}

// Run rules engine against a return request
function runRulesEngine(requestData) {
  const results = [];
  const sorted = [...RULES].filter(r=>r.enabled).sort((a,b)=>a.priority-b.priority);
  for (const rule of sorted) {
    let match = true;
    for (const cond of rule.conditions) {
      const val = getFieldValue(requestData, cond.field);
      if (!evaluateCondition(val, cond.op, cond.value)) { match=false; break; }
    }
    if (match) {
      results.push({ rule_id: rule.id, rule_name: rule.name, action: rule.action, params: rule.action_params });
      break; // First matching rule wins
    }
  }
  return results;
}

function getFieldValue(req, field) {
  switch(field) {
    case 'return_reason':      return (req.items||[]).map(i=>i.reason||'').join(' ').toLowerCase();
    case 'product_price':      return parseFloat(req.items?.[0]?.price||0);
    case 'order_value':        return parseFloat(req.total_price||0);
    case 'shipping_cost':      return parseFloat(req.shipping_cost||9);
    case 'customer_return_count': return parseInt(req.customer_return_count||0);
    case 'request_type':       return req.request_type||'return';
    case 'item_category':      return req.item_category||'';
    default: return '';
  }
}

function evaluateCondition(val, op, target) {
  const n = parseFloat(val); const t = parseFloat(target);
  switch(op) {
    case 'eq':       return String(val).toLowerCase() === String(target).toLowerCase();
    case 'neq':      return String(val).toLowerCase() !== String(target).toLowerCase();
    case 'contains': return String(val).toLowerCase().includes(String(target).toLowerCase());
    case 'gt':       return !isNaN(n) && !isNaN(t) && n > t;
    case 'lt':       return !isNaN(n) && !isNaN(t) && n < t;
    case 'gte':      return !isNaN(n) && !isNaN(t) && n >= t;
    case 'lte':      return !isNaN(n) && !isNaN(t) && n <= t;
    default:         return false;
  }
}

function delhiveryBase() {
  return DELHIVERY_MODE==='production' ? 'https://track.delhivery.com' : 'https://staging-express.delhivery.com';
}

async function delhiveryAPI(method, path, body, isForm) {
  if (!DELHIVERY_TOKEN) throw new Error('Delhivery token not configured');
  const url = delhiveryBase()+path;
  const headers = { 'Authorization':`Token ${DELHIVERY_TOKEN}` };
  const opts = { method:method||'GET', headers };
  if (body) {
    if (isForm) {
      headers['Content-Type']='application/x-www-form-urlencoded';
      opts.body = `format=json&data=${encodeURIComponent(JSON.stringify(body))}`;
    } else {
      headers['Content-Type']='application/json';
      opts.body = JSON.stringify(body);
    }
  }
  const r = await fetch(url, opts);
  const text = await r.text();
  try { return JSON.parse(text); } catch(e) { return { raw: text }; }
}

// ════════════════════════════════════════════
// AUTH
// ════════════════════════════════════════════
app.get('/', (req,res) => res.json({ status:'Returns Manager v2', connected:!!ACCESS_TOKEN, shop:SHOP_DOMAIN||'not connected' }));
app.get('/api/status', (req,res) => res.json({ connected:!!ACCESS_TOKEN, shop:SHOP_DOMAIN||null, return_window:RETURN_WINDOW_DAYS, store_credit_bonus:STORE_CREDIT_BONUS }));

app.get('/auth', (req,res) => {
  const shop = req.query.shop||SHOP_DOMAIN;
  if (!shop) return res.status(400).send('Missing ?shop=');
  res.redirect(`https://${shop}/admin/oauth/authorize?client_id=${SHOPIFY_API_KEY}&scope=${SCOPES}&redirect_uri=${BACKEND_URL}/auth/callback&state=state123`);
});

app.get('/auth/callback', async (req,res) => {
  const { shop, code } = req.query;
  if (!shop||!code) return res.status(400).send('Missing params');
  try {
    const r = await fetch(`https://${shop}/admin/oauth/access_token`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ client_id:SHOPIFY_API_KEY, client_secret:SHOPIFY_API_SECRET, code })
    });
    const d = await r.json();
    if (d.access_token) {
      ACCESS_TOKEN=d.access_token; SHOP_DOMAIN=shop;
      auditLog('system','auth_connected','system',`Connected to ${shop}`);
      res.send(`<html><body style="font-family:sans-serif;padding:40px;background:#0f0f11;color:#f0f0f5"><h2 style="color:#7effa0">Connected!</h2><p>${d.access_token}</p></body></html>`);
    } else res.status(400).send(JSON.stringify(d));
  } catch(e) { res.status(500).send(e.message); }
});

// ════════════════════════════════════════════
// ORDERS — DASHBOARD
// ════════════════════════════════════════════
app.get('/api/orders', async (req,res) => {
  try {
    const result = await graphql(`{
      orders(first:250, reverse:true) {
        edges { node {
          id name createdAt tags note
          displayFinancialStatus displayFulfillmentStatus
          totalPriceSet { shopMoney { amount currencyCode } }
          lineItems(first:20) { edges { node {
            title quantity
            originalUnitPriceSet { shopMoney { amount } }
            variant { image { url } }
          }}}
        }}
      }
    }`);
    if (result.errors) return res.status(400).json({ error:result.errors[0].message });
    const orders = result.data.orders.edges.map(({ node:o }) => {
      const tags = o.tags || [];
      const allTags = Array.isArray(tags) ? tags : tags.split(',').map(t=>t.trim());
      const has = t => allTags.includes(t);
      const hasAny = arr => arr.some(t=>allTags.includes(t));
      const return_status =
        hasAny(['return-approved','exchange-approved','mixed-approved']) ? 'approved' :
        hasAny(['return-rejected','exchange-rejected','mixed-rejected']) ? 'rejected' :
        has('return-requested')   ? 'pending' :
        has('exchange-requested') ? 'exchange-pending' :
        has('mixed-requested')    ? 'pending' : null;
      const request_type =
        hasAny(['exchange-requested','exchange-approved','exchange-rejected']) ? 'exchange' :
        hasAny(['mixed-requested','mixed-approved','mixed-rejected'])          ? 'mixed' : 'return';
      return {
        id:         o.id.replace('gid://shopify/Order/',''),
        gid:        o.id,
        order_number: o.name.replace('#',''),
        created_at: o.createdAt,
        financial_status: o.displayFinancialStatus.toLowerCase(),
        fulfillment_status: o.displayFulfillmentStatus.toLowerCase(),
        total_price: o.totalPriceSet.shopMoney.amount,
        currency:   o.totalPriceSet.shopMoney.currencyCode,
        tags: allTags, note: o.note||'',
        customer_name:'Guest', customer_email:'',
        line_items: o.lineItems.edges.map(({node:li})=>({
          title:li.title, quantity:li.quantity,
          price:li.originalUnitPriceSet?.shopMoney?.amount||'0',
          image_url:li.variant?.image?.url||null
        })),
        return_status, request_type,
        // Enrich from in-memory
        ...(RETURN_REQUESTS[o.id.replace('gid://shopify/Order/','')] || {})
      };
    });
    const withRequests = orders.filter(o=>o.return_status);
    res.json({ orders, return_requests: withRequests.length });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ════════════════════════════════════════════
// PORTAL — LOOKUP ORDER
// ════════════════════════════════════════════
app.get('/api/lookup', async (req,res) => {
  const { order_number, contact } = req.query;
  if (!order_number) return res.status(400).json({ error:'Missing order_number' });
  try {
    const data = await shopifyREST('GET',
      `orders.json?name=%23${order_number}&status=any&fields=id,order_number,created_at,financial_status,fulfillment_status,total_price,currency,line_items,tags,note,email,phone,customer,shipping_address,billing_address`
    );
    if (!data.orders?.length) return res.json({ found:false });
    const o = data.orders[0];

    // Contact verification
    if (contact && contact.trim()) {
      const inp = contact.toLowerCase().trim().replace(/\D/g,'');
      const oEmail = (o.email||'').toLowerCase();
      const oPhone = (o.phone||o.shipping_address?.phone||'').replace(/\D/g,'');
      if (oEmail || oPhone) {
        const emailOk = oEmail && oEmail.includes(contact.toLowerCase().trim());
        const phoneOk = oPhone && inp.length>=6 && oPhone.includes(inp.slice(-8));
        if (!emailOk && !phoneOk) return res.json({ found:false, mismatch:true });
      }
    }

    // Return window check
    const orderDate  = new Date(o.created_at);
    const now        = new Date();
    const daysDiff   = (now - orderDate) / (1000*60*60*24);
    const withinWindow = daysDiff <= RETURN_WINDOW_DAYS;
    const deadline   = new Date(orderDate.getTime() + RETURN_WINDOW_DAYS*24*60*60*1000).toISOString();

    // Fetch product images + variants + non-returnable tags
    const productIds = [...new Set((o.line_items||[]).map(li=>li.product_id).filter(Boolean))].join(',');
    let productData = {};
    if (productIds) {
      const prods = await shopifyREST('GET', `products.json?ids=${productIds}&fields=id,images,options,variants,tags&limit=20`);
      (prods.products||[]).forEach(p => {
        productData[p.id] = {
          image: p.images?.[0]?.src||null,
          options: p.options||[],
          variants: p.variants||[],
          non_returnable: (p.tags||'').includes('non-returnable')
        };
      });
    }

    // Tags
    const tags = o.tags||'';
    const tagArr = tags.split(',').map(t=>t.trim());
    const has = t => tagArr.includes(t);
    const returnTags = ['return-requested','return-approved','return-rejected','exchange-requested','exchange-approved','exchange-rejected','mixed-requested','mixed-approved','mixed-rejected'];
    const hasReturn  = returnTags.some(t=>tagArr.includes(t));
    const return_status =
      (has('return-approved')||has('exchange-approved')||has('mixed-approved'))   ? 'approved' :
      (has('return-rejected')||has('exchange-rejected')||has('mixed-rejected'))   ? 'rejected' :
      has('return-requested')   ? 'pending' :
      has('exchange-requested') ? 'exchange-pending' :
      has('mixed-requested')    ? 'pending' : null;
    const request_type =
      has('exchange-requested')||has('exchange-approved')||has('exchange-rejected') ? 'exchange' :
      has('mixed-requested')||has('mixed-approved')||has('mixed-rejected')          ? 'mixed' : 'return';

    // Line items — filter fulfilled, non-returned, non-returnable
    const enrichedItems = (o.line_items||[]).map(li => ({
      id: li.id, title: li.title,
      variant_title: li.variant_title||'',
      variant_id: li.variant_id,
      product_id: li.product_id,
      quantity: li.quantity,
      price: li.price,
      fulfillment_status: li.fulfillment_status,
      image_url: productData[li.product_id]?.image||null,
      non_returnable: productData[li.product_id]?.non_returnable||false,
      product_options: productData[li.product_id]?.options||[],
      product_variants: productData[li.product_id]?.variants||[]
    })).filter(li => li.fulfillment_status === 'fulfilled' || o.fulfillment_status === 'fulfilled');

    const addr = o.shipping_address||o.billing_address||null;

    res.json({
      found: true,
      order: {
        id: o.id, order_number: o.order_number,
        created_at: o.created_at,
        financial_status: o.financial_status,
        fulfillment_status: o.fulfillment_status,
        total_price: o.total_price, currency: o.currency,
        has_return: hasReturn, return_status, request_type,
        note: o.note, return_deadline: deadline,
        within_window: withinWindow, days_remaining: Math.max(0, RETURN_WINDOW_DAYS - Math.floor(daysDiff)),
        address: addr,
        store_credit_bonus: STORE_CREDIT_BONUS,
        line_items: enrichedItems
      }
    });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ════════════════════════════════════════════
// PRODUCTS
// ════════════════════════════════════════════
app.get('/api/products/featured', async (req,res) => {
  try {
    const data = await shopifyREST('GET','products.json?fields=id,title,images,variants,options&limit=8');
    res.json({ products:(data.products||[]).map(p=>({ id:p.id, title:p.title, image:p.images?.[0]?.src||null, price:p.variants?.[0]?.price||'0', compare_at_price:p.variants?.[0]?.compare_at_price||null }))});
  } catch(e) { res.json({ products:[] }); }
});

app.get('/api/products/search', async (req,res) => {
  const { q } = req.query;
  if (!q) return res.json({ products:[] });
  try {
    const data = await shopifyREST('GET',`products.json?title=${encodeURIComponent(q)}&fields=id,title,images,variants,options&limit=8`);
    res.json({ products:(data.products||[]).map(p=>({ id:p.id, title:p.title, image:p.images?.[0]?.src||null, price:p.variants?.[0]?.price||'0', options:p.options||[], variants:(p.variants||[]).map(v=>({ id:v.id, title:v.title, option1:v.option1, option2:v.option2, option3:v.option3, price:v.price, available:(v.inventory_quantity||1)>0 })) }))});
  } catch(e) { res.json({ products:[] }); }
});

app.get('/api/products/:product_id', async (req,res) => {
  try {
    const data = await shopifyREST('GET',`products/${req.params.product_id}.json?fields=id,title,images,options,variants`);
    const p=data.product; if(!p) return res.status(404).json({ error:'Not found' });
    res.json({ id:p.id, title:p.title, image:p.images?.[0]?.src||null, options:p.options||[], variants:(p.variants||[]).map(v=>({ id:v.id, title:v.title, option1:v.option1, option2:v.option2, option3:v.option3, price:v.price, available:(v.inventory_quantity||1)>0 })) });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ════════════════════════════════════════════
// SUBMIT RETURN REQUEST — CORE ENGINE
// ════════════════════════════════════════════
app.post('/api/returns/request', async (req,res) => {
  const { order_id, order_number, items, refund_method, customer_note, address, media_urls, shipping_preference } = req.body;
  if (!order_id||!items?.length) return res.status(400).json({ error:'Missing required fields' });

  try {
    // Build enriched request object
    const returns   = items.filter(i=>i.action==='return');
    const exchanges = items.filter(i=>i.action==='exchange');
    const hasBoth   = returns.length>0 && exchanges.length>0;
    const hasExchange = exchanges.length>0;
    const hasReturn   = returns.length>0;

    const requestData = {
      order_id, order_number, items, refund_method,
      total_price: items.reduce((s,i)=>s+parseFloat(i.price||0)*(i.qty||1),0),
      request_type: hasBoth?'mixed': hasExchange?'exchange':'return',
      shipping_preference: shipping_preference||'self_ship',
      media_urls: media_urls||[]
    };

    // ── RUN RULES ENGINE ──
    const ruleMatches = runRulesEngine(requestData);
    let autoAction = null;
    if (ruleMatches.length) {
      autoAction = ruleMatches[0];
      auditLog(order_id, `rule_matched:${autoAction.action}`, 'rules_engine', `Rule: ${autoAction.rule_name}`);
    }

    // Build note
    const itemLines = items.map(i=>{
      let l = `[${i.action.toUpperCase()}] ${i.title}${i.variant_title?' - '+i.variant_title:''} x${i.qty||1}`;
      if (i.reason) l += ` | Reason: ${i.reason}`;
      if (i.action==='exchange') {
        if (i.exchange_variant_title) l += ` | Exchange for: ${i.exchange_variant_title}`;
        if (i.exchange_product_title) l += ` | New Product: ${i.exchange_product_title}`;
        if (i.exchange_variant_id)    l += ` | Variant ID: ${i.exchange_variant_id}`;
        if (i.price_diff && parseFloat(i.price_diff)>0) l += ` | Price Diff: +${i.price_diff}`;
      }
      return l;
    }).join('\n');

    const tag = hasBoth?'mixed-requested': hasExchange?'exchange-requested':'return-requested';

    const mediaSection = media_urls?.length ? `\nMedia: ${media_urls.join(', ')}` : '';
    const note =
`${hasBoth?'MIXED REQUEST':hasExchange?'EXCHANGE REQUEST':'RETURN REQUEST'} #${order_number}
Type: ${requestData.request_type}
Items:
${itemLines}
Refund Method: ${refund_method||'Store Credit'}
Shipping: ${shipping_preference||'Self Ship'}
Customer Note: ${customer_note||'None'}${mediaSection}
${address?`Pickup Address: ${address.address1}, ${address.city}, ${address.zip}`:''}
Rules Engine: ${autoAction?autoAction.rule_name:'No rule matched'}
Submitted: ${new Date().toISOString()}`;

    // Determine initial status based on rules
    let initialStatus = 'pending';
    let addTags = [tag];
    if (autoAction?.action === 'auto_approve') {
      addTags = [tag.replace('-requested','-approved')];
      initialStatus = 'approved';
      ANALYTICS.auto_approved++;
    } else if (autoAction?.action === 'keep_it') {
      addTags = ['return-approved','keep-it-rule'];
      initialStatus = 'keep_it';
    }

    // Fetch current tags and update
    const orderData = await shopifyREST('GET',`orders/${order_id}.json?fields=tags`);
    let existingTags = (orderData?.order?.tags||'').split(',').map(t=>t.trim()).filter(Boolean);
    existingTags = [...new Set([...existingTags, ...addTags])];

    const updateData = await shopifyREST('PUT',`orders/${order_id}.json`,{ order:{ id:order_id, tags:existingTags.join(', '), note } });
    if (!updateData.order) return res.status(400).json({ error:'Failed to update order', details:updateData });

    // Store enriched data in memory
    RETURN_REQUESTS[String(order_id)] = {
      ...requestData, status: initialStatus,
      submitted_at: new Date().toISOString(),
      auto_action: autoAction?.action||null
    };

    // Update analytics
    ANALYTICS.total_requests++;
    items.forEach(i => {
      if (i.reason) ANALYTICS.reasons[i.reason] = (ANALYTICS.reasons[i.reason]||0)+1;
      if (i.title)  ANALYTICS.products_returned[i.title] = (ANALYTICS.products_returned[i.title]||0)+1;
    });

    auditLog(order_id, 'request_submitted', 'customer', `${requestData.request_type} request — ${items.length} items | Rules: ${autoAction?.rule_name||'none'}`);

    res.json({
      success: true,
      type: requestData.request_type,
      status: initialStatus,
      auto_action: autoAction?.action||null,
      keep_it_message: autoAction?.action==='keep_it' ? autoAction.params.message : null,
      ref: `${hasExchange?'EXC':hasReturn?'RET':'MIX'}-${order_number}-${Date.now().toString().slice(-4)}`
    });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ════════════════════════════════════════════
// APPROVE / REJECT
// ════════════════════════════════════════════
app.post('/api/returns/:order_id/approve', async (req,res) => {
  const { order_id } = req.params;
  const { type, actor } = req.body;
  try {
    const approvedTag = type==='exchange'?'exchange-approved':type==='mixed'?'mixed-approved':'return-approved';
    const removeTag   = type==='exchange'?'exchange-requested':type==='mixed'?'mixed-requested':'return-requested';
    await updateOrderTags(order_id, [approvedTag], [removeTag]);
    if (RETURN_REQUESTS[String(order_id)]) RETURN_REQUESTS[String(order_id)].status = 'approved';
    ANALYTICS.approved++;
    auditLog(order_id,'approved', actor||'merchant', `${type} request approved`);
    res.json({ success:true });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

app.post('/api/returns/:order_id/reject', async (req,res) => {
  const { order_id } = req.params;
  const { type, reason, actor } = req.body;
  try {
    const rejectedTag = type==='exchange'?'exchange-rejected':type==='mixed'?'mixed-rejected':'return-rejected';
    const removeTag   = type==='exchange'?'exchange-requested':type==='mixed'?'mixed-requested':'return-requested';
    await updateOrderTags(order_id, [rejectedTag], [removeTag]);
    if (RETURN_REQUESTS[String(order_id)]) RETURN_REQUESTS[String(order_id)].status = 'rejected';
    ANALYTICS.rejected++;
    auditLog(order_id,'rejected', actor||'merchant', `${type} request rejected${reason?': '+reason:''}`);
    res.json({ success:true });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ════════════════════════════════════════════
// SHOPIFY REFUND (with restocking fee + partial)
// ════════════════════════════════════════════
app.post('/api/shopify/refund/:order_id', async (req,res) => {
  const { order_id } = req.params;
  const { line_items, refund_method, note, restocking_fee_pct } = req.body;
  try {
    const calcPayload = {
      refund: {
        shipping: { full_refund:false },
        refund_line_items: (line_items||[]).map(li=>({ line_item_id:li.id, quantity:li.quantity||1, restock_type:'return' }))
      }
    };
    const calc = await shopifyREST('POST',`orders/${order_id}/refunds/calculate.json`,calcPayload);
    let transactions = calc?.refund?.transactions||[];

    // Apply restocking fee
    const fee = parseFloat(restocking_fee_pct||RESTOCKING_FEE_PCT);
    if (fee>0 && transactions.length) {
      transactions = transactions.map(t=>({ ...t, amount: (parseFloat(t.amount||0)*(1-fee/100)).toFixed(2) }));
    }

    const refundPayload = {
      refund: {
        notify: true,
        note: note||'Return approved via Returns Manager',
        shipping: { full_refund:false },
        refund_line_items: (line_items||[]).map(li=>({ line_item_id:li.id, quantity:li.quantity||1, restock_type:'return' })),
        transactions: refund_method==='store_credit' ? [] :
          transactions.map(t=>({ parent_id:t.parent_id, amount:t.amount, kind:'refund', gateway:t.gateway }))
      }
    };

    const result = await shopifyREST('POST',`orders/${order_id}/refunds.json`,refundPayload);
    if (result?.refund?.id) {
      await updateOrderTags(order_id,['return-refunded'],[]);
      const amount = result.refund.transactions?.[0]?.amount||'0';
      ANALYTICS.refunded_amount += parseFloat(amount);
      auditLog(order_id,'refund_created','merchant',`Refund ${result.refund.id} — ${amount} via ${refund_method}`);
      res.json({ success:true, refund_id:result.refund.id, amount });
    } else {
      res.status(400).json({ error:'Refund failed', details:result });
    }
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ════════════════════════════════════════════
// STORE CREDIT — SHOPIFY GIFT CARD
// ════════════════════════════════════════════
app.post('/api/shopify/store-credit/:order_id', async (req,res) => {
  const { order_id } = req.params;
  const { amount, customer_email, note, apply_bonus } = req.body;
  try {
    const baseAmount = parseFloat(amount||0);
    const bonus = apply_bonus ? (baseAmount * STORE_CREDIT_BONUS/100) : 0;
    const finalAmount = (baseAmount + bonus).toFixed(2);
    const code = `CREDIT-${order_id.toString().slice(-6)}-${uid().toUpperCase().slice(0,6)}`;

    // Create gift card via Shopify API
    const giftCardData = await shopifyREST('POST','gift_cards.json',{
      gift_card: {
        initial_value: finalAmount,
        code,
        note: note||`Store credit for return on order #${order_id}`,
        expires_on: null // no expiry
      }
    });

    if (giftCardData?.gift_card?.id) {
      await updateOrderTags(order_id,['store-credit-issued'],[]);
      ANALYTICS.store_credits_issued += parseFloat(finalAmount);
      auditLog(order_id,'store_credit_issued','merchant',`Gift card ${code} — ₹${finalAmount} (bonus: ₹${bonus.toFixed(2)})`);
      res.json({
        success: true,
        gift_card_id: giftCardData.gift_card.id,
        code,
        amount: finalAmount,
        bonus: bonus.toFixed(2),
        original_amount: baseAmount.toFixed(2),
        customer_email
      });
    } else {
      res.status(400).json({ error:'Failed to create gift card', details:giftCardData });
    }
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ════════════════════════════════════════════
// EXCHANGE ORDER
// ════════════════════════════════════════════
app.post('/api/shopify/exchange/:order_id', async (req,res) => {
  const { order_id } = req.params;
  const { exchange_items, customer_address, order_number } = req.body;
  try {
    const orig = await shopifyREST('GET',`orders/${order_id}.json?fields=id,email,shipping_address,billing_address,customer`);
    const o = orig?.order;
    if (!o) return res.status(400).json({ error:'Original order not found' });
    const addr = customer_address||o.shipping_address||o.billing_address;

    const draftPayload = {
      draft_order: {
        line_items: (exchange_items||[]).map(item=>({
          variant_id: item.variant_id, quantity: item.quantity||1,
          price: item.price||'0', title: item.title||'Exchange Item',
          applied_discount: { description:'Exchange', value_type:'percentage', value:'100', amount:item.price||'0', title:'Exchange' }
        })),
        customer: o.customer?{ id:o.customer.id }:undefined,
        shipping_address: addr, billing_address: o.billing_address||addr,
        email: o.email, note:`Exchange order for #${order_number||order_id}`,
        tags:'exchange-order', send_invoice:false
      }
    };

    const draft = await shopifyREST('POST','draft_orders.json',draftPayload);
    const draftId = draft?.draft_order?.id;
    if (!draftId) return res.status(400).json({ error:'Failed to create draft order', details:draft });

    const completed = await shopifyREST('PUT',`draft_orders/${draftId}/complete.json`);
    const newOrderId = completed?.draft_order?.order_id;
    await updateOrderTags(order_id,['exchange-fulfilled'],[]);
    ANALYTICS.exchanges_created++;
    auditLog(order_id,'exchange_order_created','merchant',`New order #${completed?.draft_order?.name||newOrderId}`);
    res.json({ success:true, new_order_id:newOrderId, draft_order_id:draftId, new_order_name:completed?.draft_order?.name });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// Reserve inventory for exchange
app.post('/api/shopify/reserve-inventory', async (req,res) => {
  const { variant_id, quantity } = req.body;
  try {
    // Get inventory item for variant
    const varData = await shopifyREST('GET',`variants/${variant_id}.json?fields=id,inventory_item_id,inventory_quantity`);
    const inv = varData?.variant;
    if (!inv) return res.status(404).json({ error:'Variant not found' });
    // Note: Full inventory reservation requires inventory_levels API with location
    // We log this as a reservation request
    auditLog('system','inventory_reserved','system',`Variant ${variant_id} — qty ${quantity} reserved for exchange`);
    res.json({ success:true, variant_id, available: inv.inventory_quantity, reserved:quantity, note:'Reservation logged' });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ════════════════════════════════════════════
// ORDER DETAILS (for address/line items)
// ════════════════════════════════════════════
app.get('/api/orders/:order_id/details', async (req,res) => {
  try {
    const data = await shopifyREST('GET',`orders/${req.params.order_id}.json?fields=id,order_number,shipping_address,billing_address,line_items,note,tags,total_price`);
    const o = data?.order;
    if (!o) return res.status(404).json({ error:'Not found' });
    res.json({
      id:o.id, order_number:o.order_number,
      address: o.shipping_address||o.billing_address,
      line_items:(o.line_items||[]).map(li=>({ id:li.id, title:li.title, variant_id:li.variant_id, quantity:li.quantity, price:li.price })),
      total_price:o.total_price, note:o.note, tags:o.tags
    });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ════════════════════════════════════════════
// RULES ENGINE CRUD
// ════════════════════════════════════════════
app.get('/api/rules', (req,res) => res.json({ rules:RULES }));

app.post('/api/rules', (req,res) => {
  const rule = { id:'rule_'+uid(), enabled:true, priority:RULES.length+1, ...req.body };
  RULES.push(rule);
  auditLog('system','rule_created','merchant',`Rule: ${rule.name}`);
  res.json({ success:true, rule });
});

app.put('/api/rules/:id', (req,res) => {
  const idx = RULES.findIndex(r=>r.id===req.params.id);
  if (idx<0) return res.status(404).json({ error:'Not found' });
  RULES[idx] = { ...RULES[idx], ...req.body };
  auditLog('system','rule_updated','merchant',`Rule: ${RULES[idx].name}`);
  res.json({ success:true, rule:RULES[idx] });
});

app.delete('/api/rules/:id', (req,res) => {
  const idx = RULES.findIndex(r=>r.id===req.params.id);
  if (idx<0) return res.status(404).json({ error:'Not found' });
  const [removed] = RULES.splice(idx,1);
  auditLog('system','rule_deleted','merchant',`Rule: ${removed.name}`);
  res.json({ success:true });
});

// ════════════════════════════════════════════
// AUDIT TRAIL
// ════════════════════════════════════════════
app.get('/api/audit', (req,res) => {
  const { order_id, limit } = req.query;
  let logs = AUDIT_LOG;
  if (order_id) logs = logs.filter(l=>l.order_id===String(order_id));
  res.json({ logs: logs.slice(0, parseInt(limit)||100) });
});

// ════════════════════════════════════════════
// ANALYTICS
// ════════════════════════════════════════════
app.get('/api/analytics', async (req,res) => {
  try {
    // Return prevention — top returned products with reasons
    const prevention = Object.entries(ANALYTICS.products_returned)
      .sort((a,b)=>b[1]-a[1]).slice(0,10)
      .map(([product,count])=>({ product, count }));
    const topReasons = Object.entries(ANALYTICS.reasons)
      .sort((a,b)=>b[1]-a[1]).slice(0,5)
      .map(([reason,count])=>({ reason, count }));
    res.json({
      ...ANALYTICS,
      prevention_report: prevention,
      top_reasons: topReasons,
      approval_rate: ANALYTICS.total_requests>0 ? Math.round(ANALYTICS.approved/ANALYTICS.total_requests*100) : 0,
      auto_approve_rate: ANALYTICS.total_requests>0 ? Math.round(ANALYTICS.auto_approved/ANALYTICS.total_requests*100) : 0
    });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ════════════════════════════════════════════
// APP SETTINGS
// ════════════════════════════════════════════
app.get('/api/settings', (req,res) => res.json({
  return_window_days: RETURN_WINDOW_DAYS,
  store_credit_bonus: STORE_CREDIT_BONUS,
  restocking_fee_pct: RESTOCKING_FEE_PCT,
  warehouse: WAREHOUSE_CONFIG,
  delhivery: { configured:!!DELHIVERY_TOKEN, warehouse:DELHIVERY_WAREHOUSE, mode:DELHIVERY_MODE }
}));

app.post('/api/settings', (req,res) => {
  const { return_window_days, store_credit_bonus, restocking_fee_pct, warehouse } = req.body;
  if (return_window_days) RETURN_WINDOW_DAYS = parseInt(return_window_days);
  if (store_credit_bonus!==undefined) STORE_CREDIT_BONUS = parseFloat(store_credit_bonus);
  if (restocking_fee_pct!==undefined) RESTOCKING_FEE_PCT = parseFloat(restocking_fee_pct);
  if (warehouse) WAREHOUSE_CONFIG = { ...WAREHOUSE_CONFIG, ...warehouse };
  auditLog('system','settings_updated','merchant',`Window:${RETURN_WINDOW_DAYS}d Credit bonus:${STORE_CREDIT_BONUS}%`);
  res.json({ success:true });
});

// ════════════════════════════════════════════
// DELHIVERY
// ════════════════════════════════════════════
app.post('/api/delhivery/config', (req,res) => {
  const { token, warehouse, mode } = req.body;
  if (token) DELHIVERY_TOKEN=token;
  if (warehouse) DELHIVERY_WAREHOUSE=warehouse;
  if (mode) DELHIVERY_MODE=mode;
  res.json({ success:true, configured:!!DELHIVERY_TOKEN, warehouse:DELHIVERY_WAREHOUSE, mode:DELHIVERY_MODE });
});

app.get('/api/delhivery/config', (req,res) => res.json({ configured:!!DELHIVERY_TOKEN, warehouse:DELHIVERY_WAREHOUSE, mode:DELHIVERY_MODE }));

app.get('/api/delhivery/serviceability/:pincode', async (req,res) => {
  try {
    const data = await delhiveryAPI('GET',`/c/api/pin-codes/json/?filter_codes=${req.params.pincode}`);
    const pin = data?.delivery_codes?.[0];
    res.json({ serviceable:!!pin, cod:pin?.['cod']?.toLowerCase()==='y', prepaid:pin?.['pre-paid']?.toLowerCase()==='y', pickup:pin?.pickup?.toLowerCase()==='y', pincode:req.params.pincode });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

app.get('/api/delhivery/waybill', async (req,res) => {
  try {
    const data = await delhiveryAPI('GET',`/waybill/api/bulk/json/?cl=${encodeURIComponent(req.query.client||'')}&count=1`);
    res.json({ waybill:data?.waybill_list?.[0]||null });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

app.get('/api/delhivery/track/:waybill', async (req,res) => {
  try {
    const data = await delhiveryAPI('GET',`/api/v1/packages/?waybill=${req.params.waybill}`);
    const pkg = data?.ShipmentData?.[0]?.Shipment;
    if (!pkg) return res.json({ found:false });
    res.json({ found:true, waybill:pkg.AWB, status:pkg.Status?.Status, status_detail:pkg.Status?.Instructions, status_date:pkg.Status?.StatusDateTime, expected_date:pkg.ExpectedDeliveryDate, origin:pkg.Origin, destination:pkg.Destination,
      scans:(pkg.Scans||[]).map(s=>({ status:s.ScanDetail?.Scan, detail:s.ScanDetail?.Instructions, location:s.ScanDetail?.ScannedLocation, date:s.ScanDetail?.ScanDateTime })) });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

app.post('/api/delhivery/create-pickup', async (req,res) => {
  const { order_id, order_number, customer_name, customer_phone, customer_address, customer_city, customer_state, customer_pincode, products_desc, total_amount, quantity, weight } = req.body;
  if (!DELHIVERY_TOKEN) return res.status(400).json({ error:'Delhivery not configured' });
  if (!customer_pincode) return res.status(400).json({ error:'Customer pincode required' });
  const rvpOrderId = `RVP-${order_number}-${Date.now().toString().slice(-6)}`;
  const payload = {
    pickup_location: { name:DELHIVERY_WAREHOUSE },
    shipments: [{ name:customer_name||'Customer', add:customer_address||'', pin:String(customer_pincode), city:customer_city||'', state:customer_state||'', country:'India', phone:String(customer_phone||'').replace(/\D/g,'').slice(-10), order:rvpOrderId, payment_mode:'Pickup', products_desc:products_desc||'Return Shipment', hsn_code:'62034200', cod_amount:'0', order_date:new Date().toISOString().split('T')[0], total_amount:String(total_amount||'0'), seller_name:DELHIVERY_WAREHOUSE, seller_inv:`INV-${order_number}`, quantity:parseInt(quantity)||1, weight:parseFloat(weight)||0.5, shipment_length:15, shipment_width:12, shipment_height:10 }]
  };
  try {
    const data = await delhiveryAPI('POST','/api/cmu/create.json',payload,true);
    const pkg = data?.packages?.[0];
    const waybill = pkg?.waybill||data?.waybill;
    const success = !!(waybill||pkg?.status==='Success'||data?.success);
    if (success && waybill) {
      const currentOrder = await shopifyREST('GET',`orders/${order_id}.json?fields=note,tags`);
      const existingNote = currentOrder?.order?.note||'';
      const newNote = existingNote+`\n\nDELHIVERY RVP\nAWB: ${waybill}\nRVP Order: ${rvpOrderId}\nCreated: ${new Date().toISOString()}`;
      await updateOrderTags(order_id,['pickup-scheduled'],[]);
      await shopifyREST('PUT',`orders/${order_id}.json`,{ order:{ id:order_id, note:newNote } });
      auditLog(order_id,'delhivery_pickup_created','merchant',`AWB: ${waybill}`);
    }
    res.json({ success, waybill, rvp_order_id:rvpOrderId, raw:data });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ════════════════════════════════════════════
// SHOPIFY WEBHOOK — order fulfilled → start window
// ════════════════════════════════════════════
app.post('/webhooks/order-fulfilled', express.raw({type:'application/json'}), (req,res) => {
  try {
    const body = JSON.parse(req.body.toString());
    auditLog(body.id,'return_window_started','webhook',`Order #${body.order_number} fulfilled — ${RETURN_WINDOW_DAYS}-day window started`);
    res.sendStatus(200);
  } catch(e) { res.sendStatus(200); }
});

const PORT = process.env.PORT||3000;
app.listen(PORT, () => console.log(`Returns Manager v2 on port ${PORT} | Connected: ${!!ACCESS_TOKEN}`));
