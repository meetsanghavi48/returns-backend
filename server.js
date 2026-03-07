'use strict';
const express = require('express');
const cors    = require('cors');
const fetch   = require('node-fetch');
const path    = require('path');
const fs      = require('fs');

const app = express();
app.use(cors({ origin:'*', methods:['GET','POST','PUT','DELETE','OPTIONS'], allowedHeaders:['Content-Type','Authorization','X-Shopify-Access-Token'] }));
app.use(express.json({ limit:'10mb' }));
app.options('*', cors());
app.use((_req,res,next)=>{ res.setHeader('Access-Control-Allow-Origin','*'); next(); });

// ══════════════════════════════════════════
// 1. PERSISTENCE  (must be defined FIRST)
// ══════════════════════════════════════════
const DATA_DIR = fs.existsSync('/data') ? '/data' : path.join(__dirname, '.data');
if (!fs.existsSync(DATA_DIR)) { try { fs.mkdirSync(DATA_DIR, { recursive:true }); } catch(e) {} }
console.log('[Store] dataDir:', DATA_DIR);

function dataPath(name) { return path.join(DATA_DIR, name + '.json'); }

function loadJSON(name, fallback) {
  try {
    const p = dataPath(name);
    if (fs.existsSync(p)) return JSON.parse(fs.readFileSync(p, 'utf8'));
  } catch(e) { console.error('[loadJSON]', name, e.message); }
  return fallback;
}

function saveJSON(name, data) {
  try {
    const p = dataPath(name);
    const tmp = p + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf8');
    fs.renameSync(tmp, p);
  } catch(e) { console.error('[saveJSON]', name, e.message); }
}

const _deferTimers = {};
function saveDeferred(name, data, ms) {
  ms = ms || 2000;
  clearTimeout(_deferTimers[name]);
  _deferTimers[name] = setTimeout(() => saveJSON(name, data), ms);
}

// ══════════════════════════════════════════
// 2. CONFIG  (after persistence is ready)
// ══════════════════════════════════════════
const SHOPIFY_API_KEY    = process.env.SHOPIFY_API_KEY    || 'c1542c4ed17151e558edc3f37ceb9fd2';
const SHOPIFY_API_SECRET = process.env.SHOPIFY_API_SECRET || '';
const BACKEND_URL        = process.env.BACKEND_URL        || 'https://returns-backend.onrender.com';
const SCOPES = 'read_orders,write_orders,read_products,write_products,read_draft_orders,write_draft_orders,read_customers,write_customers,read_inventory,write_inventory';

const _auth = loadJSON('auth', {});
let ACCESS_TOKEN = process.env.ACCESS_TOKEN || _auth.access_token || '';
let SHOP_DOMAIN  = process.env.SHOP_DOMAIN  || _auth.shop_domain  || '';

const _delv = loadJSON('delhivery', {});
let DELHIVERY_TOKEN     = process.env.DELHIVERY_TOKEN     || _delv.token     || '';
let DELHIVERY_WAREHOUSE = process.env.DELHIVERY_WAREHOUSE || _delv.warehouse || '';
let DELHIVERY_MODE      = process.env.DELHIVERY_MODE      || _delv.mode      || 'staging';

const _settings = loadJSON('settings', {});
let RETURN_WINDOW_DAYS = (_settings.return_window_days !== undefined) ? _settings.return_window_days : (parseInt(process.env.RETURN_WINDOW_DAYS) || 30);
let STORE_CREDIT_BONUS = (_settings.store_credit_bonus !== undefined) ? _settings.store_credit_bonus : (parseFloat(process.env.STORE_CREDIT_BONUS) || 10);
let RESTOCKING_FEE_PCT = (_settings.restocking_fee_pct !== undefined) ? _settings.restocking_fee_pct : (parseFloat(process.env.RESTOCKING_FEE_PCT) || 0);
let WAREHOUSE_CONFIG   = _settings.warehouse || {
  name:    process.env.WAREHOUSE_NAME    || '',
  address: process.env.WAREHOUSE_ADDRESS || '',
  city:    process.env.WAREHOUSE_CITY    || '',
  state:   process.env.WAREHOUSE_STATE   || '',
  pincode: process.env.WAREHOUSE_PINCODE || '',
  phone:   process.env.WAREHOUSE_PHONE   || ''
};

// ══════════════════════════════════════════
// 3. IN-MEMORY STORES  (loaded from disk)
// ══════════════════════════════════════════
const DEFAULT_RULES = [
  { id:'rule_1', name:'Auto-Approve — Size / Fit Issues', enabled:false, priority:1, match_mode:'all', category:'on_submit',
    conditions:[{field:'return_reason',op:'contains',value:'size'}],
    action:'auto_approve', action_params:{message:'Your return has been approved! We will arrange pickup shortly.'} },
  { id:'rule_2', name:'Auto-Approve — Wrong or Damaged Item', enabled:false, priority:2, match_mode:'any', category:'on_submit',
    conditions:[{field:'return_reason',op:'contains',value:'wrong'},{field:'return_reason',op:'contains',value:'damage'},{field:'return_reason',op:'contains',value:'defect'}],
    action:'auto_approve', action_params:{message:"We're sorry about that! Your return is auto-approved."} },
  { id:'rule_3', name:'Keep It — Low Value Items (under ₹300)', enabled:false, priority:3, match_mode:'all', category:'on_submit',
    conditions:[{field:'product_price',op:'lt',value:'300'}],
    action:'keep_it', action_params:{message:'No need to return! Keep or donate the item. Your refund is on its way.'} },
  { id:'rule_4', name:'Flag — High Value Order (above ₹5000)', enabled:false, priority:4, match_mode:'all', category:'on_submit',
    conditions:[{field:'order_value',op:'gt',value:'5000'}],
    action:'flag_review', action_params:{note:'High-value order — verify before approving'} },
  { id:'rule_5', name:'Flag — Serial Returner (3+ returns)', enabled:false, priority:5, match_mode:'all', category:'on_submit',
    conditions:[{field:'customer_return_count',op:'gte',value:'3'}],
    action:'flag_review', action_params:{note:'Serial returner — review carefully'} },
  { id:'rule_6', name:'Auto-Reject — Outside Return Window', enabled:false, priority:6, match_mode:'all', category:'on_submit',
    conditions:[{field:'days_since_order',op:'gt',value:'30'}],
    action:'auto_reject', action_params:{message:'This request is outside our 30-day return window and cannot be processed.'} },
  { id:'rule_7', name:'Auto-Refund — On Warehouse Delivery', enabled:false, priority:7, match_mode:'all', category:'on_carrier',
    conditions:[{field:'carrier_event',op:'eq',value:'delivered'},{field:'request_type',op:'eq',value:'return'}],
    action:'auto_refund', action_params:{} },
  { id:'rule_8', name:'Auto-Exchange — On Pickup Scan', enabled:false, priority:8, match_mode:'all', category:'on_carrier',
    conditions:[{field:'carrier_event',op:'eq',value:'pickup_scan'},{field:'request_type',op:'eq',value:'exchange'}],
    action:'auto_exchange', action_params:{} },
  { id:'rule_9', name:'COD Returns — Auto-Approve', enabled:false, priority:9, match_mode:'all', category:'on_submit',
    conditions:[{field:'payment_method',op:'eq',value:'cod'},{field:'request_type',op:'eq',value:'return'}],
    action:'auto_approve', action_params:{message:'COD return approved. Store credit will be issued on receipt.'} },
  { id:'rule_10', name:'Flag — Large Return (3+ items)', enabled:false, priority:10, match_mode:'all', category:'on_submit',
    conditions:[{field:'item_count',op:'gte',value:'3'}],
    action:'flag_review', action_params:{note:'Large return quantity — inspect all items before refunding'} },
];

let RULES           = loadJSON('rules', null) || DEFAULT_RULES;
let RETURN_REQUESTS = loadJSON('requests', {});
let ANALYTICS       = loadJSON('analytics', { total_requests:0, approved:0, rejected:0, auto_approved:0, refunded_amount:0, store_credits_issued:0, exchanges_created:0, reasons:{}, products_returned:{} });
let AUDIT_LOG       = [];

console.log(`[Boot] Token:${!!ACCESS_TOKEN} Shop:${SHOP_DOMAIN||'none'} Rules:${RULES.length} Requests:${Object.keys(RETURN_REQUESTS).length}`);

// ══════════════════════════════════════════
// 4. HELPERS
// ══════════════════════════════════════════
function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2,7); }
function gidToId(gid) { return typeof gid === 'string' ? gid.replace(/^gid:\/\/shopify\/\w+\//,'') : String(gid||''); }

function auditLog(order_id, action, actor, details) {
  const e = { id:uid(), timestamp:new Date().toISOString(), order_id:String(order_id), action, actor:actor||'system', details:details||'' };
  AUDIT_LOG.unshift(e);
  if (AUDIT_LOG.length > 2000) AUDIT_LOG = AUDIT_LOG.slice(0, 2000);
  console.log(`[AUDIT] #${order_id} | ${action} | ${actor} | ${details}`);
  return e;
}

async function shopifyREST(method, endpoint, body) {
  if (!ACCESS_TOKEN || !SHOP_DOMAIN) throw new Error('Not authenticated — set ACCESS_TOKEN and SHOP_DOMAIN env vars');
  const opts = { method, headers:{ 'X-Shopify-Access-Token':ACCESS_TOKEN, 'Content-Type':'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const r    = await fetch(`https://${SHOP_DOMAIN}/admin/api/2024-01/${endpoint}`, opts);
  const text = await r.text();
  console.log(`[Shopify] ${method} ${endpoint} → ${r.status}`);
  if (!r.ok) console.error('[Shopify ERR]', text.slice(0,500));
  try { return JSON.parse(text); } catch(e) { return { error: text }; }
}

async function graphql(query, variables) {
  if (!ACCESS_TOKEN || !SHOP_DOMAIN) throw new Error('Not authenticated');
  const r = await fetch(`https://${SHOP_DOMAIN}/admin/api/2024-01/graphql.json`, {
    method:'POST',
    headers:{ 'X-Shopify-Access-Token':ACCESS_TOKEN, 'Content-Type':'application/json' },
    body: JSON.stringify({ query, variables })
  });
  return r.json();
}

async function updateOrderTags(order_id, addTags, removeTags) {
  removeTags = removeTags || [];
  const d  = await shopifyREST('GET', `orders/${order_id}.json?fields=tags`);
  let tags = (d?.order?.tags || '').split(',').map(t=>t.trim()).filter(Boolean);
  tags = tags.filter(t => !removeTags.includes(t));
  addTags.forEach(t => { if (!tags.includes(t)) tags.push(t); });
  await shopifyREST('PUT', `orders/${order_id}.json`, { order:{ id:order_id, tags:tags.join(', ') } });
  return tags;
}

// ── Rules Engine ──
function getFieldValue(reqData, field) {
  switch(field) {
    case 'return_reason':         return (reqData.items||[]).map(i=>(i.reason||'')).join(' ').toLowerCase();
    case 'product_price':         return parseFloat((reqData.items||[])[0]?.price || 0);
    case 'order_value':           return parseFloat(reqData.total_price || 0);
    case 'shipping_cost':         return parseFloat(reqData.shipping_cost || 0);
    case 'customer_return_count': return parseInt(reqData.customer_return_count || 0);
    case 'request_type':          return (reqData.request_type || 'return').toLowerCase();
    case 'request_stage':         return (reqData.status || 'pending').toLowerCase();
    case 'carrier_event':         return (reqData.carrier_event || '').toLowerCase();
    case 'refund_method':         return (reqData.refund_method || '').toLowerCase();
    case 'payment_method':        return reqData.is_cod ? 'cod' : 'prepaid';
    case 'order_tags':            return ((reqData.tags||[]).join(',')).toLowerCase();
    case 'days_since_order':      return reqData.days_since_order || 0;
    case 'item_count':            return (reqData.items||[]).length;
    default: return '';
  }
}

function evalCond(val, op, target) {
  const n = parseFloat(val), t = parseFloat(target);
  switch(op) {
    case 'eq':       return String(val).toLowerCase() === String(target).toLowerCase();
    case 'neq':      return String(val).toLowerCase() !== String(target).toLowerCase();
    case 'contains': return String(val).toLowerCase().includes(String(target).toLowerCase());
    case 'gt':       return !isNaN(n) && !isNaN(t) && n > t;
    case 'lt':       return !isNaN(n) && !isNaN(t) && n < t;
    case 'gte':      return !isNaN(n) && !isNaN(t) && n >= t;
    case 'lte':      return !isNaN(n) && !isNaN(t) && n <= t;
    default: return false;
  }
}

function runRulesEngine(reqData) {
  const active = [...RULES].filter(r => r.enabled).sort((a,b) => a.priority - b.priority);
  for (const rule of active) {
    const mode  = rule.match_mode || 'all';
    const match = mode === 'any'
      ? rule.conditions.some(c  => evalCond(getFieldValue(reqData, c.field), c.op, c.value))
      : rule.conditions.every(c => evalCond(getFieldValue(reqData, c.field), c.op, c.value));
    if (match) {
      console.log(`[Rules] MATCH: "${rule.name}" → action:${rule.action}`);
      return { rule_id:rule.id, rule_name:rule.name, action:rule.action, params:rule.action_params || {} };
    }
  }
  return null;
}

// ── Delhivery ──
function delhiveryBase() { return DELHIVERY_MODE === 'production' ? 'https://track.delhivery.com' : 'https://staging-express.delhivery.com'; }
async function delhiveryAPI(method, urlPath, body, isForm) {
  if (!DELHIVERY_TOKEN) throw new Error('Delhivery token not configured');
  const headers = { 'Authorization':`Token ${DELHIVERY_TOKEN}` };
  const opts    = { method:method||'GET', headers };
  if (body) {
    if (isForm) { headers['Content-Type']='application/x-www-form-urlencoded'; opts.body=`format=json&data=${encodeURIComponent(JSON.stringify(body))}`; }
    else        { headers['Content-Type']='application/json'; opts.body=JSON.stringify(body); }
  }
  const r    = await fetch(delhiveryBase() + urlPath, opts);
  const text = await r.text();
  try { return JSON.parse(text); } catch(e) { return { raw:text }; }
}

// ══════════════════════════════════════════
// 5. STATIC FRONTEND
// ══════════════════════════════════════════
app.get('/dashboard', (_req,res) => {
  const f = path.join(__dirname, 'index.html');
  fs.existsSync(f) ? res.sendFile(f) : res.send('<h2>Upload index.html next to server.js</h2>');
});
app.get('/portal', (_req,res) => {
  const f = path.join(__dirname, 'portal.html');
  fs.existsSync(f) ? res.sendFile(f) : res.send('<h2>Upload portal.html next to server.js</h2>');
});
app.get('/', (_req,res) => res.redirect('/dashboard'));

// ══════════════════════════════════════════
// 6. AUTH
// ══════════════════════════════════════════
app.get('/api/status', (_req,res) => res.json({ connected:!!ACCESS_TOKEN, shop:SHOP_DOMAIN||null, return_window:RETURN_WINDOW_DAYS, store_credit_bonus:STORE_CREDIT_BONUS }));

app.get('/auth', (req,res) => {
  const shop = req.query.shop || SHOP_DOMAIN;
  if (!shop) return res.status(400).send('Missing ?shop=yourstore.myshopify.com');
  res.redirect(`https://${shop}/admin/oauth/authorize?client_id=${SHOPIFY_API_KEY}&scope=${SCOPES}&redirect_uri=${BACKEND_URL}/auth/callback&state=rms`);
});

app.get('/auth/callback', async (req,res) => {
  const { shop, code } = req.query;
  if (!shop || !code) return res.status(400).send('Missing params');
  try {
    const r = await fetch(`https://${shop}/admin/oauth/access_token`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ client_id:SHOPIFY_API_KEY, client_secret:SHOPIFY_API_SECRET, code })
    });
    const d = await r.json();
    if (d.access_token) {
      ACCESS_TOKEN = d.access_token; SHOP_DOMAIN = shop;
      saveJSON('auth', { access_token:ACCESS_TOKEN, shop_domain:SHOP_DOMAIN });
      auditLog('system','auth_connected','system',`Connected: ${shop}`);
      res.redirect('/dashboard');
    } else { res.status(400).json(d); }
  } catch(e) { res.status(500).send(e.message); }
});

// ══════════════════════════════════════════
// 7. ORDERS  (dashboard list)
// ══════════════════════════════════════════
app.get('/api/orders', async (_req,res) => {
  try {
    const result = await graphql(`{
      orders(first:250,reverse:true){
        edges{node{
          id name createdAt tags note
          displayFinancialStatus displayFulfillmentStatus
          totalPriceSet{shopMoney{amount currencyCode}}
          email phone
          customer{id displayName email phone}
          shippingAddress{name firstName lastName address1 address2 city province zip country phone}
          lineItems(first:20){edges{node{
            id title quantity
            originalUnitPriceSet{shopMoney{amount}}
            variant{id image{url}}
            product{id images(first:1){edges{node{url}}}}
          }}}
        }}
      }
    }`);
    if (result.errors) return res.status(400).json({ error:result.errors[0].message });

    const orders = result.data.orders.edges.map(({ node:o }) => {
      const allTags = Array.isArray(o.tags) ? o.tags : (o.tags||'').split(',').map(t=>t.trim()).filter(Boolean);
      const has    = t => allTags.includes(t);
      const hasAny = arr => arr.some(t => allTags.includes(t));
      const rs =
        has('return-refunded')    ? 'refunded' :
        has('exchange-fulfilled') ? 'fulfilled' :
        (has('return-inspected')||has('exchange-inspected')) ? 'inspected' :
        (has('pickup-scheduled')||has('pickup-scan'))        ? 'received'  :
        hasAny(['return-approved','exchange-approved','mixed-approved'])   ? 'approved' :
        hasAny(['return-rejected','exchange-rejected','mixed-rejected'])   ? 'rejected' :
        has('return-requested')   ? 'pending' :
        has('exchange-requested') ? 'exchange-pending' :
        has('mixed-requested')    ? 'pending' : null;
      const rt =
        hasAny(['exchange-requested','exchange-approved','exchange-fulfilled']) ? 'exchange' :
        hasAny(['mixed-requested','mixed-approved']) ? 'mixed' : 'return';
      const oid = gidToId(o.id);
      const sa  = o.shippingAddress;
      return {
        id:oid, gid:o.id,
        order_number: o.name.replace('#',''),
        created_at:   o.createdAt,
        financial_status:   (o.displayFinancialStatus||'').toLowerCase(),
        fulfillment_status: (o.displayFulfillmentStatus||'').toLowerCase(),
        total_price: o.totalPriceSet.shopMoney.amount,
        currency:    o.totalPriceSet.shopMoney.currencyCode,
        tags:allTags, note:o.note||'',
        customer_name:  o.customer?.displayName || (sa?([sa.name,sa.firstName&&sa.lastName?sa.firstName+' '+sa.lastName:null].filter(Boolean)[0]):'') || '',
        customer_email: o.customer?.email || o.email || '',
        customer_phone: o.customer?.phone || o.phone || sa?.phone || '',
        customer_id:    o.customer ? gidToId(o.customer.id) : null,
        shipping_address: sa ? {
          name:     sa.name || [sa.firstName,sa.lastName].filter(Boolean).join(' '),
          address1: sa.address1||'', address2:sa.address2||'',
          city:sa.city||'', province:sa.province||'', zip:sa.zip||'',
          country:sa.country||'', phone:sa.phone||''
        } : null,
        line_items: o.lineItems.edges.map(({ node:li }) => ({
          id:        gidToId(li.id), gid:li.id,
          title:     li.title, quantity:li.quantity,
          price:     li.originalUnitPriceSet?.shopMoney?.amount||'0',
          variant_id:li.variant?.id ? gidToId(li.variant.id) : null,
          image_url: li.variant?.image?.url || li.product?.images?.edges?.[0]?.node?.url || null,
          product_id:li.product?.id ? gidToId(li.product.id) : null
        })),
        return_status:rs, request_type:rt,
        requests: RETURN_REQUESTS[oid] || []
      };
    });
    res.json({ orders, return_requests:orders.filter(o=>o.return_status).length });
  } catch(e) { console.error('[/api/orders]',e); res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// 8. PORTAL LOOKUP
// ══════════════════════════════════════════
app.get('/api/lookup', async (req,res) => {
  const { order_number, contact } = req.query;
  if (!order_number) return res.status(400).json({ error:'Missing order_number' });
  try {
    const clean = String(order_number).replace(/^#+/,'').trim();
    const data  = await shopifyREST('GET',
      `orders.json?name=%23${clean}&status=any&fields=id,order_number,created_at,financial_status,fulfillment_status,total_price,currency,line_items,tags,note,email,phone,customer,shipping_address,billing_address,payment_gateway`
    );
    if (!data.orders?.length) return res.json({ found:false });
    const o = data.orders[0];

    // Contact verification
    if (contact && contact.trim()) {
      const inp        = contact.trim();
      const inpDigits  = inp.replace(/\D/g,'');
      const oEmail     = (o.email || '').toLowerCase();
      const oCPhone    = (o.customer?.phone||'').replace(/\D/g,'');
      const oSPhone    = (o.shipping_address?.phone||'').replace(/\D/g,'');
      const emailOk    = oEmail && oEmail.includes(inp.toLowerCase());
      const phoneOk    = inpDigits.length >= 6 && (
        (oCPhone && oCPhone.slice(-8) === inpDigits.slice(-8)) ||
        (oSPhone && oSPhone.slice(-8) === inpDigits.slice(-8))
      );
      if (!emailOk && !phoneOk) return res.json({ found:false, mismatch:true });
    }

    const orderDate = new Date(o.created_at);
    const daysDiff  = (Date.now() - orderDate) / (1000*60*60*24);
    const deadline  = new Date(orderDate.getTime() + RETURN_WINDOW_DAYS*24*60*60*1000).toISOString();

    // Fetch product images + variants
    const pids = [...new Set((o.line_items||[]).map(li=>li.product_id).filter(Boolean))].join(',');
    let prodCache = {};
    if (pids) {
      const pd = await shopifyREST('GET', `products.json?ids=${pids}&fields=id,images,options,variants,tags&limit=20`);
      (pd.products||[]).forEach(p => {
        prodCache[p.id] = {
          image:         p.images?.[0]?.src || null,
          options:       p.options || [],
          variants:      p.variants || [],
          non_returnable:(p.tags||'').includes('non-returnable')
        };
      });
    }

    const tagArr = (o.tags||'').split(',').map(t=>t.trim()).filter(Boolean);
    const has    = t => tagArr.includes(t);
    const return_status =
      has('return-refunded')    ? 'refunded' :
      has('exchange-fulfilled') ? 'fulfilled' :
      (has('return-inspected')||has('exchange-inspected')) ? 'inspected' :
      (has('pickup-scheduled')||has('pickup-scan'))        ? 'received'  :
      (has('return-approved')||has('exchange-approved')||has('mixed-approved'))   ? 'approved' :
      (has('return-rejected')||has('exchange-rejected')||has('mixed-rejected'))   ? 'rejected' :
      has('return-requested')   ? 'pending' :
      has('exchange-requested') ? 'exchange-pending' :
      has('mixed-requested')    ? 'pending' : null;
    const request_type =
      (has('exchange-requested')||has('exchange-approved')||has('exchange-fulfilled')) ? 'exchange' :
      (has('mixed-requested')||has('mixed-approved')) ? 'mixed' : 'return';

    // Normalize address — handle all Shopify field variants
    const sa = o.shipping_address || o.billing_address || null;
    const address = sa ? {
      name:     sa.name || [sa.first_name,sa.last_name].filter(Boolean).join(' ') || (o.customer ? `${o.customer.first_name||''} ${o.customer.last_name||''}`.trim() : '') || '',
      address1: sa.address1 || '',
      address2: sa.address2 || '',
      city:     sa.city     || '',
      province: sa.province || '',
      zip:      sa.zip      || '',
      country:  sa.country  || 'India',
      phone:    sa.phone || o.phone || o.customer?.phone || ''
    } : null;

    const is_cod = ['cash_on_delivery','cod','manual'].includes((o.payment_gateway||'').toLowerCase());

    res.json({
      found:true,
      order:{
        id:o.id, order_number:o.order_number,
        created_at:o.created_at,
        financial_status:o.financial_status, fulfillment_status:o.fulfillment_status,
        total_price:o.total_price, currency:o.currency,
        has_return:!!return_status, return_status, request_type,
        note:o.note, return_deadline:deadline,
        within_window:daysDiff <= RETURN_WINDOW_DAYS,
        days_remaining:Math.max(0, RETURN_WINDOW_DAYS - Math.floor(daysDiff)),
        address,
        store_credit_bonus:STORE_CREDIT_BONUS,
        customer_name:  o.customer ? `${o.customer.first_name||''} ${o.customer.last_name||''}`.trim() : sa?.name || '',
        customer_email: o.email || o.customer?.email || '',
        customer_phone: o.phone || sa?.phone || o.customer?.phone || '',
        payment_gateway:o.payment_gateway||'',
        is_cod,
        requests: RETURN_REQUESTS[String(o.id)] || [],
        line_items:(o.line_items||[]).map(li => ({
          id:li.id, title:li.title, variant_title:li.variant_title||'',
          variant_id:li.variant_id, product_id:li.product_id,
          quantity:li.quantity, price:li.price,
          fulfillment_status:li.fulfillment_status,
          image_url:    prodCache[li.product_id]?.image || null,
          non_returnable:prodCache[li.product_id]?.non_returnable || false,
          product_options: prodCache[li.product_id]?.options || [],
          product_variants:prodCache[li.product_id]?.variants || []
        })).filter(li => li.fulfillment_status==='fulfilled' || o.fulfillment_status==='fulfilled')
      }
    });
  } catch(e) { console.error('[/api/lookup]',e); res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// 9. ORDER DETAILS  (dashboard modal)
// ══════════════════════════════════════════
app.get('/api/orders/:order_id/details', async (req,res) => {
  try {
    const d = await shopifyREST('GET', `orders/${req.params.order_id}.json?fields=id,order_number,email,phone,customer,shipping_address,billing_address,line_items,note,tags,total_price,financial_status,payment_gateway`);
    const o = d?.order;
    if (!o) return res.status(404).json({ error:'Order not found' });
    const sa = o.shipping_address || o.billing_address || null;
    const address = sa ? {
      name:     sa.name || (o.customer ? `${o.customer.first_name||''} ${o.customer.last_name||''}`.trim() : ''),
      address1: sa.address1||'', address2:sa.address2||'',
      city:sa.city||'', province:sa.province||'', zip:sa.zip||'',
      country:sa.country||'India', phone:sa.phone||o.phone||o.customer?.phone||''
    } : null;
    res.json({
      id:o.id, order_number:o.order_number,
      customer_name:  o.customer ? `${o.customer.first_name||''} ${o.customer.last_name||''}`.trim() : sa?.name||'',
      customer_email: o.email || o.customer?.email || '',
      customer_phone: o.phone || o.customer?.phone || sa?.phone || '',
      customer_id:    o.customer?.id || null,
      payment_gateway:o.payment_gateway||'',
      is_cod:['cash_on_delivery','cod','manual'].includes((o.payment_gateway||'').toLowerCase()),
      address,
      line_items:(o.line_items||[]).map(li=>({ id:li.id, title:li.title, variant_id:li.variant_id, variant_title:li.variant_title||'', quantity:li.quantity, price:li.price, sku:li.sku||'' })),
      total_price:o.total_price, financial_status:o.financial_status,
      note:o.note, tags:o.tags,
      requests: RETURN_REQUESTS[String(o.id)] || []
    });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// 10. PRODUCTS
// ══════════════════════════════════════════
app.get('/api/products/featured', async (_req,res) => {
  try {
    const d = await shopifyREST('GET','products.json?fields=id,title,images,variants&limit=8');
    res.json({ products:(d.products||[]).map(p=>({ id:p.id, title:p.title, image:p.images?.[0]?.src||null, price:p.variants?.[0]?.price||'0' })) });
  } catch(e) { res.json({ products:[] }); }
});
app.get('/api/products/search', async (req,res) => {
  const { q } = req.query;
  if (!q) return res.json({ products:[] });
  try {
    const d = await shopifyREST('GET',`products.json?title=${encodeURIComponent(q)}&fields=id,title,images,variants,options&limit=8`);
    res.json({ products:(d.products||[]).map(p=>({ id:p.id, title:p.title, image:p.images?.[0]?.src||null, price:p.variants?.[0]?.price||'0', options:p.options||[], variants:(p.variants||[]).map(v=>({ id:v.id, title:v.title, option1:v.option1, option2:v.option2, option3:v.option3, price:v.price, available:(v.inventory_quantity||1)>0 })) })) });
  } catch(e) { res.json({ products:[] }); }
});

// ══════════════════════════════════════════
// 11. SUBMIT RETURN REQUEST
// ══════════════════════════════════════════
app.post('/api/returns/request', async (req,res) => {
  const { order_id, order_number, items, refund_method, customer_note, address, shipping_preference } = req.body;
  if (!order_id || !items?.length) return res.status(400).json({ error:'Missing required fields' });
  try {
    const returns   = items.filter(i=>i.action==='return');
    const exchanges = items.filter(i=>i.action==='exchange');
    const hasBoth   = returns.length > 0 && exchanges.length > 0;
    const request_type = hasBoth ? 'mixed' : exchanges.length ? 'exchange' : 'return';
    const total_price  = items.reduce((s,i)=>s+parseFloat(i.price||0)*(i.qty||1), 0);

    // Get real order data for rules (days_since_order, payment method etc)
    const fresh = await shopifyREST('GET', `orders/${order_id}.json?fields=created_at,tags,note,payment_gateway,customer`);
    const fo    = fresh?.order || {};
    const days_since_order = fo.created_at
      ? Math.floor((Date.now() - new Date(fo.created_at)) / (1000*60*60*24))
      : 0;
    const is_cod = ['cash_on_delivery','cod','manual'].includes((fo.payment_gateway||'').toLowerCase());
    const existTags  = (fo.tags||'').split(',').map(t=>t.trim()).filter(Boolean);
    const existNote  = fo.note || '';
    const existingReqs = RETURN_REQUESTS[String(order_id)] || [];

    // Build rule input
    const ruleInput = {
      order_id, order_number, items, refund_method, total_price,
      request_type, days_since_order, is_cod,
      customer_return_count: existingReqs.length,
      shipping_preference, tags: existTags
    };

    // Run rules engine
    const matched = runRulesEngine(ruleInput);
    if (matched) auditLog(order_id, `rule_match:${matched.action}`, 'rules_engine', `"${matched.rule_name}"`);

    // Determine initial status + tags
    const baseTag = hasBoth ? 'mixed-requested' : exchanges.length ? 'exchange-requested' : 'return-requested';
    let addTags       = [baseTag];
    let initialStatus = 'pending';

    if (matched?.action === 'auto_approve') {
      addTags = [baseTag.replace('-requested','-approved')];
      initialStatus = 'approved';
      ANALYTICS.auto_approved++;
    } else if (matched?.action === 'auto_reject') {
      addTags = [baseTag.replace('-requested','-rejected')];
      initialStatus = 'rejected';
      ANALYTICS.rejected++;
    } else if (matched?.action === 'keep_it') {
      addTags = ['return-approved','keep-it-rule'];
      initialStatus = 'keep_it';
    } else if (matched?.action === 'flag_review') {
      addTags = [baseTag, 'flagged-review'];
    }

    // Build note
    const req_id  = `REQ-${order_number}-${uid()}`;
    const reqNum  = existingReqs.length + 1;
    const itemLines = items.map(i => {
      let l = `[${i.action.toUpperCase()}] ${i.title}${i.variant_title?' - '+i.variant_title:''} x${i.qty||1}`;
      if (i.reason) l += ` | Reason: ${i.reason}`;
      if (i.action==='exchange' && i.exchange_variant_id) l += ` | ExchVarID: ${i.exchange_variant_id}`;
      if (i.action==='exchange' && i.exchange_variant_title) l += ` | ExchFor: ${i.exchange_variant_title}`;
      return l;
    }).join('\n');

    const noteBlock =
`\n---REQUEST ${reqNum} (${req_id})---\n` +
`Type: ${request_type.toUpperCase()} | Status: ${initialStatus}\n` +
`Items:\n${itemLines}\n` +
`Refund: ${refund_method||'store_credit'} | Shipping: ${shipping_preference||'pickup'}\n` +
(customer_note ? `Note: ${customer_note}\n` : '') +
(address ? `Pickup: ${[address.name,address.address1,address.city,address.zip].filter(Boolean).join(', ')}\n` : '') +
`Rule: ${matched ? matched.rule_name+' → '+matched.action : 'none (manual review)'}\n` +
`Date: ${new Date().toISOString()}\n` +
`---END ${reqNum}---`;

    const newNote = (existNote + noteBlock).slice(0, 5000);
    const newTags = [...new Set([...existTags, ...addTags])];

    const upd = await shopifyREST('PUT', `orders/${order_id}.json`, { order:{ id:order_id, tags:newTags.join(', '), note:newNote } });
    if (!upd.order) return res.status(400).json({ error:'Failed to update order on Shopify', detail:upd });

    // Save request to memory
    if (!RETURN_REQUESTS[String(order_id)]) RETURN_REQUESTS[String(order_id)] = [];
    const reqRecord = {
      req_id, req_num:reqNum, order_id, order_number, items, refund_method,
      total_price, request_type, shipping_preference,
      days_since_order, is_cod, status:initialStatus,
      submitted_at: new Date().toISOString(),
      auto_action:  matched?.action || null,
      address:      address || null,
      awb:null, awb_final:false, last_carrier_status:''
    };
    RETURN_REQUESTS[String(order_id)].push(reqRecord);

    ANALYTICS.total_requests++;
    items.forEach(i => {
      if (i.reason) ANALYTICS.reasons[i.reason] = (ANALYTICS.reasons[i.reason]||0)+1;
      if (i.title)  ANALYTICS.products_returned[i.title] = (ANALYTICS.products_returned[i.title]||0)+1;
    });
    saveDeferred('requests',  RETURN_REQUESTS);
    saveDeferred('analytics', ANALYTICS);

    auditLog(order_id,'request_submitted','customer',`${request_type} | ${items.length} item(s) | auto:${matched?.action||'none'}`);

    res.json({
      success:true, req_id, req_num:reqNum,
      type:request_type, status:initialStatus,
      auto_action:    matched?.action || null,
      auto_message:   matched?.params?.message || null,
      keep_it_message:matched?.action==='keep_it' ? matched.params?.message : null
    });
  } catch(e) { console.error('[/api/returns/request]',e); res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// 12. APPROVE / REJECT / INSPECT
// ══════════════════════════════════════════
app.post('/api/returns/:order_id/approve', async (req,res) => {
  const { order_id } = req.params;
  const { type, actor, req_id } = req.body;
  try {
    const at = type==='exchange'?'exchange-approved':type==='mixed'?'mixed-approved':'return-approved';
    const rt = type==='exchange'?'exchange-requested':type==='mixed'?'mixed-requested':'return-requested';
    await updateOrderTags(order_id,[at],[rt]);
    const reqs = RETURN_REQUESTS[String(order_id)] || [];
    const r    = req_id ? reqs.find(r=>r.req_id===req_id) : reqs[reqs.length-1];
    if (r) r.status = 'approved'; else reqs.forEach(r=>r.status='approved');
    ANALYTICS.approved++;
    saveDeferred('requests',RETURN_REQUESTS);
    saveDeferred('analytics',ANALYTICS);
    auditLog(order_id,'approved',actor||'merchant',`${type} approved`);
    res.json({ success:true });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

app.post('/api/returns/:order_id/reject', async (req,res) => {
  const { order_id } = req.params;
  const { type, reason, actor, req_id } = req.body;
  try {
    const rt  = type==='exchange'?'exchange-rejected':type==='mixed'?'mixed-rejected':'return-rejected';
    const rem = type==='exchange'?'exchange-requested':type==='mixed'?'mixed-requested':'return-requested';
    await updateOrderTags(order_id,[rt],[rem]);
    const reqs = RETURN_REQUESTS[String(order_id)] || [];
    const r    = req_id ? reqs.find(r=>r.req_id===req_id) : reqs[reqs.length-1];
    if (r) r.status = 'rejected';
    ANALYTICS.rejected++;
    saveDeferred('requests',RETURN_REQUESTS);
    saveDeferred('analytics',ANALYTICS);
    auditLog(order_id,'rejected',actor||'merchant',reason||'');
    res.json({ success:true });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

app.post('/api/returns/:order_id/inspect', async (req,res) => {
  const { order_id } = req.params;
  try {
    await updateOrderTags(order_id,['return-inspected'],[]);
    const reqs = RETURN_REQUESTS[String(order_id)] || [];
    reqs.forEach(r => { if (['approved','received'].includes(r.status)) r.status='inspected'; });
    saveDeferred('requests',RETURN_REQUESTS);
    auditLog(order_id,'inspected',req.body?.actor||'merchant',req.body?.notes||'');
    res.json({ success:true });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// 13. REFUND
// ══════════════════════════════════════════
app.post('/api/shopify/refund/:order_id', async (req,res) => {
  const { order_id } = req.params;
  const { refund_method, note, line_item_ids } = req.body;
  try {
    const fo = await shopifyREST('GET',`orders/${order_id}.json?fields=id,line_items,financial_status`);
    const lines = fo?.order?.line_items || [];
    if (!lines.length) return res.status(400).json({ error:'No line items found' });
    const useLines = line_item_ids?.length ? lines.filter(li=>line_item_ids.map(String).includes(String(li.id))) : lines;
    const rli = useLines.map(li=>({ line_item_id:li.id, quantity:li.quantity||1, restock_type:'return' }));
    const calc = await shopifyREST('POST',`orders/${order_id}/refunds/calculate.json`,{ refund:{ refund_line_items:rli } });
    if (calc.errors) return res.status(400).json({ error:'Calc failed: '+JSON.stringify(calc.errors) });
    let txns = calc?.refund?.transactions || [];
    const fee = parseFloat(RESTOCKING_FEE_PCT);
    if (fee > 0) txns = txns.map(t=>({...t, amount:(parseFloat(t.amount||0)*(1-fee/100)).toFixed(2)}));
    const result = await shopifyREST('POST',`orders/${order_id}/refunds.json`,{
      refund:{
        notify:true, note:note||'Return approved',
        refund_line_items:rli,
        transactions: refund_method==='store_credit' ? [] :
          txns.map(t=>({ parent_id:t.parent_id, amount:t.amount, kind:'refund', gateway:t.gateway }))
      }
    });
    if (result?.refund?.id) {
      await updateOrderTags(order_id,['return-refunded'],[]);
      const amount = result.refund.transactions?.[0]?.amount || '0';
      ANALYTICS.refunded_amount += parseFloat(amount);
      saveDeferred('analytics',ANALYTICS);
      auditLog(order_id,'refund_created','merchant',`₹${amount}`);
      res.json({ success:true, refund_id:result.refund.id, amount });
    } else {
      res.status(400).json({ error:'Refund failed: '+(JSON.stringify(result?.errors||result)).slice(0,300) });
    }
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// 14. STORE CREDIT
// ══════════════════════════════════════════
app.post('/api/shopify/store-credit/:order_id', async (req,res) => {
  const { order_id } = req.params;
  const { amount, apply_bonus, customer_email, note } = req.body;
  try {
    const base  = parseFloat(amount||0);
    const bonus = apply_bonus ? (base * STORE_CREDIT_BONUS / 100) : 0;
    const total = (base+bonus).toFixed(2);
    const code  = `CREDIT-${String(order_id).slice(-6)}-${uid().toUpperCase().slice(0,6)}`;
    let gcr = null;
    try { gcr = await shopifyREST('POST','gift_cards.json',{ gift_card:{ initial_value:total, code, note:note||`Store credit #${order_id}` } }); } catch(e){}
    if (gcr?.gift_card?.id) {
      await updateOrderTags(order_id,['store-credit-issued'],[]);
      ANALYTICS.store_credits_issued += parseFloat(total);
      saveDeferred('analytics',ANALYTICS);
      auditLog(order_id,'store_credit','merchant',`GC ${code} ₹${total}`);
      res.json({ success:true, method:'gift_card', code, amount:total, bonus:bonus.toFixed(2) });
    } else {
      const enot = (await shopifyREST('GET',`orders/${order_id}.json?fields=note`))?.order?.note||'';
      const cnot = (enot+`\n[STORE CREDIT] Code:${code} Amount:₹${total} Bonus:₹${bonus.toFixed(2)} Email:${customer_email||''} Date:${new Date().toISOString()}`).slice(0,5000);
      await shopifyREST('PUT',`orders/${order_id}.json`,{ order:{ id:order_id, note:cnot } });
      await updateOrderTags(order_id,['store-credit-issued'],[]);
      ANALYTICS.store_credits_issued += parseFloat(total);
      saveDeferred('analytics',ANALYTICS);
      auditLog(order_id,'store_credit_manual','merchant',`${code} ₹${total}`);
      res.json({ success:true, method:'manual_note', code, amount:total, bonus:bonus.toFixed(2) });
    }
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// 15. EXCHANGE ORDER
// ══════════════════════════════════════════
app.post('/api/shopify/exchange/:order_id', async (req,res) => {
  const { order_id } = req.params;
  const { exchange_items, customer_address, order_number, req_id } = req.body;
  try {
    const orig  = await shopifyREST('GET',`orders/${order_id}.json?fields=id,email,shipping_address,billing_address,customer`);
    const o     = orig?.order;
    if (!o) return res.status(400).json({ error:'Order not found' });
    const addr  = customer_address || o.shipping_address || o.billing_address;
    const valid = (exchange_items||[]).filter(i=>i.variant_id);
    if (!valid.length) return res.status(400).json({ error:'No exchange items with variant_id' });
    const draft = await shopifyREST('POST','draft_orders.json',{ draft_order:{
      line_items: valid.map(i=>({ variant_id:parseInt(i.variant_id)||i.variant_id, quantity:i.quantity||1, applied_discount:{ description:'Exchange', value_type:'percentage', value:'100', amount:i.price||'0', title:'Exchange' } })),
      customer:   o.customer ? { id:o.customer.id } : undefined,
      shipping_address:addr, billing_address:o.billing_address||addr,
      email:o.email, note:`Exchange for #${order_number||order_id}${req_id?' ('+req_id+')':''}`,
      tags:'exchange-order', send_invoice:false
    }});
    if (!draft?.draft_order?.id) return res.status(400).json({ error:'Draft creation failed: '+(JSON.stringify(draft?.errors||draft)).slice(0,200) });
    const done = await shopifyREST('PUT',`draft_orders/${draft.draft_order.id}/complete.json`);
    await updateOrderTags(order_id,['exchange-fulfilled'],[]);
    ANALYTICS.exchanges_created++;
    saveDeferred('analytics',ANALYTICS);
    auditLog(order_id,'exchange_created','merchant',`Order ${done?.draft_order?.name}`);
    res.json({ success:true, new_order_id:done?.draft_order?.order_id, new_order_name:done?.draft_order?.name });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// 16. BULK ACTIONS
// ══════════════════════════════════════════
app.post('/api/bulk/approve', async (req,res) => {
  const { order_ids, actor } = req.body;
  if (!order_ids?.length) return res.status(400).json({ error:'No order IDs' });
  const results = [];
  for (const oid of order_ids) {
    try {
      const reqs = RETURN_REQUESTS[String(oid)] || [];
      const type = reqs[reqs.length-1]?.request_type || 'return';
      const at   = type==='exchange'?'exchange-approved':type==='mixed'?'mixed-approved':'return-approved';
      const rt   = type==='exchange'?'exchange-requested':type==='mixed'?'mixed-requested':'return-requested';
      await updateOrderTags(oid,[at],[rt]);
      if (reqs.length) reqs[reqs.length-1].status='approved';
      ANALYTICS.approved++;
      auditLog(oid,'bulk_approved',actor||'merchant','');
      results.push({ order_id:oid, success:true });
    } catch(e) { results.push({ order_id:oid, success:false, error:e.message }); }
  }
  saveDeferred('requests',RETURN_REQUESTS);
  saveDeferred('analytics',ANALYTICS);
  res.json({ results, succeeded:results.filter(r=>r.success).length, failed:results.filter(r=>!r.success).length });
});

app.post('/api/bulk/reject', async (req,res) => {
  const { order_ids, reason, actor } = req.body;
  if (!order_ids?.length) return res.status(400).json({ error:'No order IDs' });
  const results = [];
  for (const oid of order_ids) {
    try {
      const reqs = RETURN_REQUESTS[String(oid)] || [];
      const type = reqs[reqs.length-1]?.request_type || 'return';
      const rt   = type==='exchange'?'exchange-rejected':type==='mixed'?'mixed-rejected':'return-rejected';
      const rem  = type==='exchange'?'exchange-requested':type==='mixed'?'mixed-requested':'return-requested';
      await updateOrderTags(oid,[rt],[rem]);
      if (reqs.length) reqs[reqs.length-1].status='rejected';
      ANALYTICS.rejected++;
      auditLog(oid,'bulk_rejected',actor||'merchant',reason||'');
      results.push({ order_id:oid, success:true });
    } catch(e) { results.push({ order_id:oid, success:false, error:e.message }); }
  }
  saveDeferred('requests',RETURN_REQUESTS);
  saveDeferred('analytics',ANALYTICS);
  res.json({ results, succeeded:results.filter(r=>r.success).length, failed:results.filter(r=>!r.success).length });
});

app.post('/api/bulk/refund', async (req,res) => {
  const { order_ids } = req.body;
  if (!order_ids?.length) return res.status(400).json({ error:'No order IDs' });
  const results = [];
  for (const oid of order_ids) {
    try {
      const fo    = await shopifyREST('GET',`orders/${oid}.json?fields=line_items,financial_status`);
      const lines = fo?.order?.line_items || [];
      if (!lines.length) { results.push({ order_id:oid, success:false, error:'No items' }); continue; }
      const rli   = lines.map(li=>({ line_item_id:li.id, quantity:li.quantity, restock_type:'return' }));
      const calc  = await shopifyREST('POST',`orders/${oid}/refunds/calculate.json`,{ refund:{ refund_line_items:rli } });
      const txns  = (calc?.refund?.transactions||[]).map(t=>({ parent_id:t.parent_id, amount:t.amount, kind:'refund', gateway:t.gateway }));
      const rr    = await shopifyREST('POST',`orders/${oid}/refunds.json`,{ refund:{ notify:true, note:'Bulk refund', refund_line_items:rli, transactions:txns } });
      if (rr?.refund?.id) {
        await updateOrderTags(oid,['return-refunded'],[]);
        ANALYTICS.refunded_amount += parseFloat(rr.refund.transactions?.[0]?.amount||0);
        auditLog(oid,'bulk_refund','merchant','');
        results.push({ order_id:oid, success:true });
      } else { results.push({ order_id:oid, success:false, error:(JSON.stringify(rr?.errors||'')).slice(0,80) }); }
    } catch(e) { results.push({ order_id:oid, success:false, error:e.message }); }
  }
  saveDeferred('analytics',ANALYTICS);
  res.json({ results, succeeded:results.filter(r=>r.success).length, failed:results.filter(r=>!r.success).length });
});

// ══════════════════════════════════════════
// 17. RULES CRUD
// ══════════════════════════════════════════
app.get('/api/rules', (_req,res) => res.json({ rules:RULES }));

app.post('/api/rules', (req,res) => {
  const r = { id:'rule_'+uid(), enabled:true, priority:RULES.length+1, match_mode:'all', category:'on_submit', ...req.body };
  RULES.push(r);
  saveJSON('rules',RULES);
  auditLog('system','rule_created','merchant',r.name);
  res.json({ success:true, rule:r });
});

app.put('/api/rules/:id', (req,res) => {
  const i = RULES.findIndex(r=>r.id===req.params.id);
  if (i<0) return res.status(404).json({ error:'Rule not found' });
  RULES[i] = { ...RULES[i], ...req.body };
  saveJSON('rules',RULES);
  res.json({ success:true, rule:RULES[i] });
});

app.delete('/api/rules/:id', (req,res) => {
  const i = RULES.findIndex(r=>r.id===req.params.id);
  if (i<0) return res.status(404).json({ error:'Rule not found' });
  RULES.splice(i,1);
  saveJSON('rules',RULES);
  res.json({ success:true });
});

// ══════════════════════════════════════════
// 18. AUTOMATION TEMPLATES
// ══════════════════════════════════════════
app.get('/api/automation/templates', (_req,res) => res.json({ templates:[
  { id:'t1', name:'Auto-Approve Size Issues', category:'Customer Experience', icon:'👕', description:'Instantly approve returns for size/fit issues.',
    rule:{ name:'Auto-Approve — Size/Fit', match_mode:'all', priority:1, category:'on_submit', conditions:[{field:'return_reason',op:'contains',value:'size'}], action:'auto_approve', action_params:{message:'Return approved! Pickup will be arranged shortly.'} }},
  { id:'t2', name:'Auto-Approve Wrong/Damaged', category:'Customer Experience', icon:'❌', description:'Auto-approve wrong or damaged items.',
    rule:{ name:'Auto-Approve — Wrong/Damaged', match_mode:'any', priority:2, category:'on_submit', conditions:[{field:'return_reason',op:'contains',value:'wrong'},{field:'return_reason',op:'contains',value:'damage'},{field:'return_reason',op:'contains',value:'defect'}], action:'auto_approve', action_params:{message:"We're sorry! Return auto-approved."} }},
  { id:'t3', name:'Keep It — Low Value (< ₹300)', category:'Revenue Recovery', icon:'📦', description:'No return needed for low-value items.',
    rule:{ name:'Keep It — Low Value', match_mode:'all', priority:3, category:'on_submit', conditions:[{field:'product_price',op:'lt',value:'300'}], action:'keep_it', action_params:{message:'No need to return! Keep/donate it. Refund on its way.'} }},
  { id:'t4', name:'Flag — High Value (> ₹5000)', category:'Fraud Prevention', icon:'💰', description:'Flag high-value orders for manual review.',
    rule:{ name:'Flag — High Value', match_mode:'all', priority:4, category:'on_submit', conditions:[{field:'order_value',op:'gt',value:'5000'}], action:'flag_review', action_params:{note:'High-value — verify before approving'} }},
  { id:'t5', name:'Flag — Serial Returner (3+)', category:'Fraud Prevention', icon:'🚩', description:'Flag customers with 3+ returns.',
    rule:{ name:'Flag — Serial Returner', match_mode:'all', priority:5, category:'on_submit', conditions:[{field:'customer_return_count',op:'gte',value:'3'}], action:'flag_review', action_params:{note:'Serial returner — review carefully'} }},
  { id:'t6', name:'Auto-Reject Outside Window', category:'Policy', icon:'📅', description:'Reject requests after 30 days.',
    rule:{ name:'Auto-Reject — Outside Window', match_mode:'all', priority:6, category:'on_submit', conditions:[{field:'days_since_order',op:'gt',value:'30'}], action:'auto_reject', action_params:{message:'Request is outside our 30-day return window.'} }},
  { id:'t7', name:'Auto-Refund on Warehouse Delivery', category:'Logistics', icon:'🏭', description:'Auto-refund when item reaches warehouse.',
    rule:{ name:'Auto-Refund — Warehouse Delivery', match_mode:'all', priority:7, category:'on_carrier', conditions:[{field:'carrier_event',op:'eq',value:'delivered'},{field:'request_type',op:'eq',value:'return'}], action:'auto_refund', action_params:{}}},
  { id:'t8', name:'Auto-Exchange on Pickup Scan', category:'Logistics', icon:'🔄', description:'Create exchange order on pickup.',
    rule:{ name:'Auto-Exchange — On Pickup', match_mode:'all', priority:8, category:'on_carrier', conditions:[{field:'carrier_event',op:'eq',value:'pickup_scan'},{field:'request_type',op:'eq',value:'exchange'}], action:'auto_exchange', action_params:{}}},
  { id:'t9', name:'COD Returns — Auto Approve', category:'COD Orders', icon:'💵', description:'Auto-approve COD returns for store credit.',
    rule:{ name:'COD — Auto Approve', match_mode:'all', priority:9, category:'on_submit', conditions:[{field:'payment_method',op:'eq',value:'cod'},{field:'request_type',op:'eq',value:'return'}], action:'auto_approve', action_params:{message:'COD return approved. Store credit issued on receipt.'} }},
  { id:'t10', name:'Flag — Large Return (3+ items)', category:'Fraud Prevention', icon:'🛒', description:'Flag requests with 3+ items.',
    rule:{ name:'Flag — Large Return', match_mode:'all', priority:10, category:'on_submit', conditions:[{field:'item_count',op:'gte',value:'3'}], action:'flag_review', action_params:{note:'Large quantity — inspect before refunding'} }},
]}));

// ══════════════════════════════════════════
// 19. ANALYTICS + AUDIT
// ══════════════════════════════════════════
app.get('/api/analytics', (_req,res) => {
  const allReqs = Object.values(RETURN_REQUESTS).flat();
  const revenueAtRisk = allReqs.reduce((s,r)=>s+parseFloat(r.total_price||0),0);
  const topReasons  = Object.entries(ANALYTICS.reasons).sort((a,b)=>b[1]-a[1]).slice(0,5).map(([reason,count])=>({reason,count}));
  const topProducts = Object.entries(ANALYTICS.products_returned).sort((a,b)=>b[1]-a[1]).slice(0,10).map(([product,count])=>({product,count}));
  res.json({ ...ANALYTICS, revenue_at_risk:revenueAtRisk, top_reasons:topReasons, prevention_report:topProducts,
    approval_rate:     ANALYTICS.total_requests>0 ? Math.round(ANALYTICS.approved/ANALYTICS.total_requests*100) : 0,
    auto_approve_rate: ANALYTICS.total_requests>0 ? Math.round(ANALYTICS.auto_approved/ANALYTICS.total_requests*100) : 0
  });
});

app.get('/api/audit', (req,res) => {
  let logs = AUDIT_LOG;
  if (req.query.order_id) logs = logs.filter(l=>l.order_id===String(req.query.order_id));
  res.json({ logs:logs.slice(0, parseInt(req.query.limit)||100) });
});

// ══════════════════════════════════════════
// 20. SETTINGS
// ══════════════════════════════════════════
app.get('/api/settings', (_req,res) => res.json({
  return_window_days:RETURN_WINDOW_DAYS, store_credit_bonus:STORE_CREDIT_BONUS, restocking_fee_pct:RESTOCKING_FEE_PCT,
  warehouse:WAREHOUSE_CONFIG, delhivery:{ configured:!!DELHIVERY_TOKEN, warehouse:DELHIVERY_WAREHOUSE, mode:DELHIVERY_MODE }
}));

app.post('/api/settings', (req,res) => {
  const { return_window_days, store_credit_bonus, restocking_fee_pct, warehouse } = req.body;
  if (return_window_days !== undefined) RETURN_WINDOW_DAYS = parseInt(return_window_days);
  if (store_credit_bonus !== undefined) STORE_CREDIT_BONUS = parseFloat(store_credit_bonus);
  if (restocking_fee_pct !== undefined) RESTOCKING_FEE_PCT = parseFloat(restocking_fee_pct);
  if (warehouse) WAREHOUSE_CONFIG = { ...WAREHOUSE_CONFIG, ...warehouse };
  saveJSON('settings',{ return_window_days:RETURN_WINDOW_DAYS, store_credit_bonus:STORE_CREDIT_BONUS, restocking_fee_pct:RESTOCKING_FEE_PCT, warehouse:WAREHOUSE_CONFIG });
  auditLog('system','settings_saved','merchant',`Window:${RETURN_WINDOW_DAYS}d Bonus:${STORE_CREDIT_BONUS}%`);
  res.json({ success:true });
});

// ══════════════════════════════════════════
// 21. DELHIVERY
// ══════════════════════════════════════════
app.get('/api/delhivery/config',  (_req,res) => res.json({ configured:!!DELHIVERY_TOKEN, warehouse:DELHIVERY_WAREHOUSE, mode:DELHIVERY_MODE }));
app.post('/api/delhivery/config', (req,res) => {
  const { token, warehouse, mode } = req.body;
  if (token)     DELHIVERY_TOKEN     = token;
  if (warehouse) DELHIVERY_WAREHOUSE = warehouse;
  if (mode)      DELHIVERY_MODE      = mode;
  saveJSON('delhivery',{ token:DELHIVERY_TOKEN, warehouse:DELHIVERY_WAREHOUSE, mode:DELHIVERY_MODE });
  res.json({ success:true, configured:!!DELHIVERY_TOKEN, warehouse:DELHIVERY_WAREHOUSE, mode:DELHIVERY_MODE });
});

app.get('/api/delhivery/serviceability/:pincode', async (req,res) => {
  try {
    const d   = await delhiveryAPI('GET',`/c/api/pin-codes/json/?filter_codes=${req.params.pincode}`);
    const pin = d?.delivery_codes?.[0];
    res.json({ serviceable:!!pin, cod:pin?.cod?.toLowerCase()==='y', prepaid:pin?.['pre-paid']?.toLowerCase()==='y', pickup:pin?.pickup?.toLowerCase()==='y', pincode:req.params.pincode });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

app.get('/api/delhivery/track/:waybill', async (req,res) => {
  try {
    const d   = await delhiveryAPI('GET',`/api/v1/packages/?waybill=${req.params.waybill}`);
    const pkg = d?.ShipmentData?.[0]?.Shipment;
    if (!pkg) return res.json({ found:false });
    res.json({ found:true, waybill:pkg.AWB, status:pkg.Status?.Status, status_detail:pkg.Status?.Instructions,
      scans:(pkg.Scans||[]).map(s=>({ status:s.ScanDetail?.Scan, detail:s.ScanDetail?.Instructions, location:s.ScanDetail?.ScannedLocation, date:s.ScanDetail?.ScanDateTime })) });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

app.post('/api/delhivery/create-pickup', async (req,res) => {
  const { order_id, order_number, customer_name, customer_phone, customer_address, customer_city, customer_state, customer_pincode, products_desc, total_amount, quantity, weight } = req.body;
  if (!DELHIVERY_TOKEN)   return res.status(400).json({ error:'Delhivery not configured — add token in Settings' });
  if (!customer_pincode)  return res.status(400).json({ error:'Customer pincode required' });
  const rvpId   = `RVP-${order_number}-${Date.now().toString().slice(-6)}`;
  const payload = { pickup_location:{ name:DELHIVERY_WAREHOUSE }, shipments:[{
    name:customer_name||'Customer', add:customer_address||'', pin:String(customer_pincode),
    city:customer_city||'', state:customer_state||'', country:'India',
    phone:String(customer_phone||'').replace(/\D/g,'').slice(-10),
    order:rvpId, payment_mode:'Pickup',
    products_desc:products_desc||'Return Shipment', hsn_code:'62034200', cod_amount:'0',
    order_date:new Date().toISOString().split('T')[0], total_amount:String(total_amount||'0'),
    seller_name:DELHIVERY_WAREHOUSE, seller_inv:`INV-${order_number}`,
    quantity:parseInt(quantity)||1, weight:parseFloat(weight)||0.5,
    shipment_length:15, shipment_width:12, shipment_height:10
  }]};
  try {
    const data    = await delhiveryAPI('POST','/api/cmu/create.json',payload,true);
    const waybill = data?.packages?.[0]?.waybill || data?.waybill;
    if (waybill) {
      await updateOrderTags(order_id,['pickup-scheduled'],[]);
      const reqs = RETURN_REQUESTS[String(order_id)] || [];
      if (reqs.length) { reqs[reqs.length-1].awb = waybill; reqs[reqs.length-1].awb_final = false; }
      saveDeferred('requests',RETURN_REQUESTS);
      // Append AWB to order note
      const fn = await shopifyREST('GET',`orders/${order_id}.json?fields=note`);
      const en = fn?.order?.note || '';
      await shopifyREST('PUT',`orders/${order_id}.json`,{ order:{ id:order_id, note:(en+`\nDELHIVERY AWB: ${waybill} | RVP: ${rvpId} | ${new Date().toISOString()}`).slice(0,5000) } });
      auditLog(order_id,'pickup_created','merchant',`AWB:${waybill}`);
    }
    res.json({ success:!!waybill, waybill, rvp_order_id:rvpId, raw:data });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// 22. DATA EXPORT
// ══════════════════════════════════════════
app.get('/api/store/export', (_req,res) => {
  res.setHeader('Content-Disposition',`attachment; filename=returns-backup-${Date.now()}.json`);
  res.json({ exported_at:new Date().toISOString(), shop:SHOP_DOMAIN, settings:{ return_window_days:RETURN_WINDOW_DAYS, store_credit_bonus:STORE_CREDIT_BONUS, restocking_fee_pct:RESTOCKING_FEE_PCT, warehouse:WAREHOUSE_CONFIG }, rules:RULES, analytics:ANALYTICS, return_requests:RETURN_REQUESTS });
});

app.get('/api/store/status', (_req,res) => {
  const files = ['auth','settings','delhivery','rules','requests','analytics'];
  const fileInfo = {};
  files.forEach(f => {
    const p = dataPath(f);
    if (fs.existsSync(p)) { const s=fs.statSync(p); fileInfo[f]={ exists:true, size_kb:Math.round(s.size/102.4)/10, modified:s.mtime }; }
    else fileInfo[f] = { exists:false };
  });
  res.json({ data_dir:DATA_DIR, is_persistent:fs.existsSync('/data'), files:fileInfo,
    memory:{ rules:RULES.length, return_requests:Object.keys(RETURN_REQUESTS).length, rules_enabled:RULES.filter(r=>r.enabled).length, analytics_requests:ANALYTICS.total_requests } });
});

// ══════════════════════════════════════════
// 23. DELHIVERY POLLING  (every 30 min)
// ══════════════════════════════════════════
async function pollDelhiveryAWBs() {
  if (!DELHIVERY_TOKEN) return;
  const active = Object.entries(RETURN_REQUESTS).filter(([,reqs])=>reqs.some(r=>r.awb&&!r.awb_final));
  if (!active.length) return;
  console.log(`[Poll] Checking ${active.length} active AWBs`);
  for (const [order_id, reqs] of active) {
    for (const r of reqs) {
      if (!r.awb || r.awb_final) continue;
      try {
        const d   = await delhiveryAPI('GET',`/api/v1/packages/?waybill=${r.awb}`);
        const pkg = d?.ShipmentData?.[0]?.Shipment;
        if (!pkg) continue;
        const status = (pkg.Status?.Status||'').toLowerCase();
        if (status === r.last_carrier_status) continue;
        r.last_carrier_status = status;
        let event = null;
        if (status.includes('picked up')||status.includes('pickup'))              { event='pickup_scan'; await updateOrderTags(order_id,['pickup-scan'],['pickup-scheduled']); }
        else if (status.includes('delivered')||status.includes('rto delivered'))  { event='delivered';   await updateOrderTags(order_id,['return-received'],['pickup-scan']); r.awb_final=true; }
        if (!event) continue;
        auditLog(order_id,`carrier_${event}`,'poll',`AWB ${r.awb} → ${status}`);
        saveDeferred('requests',RETURN_REQUESTS);

        const match = runRulesEngine({ ...r, carrier_event:event });
        if (!match) continue;
        auditLog(order_id,`carrier_rule:${match.action}`,'poll',`"${match.rule_name}"`);

        if (match.action === 'auto_refund') {
          const fo  = await shopifyREST('GET',`orders/${order_id}.json?fields=line_items`);
          const lis = fo?.order?.line_items||[];
          if (!lis.length) continue;
          const rli  = lis.map(li=>({ line_item_id:li.id, quantity:li.quantity, restock_type:'return' }));
          const calc = await shopifyREST('POST',`orders/${order_id}/refunds/calculate.json`,{ refund:{ refund_line_items:rli } });
          const txns = (calc?.refund?.transactions||[]).map(t=>({ parent_id:t.parent_id, amount:t.amount, kind:'refund', gateway:t.gateway }));
          const rr   = await shopifyREST('POST',`orders/${order_id}/refunds.json`,{ refund:{ notify:true, note:`Auto refund — ${event}`, refund_line_items:rli, transactions:txns } });
          if (rr?.refund?.id) { await updateOrderTags(order_id,['return-refunded'],[]); auditLog(order_id,'auto_refund','poll',`₹${rr.refund.transactions?.[0]?.amount||0}`); }
        }
        if (match.action === 'auto_exchange') {
          const exchItems = (r.items||[]).filter(i=>i.action==='exchange'&&i.exchange_variant_id);
          if (!exchItems.length) continue;
          const orig  = await shopifyREST('GET',`orders/${order_id}.json?fields=email,shipping_address`);
          const draft = await shopifyREST('POST','draft_orders.json',{ draft_order:{
            line_items:exchItems.map(i=>({ variant_id:parseInt(i.exchange_variant_id), quantity:i.qty||1, applied_discount:{ description:'Exchange', value_type:'percentage', value:'100', amount:'0', title:'Exchange' } })),
            shipping_address:orig?.order?.shipping_address, email:orig?.order?.email,
            note:`Auto exchange — AWB ${r.awb}`, tags:'exchange-order', send_invoice:false
          }});
          if (draft?.draft_order?.id) {
            await shopifyREST('PUT',`draft_orders/${draft.draft_order.id}/complete.json`);
            await updateOrderTags(order_id,['exchange-fulfilled'],[]);
            auditLog(order_id,'auto_exchange','poll',`on ${event}`);
          }
        }
      } catch(e) { console.error(`[Poll] AWB ${r.awb}:`,e.message); }
    }
  }
}
setInterval(pollDelhiveryAWBs, 30*60*1000);
setTimeout(pollDelhiveryAWBs,   2*60*1000);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`[Server] ✅ Returns Manager on :${PORT} | Auth:${!!ACCESS_TOKEN} | Shop:${SHOP_DOMAIN||'none'} | Rules:${RULES.length} (${RULES.filter(r=>r.enabled).length} enabled)`));
