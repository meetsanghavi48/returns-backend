'use strict';
const express    = require('express');
const cors       = require('cors');
const fetch      = require('node-fetch');
const path       = require('path');
const fs         = require('fs');
const crypto     = require('crypto');
const nodemailer = require('nodemailer');
const { createClient } = require('@supabase/supabase-js');

const app = express();
app.use(cors({ origin:'*', methods:['GET','POST','PUT','DELETE','OPTIONS'], allowedHeaders:['Content-Type','Authorization','X-Shopify-Access-Token'] }));
app.use(express.json({ limit:'10mb' }));
app.options('*', cors());
app.use((_req,res,next)=>{ res.setHeader('Access-Control-Allow-Origin','*'); next(); });

// ══════════════════════════════════════════
// SUPABASE  (must be before everything)
// ══════════════════════════════════════════
const supabase = createClient(
  process.env.SUPABASE_URL    || '',
  process.env.SUPABASE_SERVICE_KEY || ''
);

// ══════════════════════════════════════════
// CONFIG
// ══════════════════════════════════════════
const SHOPIFY_API_KEY     = process.env.SHOPIFY_API_KEY     || '';
const SHOPIFY_API_SECRET  = process.env.SHOPIFY_API_SECRET  || '';
const SHOPIFY_API_VERSION = process.env.SHOPIFY_API_VERSION || '2026-01';
const BACKEND_URL         = process.env.BACKEND_URL         || 'https://returns-backend.onrender.com';
const SCOPES = 'read_orders,write_orders,read_products,write_products,read_draft_orders,write_draft_orders,read_customers,write_customers,read_inventory,write_inventory';

let ACCESS_TOKEN = process.env.ACCESS_TOKEN || '';
let SHOP_DOMAIN  = process.env.SHOP_DOMAIN  || '';

// Load from Supabase if not in env vars
async function loadAuthFromSupabase() {
  // Only load from Supabase if BOTH are missing from env vars
  if (ACCESS_TOKEN && SHOP_DOMAIN) {
    console.log(`[Auth] Using env vars: ${SHOP_DOMAIN}`);
    return;
  }
  try {
    const { data } = await supabase.from('settings').select('value').eq('key','auth').single();
    if (data?.value?.access_token) {
      if (!ACCESS_TOKEN) ACCESS_TOKEN = data.value.access_token;
      if (!SHOP_DOMAIN)  SHOP_DOMAIN  = data.value.shop_domain;
      console.log(`[Auth] Loaded from Supabase: ${SHOP_DOMAIN}`);
    }
  } catch(e) { console.log('[Auth] No saved token in Supabase yet'); }
}
// Run on startup
loadAuthFromSupabase();

// Delhivery — production
const DELHIVERY_TOKEN     = process.env.DELHIVERY_TOKEN     || 'bcfa63601f1cf0a2eaee2b06caa25e2134496770';
const DELHIVERY_WAREHOUSE = process.env.DELHIVERY_WAREHOUSE || 'Blakc';
const DELHIVERY_BASE      = 'https://track.delhivery.com';

// ── Easebuzz ──
const EASEBUZZ_KEY  = process.env.EASEBUZZ_KEY  || '';
const EASEBUZZ_SALT = process.env.EASEBUZZ_SALT || '';
const EASEBUZZ_MID  = process.env.EASEBUZZ_MID  || '';
const EASEBUZZ_ENV  = process.env.EASEBUZZ_ENV  || 'prod';
const EASEBUZZ_BASE = EASEBUZZ_ENV === 'test' ? 'https://testpay.easebuzz.in' : 'https://pay.easebuzz.in';

// ══════════════════════════════════════════
// EMAIL — Gmail SMTP
// ══════════════════════════════════════════
const GMAIL_USER     = process.env.GMAIL_USER         || 'storeblakc@gmail.com';
const GMAIL_PASS     = process.env.GMAIL_APP_PASSWORD || '';
const SUPPORT_EMAIL  = 'storeblakc@gmail.com';

let _mailer = null;
function getMailer() {
  if (!_mailer && GMAIL_USER && GMAIL_PASS) {
    _mailer = nodemailer.createTransport({
      service: 'gmail',
      auth: { user: GMAIL_USER, pass: GMAIL_PASS }
    });
  }
  return _mailer;
}

async function sendEmail(to, subject, html) {
  if (!to || !to.includes('@')) return;
  const mailer = getMailer();
  if (!mailer) { console.log('[Email] Not configured, skipping:', subject); return; }
  try {
    await mailer.sendMail({ from:`BLAKC Returns <${GMAIL_USER}>`, replyTo:SUPPORT_EMAIL, to, subject, html });
    console.log(`[Email] ✅ "${subject}" → ${to}`);
  } catch(e) { console.error('[Email] ❌', e.message); }
}

function emailBase(content) {
  const yr = new Date().getFullYear();
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#F3F4F6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,sans-serif">
<div style="max-width:560px;margin:0 auto;padding:32px 16px">
  <div style="background:#000;border-radius:14px 14px 0 0;padding:22px 32px;text-align:center">
    <div style="color:#fff;font-size:20px;font-weight:800;letter-spacing:3px">BLAKC</div>
    <div style="color:#777;font-size:10px;letter-spacing:1.5px;margin-top:3px;text-transform:uppercase">Exchange &amp; Returns</div>
  </div>
  <div style="background:#fff;padding:32px;border-radius:0 0 14px 14px;border:1px solid #E5E7EB;border-top:none">
    ${content}
    <div style="border-top:1px solid #F3F4F6;margin:28px 0 20px"></div>
    <div style="text-align:center;color:#9CA3AF;font-size:11px;line-height:1.8">
      For any queries, reply to this email or write to
      <a href="mailto:${SUPPORT_EMAIL}" style="color:#374151;font-weight:600">${SUPPORT_EMAIL}</a><br>
      &copy; ${yr} BLAKC &middot; <a href="https://blakc.store" style="color:#9CA3AF">blakc.store</a>
    </div>
  </div>
</div>
</body></html>`;
}

// ── Email: 1. Request Received ──
function emailRequestReceived({ name, orderNumber, reqId, items, refundMethod }) {
  const itemRows = (items||[]).map(i=>`
    <tr>
      <td style="padding:8px 0;border-bottom:1px solid #F3F4F6;font-size:13px;color:#111">${i.title||''}${i.variant_title?' — '+i.variant_title:''}</td>
      <td style="padding:8px 0;border-bottom:1px solid #F3F4F6;font-size:13px;color:#6B7280;text-align:right;white-space:nowrap">${i.action==='exchange'?'Exchange':'Return'} × ${i.qty||1}</td>
    </tr>`).join('');
  const refundLabel = refundMethod==='store_credit' ? 'Store Credit (Gift Card)' : refundMethod==='original' ? 'Original Payment Method' : 'Store Credit';
  return emailBase(`
    <div style="margin-bottom:6px">
      <span style="background:#F0FDF4;color:#15803D;font-size:11px;font-weight:700;padding:4px 10px;border-radius:20px;letter-spacing:.5px">✓ REQUEST RECEIVED</span>
    </div>
    <h2 style="font-size:20px;font-weight:800;color:#111;margin:14px 0 6px">Hi ${name||'there'},</h2>
    <p style="font-size:14px;color:#4B5563;line-height:1.6;margin:0 0 24px">We've received your return/exchange request for order <strong>#${orderNumber}</strong>. Our team will review it and schedule a pickup shortly.</p>
    <div style="background:#F9FAFB;border-radius:10px;padding:20px;margin-bottom:24px">
      <div style="font-size:11px;font-weight:700;color:#9CA3AF;letter-spacing:1px;text-transform:uppercase;margin-bottom:12px">Request Details</div>
      <table style="width:100%;border-collapse:collapse">
        ${itemRows}
      </table>
      <div style="display:flex;justify-content:space-between;margin-top:14px;padding-top:12px;border-top:1px solid #E5E7EB">
        <span style="font-size:12px;color:#6B7280">Request ID</span>
        <span style="font-size:12px;font-weight:700;color:#111">${reqId}</span>
      </div>
      <div style="display:flex;justify-content:space-between;margin-top:8px">
        <span style="font-size:12px;color:#6B7280">Refund Method</span>
        <span style="font-size:12px;font-weight:600;color:#111">${refundLabel}</span>
      </div>
    </div>
    <p style="font-size:13px;color:#6B7280;line-height:1.6;margin:0">You'll receive another email once your pickup is scheduled. Keep your item packed and ready.</p>`);
}

// ── Email: 2. Pickup Booked ──
function emailPickupBooked({ name, orderNumber, reqId, awb }) {
  return emailBase(`
    <div style="margin-bottom:6px">
      <span style="background:#EFF6FF;color:#1D4ED8;font-size:11px;font-weight:700;padding:4px 10px;border-radius:20px;letter-spacing:.5px">🚚 PICKUP BOOKED</span>
    </div>
    <h2 style="font-size:20px;font-weight:800;color:#111;margin:14px 0 6px">Pickup Scheduled — #${orderNumber}</h2>
    <p style="font-size:14px;color:#4B5563;line-height:1.6;margin:0 0 24px">Hi ${name||'there'}, your pickup has been scheduled with Delhivery. Please keep your item ready and packed.</p>
    <div style="background:#F9FAFB;border-radius:10px;padding:20px;margin-bottom:24px">
      <div style="font-size:11px;font-weight:700;color:#9CA3AF;letter-spacing:1px;text-transform:uppercase;margin-bottom:14px">Tracking Info</div>
      <div style="display:flex;justify-content:space-between;margin-bottom:8px">
        <span style="font-size:12px;color:#6B7280">Tracking Number</span>
        <a href="https://www.delhivery.com/track/package/${awb}" style="font-size:13px;font-weight:800;color:#1D4ED8;text-decoration:none">${awb}</a>
      </div>
      <div style="display:flex;justify-content:space-between;margin-bottom:8px">
        <span style="font-size:12px;color:#6B7280">Courier Partner</span>
        <span style="font-size:12px;font-weight:600;color:#111">Delhivery</span>
      </div>
      <div style="display:flex;justify-content:space-between">
        <span style="font-size:12px;color:#6B7280">Request ID</span>
        <span style="font-size:12px;font-weight:600;color:#111">${reqId}</span>
      </div>
    </div>
    <a href="https://www.delhivery.com/track/package/${awb}" style="display:block;text-align:center;background:#000;color:#fff;text-decoration:none;font-size:13px;font-weight:700;padding:14px;border-radius:10px;margin-bottom:20px;letter-spacing:.3px">Track My Pickup →</a>
    <p style="font-size:13px;color:#6B7280;line-height:1.6;margin:0">The delivery executive will arrive at your address. Please hand over the packed item when they arrive.</p>`);
}

// ── Email: 3. In Transit ──
function emailInTransit({ name, orderNumber, reqId, requestType, exchangeOrderName }) {
  const isExchange = requestType === 'exchange' || requestType === 'mixed';
  const exchangeNote = isExchange && exchangeOrderName
    ? `<div style="background:#F0FDF4;border:1px solid #BBF7D0;border-radius:10px;padding:16px;margin:20px 0">
        <div style="font-size:12px;font-weight:700;color:#15803D;margin-bottom:4px">🎉 Exchange Order Created</div>
        <div style="font-size:13px;color:#166534">Your new item exchange order <strong>${exchangeOrderName}</strong> has been created and will be dispatched once we receive your return.</div>
       </div>`
    : isExchange
    ? `<div style="background:#FFF7ED;border:1px solid #FED7AA;border-radius:10px;padding:16px;margin:20px 0">
        <div style="font-size:13px;color:#92400E">Your exchange order will be created and dispatched as soon as we receive your returned item at our warehouse.</div>
       </div>`
    : `<div style="background:#FFF7ED;border:1px solid #FED7AA;border-radius:10px;padding:16px;margin:20px 0">
        <div style="font-size:13px;color:#92400E">Your refund will be processed as soon as your item is delivered to our warehouse.</div>
       </div>`;
  return emailBase(`
    <div style="margin-bottom:6px">
      <span style="background:#F5F3FF;color:#6D28D9;font-size:11px;font-weight:700;padding:4px 10px;border-radius:20px;letter-spacing:.5px">📦 WE HAVE YOUR PACKAGE</span>
    </div>
    <h2 style="font-size:20px;font-weight:800;color:#111;margin:14px 0 6px">Package Picked Up — #${orderNumber}</h2>
    <p style="font-size:14px;color:#4B5563;line-height:1.6;margin:0 0 4px">Hi ${name||'there'}, Delhivery has picked up your package. It is now in transit to our warehouse.</p>
    ${exchangeNote}
    <div style="background:#F9FAFB;border-radius:10px;padding:16px;margin-top:4px">
      <div style="display:flex;justify-content:space-between">
        <span style="font-size:12px;color:#6B7280">Request ID</span>
        <span style="font-size:12px;font-weight:600;color:#111">${reqId}</span>
      </div>
    </div>`);
}

// ── Email: 4. Delivered to Warehouse ──
function emailDelivered({ name, orderNumber, reqId, requestType, refundMethod, giftCardCode, refundAmount }) {
  const isExchange = requestType === 'exchange' || requestType === 'mixed';
  const isStoreCredit = refundMethod === 'store_credit';
  let refundBlock = '';
  if (!isExchange) {
    if (isStoreCredit && giftCardCode) {
      refundBlock = `
        <div style="background:#F0FDF4;border:1px solid #BBF7D0;border-radius:10px;padding:20px;margin:20px 0;text-align:center">
          <div style="font-size:12px;font-weight:700;color:#15803D;margin-bottom:8px;text-transform:uppercase;letter-spacing:.5px">🎁 Your Store Credit Code</div>
          <div style="font-size:22px;font-weight:900;color:#111;letter-spacing:3px;font-family:monospace;background:#fff;border:1.5px dashed #86EFAC;border-radius:8px;padding:12px 20px;display:inline-block">${giftCardCode}</div>
          <div style="font-size:12px;color:#4B5563;margin-top:10px">₹${refundAmount} credit — use at <a href="https://blakc.store" style="color:#15803D">blakc.store</a></div>
        </div>`;
    } else {
      refundBlock = `
        <div style="background:#F0FDF4;border:1px solid #BBF7D0;border-radius:10px;padding:16px;margin:20px 0">
          <div style="font-size:13px;color:#166534;line-height:1.6">Your refund will be processed by tomorrow and will reflect in your source account within <strong>5 working days</strong>.</div>
        </div>`;
    }
  } else {
    refundBlock = `
      <div style="background:#EFF6FF;border:1px solid #BFDBFE;border-radius:10px;padding:16px;margin:20px 0">
        <div style="font-size:13px;color:#1E40AF;line-height:1.6">Your exchange order is being dispatched. You'll receive a shipping confirmation once it's on its way.</div>
      </div>`;
  }
  return emailBase(`
    <div style="margin-bottom:6px">
      <span style="background:#F0FDF4;color:#15803D;font-size:11px;font-weight:700;padding:4px 10px;border-radius:20px;letter-spacing:.5px">✅ RECEIVED AT WAREHOUSE</span>
    </div>
    <h2 style="font-size:20px;font-weight:800;color:#111;margin:14px 0 6px">We've Received Your Item — #${orderNumber}</h2>
    <p style="font-size:14px;color:#4B5563;line-height:1.6;margin:0 0 4px">Hi ${name||'there'}, your returned item has been delivered to our warehouse and is being inspected.</p>
    ${refundBlock}
    <div style="background:#F9FAFB;border-radius:10px;padding:16px">
      <div style="display:flex;justify-content:space-between">
        <span style="font-size:12px;color:#6B7280">Request ID</span>
        <span style="font-size:12px;font-weight:600;color:#111">${reqId}</span>
      </div>
    </div>`);
}

// ── Email: 5. Refund Processed ──
function emailRefundProcessed({ name, orderNumber, reqId, amount, method }) {
  const isStoreCredit = method === 'store_credit';
  return emailBase(`
    <div style="margin-bottom:6px">
      <span style="background:#F0FDF4;color:#15803D;font-size:11px;font-weight:700;padding:4px 10px;border-radius:20px;letter-spacing:.5px">💰 REFUND PROCESSED</span>
    </div>
    <h2 style="font-size:20px;font-weight:800;color:#111;margin:14px 0 6px">Refund of ₹${parseFloat(amount||0).toFixed(0)} Initiated</h2>
    <p style="font-size:14px;color:#4B5563;line-height:1.6;margin:0 0 24px">Hi ${name||'there'}, your refund for order <strong>#${orderNumber}</strong> has been processed successfully.</p>
    <div style="background:#F9FAFB;border-radius:10px;padding:20px;margin-bottom:24px">
      <div style="display:flex;justify-content:space-between;margin-bottom:10px">
        <span style="font-size:12px;color:#6B7280">Refund Amount</span>
        <span style="font-size:16px;font-weight:900;color:#15803D">₹${parseFloat(amount||0).toFixed(0)}</span>
      </div>
      <div style="display:flex;justify-content:space-between;margin-bottom:10px">
        <span style="font-size:12px;color:#6B7280">Refund Method</span>
        <span style="font-size:12px;font-weight:600;color:#111">${isStoreCredit ? 'Store Credit (Gift Card)' : 'Original Payment Method'}</span>
      </div>
      <div style="display:flex;justify-content:space-between">
        <span style="font-size:12px;color:#6B7280">Request ID</span>
        <span style="font-size:12px;font-weight:600;color:#111">${reqId}</span>
      </div>
    </div>
    ${isStoreCredit
      ? `<p style="font-size:13px;color:#6B7280;line-height:1.6;margin:0">Your store credit gift card has been issued. Use it on your next order at <a href="https://blakc.store" style="color:#374151;font-weight:600">blakc.store</a>.</p>`
      : `<p style="font-size:13px;color:#6B7280;line-height:1.6;margin:0">The amount will reflect in your source account within <strong>5 working days</strong> depending on your bank.</p>`
    }`);
}

function ebHash(p) {
  const str = [p.key,p.txnid,p.amount,p.productinfo,p.firstname,p.email,
    p.udf1||'',p.udf2||'',p.udf3||'',p.udf4||'',p.udf5||'',
    '','','','','',EASEBUZZ_SALT].join('|');
  return crypto.createHash('sha512').update(str).digest('hex');
}
function ebVerify(p) {
  const str = [EASEBUZZ_SALT,p.status,
    p.udf5||'',p.udf4||'',p.udf3||'',p.udf2||'',p.udf1||'',
    p.email,p.firstname,p.productinfo,p.amount,p.txnid,p.key].join('|');
  return crypto.createHash('sha512').update(str).digest('hex');
}

let RETURN_WINDOW_DAYS   = parseInt(process.env.RETURN_WINDOW_DAYS) || 10;
let EXCHANGE_WINDOW_DAYS = parseInt(process.env.EXCHANGE_WINDOW_DAYS) || 10;
const STORE_CREDIT_BONUS = 0;
let RESTOCKING_FEE_PCT   = parseFloat(process.env.RESTOCKING_FEE_PCT) || 0;
const RETURN_SHIPPING_FEE = parseFloat(process.env.RETURN_SHIPPING_FEE) || 100;
const NON_RETURNABLE_KEYWORDS = ['watch','wallet'];
const isNonReturnable = (title='') => NON_RETURNABLE_KEYWORDS.some(kw => title.toLowerCase().includes(kw));
let WAREHOUSE_CONFIG = {
  name:    process.env.WAREHOUSE_NAME    || 'Blakc',
  address: process.env.WAREHOUSE_ADDRESS || '',
  city:    process.env.WAREHOUSE_CITY    || '',
  state:   process.env.WAREHOUSE_STATE   || '',
  pincode: process.env.WAREHOUSE_PINCODE || '',
  phone:   process.env.WAREHOUSE_PHONE   || ''
};

// Load persisted settings from Supabase on startup (overrides env defaults)
async function loadPersistedSettings() {
  try {
    const { data } = await supabase.from('settings').select('value').eq('key','app_settings').single();
    if (!data?.value) return;
    const s = data.value;
    if (s.return_window_days   != null) RETURN_WINDOW_DAYS   = parseInt(s.return_window_days);
    if (s.exchange_window_days != null) EXCHANGE_WINDOW_DAYS = parseInt(s.exchange_window_days);
    if (s.restocking_fee_pct   != null) RESTOCKING_FEE_PCT   = parseFloat(s.restocking_fee_pct);
    if (s.warehouse)                    WAREHOUSE_CONFIG      = { ...WAREHOUSE_CONFIG, ...s.warehouse };
    console.log(`[Settings] Loaded from DB — ReturnWindow:${RETURN_WINDOW_DAYS}d ExchangeWindow:${EXCHANGE_WINDOW_DAYS}d`);
  } catch(e) { console.log('[Settings] Using defaults:', e.message); }
}

console.log(`[Boot] Token:${!!ACCESS_TOKEN} Shop:${SHOP_DOMAIN||'none'}`);

// ══════════════════════════════════════════
// HELPERS
// ══════════════════════════════════════════
function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2,7); }
function gidToId(gid) { return typeof gid==='string' ? gid.replace(/^gid:\/\/shopify\/\w+\//,'') : String(gid||''); }

async function auditLog(order_id, req_id, action, actor, details) {
  console.log(`[AUDIT] #${order_id} | ${action} | ${actor} | ${details}`);
  try { await supabase.from('audit_log').insert({ order_id:String(order_id||''), req_id:req_id||null, action, actor:actor||'system', details:details||'' }); }
  catch(e) { console.error('[auditLog]', e.message); }
}

async function shopifyREST(method, endpoint, body) {
  if (!ACCESS_TOKEN||!SHOP_DOMAIN) throw new Error('Not authenticated');
  const opts = { method, headers:{ 'X-Shopify-Access-Token':ACCESS_TOKEN, 'Content-Type':'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const r    = await fetch(`https://${SHOP_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}/${endpoint}`, opts);
  const text = await r.text();
  console.log(`[Shopify] ${method} ${endpoint} → ${r.status}`);
  if (!r.ok) console.error('[Shopify ERR]', text.slice(0,400));
  try { return JSON.parse(text); } catch(e) { return { error:text }; }
}

async function graphql(query, variables) {
  if (!ACCESS_TOKEN||!SHOP_DOMAIN) throw new Error('Not authenticated');
  const r = await fetch(`https://${SHOP_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`, {
    method:'POST', headers:{ 'X-Shopify-Access-Token':ACCESS_TOKEN, 'Content-Type':'application/json' },
    body: JSON.stringify({ query, variables })
  });
  return r.json();
}

async function updateOrderTags(order_id, addTags, removeTags) {
  removeTags = removeTags || [];
  const d  = await shopifyREST('GET', `orders/${order_id}.json?fields=tags`);
  let tags = (d?.order?.tags||'').split(',').map(t=>t.trim()).filter(Boolean);
  tags = tags.filter(t=>!removeTags.includes(t));
  addTags.forEach(t=>{ if(!tags.includes(t)) tags.push(t); });
  await shopifyREST('PUT', `orders/${order_id}.json`, { order:{ id:order_id, tags:tags.join(', ') } });
  return tags;
}

async function delhiveryAPI(method, urlPath, body, isForm) {
  const url     = DELHIVERY_BASE + urlPath;
  const headers = { 'Authorization':`Token ${DELHIVERY_TOKEN}` };
  const opts    = { method:method||'GET', headers };
  if (body) {
    if (isForm) { headers['Content-Type']='application/x-www-form-urlencoded'; opts.body=`format=json&data=${encodeURIComponent(JSON.stringify(body))}`; }
    else        { headers['Content-Type']='application/json'; opts.body=JSON.stringify(body); }
  }
  const r    = await fetch(url, opts);
  const text = await r.text();
  try { return JSON.parse(text); } catch(e) { return { raw:text }; }
}

// ══════════════════════════════════════════
// EXCHANGE COUNTER  (#EXC9001, #EXC9002...)
// ══════════════════════════════════════════
async function getNextExcNumber() {
  const { data, error } = await supabase.rpc('increment_exc_counter');
  if (error) {
    // Fallback: manual increment
    const { data:curr } = await supabase.from('exc_counter').select('last_number').eq('id',1).single();
    const next = (curr?.last_number||9000) + 1;
    await supabase.from('exc_counter').update({ last_number:next }).eq('id',1);
    return next;
  }
  return data;
}

// ══════════════════════════════════════════
// FREEBIE CHECK  (add socks if freebie tag)
// ══════════════════════════════════════════
function checkFreebieItems(orderTags, orderLineItems, requestItems, requestType) {
  if (requestType === 'exchange') return requestItems;
  const tags = Array.isArray(orderTags) ? orderTags : (orderTags||'').split(',').map(t=>t.trim());
  if (!tags.includes('freebie')) return requestItems;
  const hasSocks = requestItems.some(i=>(i.title||'').toLowerCase().includes('sock'));
  if (hasSocks) return requestItems;
  const socksItem = (orderLineItems||[]).find(li=>(li.title||'').toLowerCase().includes('sock'));
  if (!socksItem) return requestItems;
  return [...requestItems, {
    id: String(socksItem.id), title:socksItem.title,
    variant_title: socksItem.variant_title||'', variant_id:socksItem.variant_id,
    product_id: socksItem.product_id, price:socksItem.price||'0',
    qty:1, action:'return', reason:'Freebie return', auto_added:true,
    image_url: socksItem.image_url||null
  }];
}

// ══════════════════════════════════════════
// AUTO APPROVE
// ══════════════════════════════════════════
async function autoApproveRequest(req_id, order_id, request_type) {
  try {
    const at = request_type==='exchange'?'exchange-approved':request_type==='mixed'?'mixed-approved':'return-approved';
    const rt = request_type==='exchange'?'exchange-requested':request_type==='mixed'?'mixed-requested':'return-requested';
    await updateOrderTags(order_id,[at],[rt]);
    const approvedAt = new Date().toISOString();
    await supabase.from('requests').update({ status:'approved', approved_at:approvedAt }).eq('req_id',req_id);
    await auditLog(order_id, req_id, 'auto_approved', 'system', 'Auto-approved on submission');
    console.log(`[AutoApprove] ${req_id} at ${approvedAt}`);
  } catch(e) { console.error('[autoApprove]', e.message); }
}

// ══════════════════════════════════════════
// CREATE DELHIVERY PICKUP
// ══════════════════════════════════════════
async function createDelhiveryPickup(request) {
  try {
    const addr  = request.address || {};
    const items = request.items   || [];
    const pincode = addr.zip || addr.pincode || '';

    // Check serviceability — block if pincode is not serviceable for pickup
    if (pincode) {
      try {
        const svc = await delhiveryAPI('GET', `/c/api/pin-codes/json/?filter_codes=${pincode}`);
        const pin = svc?.delivery_codes?.[0]?.postal_code || svc?.delivery_codes?.[0];
        const pickupOk = (pin?.pickup||'').toLowerCase() === 'y';
        console.log(`[Pickup] Pincode ${pincode} pickup serviceable:`, pickupOk);
        if (!pickupOk && svc?.delivery_codes) {
          const err = `Pincode ${pincode} is not serviceable for pickup by Delhivery. Please ask the customer to use a nearby serviceable pincode or self-ship.`;
          await supabase.from('requests').update({ awb_status: 'Non-serviceable pincode: '+pincode }).eq('req_id', request.req_id).throwOnError();
          throw Object.assign(new Error(err), { code:'NON_SERVICEABLE', pincode });
        }
      } catch(e) {
        if (e.code === 'NON_SERVICEABLE') throw e;
        console.error('[serviceability check]', e.message); // API error — proceed anyway
      }
    }

    const totalQty    = items.reduce((s,i)=>s+(parseInt(i.qty)||1),0) || 1;
    const totalWeight = Math.max(0.5, totalQty * 0.5);
    const totalAmount = items.reduce((s,i)=>s+parseFloat(i.price||0)*(parseInt(i.qty)||1),0);
    const productsDesc = items.map(i=>`${i.title} x${parseInt(i.qty)||1}`).join(', ').slice(0,200) || 'Return Shipment';
    const rvpId = `#9${request.order_number}_${(request.req_id||"").replace(/^[^_]+_/,"")}`;

    const payload = {
      pickup_location: { name: DELHIVERY_WAREHOUSE },
      shipments: [{
        name:         (addr.name||'Customer').slice(0,50),
        add:          [addr.address1,addr.address2].filter(Boolean).join(', ').slice(0,200) || 'N/A',
        pin:          String(pincode||'400001'),
        city:         addr.city||'',
        state:        addr.province||addr.state||'',
        country:      'India',
        phone:        String(addr.phone||'').replace(/[^0-9]/g,'').slice(-10) || '9999999999',
        order:        rvpId,
        payment_mode: 'Pickup',
        products_desc: productsDesc,
        hsn_code:     '62034200',
        cod_amount:   '0',
        order_date:   new Date().toISOString().split('T')[0],
        total_amount: String(totalAmount.toFixed(2)),
        seller_name:  DELHIVERY_WAREHOUSE,
        seller_inv:   `INV-${request.order_number}`,
        quantity:     totalQty,
        weight:       totalWeight,
        shipment_length:30, shipment_width:25, shipment_height:10
      }]
    };

    console.log('[Pickup] Creating for', request.req_id, 'pincode:', pincode, 'order:', rvpId);
    const data    = await delhiveryAPI('POST', '/api/cmu/create.json', payload, true);
    const waybill = data?.packages?.[0]?.waybill || data?.waybill;
    console.log('[Pickup] Response:', JSON.stringify(data).slice(0,300));

    if (waybill) {
      await supabase.from('requests').update({
        awb: waybill,
        awb_status: 'Pickup Scheduled',
        awb_status_code: 'X-ASP',
        status: 'pickup_scheduled',
        pickup_created_at: new Date().toISOString()
      }).eq('req_id', request.req_id);

      // Email 2: Pickup booked
      if (request.customer_email) {
        sendEmail(request.customer_email, `Pickup Scheduled — Order #${request.order_number}`,
          emailPickupBooked({ name:(request.customer_name||'').split(' ')[0], orderNumber:request.order_number, reqId:request.req_id, awb:waybill }))
          .catch(()=>{});
      }

      await updateOrderTags(request.order_id, ['pickup-scheduled'], []);

      // Exchange order is created ONLY after pickup scan (when Delhivery confirms pickup)
      // NOT here — creating AWB does not mean item has been picked up yet

      // Append AWB to order note
      try {
        const fn = await shopifyREST('GET', `orders/${request.order_id}.json?fields=note`);
        const en = fn?.order?.note || '';
        await shopifyREST('PUT', `orders/${request.order_id}.json`, {
          order: { id:request.order_id, note:(en+`
DELHIVERY AWB: ${waybill} | REQ: ${request.req_id} | ${new Date().toISOString()}`).slice(0,5000) }
        });
      } catch(e) { console.error('[Pickup] Note update failed:', e.message); }

      await auditLog(request.order_id, request.req_id, 'pickup_created', 'system', `AWB:${waybill}`);
      console.log(`[Pickup] ✅ AWB ${waybill} for ${request.req_id}`);
      return waybill;
    } else {
      const errMsg = data?.rmk || data?.packages?.[0]?.remarks || JSON.stringify(data).slice(0,300);
      console.error('[Pickup] ❌ No waybill returned:', errMsg);
      await supabase.from('requests').update({
        awb_status: 'Pickup failed: ' + String(errMsg).slice(0,120)
      }).eq('req_id', request.req_id);
      throw new Error(errMsg || 'Delhivery did not return a waybill');
    }
  } catch(e) {
    console.error('[createDelhiveryPickup]', e.message);
    throw e; // re-throw so callers can surface the message
  }
}


// ══════════════════════════════════════════
// CREATE EXCHANGE ORDER  (#EXC9001...)
// ══════════════════════════════════════════
async function createExchangeOrder(request) {
  try {
    const exchItems = (request.items||[]).filter(i=>i.action==='exchange'&&i.exchange_variant_id);
    if (!exchItems.length) { console.log(`[Exchange] No exchange items for ${request.req_id}`); return null; }

    const excNum  = await getNextExcNumber();
    const excTag  = `EXC${excNum}`;

    const orig  = await shopifyREST('GET',`orders/${request.order_id}.json?fields=email,shipping_address,billing_address,customer`);
    const o     = orig?.order;

    const draft = await shopifyREST('POST','draft_orders.json',{ draft_order:{
      line_items: exchItems.map(i=>({
        variant_id: parseInt(i.exchange_variant_id),
        quantity:   parseInt(i.qty)||1,
        applied_discount: { description:'Exchange', value_type:'percentage', value:'100', amount:String(i.price||'0'), title:'Exchange' }
      })),
      customer:          o?.customer ? { id:o.customer.id } : undefined,
      shipping_address:  request.address || o?.shipping_address,
      billing_address:   o?.billing_address || request.address || o?.shipping_address,
      email:             o?.email,
      note:              `${excTag} — Exchange for #${request.order_number} | Original: ${request.req_id}`,
      tags:              `exchange-order,${excTag}`,
      send_invoice:      false
    }});

    if (!draft?.draft_order?.id) {
      console.error('[Exchange] Draft failed:', JSON.stringify(draft?.errors||draft).slice(0,200));
      return null;
    }

    const done     = await shopifyREST('PUT',`draft_orders/${draft.draft_order.id}/complete.json`);
    const newOid   = String(done?.draft_order?.order_id||'');
    const newName  = done?.draft_order?.name || `#${excTag}`;

    // Store EXC9001 as display name — Shopify assigns its own #916xxx number
    await supabase.from('requests').update({
      exchange_order_id:     newOid,
      exchange_order_name:   excTag,        // Show as EXC9001 in our dashboard
      exchange_order_number: excTag,
      exchange_shopify_name: newName,       // Store actual Shopify #916xxx for reference
      status: 'exchange_fulfilled'
    }).eq('req_id',request.req_id);

    // Tag original order
    await updateOrderTags(request.order_id,['exchange-fulfilled',excTag],[]);
    await auditLog(request.order_id, request.req_id, 'exchange_created', 'system', `${excTag} (Shopify:${newName}) orderId:${newOid}`);
    console.log(`[Exchange] Created ${excTag} (${newName}) for ${request.req_id}`);
    return { order_id:newOid, order_name:excTag, shopify_name:newName, exc_tag:excTag };
  } catch(e) { console.error('[createExchange]', e.message); return null; }
}

// ══════════════════════════════════════════
// PROCESS REFUND
// ══════════════════════════════════════════
async function processRefund(request) {
  try {
    const returnItems = (request.items||[]).filter(i=>i.action==='return');
    if (!returnItems.length) return null;

    // ── DOUBLE-REFUND GUARD ──
    // 1. Check our DB — if already refunded, skip
    const { data:dbReq } = await supabase.from('requests').select('status,refund_id,refund_amount').eq('req_id',request.req_id).single();
    if (dbReq?.status === 'refunded' || dbReq?.refund_id) {
      console.log(`[Refund] SKIP — already refunded (DB): ${request.req_id} refund_id:${dbReq.refund_id} amount:${dbReq.refund_amount}`);
      return null;
    }

    // 2. Check Shopify — use refundable_quantity on each line item
    // If all return items have refundable_quantity = 0, they're already fully refunded
    const fo = await shopifyREST('GET',`orders/${request.order_id}.json?fields=line_items,financial_status`);
    const lines = fo?.order?.line_items||[];
    if (!lines.length) return null;

    const returnIds = returnItems.map(i=>String(i.id)).filter(Boolean);
    const useLines  = returnIds.length ? lines.filter(li=>returnIds.includes(String(li.id))) : lines;

    // Shopify refundable_quantity check — if every return line item has 0 refundable quantity, already refunded
    const allAlreadyRefunded = useLines.length > 0 && useLines.every(li => {
      const reqItem = returnItems.find(i=>String(i.id)===String(li.id));
      const wantQty = parseInt(reqItem?.qty) || li.quantity;
      const refundable = parseInt(li.refundable_quantity ?? li.quantity);
      return refundable < wantQty; // less than what we want to refund = partially or fully done
    });
    if (allAlreadyRefunded) {
      console.log(`[Refund] SKIP — Shopify refundable_quantity=0 for all return items in ${request.req_id}`);
      await supabase.from('requests').update({ status:'refunded' }).eq('req_id',request.req_id);
      return null;
    }

    // Fetch primary location for restocking (Shopify requires location_id with restock_type:'return')
    let locationId = null;
    try { const locs = await shopifyREST('GET','locations.json?active=true&limit=1'); locationId = locs?.locations?.[0]?.id||null; } catch(e) {}

    // Use the customer's requested qty, not the full order qty (e.g. ordered 2, returning 1)
    const rli = useLines.map(li=>{ const reqItem=returnItems.find(i=>String(i.id)===String(li.id)); const qty=Math.min(parseInt(reqItem?.qty)||li.quantity, li.quantity); const item={line_item_id:li.id,quantity:qty,restock_type:locationId?'return':'no_restock'}; if(locationId)item.location_id=locationId; return item; });

    const calc = await shopifyREST('POST',`orders/${request.order_id}/refunds/calculate.json`,{ refund:{ refund_line_items:rli } });
    if (calc.errors) { console.error('[Refund calc]',calc.errors); return null; }

    let txns = calc?.refund?.transactions||[];
    const fee = parseFloat(RESTOCKING_FEE_PCT);
    if (fee>0) txns=txns.map(t=>({...t,amount:(parseFloat(t.amount||0)*(1-fee/100)).toFixed(2)}));
    // Deduct ₹100 return shipping fee for original payment
    if (RETURN_SHIPPING_FEE>0) {
      txns=txns.map(t=>({...t,amount:Math.max(0,parseFloat(t.amount||0)-RETURN_SHIPPING_FEE).toFixed(2)}));
    }

    const refundPayload = {
      refund:{
        notify:true,
        note:`Return delivered to warehouse — ${request.req_id}`,
        refund_line_items: rli,
        transactions: request.refund_method==='store_credit' ? [] :
          txns.map(t=>({ parent_id:t.parent_id, amount:t.amount, kind:'refund', gateway:t.gateway }))
      }
    };

    // ── STORE CREDIT PATH — issue gift card BEFORE calling Shopify refund ──
    if (request.refund_method==='store_credit') {
      const base  = parseFloat(txns[0]?.amount || returnItems.reduce((s,i)=>s+parseFloat(i.price||0)*(parseInt(i.qty)||1),0));
      const bonus = 0; // No bonus — store credit same as refund amount
      const total = base.toFixed(2);
      const code  = `CREDIT-${String(request.order_id).slice(-6)}-${uid().toUpperCase().slice(0,6)}`;

      // Get customer ID to attach gift card
      let customerId = null;
      try {
        const cd = await shopifyREST('GET',`orders/${request.order_id}.json?fields=customer`);
        customerId = cd?.order?.customer?.id || null;
      } catch(e) {}

      // Create gift card
      let gcr = null;
      try {
        const gcp = { gift_card:{ initial_value:total, code, note:`Store credit — ${request.req_id}` } };
        if (customerId) gcp.gift_card.customer_id = customerId;
        gcr = await shopifyREST('POST','gift_cards.json', gcp);
      } catch(e) { console.error('[GiftCard]', e.message); }

      if (!gcr?.gift_card?.id) {
        // Fallback: save code to order note
        const en = (await shopifyREST('GET',`orders/${request.order_id}.json?fields=note`))?.order?.note||'';
        await shopifyREST('PUT',`orders/${request.order_id}.json`,{
          order:{ id:request.order_id, note:(en+`\n[STORE CREDIT] Code:${code} ₹${total} REQ:${request.req_id} ${new Date().toISOString()}`).slice(0,5000) }
        });
      }

      // Also call Shopify refund to restock inventory (no transaction = no payment reversal)
      try {
        await shopifyREST('POST',`orders/${request.order_id}/refunds.json`,{
          refund:{ notify:false, note:`Store credit issued — ${request.req_id}`, refund_line_items:rli.map(r=>({...r,location_id:locationId||undefined})), transactions:[] }
        });
      } catch(e) { console.error('[Refund restock]', e.message); }

      await updateOrderTags(request.order_id,['store-credit-issued','return-refunded'],[]);
      await supabase.from('requests').update({ refund_amount:parseFloat(total), status:'refunded' }).eq('req_id',request.req_id);
      await auditLog(request.order_id, request.req_id, 'store_credit_auto', 'system', `${code} ₹${total}`);
      console.log(`[StoreCredit] ${code} ₹${total} for ${request.req_id}`);
      return { code, amount:total, method:'store_credit' };
    }

    // ── ORIGINAL PAYMENT PATH ──
    const result = await shopifyREST('POST',`orders/${request.order_id}/refunds.json`,refundPayload);
    if (result?.refund?.id) {
      const amount = result.refund.transactions?.[0]?.amount || txns[0]?.amount || '0';
      await supabase.from('requests').update({ refund_id:String(result.refund.id), refund_amount:parseFloat(amount), status:'refunded' }).eq('req_id',request.req_id);
      await updateOrderTags(request.order_id,['return-refunded'],[]);
      await auditLog(request.order_id, request.req_id, 'refunded_original', 'system', `₹${amount} to original payment`);
      console.log(`[Refund] ₹${amount} original payment for ${request.req_id}`);
      return { refund_id:result.refund.id, amount, method:'original' };
    }
    console.error('[Refund] Failed:', JSON.stringify(result?.errors||result).slice(0,200));
    return null;
  } catch(e) { console.error('[processRefund]', e.message); return null; }
}

// ══════════════════════════════════════════
// ARCHIVE
// ══════════════════════════════════════════
async function archiveRequest(req_id, order_id) {
  try {
    // If it's a return with no refund yet, auto-process refund before archiving
    const { data:req } = await supabase.from('requests').select('*').eq('req_id',req_id).single();
    if (req?.request_type==='return' && !req?.refund_id) {
      try { await processRefund(req); console.log('[archive] auto-refunded',req_id); }
      catch(e) { console.error('[archive refund]', e.message); }
    }
    await supabase.from('requests').update({ status:'archived', archived_at:new Date().toISOString() }).eq('req_id',req_id);
    await auditLog(order_id, req_id, 'archived', 'system', 'Auto-archived after completion');
  } catch(e) { console.error('[archive]', e.message); }
}

// ══════════════════════════════════════════
// STATIC FRONTEND
// ══════════════════════════════════════════
// ══════════════════════════════════════════
// ADMIN AUTH  (dashboard only — portal is public)
// ══════════════════════════════════════════
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'blakc2024';
const AUTH_SECRET    = process.env.AUTH_SECRET    || crypto.randomBytes(32).toString('hex');

function signToken(val) {
  return val + '.' + crypto.createHmac('sha256', AUTH_SECRET).update(val).digest('hex');
}
function verifyToken(token) {
  if (!token) return false;
  const [val, sig] = token.split('.');
  if (!val || !sig) return false;
  const expected = crypto.createHmac('sha256', AUTH_SECRET).update(val).digest('hex');
  return crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(expected)) && val === 'admin';
}
function parseCookies(req) {
  const list = {};
  (req.headers.cookie || '').split(';').forEach(c => {
    const [k, ...v] = c.trim().split('=');
    if (k) list[k.trim()] = decodeURIComponent(v.join('='));
  });
  return list;
}
function requireAdmin(req, res, next) {
  const token = parseCookies(req)['_blakc_admin'];
  if (verifyToken(token)) return next();
  res.redirect('/admin/login?next=' + encodeURIComponent(req.url));
}

// Login page
app.get('/admin/login', (req, res) => {
  const err = req.query.err ? '<p style="color:#ef4444;font-size:13px;margin-bottom:12px">Incorrect password. Try again.</p>' : '';
  res.send(`<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
  <title>BLAKC Admin Login</title>
  <style>*{box-sizing:border-box;margin:0;padding:0}body{font-family:-apple-system,sans-serif;background:#0a0a0a;display:flex;align-items:center;justify-content:center;min-height:100vh}
  .card{background:#fff;border-radius:16px;padding:40px 36px;width:360px;box-shadow:0 20px 60px rgba(0,0,0,.4)}
  .logo{font-size:24px;font-weight:900;letter-spacing:2px;color:#0a0a0a;text-align:center;margin-bottom:6px}
  .sub{font-size:12px;color:#9ca3af;text-align:center;margin-bottom:28px;text-transform:uppercase;letter-spacing:1px}
  label{font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.8px;display:block;margin-bottom:6px}
  input{width:100%;padding:11px 14px;border:1.5px solid #e5e7eb;border-radius:8px;font-size:14px;outline:none;transition:.15s}
  input:focus{border-color:#0a0a0a}
  button{width:100%;padding:12px;background:#0a0a0a;color:#fff;border:none;border-radius:8px;font-size:14px;font-weight:700;cursor:pointer;margin-top:16px;transition:.15s}
  button:hover{background:#1f2937}</style></head>
  <body><div class="card">
    <div class="logo">BLAKC</div>
    <div class="sub">Returns Manager</div>
    ${err}
    <form method="POST" action="/admin/login">
      <input type="hidden" name="next" value="${req.query.next||'/dashboard'}">
      <label>Password</label>
      <input type="password" name="password" autofocus placeholder="Enter admin password">
      <button type="submit">Sign In →</button>
    </form>
  </div></body></html>`);
});

app.use(express.urlencoded({ extended: false }));

app.post('/admin/login', (req, res) => {
  const { password, next } = req.body;
  if (password === ADMIN_PASSWORD) {
    const token = signToken('admin');
    res.setHeader('Set-Cookie', `_blakc_admin=${encodeURIComponent(token)};Path=/;HttpOnly;SameSite=Strict;Max-Age=86400`);
    res.redirect(next || '/dashboard');
  } else {
    res.redirect('/admin/login?err=1&next=' + encodeURIComponent(next||'/dashboard'));
  }
});

app.get('/admin/logout', (_req, res) => {
  res.setHeader('Set-Cookie', '_blakc_admin=;Path=/;HttpOnly;Max-Age=0');
  res.redirect('/admin/login');
});

// Protected dashboard — portal stays public (customers use it)
app.get('/dashboard', requireAdmin, (_req,res)=>{ const f=path.join(__dirname,'index.html'); fs.existsSync(f)?res.sendFile(f):res.send('<h2>Upload index.html</h2>'); });
app.get('/portal',    (_req,res)=>{ const f=path.join(__dirname,'portal.html'); fs.existsSync(f)?res.sendFile(f):res.send('<h2>Upload portal.html</h2>'); });
app.get('/',          (_req,res)=>res.redirect('/dashboard'));

// ══════════════════════════════════════════
// AUTH
// ══════════════════════════════════════════
// Public API routes (portal-facing — no admin auth needed)
const PUBLIC_API = ['/api/lookup','/api/returns/request','/api/portal/','/api/payments/','/api/status'];
app.use('/api', (req, res, next) => {
  const isPublic = PUBLIC_API.some(p => req.path.startsWith(p.replace('/api','')));
  if (isPublic) return next();
  const token = parseCookies(req)['_blakc_admin'];
  if (verifyToken(token)) return next();
  res.status(401).json({ error:'Unauthorized' });
});

app.get('/api/status', (_req,res)=>res.json({ connected:!!ACCESS_TOKEN, shop:SHOP_DOMAIN||null, return_window:RETURN_WINDOW_DAYS, store_credit_bonus:STORE_CREDIT_BONUS }));

// ── DEBUG LOOKUP (remove after testing) ──
app.get('/api/debug/lookup', async (req,res)=>{
  const { n } = req.query;
  if (!n) return res.json({ error:'pass ?n=ordernumber' });
  const clean = String(n).replace(/^#+/,'').trim();
  const results = {};

  // Test 1: GraphQL with #
  try {
    const g1 = await graphql(`{ orders(first:3, query:"name:#${clean}") { edges { node { id name legacyResourceId displayFinancialStatus shippingAddress { zip } } } } }`);
    results.gql_with_hash = g1?.data?.orders?.edges || g1?.errors || 'no data';
  } catch(e) { results.gql_with_hash = 'ERROR: '+e.message; }

  // Test 2: GraphQL without #
  try {
    const g2 = await graphql(`{ orders(first:3, query:"name:${clean}") { edges { node { id name legacyResourceId displayFinancialStatus shippingAddress { zip } } } } }`);
    results.gql_no_hash = g2?.data?.orders?.edges || g2?.errors || 'no data';
  } catch(e) { results.gql_no_hash = 'ERROR: '+e.message; }

  // Test 3: REST with #
  try {
    const r1 = await shopifyREST('GET', `orders.json?name=%23${clean}&status=any&fields=id,order_number,name,tags,shipping_address`);
    results.rest_with_hash = r1?.orders || r1?.errors || r1;
  } catch(e) { results.rest_with_hash = 'ERROR: '+e.message; }

  // Test 4: REST without #
  try {
    const r2 = await shopifyREST('GET', `orders.json?name=${clean}&status=any&fields=id,order_number,name,tags,shipping_address`);
    results.rest_no_hash = r2?.orders || r2?.errors || r2;
  } catch(e) { results.rest_no_hash = 'ERROR: '+e.message; }

  // Test 5: REST by order_number field
  try {
    const r3 = await shopifyREST('GET', `orders.json?status=any&fields=id,order_number,name,shipping_address&limit=5`);
    results.rest_recent5 = (r3?.orders||[]).map(o=>({ id:o.id, number:o.order_number, name:o.name, zip:o.shipping_address?.zip }));
  } catch(e) { results.rest_recent5 = 'ERROR: '+e.message; }

  res.json({ searching_for:clean, auth:!!ACCESS_TOKEN, shop:SHOP_DOMAIN, results });
});


// ── SHOW TOKEN (for setting env var) ──
app.get('/api/showtoken', async (_req,res)=>{
  const safeToken = (ACCESS_TOKEN||'NOT SET - visit /auth first').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  res.send(`
    <html><body style="font-family:sans-serif;padding:40px;background:#0f0f11;color:#f0f0f5">
    <h2 style="color:#7effa0">Current Token</h2>
    <p style="color:#aaa">Set this as <strong style="color:#fff">ACCESS_TOKEN</strong> on Render:</p>
    <div id="tok" style="background:#1a1a2e;padding:20px;border-radius:8px;font-family:monospace;font-size:14px;word-break:break-all;color:#5ee8ff;border:1px solid #333;margin:20px 0">${safeToken}</div>
    <p style="color:#aaa">Shop: <strong style="color:#fff">${(SHOP_DOMAIN||'NOT SET').replace(/</g,'&lt;')}</strong></p>
    <button onclick="navigator.clipboard.writeText(document.getElementById('tok').textContent.trim());this.textContent='Copied!'"
      style="background:#7effa0;color:#000;border:none;padding:10px 24px;border-radius:6px;font-size:14px;font-weight:700;cursor:pointer">
      Copy Token
    </button>
    <p style="margin-top:20px"><a href="/dashboard" style="color:#5ee8ff">Go to Dashboard →</a></p>
    </body></html>
  `);
});


// ── DEBUG DELHIVERY ──
app.get('/api/debug/delhivery', async (_req,res)=>{
  const out = { config:{ token_set:!!DELHIVERY_TOKEN, token_preview:DELHIVERY_TOKEN?DELHIVERY_TOKEN.slice(0,8)+'...':'NOT SET', warehouse:DELHIVERY_WAREHOUSE, base:DELHIVERY_BASE } };

  // Test token — pincode check
  try {
    const r = await delhiveryAPI('GET', '/c/api/pin-codes/json/?filter_codes=400001');
    out.token_test = { status:'ok', data:r };
  } catch(e) { out.token_test = { status:'error', msg:e.message }; }

  // Show pending requests from Supabase
  try {
    const twoHoursAgo = new Date(Date.now()-2*60*60*1000).toISOString();
    const { data, error } = await supabase.from('requests').select('req_id,status,awb,approved_at,address').eq('status','approved').is('awb',null).order('approved_at',{ascending:false}).limit(5);
    out.pending_pickups = error ? { error:error.message } : (data||[]);
    // Also show all requests
    const { data:all } = await supabase.from('requests').select('req_id,status,awb,approved_at').order('created_at',{ascending:false}).limit(10);
    out.recent_requests = all||[];
  } catch(e) { out.supabase = 'ERROR: '+e.message; }

  res.json(out);
});

// ── MANUAL PICKUP TRIGGER (GET for browser, POST for API) ──
app.get('/api/debug/trigger-pickup/:req_id', async (req,res)=>{
  try {
    const { data:request } = await supabase.from('requests').select('*').eq('req_id',req.params.req_id).single();
    if (!request) return res.status(404).json({ error:'Request not found in Supabase' });

    const addr  = request.address || {};
    const items = request.items   || [];
    const pincode = addr.zip || addr.pincode || '';

    // Step 1: Serviceability check
    let svcResult = null;
    try {
      svcResult = await delhiveryAPI('GET', `/c/api/pin-codes/json/?filter_codes=${pincode}`);
    } catch(e) { svcResult = { error: e.message }; }

    // Step 2: Build payload
    const totalQty    = items.reduce((s,i)=>s+(parseInt(i.qty)||1),0)||1;
    const totalAmount = items.reduce((s,i)=>s+parseFloat(i.price||0)*(parseInt(i.qty)||1),0)||0;
    const rvpId       = `#9${request.order_number}_${(request.req_id||"").replace(/^[^_]+_/,"")}`;
    const payload = {
      pickup_location: { name: DELHIVERY_WAREHOUSE },
      shipments: [{
        name:         (addr.name||'Customer').slice(0,50),
        add:          [addr.address1,addr.address2].filter(Boolean).join(', ').slice(0,200)||'N/A',
        pin:          String(pincode||'400001'),
        city:         addr.city||'',
        state:        addr.province||addr.state||'',
        country:      'India',
        phone:        String(addr.phone||'').replace(/\D/g,'').slice(-10)||'9999999999',
        order:        rvpId,
        payment_mode: 'Pickup',
        products_desc:'Return Shipment',
        hsn_code:     '62034200',
        cod_amount:   '0',
        order_date:   new Date().toISOString().split('T')[0],
        total_amount: String(totalAmount.toFixed(2)),
        seller_name:  DELHIVERY_WAREHOUSE,
        seller_inv:   `INV-${request.order_number}`,
        quantity:     totalQty,
        weight:       0.5,
        shipment_length:30, shipment_width:25, shipment_height:10
      }]
    };

    // Step 3: Call Delhivery — get RAW response
    let delhiveryRaw = null;
    try {
      delhiveryRaw = await delhiveryAPI('POST', '/api/cmu/create.json', payload, true);
    } catch(e) { delhiveryRaw = { error: e.message }; }

    const waybill = delhiveryRaw?.packages?.[0]?.waybill || delhiveryRaw?.waybill;

    res.json({
      success: !!waybill,
      waybill,
      req_id: request.req_id,
      address: addr,
      serviceability: svcResult?.delivery_codes?.[0]?.postal_code || svcResult,
      payload_sent: payload,
      delhivery_response: delhiveryRaw
    });
  } catch(e) { res.status(500).json({ error:e.message }); }
});
// ══════════════════════════════════════════
// MIGRATION: backfill requests from Shopify notes → Supabase
// GET /api/migrate/from-notes  — safe to run multiple times (skips existing req_ids)
// ══════════════════════════════════════════
app.get('/api/migrate/from-notes', async (_req, res) => {
  try {
    // Load all existing req_ids from Supabase so we skip them
    const { data: existing } = await supabase.from('requests').select('req_id');
    const existingIds = new Set((existing||[]).map(r => r.req_id));

    // Fetch all orders with return-related tags (paginate through all pages)
    const returnTags = ['return-requested','exchange-requested','mixed-requested','return-approved','exchange-approved','return-refunded','exchange-fulfilled','pickup-scheduled'];
    let allOrders = [];
    for (const tag of returnTags) {
      let pageInfo = null; let hasNext = true;
      while (hasNext) {
        const afterClause = pageInfo ? `,after:${JSON.stringify(pageInfo)}` : '';
        const gql = await graphql(`{orders(first:50,query:"tag:${tag}"${afterClause}){edges{cursor node{id name tags note email phone createdAt customer{id displayName email phone} shippingAddress{name address1 address2 city province zip country phone} lineItems(first:20){edges{node{id title quantity originalUnitPriceSet{shopMoney{amount}} discountedUnitPriceSet{shopMoney{amount}} variant{id} product{id}}}}}} pageInfo{hasNextPage endCursor}}}`);
        const edges = gql?.data?.orders?.edges || [];
        edges.forEach(e => { if (!allOrders.find(o => o.id === e.node.id)) allOrders.push(e.node); });
        hasNext = gql?.data?.orders?.pageInfo?.hasNextPage || false;
        pageInfo = gql?.data?.orders?.pageInfo?.endCursor || null;
      }
    }

    console.log(`[Migrate] Found ${allOrders.length} orders with return tags`);

    // Parse note blocks and insert missing ones
    const NOTE_RE = /---([A-Z0-9_]+)---\s*\nType:(\w+)\s*\nItems:\s*\n([\s\S]*?)\nRefund:(\w+)(?:\nNote:(.*?))?\nDate:([\d\-T:.Z]+)\s*\n---END---/g;
    const inserted = [], skipped = [], errors = [];

    const MIGRATE_SKIP_ORDERS = ['917030','916834','EXC16862']; // test/invalid orders to exclude
    for (const o of allOrders) {
      if (!o.note) continue;
      const note = o.note;
      const oid = gidToId(o.id);
      const orderNum = String(o.name||'').replace('#','');
      if (MIGRATE_SKIP_ORDERS.includes(orderNum)) { console.log('[Migrate] Skipping test order #'+orderNum); continue; }
      const allTags = Array.isArray(o.tags) ? o.tags : (o.tags||'').split(',').map(t=>t.trim()).filter(Boolean);

      // Determine status from tags
      const has = t => allTags.includes(t);
      const status = has('return-refunded')?'refunded':has('exchange-fulfilled')?'exchange_fulfilled':has('pickup-scan')?'picked_up':has('pickup-scheduled')?'pickup_scheduled':has('return-approved')||has('exchange-approved')||has('mixed-approved')?'approved':'pending';
      const request_type = has('exchange-requested')||has('exchange-approved')||has('exchange-fulfilled')?'exchange':has('mixed-requested')||has('mixed-approved')?'mixed':'return';

      // Find AWB from note
      const awbMatch = note.match(/DELHIVERY AWB:\s*([\w\-]+)/);
      const awb = awbMatch ? awbMatch[1] : null;

      let match;
      NOTE_RE.lastIndex = 0;
      while ((match = NOTE_RE.exec(note)) !== null) {
        const [, req_id, type, itemsRaw, refund_method, customer_note_raw, submitted_at] = match;
        if (existingIds.has(req_id)) { skipped.push(req_id); continue; }

        // Parse items from the pipe-separated line format
        const items = itemsRaw.split('|').map(s => s.trim()).filter(Boolean).map(s => {
          const actionMatch = s.match(/^\[(\w+)\]/);
          const action = actionMatch ? actionMatch[1].toLowerCase() : 'return';
          const rest = s.replace(/^\[\w+\]\s*/, '');
          const titleMatch = rest.match(/^(.+?)\s*-\s*(\S+)\s+x(\d+)/);
          const exchMatch = rest.match(/ExchVarID:(\d+)/);
          return {
            title: titleMatch ? titleMatch[1].trim() : rest,
            variant_title: titleMatch ? titleMatch[2] : null,
            qty: titleMatch ? parseInt(titleMatch[3]) : 1,
            action,
            exchange_variant_id: exchMatch ? exchMatch[1] : null,
            price: '0', original_price: '0', discount_allocated: '0'
          };
        });

        // Build req_num from req_id (e.g. 916907_RET001 → 1)
        const numMatch = req_id.match(/(\d+)$/);
        const req_num = numMatch ? parseInt(numMatch[1]) : null;
        const order_number = String(o.name||'').replace('#','');

        const sa = o.shippingAddress;
        const address = sa ? { name:sa.name||'', address1:sa.address1||'', address2:sa.address2||'', city:sa.city||'', province:sa.province||'', zip:sa.zip||'', country:sa.country||'India', phone:sa.phone||'' } : null;

        const record = {
          req_id, req_num, order_id: String(oid), order_number,
          items, refund_method: refund_method || 'store_credit',
          status, request_type,
          shipping_preference: 'pickup',
          total_price: 0, is_cod: false, days_since_order: 0,
          address,
          customer_name:  o.customer?.displayName || sa?.name || '',
          customer_email: o.customer?.email || o.email || '',
          customer_phone: o.customer?.phone || o.phone || sa?.phone || '',
          customer_id:    o.customer ? gidToId(o.customer.id) : null,
          awb: awb || null,
          submitted_at: submitted_at || new Date().toISOString(),
          created_at: submitted_at || new Date().toISOString()
        };

        const { error: ie } = await supabase.from('requests').insert(record);
        if (ie) { console.error('[Migrate] Insert error:', req_id, ie.message); errors.push({req_id, error: ie.message}); }
        else { inserted.push(req_id); existingIds.add(req_id); console.log('[Migrate] Inserted:', req_id); }
      }
    }

    res.json({ success: true, scanned: allOrders.length, inserted: inserted.length, skipped: skipped.length, errors: errors.length, inserted_ids: inserted, error_details: errors });
  } catch(e) { console.error('[Migrate]', e.message); res.status(500).json({ error: e.message }); }
});

// GET /api/migrate/preview — dry-run: shows what WOULD be migrated without inserting
app.get('/api/migrate/preview', async (_req, res) => {
  try {
    const { data: existing } = await supabase.from('requests').select('req_id,order_id');
    const existingIds = new Set((existing||[]).map(r => r.req_id));
    const existingOrderIds = new Set((existing||[]).map(r => r.order_id));

    const returnTags = ['return-requested','exchange-requested','mixed-requested','return-approved','exchange-approved','return-refunded','exchange-fulfilled','pickup-scheduled'];
    let allOrders = [];
    for (const tag of returnTags) {
      let pageInfo = null; let hasNext = true;
      while (hasNext) {
        const afterClause = pageInfo ? `,after:${JSON.stringify(pageInfo)}` : '';
        const gql = await graphql(`{orders(first:50,query:"tag:${tag}"${afterClause}){edges{cursor node{id name tags note}} pageInfo{hasNextPage endCursor}}}`);
        const edges = gql?.data?.orders?.edges || [];
        edges.forEach(e => { if (!allOrders.find(o => o.id === e.node.id)) allOrders.push(e.node); });
        hasNext = gql?.data?.orders?.pageInfo?.hasNextPage || false;
        pageInfo = gql?.data?.orders?.pageInfo?.endCursor || null;
      }
    }

    const NOTE_RE = /---([A-Z0-9_]+)---\s*\nType:(\w+)\s*\nItems:\s*\n([\s\S]*?)\nRefund:(\w+)(?:\nNote:(.*?))?\nDate:([\d\-T:.Z]+)\s*\n---END---/g;
    const toInsert = [], alreadyIn = [];

    const MIGRATE_SKIP_ORDERS_P = ['917030','916834','EXC16862']; // test/invalid orders to exclude
    for (const o of allOrders) {
      if (!o.note) continue;
      const orderNumP = String(o.name||'').replace('#','');
      if (MIGRATE_SKIP_ORDERS_P.includes(orderNumP)) continue;
      const oid = gidToId(o.id);
      const allTags = Array.isArray(o.tags) ? o.tags : (o.tags||'').split(',').map(t=>t.trim()).filter(Boolean);
      const has = t => allTags.includes(t);
      const status = has('return-refunded')?'refunded':has('exchange-fulfilled')?'exchange_fulfilled':has('pickup-scan')?'picked_up':has('pickup-scheduled')?'pickup_scheduled':has('return-approved')||has('exchange-approved')||has('mixed-approved')?'approved':'pending';
      const awbMatch = o.note.match(/DELHIVERY AWB:\s*([\w\-]+)/);

      NOTE_RE.lastIndex = 0;
      let match;
      while ((match = NOTE_RE.exec(o.note)) !== null) {
        const [, req_id, type] = match;
        const entry = { req_id, order_number: String(o.name||'').replace('#',''), order_id: String(oid), status, type, awb: awbMatch?awbMatch[1]:null };
        if (existingIds.has(req_id)) alreadyIn.push(entry);
        else toInsert.push(entry);
      }
    }

    // Orders in Supabase with no AWB and status=approved
    const { data: needsPickup } = await supabase.from('requests').select('req_id,order_id,order_number,status,awb,request_type').eq('status','approved').is('awb',null);

    res.json({
      scanned_orders: allOrders.length,
      will_insert: toInsert.length,
      already_in_supabase: alreadyIn.length,
      to_insert: toInsert,
      approved_no_awb: (needsPickup||[]).map(r=>({ req_id:r.req_id, order_number:r.order_number, type:r.request_type }))
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/debug/trigger-pickup/:req_id', async (req,res)=>{
  try {
    const { data:request } = await supabase.from('requests').select('*').eq('req_id',req.params.req_id).single();
    if (!request) return res.status(404).json({ error:'Request not found' });
    console.log('[Manual Pickup] Triggering for:', request.req_id);
    const waybill = await createDelhiveryPickup(request);
    res.json({ success:!!waybill, waybill, request_status:request.status });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

app.get('/auth', (req,res)=>{ const shop=req.query.shop||SHOP_DOMAIN; if(!shop)return res.status(400).send('Missing ?shop='); res.redirect(`https://${shop}/admin/oauth/authorize?client_id=${SHOPIFY_API_KEY}&scope=${SCOPES}&redirect_uri=${BACKEND_URL}/auth/callback&state=rms`); });
app.get('/auth/callback', async (req,res)=>{
  const { shop,code }=req.query; if(!shop||!code)return res.status(400).send('Missing params');
  try {
    const r=await fetch(`https://${shop}/admin/oauth/access_token`,{ method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ client_id:SHOPIFY_API_KEY,client_secret:SHOPIFY_API_SECRET,code }) });
    const d=await r.json();
    if (d.access_token) {
      ACCESS_TOKEN=d.access_token; SHOP_DOMAIN=shop;
      try { await supabase.from('settings').upsert({ key:'auth', value:{ access_token:ACCESS_TOKEN, shop_domain:SHOP_DOMAIN } }); } catch(e){}
      console.log('=== ACCESS TOKEN ===', d.access_token);
      // Show token on screen so it can be copied to Render env vars
      const safeToken = d.access_token.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
      res.send(`
        <html><body style="font-family:sans-serif;padding:40px;background:#0f0f11;color:#f0f0f5">
        <h2 style="color:#7effa0">&#x2705; Connected to ${shop.replace(/</g,'&lt;')}</h2>
        <p style="color:#aaa">Copy this token and set it as <strong style="color:#fff">ACCESS_TOKEN</strong> env var on Render:</p>
        <div id="tok" style="background:#1a1a2e;padding:20px;border-radius:8px;font-family:monospace;font-size:14px;word-break:break-all;color:#5ee8ff;border:1px solid #333;margin:20px 0">${safeToken}</div>
        <button onclick="navigator.clipboard.writeText(document.getElementById('tok').textContent.trim());this.textContent='Copied!'"
          style="background:#7effa0;color:#000;border:none;padding:10px 24px;border-radius:6px;font-size:14px;font-weight:700;cursor:pointer">
          Copy Token
        </button>
        <p style="margin-top:20px;color:#aaa">After setting it on Render: <a href="/dashboard" style="color:#5ee8ff">Open Dashboard &#x2192;</a></p>
        </body></html>
      `);
    } else { res.status(400).json(d); }
  } catch(e) { res.status(500).send(e.message); }
});

// ══════════════════════════════════════════
// ORDERS  (dashboard list)
// ══════════════════════════════════════════
app.get('/api/orders', async (_req,res)=>{
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
            discountedUnitPriceSet{shopMoney{amount}}
            discountAllocations{allocatedAmountSet{shopMoney{amount}}}
            variant{id image{url}}
            product{id images(first:1){edges{node{url}}}}
          }}}
        }}
      }
    }`);
    if (result.errors) return res.status(400).json({ error:result.errors[0].message });

    // Load all requests from Supabase (including archived, so Archived tab works)
    const { data:allReqs } = await supabase.from('requests').select('*').order('created_at',{ascending:true});
    const reqByOrder = {};
    (allReqs||[]).forEach(r=>{ if(!reqByOrder[String(r.order_id)])reqByOrder[String(r.order_id)]=[]; reqByOrder[String(r.order_id)].push(r); });

    function buildReturnStatus(allTags) {
      const has    = t=>allTags.includes(t);
      const hasAny = arr=>arr.some(t=>allTags.includes(t));
      return {
        rs: has('return-refunded')?'refunded':has('exchange-fulfilled')?'fulfilled':(has('return-inspected')||has('exchange-inspected'))?'inspected':(has('pickup-scheduled')||has('pickup-scan'))?'received':hasAny(['return-approved','exchange-approved','mixed-approved'])?'approved':hasAny(['return-rejected','exchange-rejected','mixed-rejected'])?'rejected':has('return-requested')?'pending':has('exchange-requested')?'exchange-pending':has('mixed-requested')?'pending':null,
        rt: hasAny(['exchange-requested','exchange-approved','exchange-fulfilled'])?'exchange':hasAny(['mixed-requested','mixed-approved'])?'mixed':'return'
      };
    }

    const orders = result.data.orders.edges.map(({ node:o })=>{
      const allTags = Array.isArray(o.tags)?o.tags:(o.tags||'').split(',').map(t=>t.trim()).filter(Boolean);
      const oid = gidToId(o.id);
      const sa  = o.shippingAddress;
      const latestReq = reqByOrder[oid]?.slice(-1)[0];
      // Use Supabase as source of truth for status; fall back to tags only for request_type detection on tag-only orders
      const rs = latestReq?.status || null;
      const rt = latestReq?.request_type || buildReturnStatus(allTags).rt;
      return {
        id:oid, gid:o.id, order_number:o.name.replace('#',''), created_at:o.createdAt,
        financial_status:(o.displayFinancialStatus||'').toLowerCase(),
        fulfillment_status:(o.displayFulfillmentStatus||'').toLowerCase(),
        total_price:o.totalPriceSet.shopMoney.amount, currency:o.totalPriceSet.shopMoney.currencyCode,
        tags:allTags, note:o.note||'',
        customer_name:  o.customer?.displayName||(sa?sa.name||[sa.firstName,sa.lastName].filter(Boolean).join(' '):'')|| '',
        customer_email: o.customer?.email||o.email||'',
        customer_phone: o.customer?.phone||o.phone||sa?.phone||'',
        customer_id:    o.customer?gidToId(o.customer.id):null,
        shipping_address: sa?{ name:sa.name||[sa.firstName,sa.lastName].filter(Boolean).join(' '), address1:sa.address1||'', address2:sa.address2||'', city:sa.city||'', province:sa.province||'', zip:sa.zip||'', country:sa.country||'', phone:sa.phone||'' }:null,
        line_items: o.lineItems.edges.map(({ node:li })=>{
          const origPrice = parseFloat(li.originalUnitPriceSet?.shopMoney?.amount||'0');
          const netPrice  = parseFloat(li.discountedUnitPriceSet?.shopMoney?.amount||li.originalUnitPriceSet?.shopMoney?.amount||'0');
          const qty = li.quantity||1;
          const totalDisc = Math.max(0, (origPrice - netPrice) * qty);
          return { id:gidToId(li.id),gid:li.id,title:li.title,quantity:qty,
            price: netPrice.toFixed(2),
            original_price: origPrice.toFixed(2),
            discount_allocated: totalDisc.toFixed(2),
            variant_id:li.variant?.id?gidToId(li.variant.id):null,
            image_url:li.variant?.image?.url||li.product?.images?.edges?.[0]?.node?.url||null,
            product_id:li.product?.id?gidToId(li.product.id):null };
        }),
        return_status: rs, request_type: rt,
        requests: reqByOrder[oid]||[]
      };
    });

    // Also fetch orders that have Supabase requests but weren't in the top 250 from Shopify
    const fetchedIds = new Set(orders.map(o=>String(o.id)));
    const missingIds = Object.keys(reqByOrder).filter(id=>!fetchedIds.has(id));
    if (missingIds.length) {
      const extras = await Promise.all(missingIds.map(async (oid)=>{
        try {
          const d = await shopifyREST('GET',`orders/${oid}.json?fields=id,name,created_at,tags,note,financial_status,fulfillment_status,total_price,currency,email,phone,customer,shipping_address,line_items`);
          if (!d.order) return null;
          const ro = d.order;
          const allTags = (ro.tags||'').split(',').map(t=>t.trim()).filter(Boolean);
          const latestReq2 = reqByOrder[oid]?.slice(-1)[0];
          const rs = latestReq2?.status || null;
          const rt = latestReq2?.request_type || buildReturnStatus(allTags).rt;
          const sa = ro.shipping_address;
          return {
            id:String(oid), gid:null, order_number:String(ro.name||'').replace('#',''), created_at:ro.created_at,
            financial_status:(ro.financial_status||'').toLowerCase(),
            fulfillment_status:(ro.fulfillment_status||'').toLowerCase(),
            total_price:String(ro.total_price||'0'), currency:ro.currency||'INR',
            tags:allTags, note:ro.note||'',
            customer_name: ro.customer?`${ro.customer.first_name||''} ${ro.customer.last_name||''}`.trim():sa?.name||'',
            customer_email: ro.email||ro.customer?.email||'',
            customer_phone: ro.phone||ro.customer?.phone||sa?.phone||'',
            customer_id: ro.customer?.id?String(ro.customer.id):null,
            shipping_address: sa?{ name:sa.name||'', address1:sa.address1||'', address2:sa.address2||'', city:sa.city||'', province:sa.province||'', zip:sa.zip||'', country:sa.country||'', phone:sa.phone||'' }:null,
            line_items: (ro.line_items||[]).map(li=>{
              const origPrice = parseFloat(li.price||'0');
              const totalDisc = (li.discount_allocations||[]).reduce((s,d)=>s+parseFloat(d.amount||0),0);
              const qty = li.quantity||1;
              const netPrice = Math.max(0, origPrice - totalDisc/qty);
              return { id:String(li.id), title:li.title, quantity:qty, price:netPrice.toFixed(2), original_price:origPrice.toFixed(2), discount_allocated:totalDisc.toFixed(2), variant_id:li.variant_id?String(li.variant_id):null, image_url:null, product_id:li.product_id?String(li.product_id):null };
            }),
            return_status: rs, request_type: rt,
            requests: reqByOrder[oid]||[]
          };
        } catch(e) { console.error('[fetchMissing]',oid,e.message); return null; }
      }));
      orders.push(...extras.filter(Boolean));
    }

    res.json({ orders, return_requests:orders.filter(o=>o.return_status).length });
  } catch(e) { console.error('[/api/orders]',e); res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// PORTAL LOOKUP
// ══════════════════════════════════════════
app.get('/api/lookup', async (req,res)=>{
  const { order_number, pincode } = req.query;
  if (!order_number) return res.status(400).json({ error:'Missing order_number' });
  try {
    const clean = String(order_number).replace(/^#+/,'').trim();
    const FIELDS = 'id,order_number,created_at,financial_status,fulfillment_status,total_price,currency,line_items,tags,note,email,phone,customer,shipping_address,billing_address,payment_gateway';

    // Step 1: Find the order via GraphQL (handles archived, Fastrr, all sources)
    let o = null;

    try {
      const gql = await graphql(`{
        orders(first:5, query:"name:#${clean}") {
          edges { node {
            id name legacyResourceId createdAt tags note
            displayFinancialStatus displayFulfillmentStatus
            totalPriceSet { shopMoney { amount currencyCode } }
            email phone paymentGateway
            customer { id firstName lastName email phone }
            shippingAddress { name firstName lastName address1 address2 city province zip country phone }
            billingAddress  { name address1 address2 city province zip country phone }
            lineItems(first:20) { edges { node {
              id title quantity variantTitle
              fulfillableQuantity
              originalUnitPriceSet    { shopMoney { amount } }
              discountedUnitPriceSet { shopMoney { amount } }
              discountAllocations { allocatedAmountSet { shopMoney { amount } } }
              variant { id }
              product { id images(first:1){edges{node{url}}} options { name values } variants(first:50){edges{node{id title selectedOptions{name value} price availableForSale}}} }
            }}}
          }}
        }
      }`);
      const edges = gql?.data?.orders?.edges || [];
      if (edges.length) {
        const node = edges[0].node;
        const sa   = node.shippingAddress;
        const ba   = node.billingAddress;
        o = {
          _gql: true,
          id:                 node.legacyResourceId || gidToId(node.id),
          order_number:       clean,
          created_at:         node.createdAt,
          financial_status:   (node.displayFinancialStatus||'').toLowerCase(),
          fulfillment_status: (node.displayFulfillmentStatus||'').toLowerCase(),
          total_price:        node.totalPriceSet?.shopMoney?.amount||'0',
          currency:           node.totalPriceSet?.shopMoney?.currencyCode||'INR',
          tags:               Array.isArray(node.tags) ? node.tags : (node.tags||'').split(',').map(t=>t.trim()).filter(Boolean),
          note:               node.note||'',
          email:              node.email||'',
          phone:              node.phone||'',
          payment_gateway:    node.paymentGateway||'',
          customer:           node.customer ? {
            first_name: node.customer.firstName||'',
            last_name:  node.customer.lastName||'',
            email:      node.customer.email||'',
            phone:      node.customer.phone||'',
            id:         gidToId(node.customer.id)
          } : null,
          shipping_address: sa ? { name:sa.name||[sa.firstName,sa.lastName].filter(Boolean).join(' '), address1:sa.address1||'', address2:sa.address2||'', city:sa.city||'', province:sa.province||'', zip:sa.zip||'', country:sa.country||'India', phone:sa.phone||'' } : null,
          billing_address:  ba ? { name:ba.name||'', address1:ba.address1||'', city:ba.city||'', province:ba.province||'', zip:ba.zip||'', country:ba.country||'India' } : null,
          line_items: (node.lineItems?.edges||[]).map(({node:li})=>{
            const origPrice = parseFloat(li.originalUnitPriceSet?.shopMoney?.amount||'0');
            const netPrice  = parseFloat(li.discountedUnitPriceSet?.shopMoney?.amount||li.originalUnitPriceSet?.shopMoney?.amount||'0');
            const qty = li.quantity||1;
            const totalDisc = Math.max(0, (origPrice - netPrice) * qty);
            return {
              id:               gidToId(li.id),
              title:            li.title,
              variant_title:    li.variantTitle||'',
              variant_id:       li.variant?.id ? gidToId(li.variant.id) : null,
              product_id:       li.product?.id ? gidToId(li.product.id) : null,
              quantity:         qty,
              price:            netPrice.toFixed(2),
              original_price:   origPrice.toFixed(2),
              discount_allocated: totalDisc.toFixed(2),
              fulfillment_status: 'fulfilled',
              image_url:        li.product?.images?.edges?.[0]?.node?.url||null,
              non_returnable:   false,
              product_options:  li.product?.options||[],
              product_variants: (li.product?.variants?.edges||[]).map(({node:v})=>({ id:gidToId(v.id), title:v.title, selectedOptions:v.selectedOptions, price:v.price, available:v.availableForSale }))
            };
          })
        };
      }
    } catch(ge) { console.error('[GQL lookup]', ge.message); }

    // Fallback to REST if GraphQL missed it
    if (!o) {
      for (const nameVal of [`%23${clean}`, clean]) {
        const d = await shopifyREST('GET', `orders.json?name=${nameVal}&status=any&fields=${FIELDS}`);
        if (d.orders?.length) { o = d.orders[0]; o.tags=(o.tags||'').split(',').map(t=>t.trim()).filter(Boolean); o.order_number=clean; break; }
      }
    }

    // Transform REST line_items to compute discounted prices (GQL path already uses discountedUnitPriceSet)
    if (o && !o._gql) {
      o.line_items = (o.line_items||[]).map(li => {
        const origPrice = parseFloat(li.price||'0');
        const totalDisc = (li.discount_allocations||[]).reduce((s,d)=>s+parseFloat(d.amount||0),0);
        const qty = li.quantity||1;
        const netPrice = Math.max(0, origPrice - totalDisc/qty);
        return { ...li, original_price:origPrice.toFixed(2), price:netPrice.toFixed(2), discount_allocated:totalDisc.toFixed(2) };
      });
    }

    if (!o) { console.log(`[Lookup] Not found: ${clean}`); return res.json({ found:false }); }

    // Step 2: Pincode verification (only if pincode provided)
    if (pincode && pincode.trim()) {
      const inp = pincode.trim().replace(/\D/g,'');
      const oPincode = String(
        o.shipping_address?.zip || o.billing_address?.zip || ''
      ).replace(/\D/g,'');
      if (oPincode && inp !== oPincode) {
        return res.json({ found:false, mismatch:true });
      }
    }

    // Step 3: Build response
    const orderDate = new Date(o.created_at);
    const daysDiff  = (Date.now()-orderDate)/(1000*60*60*24);
    const deadline  = new Date(orderDate.getTime()+RETURN_WINDOW_DAYS*24*60*60*1000).toISOString();

    // Fetch product details for REST orders (GQL already has them)
    if (!o._gql) {
      const pids = [...new Set((o.line_items||[]).map(li=>li.product_id).filter(Boolean))].join(',');
      if (pids) {
        const pd = await shopifyREST('GET',`products.json?ids=${pids}&fields=id,images,options,variants,tags&limit=20`);
        const pc = {};
        (pd.products||[]).forEach(p=>{ pc[p.id]={ image:p.images?.[0]?.src||null, options:p.options||[], variants:p.variants||[], non_returnable:(p.tags||'').includes('non-returnable') }; });
        o.line_items = (o.line_items||[]).map(li=>({ ...li, image_url:pc[li.product_id]?.image||null, non_returnable:pc[li.product_id]?.non_returnable||false, product_options:pc[li.product_id]?.options||[], product_variants:pc[li.product_id]?.variants||[] }));
      }
    }

    const tagArr = Array.isArray(o.tags) ? o.tags : (o.tags||'').split(',').map(t=>t.trim()).filter(Boolean);
    // Use Supabase as source of truth (existingRequests fetched below)

    const sa = o.shipping_address||o.billing_address||null;
    const address = sa ? {
      name:     sa.name||[sa.first_name,sa.last_name].filter(Boolean).join(' ')||o.customer?`${o.customer.first_name||''} ${o.customer.last_name||''}`.trim():'',
      address1: sa.address1||'', address2:sa.address2||'',
      city:sa.city||'', province:sa.province||'', zip:sa.zip||'',
      country:sa.country||'India', phone:sa.phone||o.phone||o.customer?.phone||''
    } : null;

    const is_cod = (()=>{ const gw=(o.payment_gateway||'').toLowerCase(); const fin=(o.financial_status||'').toLowerCase(); return gw.includes('cod')||gw.includes('cash on delivery')||gw==='cash_on_delivery'||fin==='pending'; })();
    const { data:existingRequests } = await supabase.from('requests').select('*').eq('order_id',String(o.id)).neq('status','archived').order('created_at',{ascending:false});
    const latestExisting = existingRequests?.[0];
    const rs = latestExisting?.status || null;
    const rsType = latestExisting?.request_type || 'return';

    // Only return fulfilled line items
    const line_items = (o.line_items||[]).filter(li=>
      li.fulfillment_status==='fulfilled' ||
      o.fulfillment_status==='fulfilled'  ||
      o.fulfillment_status==='partial'    ||
      o._gql
    );

    res.json({ found:true, order:{
      id:o.id, order_number:o.order_number, created_at:o.created_at,
      financial_status:o.financial_status, fulfillment_status:o.fulfillment_status,
      total_price:o.total_price, currency:o.currency,
      has_return:!!rs, return_status:rs,
      request_type: rsType,
      note:o.note, return_deadline:deadline,
      within_window:daysDiff<=RETURN_WINDOW_DAYS,
      days_remaining:Math.max(0,RETURN_WINDOW_DAYS-Math.floor(daysDiff)),
      return_window_days:RETURN_WINDOW_DAYS,
      address,
      customer_name:  o.customer?`${o.customer.first_name||''} ${o.customer.last_name||''}`.trim():sa?.name||'',
      customer_email: o.email||o.customer?.email||'',
      customer_phone: o.phone||sa?.phone||o.customer?.phone||'',
      payment_gateway:o.payment_gateway||'', is_cod, tags:tagArr,
      requests:existingRequests||[], line_items
    }});
  } catch(e) { console.error('[/api/lookup]',e); res.status(500).json({ error:e.message }); }
});


app.get('/api/portal/tracking/:req_id', async (req,res)=>{
  try {
    const { data:request, error } = await supabase.from('requests').select('*').eq('req_id',req.params.req_id).single();
    if (error||!request) return res.status(404).json({ error:'Request not found' });
    let scans=[];
    if (request.awb) {
      try {
        const d=await delhiveryAPI('GET',`/api/v1/packages/json/?waybill=${request.awb}`);
        const pkg=d?.ShipmentData?.[0]?.Shipment;
        if (pkg) scans=(pkg.Scans||[]).map(s=>({ status:s.ScanDetail?.Scan,detail:s.ScanDetail?.Instructions,location:s.ScanDetail?.ScannedLocation,date:s.ScanDetail?.ScanDateTime,code:s.ScanDetail?.StatusCode })).reverse();
      } catch(e) { console.error('[tracking scans]',e.message); }
    }
    res.json({ request, scans });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// SUBMIT RETURN REQUEST
// ══════════════════════════════════════════
app.post('/api/returns/request', async (req,res)=>{
  const { order_id, order_number, items, refund_method, customer_note, address, shipping_preference, payment_txnid, payment_status } = req.body;
  if (!order_id||!items?.length) return res.status(400).json({ error:'Missing required fields' });

  // Block non-returnable items (watch, wallet) — server-side guard
  const blockedItem = items.find(i => isNonReturnable(i.title||''));
  if (blockedItem) return res.status(400).json({ error:`"${blockedItem.title}" cannot be returned or exchanged.` });

  // Payment enforcement removed — handled client-side via Easebuzz portal flow
  // Re-enable once Easebuzz is confirmed live and payments table exists in Supabase
  try {
    const fresh=await shopifyREST('GET',`orders/${order_id}.json?fields=created_at,tags,note,payment_gateway,customer,line_items,shipping_address,email,phone`);
    const fo=fresh?.order||{};
    const days_since_order=fo.created_at?Math.floor((Date.now()-new Date(fo.created_at))/(1000*60*60*24)):0;
    const is_cod=(()=>{ const gw=(fo.payment_gateway||'').toLowerCase(); return gw.includes('cod')||gw.includes('cash on delivery')||gw==='cash_on_delivery'; })();
    const existTags=(fo.tags||'').split(',').map(t=>t.trim()).filter(Boolean);

    // Block orders tagged Non_Returnable or Non_Exchangeable
    const tagLower = existTags.map(t=>t.toLowerCase());
    const hasNonReturnable   = tagLower.includes('non_returnable');
    const hasNonExchangeable = tagLower.includes('non_exchangeable');
    if (hasNonReturnable && hasNonExchangeable) return res.status(403).json({ error:'This order is not eligible for returns or exchanges.' });
    if (hasNonReturnable)   { const hasReturn   = items.some(i=>i.action==='return');   if (hasReturn)   return res.status(403).json({ error:'This order is not eligible for returns.' }); }
    if (hasNonExchangeable) { const hasExchange  = items.some(i=>i.action==='exchange'); if (hasExchange) return res.status(403).json({ error:'This order is not eligible for exchanges.' }); }
    const existNote=fo.note||'';
    const sa=fo.shipping_address||{};
    const customer_name  = fo.customer ? `${fo.customer.first_name||''} ${fo.customer.last_name||''}`.trim() : sa.name||'Customer';
    const customer_email = fo.email||fo.customer?.email||'';

    // ── CHECK: Existing request with no AWB yet (combine into one) ──
    const { data:existingReqs } = await supabase.from('requests')
      .select('*').eq('order_id',String(order_id))
      .not('status','in','("rejected","deleted","archived")')
      .order('created_at',{ascending:false});

    // Merge with any open request that hasn't been picked up yet
    const openReq = (existingReqs||[]).find(r => !['picked_up','delivered','refunded','exchange_fulfilled'].includes(r.status));

    if (openReq) {
      // Append new items to existing request — make it mixed
      const existingItems = openReq.items || [];
      const newItemIds    = new Set(items.map(i=>String(i.id)));
      const dedupedExisting = existingItems.filter(i=>!newItemIds.has(String(i.id)));
      const mergedItems   = [...dedupedExisting, ...items];

      // Freebie check on merged
      const finalItems = checkFreebieItems(fo.tags, fo.line_items||[], mergedItems, 'mixed');

      const hasReturns   = finalItems.some(i=>i.action==='return');
      const hasExchanges = finalItems.some(i=>i.action==='exchange');
      const new_type     = hasReturns&&hasExchanges?'mixed':hasExchanges?'exchange':'return';
      const new_total    = finalItems.reduce((s,i)=>s+parseFloat(i.price||0)*(parseInt(i.qty)||1),0);

      // Update existing request in Supabase
      await supabase.from('requests').update({
        items:       finalItems,
        request_type:new_type,
        total_price: new_total,
        refund_method: refund_method||openReq.refund_method,
        address:     address||openReq.address
      }).eq('req_id', openReq.req_id);

      // Update Shopify note
      const itemLines = finalItems.map(i=>`[${i.action.toUpperCase()}] ${i.title}${i.variant_title?' - '+i.variant_title:''} x${parseInt(i.qty)||1}${i.reason?' | '+i.reason:''}`).join('\n');
      const noteAppend = `\n[UPDATED ${openReq.req_id}] ${new_type.toUpperCase()} — ${finalItems.length} items — ${new Date().toISOString()}\n${itemLines}`;
      await shopifyREST('PUT',`orders/${order_id}.json`,{ order:{ id:order_id, note:(existNote+noteAppend).slice(0,5000) }});

      await auditLog(order_id, openReq.req_id, 'request_appended', 'customer', `+${items.length} items merged, total ${finalItems.length}`);

      // Remove all old type tags and auto-approve with the new type
      const allTypeTags = ['return-requested','return-approved','exchange-requested','exchange-approved','mixed-requested','mixed-approved'];
      await updateOrderTags(order_id, [], allTypeTags);
      await autoApproveRequest(openReq.req_id, order_id, new_type);

      console.log(`[Request] Appended ${items.length} items to existing ${openReq.req_id}, auto-approved as ${new_type}`);
      return res.json({ success:true, req_id:openReq.req_id, req_num:openReq.req_num, type:new_type, status:'approved', merged:true });
    }

    // ── NEW REQUEST ──
    const returns=items.filter(i=>i.action==='return');
    const exchanges=items.filter(i=>i.action==='exchange');
    const hasBoth=returns.length>0&&exchanges.length>0;
    const request_type=hasBoth?'mixed':exchanges.length?'exchange':'return';
    const total_price=items.reduce((s,i)=>s+parseFloat(i.price||0)*(parseInt(i.qty)||1),0);

    // Freebie check
    const finalItems=checkFreebieItems(fo.tags, fo.line_items||[], items, request_type);

    // Count ALL requests (incl. archived/rejected) to avoid req_id UNIQUE constraint collisions
    const { count:allReqCount } = await supabase.from('requests').select('req_id',{ count:'exact', head:true }).eq('order_id',String(order_id));
    const reqNum=(allReqCount||0)+1;
    const req_id=`${order_number}_${request_type==='exchange'?'EXC':'RET'}${String(reqNum).padStart(3,'0')}`;
    const baseTag=hasBoth?'mixed-requested':exchanges.length?'exchange-requested':'return-requested';

    const finalAddress=address||(fo.shipping_address?{
      name:     fo.shipping_address.name||[fo.shipping_address.first_name,fo.shipping_address.last_name].filter(Boolean).join(' ')||'',
      address1: fo.shipping_address.address1||'',
      address2: fo.shipping_address.address2||'',
      city:     fo.shipping_address.city||'',
      province: fo.shipping_address.province||'',
      zip:      fo.shipping_address.zip||'',
      country:  fo.shipping_address.country||'India',
      phone:    fo.shipping_address.phone||fo.customer?.phone||''
    }:null);

    const itemLines=finalItems.map(i=>`[${i.action.toUpperCase()}] ${i.title}${i.variant_title?' - '+i.variant_title:''} x${parseInt(i.qty)||1}${i.auto_added?' (auto-added)':''}${i.reason?' | '+i.reason:''}${i.exchange_variant_id?' | ExchVarID:'+i.exchange_variant_id:''}`).join('\n');
    const noteBlock=`\n---${req_id}---\nType:${request_type.toUpperCase()}\nItems:\n${itemLines}\nRefund:${refund_method||'store_credit'}\n${customer_note?'Note:'+customer_note+'\n':''}Date:${new Date().toISOString()}\n---END---`;
    const newNote=(existNote+noteBlock).slice(0,5000);
    const newTags=[...new Set([...existTags,baseTag])];

    const upd=await shopifyREST('PUT',`orders/${order_id}.json`,{ order:{ id:order_id,tags:newTags.join(', '),note:newNote } });
    if (!upd.order) return res.status(400).json({ error:'Failed to update order',detail:upd });

    const requestRecord={ req_id, req_num:reqNum, order_id:String(order_id), order_number:String(order_number), items:finalItems, refund_method, shipping_preference:'pickup', status:'pending', request_type, total_price, address:finalAddress, is_cod, days_since_order, customer_name, customer_email, submitted_at:new Date().toISOString() };
    const { error:ie } = await supabase.from('requests').insert(requestRecord);
    if (ie) console.error('[Insert]', ie.message);

    await auditLog(order_id, req_id, 'request_submitted', 'customer', `${request_type}|${finalItems.length}items`);

    // Email 1: Request received
    sendEmail(customer_email, `Return Request Received — Order #${order_number}`,
      emailRequestReceived({ name:customer_name.split(' ')[0], orderNumber:order_number, reqId:req_id, items:finalItems, refundMethod:refund_method }))
      .catch(()=>{});

    // Auto-approve synchronously
    await autoApproveRequest(req_id, order_id, request_type);
    res.json({ success:true, req_id, req_num:reqNum, type:request_type, status:'approved' });
  } catch(e) { console.error('[/api/returns/request]',e); res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// MANUAL REQUEST  (from dashboard)
// ══════════════════════════════════════════
app.post('/api/returns/manual', async (req,res)=>{
  const { order_id, order_number, items, refund_method, customer_note, address } = req.body;
  if (!order_id||!items?.length) return res.status(400).json({ error:'Missing required fields' });

  // Block non-returnable items (watch, wallet)
  const blockedManual = items.find(i => isNonReturnable(i.title||''));
  if (blockedManual) return res.status(400).json({ error:`"${blockedManual.title}" cannot be returned or exchanged.` });

  try {
    const returns=items.filter(i=>i.action==='return');
    const exchanges=items.filter(i=>i.action==='exchange');
    const hasBoth=returns.length>0&&exchanges.length>0;
    const request_type=hasBoth?'mixed':exchanges.length?'exchange':'return';
    const total_price=items.reduce((s,i)=>s+parseFloat(i.price||0)*(parseInt(i.qty)||1),0);

    const fresh=await shopifyREST('GET',`orders/${order_id}.json?fields=created_at,tags,note,payment_gateway,shipping_address,customer`);
    const fo=fresh?.order||{};
    const is_cod=(()=>{ const gw=(fo.payment_gateway||'').toLowerCase(); return gw.includes('cod')||gw.includes('cash on delivery')||gw==='cash_on_delivery'; })();
    const existTags=(fo.tags||'').split(',').map(t=>t.trim()).filter(Boolean);
    const existNote=fo.note||'';

    const { data:existingReqs } = await supabase.from('requests').select('req_id').eq('order_id',String(order_id));
    const reqNum=(existingReqs?.length||0)+1;
    const req_id=`${order_number}_${request_type==='exchange'?'EXC':'RET'}${String(reqNum).padStart(3,'0')}`;
    const baseTag=hasBoth?'mixed-requested':exchanges.length?'exchange-requested':'return-requested';

    const finalAddress=address||(fo.shipping_address?{ name:fo.shipping_address.name||'', address1:fo.shipping_address.address1||'', address2:fo.shipping_address.address2||'', city:fo.shipping_address.city||'', province:fo.shipping_address.province||'', zip:fo.shipping_address.zip||'', country:fo.shipping_address.country||'India', phone:fo.shipping_address.phone||'' }:null);

    const itemLines=items.map(i=>`[${i.action.toUpperCase()}] ${i.title} x${parseInt(i.qty)||1}${i.reason?' | '+i.reason:''}`).join('\n');
    const noteBlock=`\n---${req_id} (MANUAL)---\nType:${request_type.toUpperCase()}\nItems:\n${itemLines}\nRefund:${refund_method||'store_credit'}\n${customer_note?'Note:'+customer_note+'\n':''}Date:${new Date().toISOString()}\n---END---`;
    const newTags=[...new Set([...existTags,baseTag])];
    await shopifyREST('PUT',`orders/${order_id}.json`,{ order:{ id:order_id,tags:newTags.join(', '),note:(existNote+noteBlock).slice(0,5000) }});

    const requestRecord={ req_id, req_num:reqNum, order_id:String(order_id), order_number:String(order_number), items, refund_method, shipping_preference:'pickup', status:'pending', request_type, total_price, address:finalAddress, is_cod, submitted_at:new Date().toISOString() };
    await supabase.from('requests').insert(requestRecord);
    await auditLog(order_id, req_id, 'manual_request', 'merchant', `${request_type}|${items.length}items`);

    // Auto-approve immediately for manual requests
    await autoApproveRequest(req_id, order_id, request_type);
    res.json({ success:true, req_id, req_num:reqNum, type:request_type, status:'approved' });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// EDIT LINE ITEMS
// ══════════════════════════════════════════
app.put('/api/returns/:req_id/items', async (req,res)=>{
  const { items } = req.body;
  if (!items) return res.status(400).json({ error:'No items' });
  try {
    const { data:r } = await supabase.from('requests').select('order_id').eq('req_id',req.params.req_id).single();
    const tp=items.reduce((s,i)=>s+parseFloat(i.price||0)*(parseInt(i.qty)||1),0);
    await supabase.from('requests').update({ items, total_price:tp }).eq('req_id',req.params.req_id);
    await auditLog(r?.order_id||'', req.params.req_id, 'items_edited', 'merchant', `${items.length}items`);
    res.json({ success:true });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// UTR NUMBER
// ══════════════════════════════════════════
app.post('/api/returns/:req_id/utr', async (req,res)=>{
  const { utr_number } = req.body;
  try {
    await supabase.from('requests').update({ utr_number }).eq('req_id',req.params.req_id);
    const { data:r } = await supabase.from('requests').select('order_id').eq('req_id',req.params.req_id).single();
    await auditLog(r?.order_id||'', req.params.req_id, 'utr_added', 'merchant', `UTR:${utr_number}`);
    res.json({ success:true });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// APPROVE / REJECT / INSPECT / ARCHIVE
// ══════════════════════════════════════════
app.post('/api/returns/:order_id/approve', async (req,res)=>{
  const { order_id }=req.params; const { type, actor, req_id }=req.body;
  try {
    const at=type==='exchange'?'exchange-approved':type==='mixed'?'mixed-approved':'return-approved';
    const rt=type==='exchange'?'exchange-requested':type==='mixed'?'mixed-requested':'return-requested';
    await updateOrderTags(order_id,[at],[rt]);
    const q=supabase.from('requests').update({ status:'approved', approved_at:new Date().toISOString() });
    req_id ? await q.eq('req_id',req_id) : await q.eq('order_id',String(order_id)).eq('status','pending');
    await auditLog(order_id,req_id||null,'approved',actor||'merchant','');
    res.json({ success:true });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

app.post('/api/returns/:order_id/reject', async (req,res)=>{
  const { order_id }=req.params; const { type, reason, actor, req_id }=req.body;
  try {
    const rt=type==='exchange'?'exchange-rejected':type==='mixed'?'mixed-rejected':'return-rejected';
    const rem=type==='exchange'?'exchange-requested':type==='mixed'?'mixed-requested':'return-requested';
    await updateOrderTags(order_id,[rt],[rem]);
    const q=supabase.from('requests').update({ status:'rejected' });
    req_id ? await q.eq('req_id',req_id) : await q.eq('order_id',String(order_id));
    await auditLog(order_id,req_id||null,'rejected',actor||'merchant',reason||'');
    res.json({ success:true });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ── Manual status update (used by markReceived) ──
app.patch('/api/requests/:reqId/status', async (req,res)=>{
  const { reqId }=req.params; const { status }=req.body;
  if (!status) return res.status(400).json({ error:'status required' });
  try {
    const { data:r } = await supabase.from('requests').select('order_id').eq('req_id',reqId).single();
    await supabase.from('requests').update({ status }).eq('req_id',reqId);
    if (r?.order_id) await auditLog(r.order_id, reqId, status, 'merchant', 'Manual status update');
    res.json({ success:true });
  } catch(e){ res.status(500).json({ error:e.message }); }
});

// ── Force-complete: mark delivered + process refund + archive (for stuck orders) ──
app.post('/api/requests/:reqId/force-complete', async (req,res)=>{
  const { reqId }=req.params;
  try {
    const { data:request } = await supabase.from('requests').select('*').eq('req_id',reqId).single();
    if (!request) return res.status(404).json({ error:'Request not found' });

    // Mark delivered
    await supabase.from('requests').update({ status:'delivered', awb_final:true }).eq('req_id',reqId);
    await updateOrderTags(request.order_id, ['return-received'], ['pickup-scan','pickup-scheduled']);
    await auditLog(request.order_id, reqId, 'force_delivered', 'merchant', 'Manually marked as delivered');

    if (request.request_type==='return') {
      // Auto-refund based on stored refund_method
      const refResult = await processRefund(request);
      await supabase.from('requests').update({ status:'refunded' }).eq('req_id',reqId);
      await auditLog(request.order_id, reqId, 'refunded', 'merchant', 'Auto-refunded on force-complete');
    } else if (request.request_type==='exchange'||request.request_type==='mixed') {
      if (!request.exchange_order_id) await createExchangeOrder(request);
      await supabase.from('requests').update({ status:'exchange_fulfilled' }).eq('req_id',reqId);
    }

    await archiveRequest(reqId, request.order_id);
    res.json({ success:true });
  } catch(e){ console.error('[force-complete]',e.message); res.status(500).json({ error:e.message }); }
});

app.post('/api/returns/:order_id/inspect', async (req,res)=>{
  const { order_id }=req.params;
  try {
    await updateOrderTags(order_id,['return-inspected'],[]);
    await supabase.from('requests').update({ status:'inspected' }).eq('order_id',String(order_id)).in('status',['approved','received','pickup_scheduled','picked_up']);
    await auditLog(order_id,null,'inspected',req.body?.actor||'merchant','');
    res.json({ success:true });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

app.post('/api/returns/:req_id/archive', async (req,res)=>{
  try {
    const { data:r }=await supabase.from('requests').select('order_id').eq('req_id',req.params.req_id).single();
    await archiveRequest(req.params.req_id, r?.order_id||'');
    res.json({ success:true });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// ORDER DETAILS
// ══════════════════════════════════════════
app.get('/api/orders/:order_id/details', async (req,res)=>{
  try {
    const d=await shopifyREST('GET',`orders/${req.params.order_id}.json?fields=id,order_number,email,phone,customer,shipping_address,billing_address,line_items,note,tags,total_price,financial_status,payment_gateway`);
    const o=d?.order; if(!o)return res.status(404).json({ error:'Not found' });
    const sa=o.shipping_address||o.billing_address||null;
    const address=sa?{ name:sa.name||(o.customer?`${o.customer.first_name||''} ${o.customer.last_name||''}`.trim():''), address1:sa.address1||'', address2:sa.address2||'', city:sa.city||'', province:sa.province||'', zip:sa.zip||'', country:sa.country||'India', phone:sa.phone||o.phone||o.customer?.phone||'' }:null;
    const { data:requests }=await supabase.from('requests').select('*').eq('order_id',String(o.id)).order('created_at',{ ascending:false });
    res.json({ id:o.id, order_number:o.order_number, customer_name:o.customer?`${o.customer.first_name||''} ${o.customer.last_name||''}`.trim():sa?.name||'', customer_email:o.email||o.customer?.email||'', customer_phone:o.phone||o.customer?.phone||sa?.phone||'', payment_gateway:o.payment_gateway||'', is_cod:(()=>{ const gw=(o.payment_gateway||'').toLowerCase(); return gw.includes('cod')||gw.includes('cash on delivery')||gw==='cash_on_delivery'; })(), address, line_items:(o.line_items||[]).map(li=>({ id:li.id,title:li.title,variant_id:li.variant_id,variant_title:li.variant_title||'',quantity:li.quantity,price:li.price,sku:li.sku||'' })), total_price:o.total_price, financial_status:o.financial_status, note:o.note, tags:o.tags, requests:requests||[] });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// PRODUCTS
// ══════════════════════════════════════════
app.get('/api/products/featured', async (_req,res)=>{ try { const d=await shopifyREST('GET','products.json?fields=id,title,images,variants&limit=8'); res.json({ products:(d.products||[]).map(p=>({ id:p.id,title:p.title,image:p.images?.[0]?.src||null,price:p.variants?.[0]?.price||'0' })) }); } catch(e){ res.json({ products:[] }); } });
app.get('/api/products/all', async (req,res)=>{
  try {
    const cursor = req.query.cursor||null;
    const afterClause = cursor ? `,after:${JSON.stringify(cursor)}` : '';
    // No status filter — return all published products. status:ACTIVE fails in some API versions.
    const gql = await graphql(`{products(first:50${afterClause}){edges{cursor node{id title status images(first:1){edges{node{url}}} options{name position} variants(first:100){edges{node{id title price selectedOptions{name value}}}}}} pageInfo{hasNextPage endCursor}}}`);
    if (gql?.errors?.length) console.error('[products/all] GQL errors:', JSON.stringify(gql.errors).slice(0,300));
    const edges = gql?.data?.products?.edges||[];
    const pageInfo = gql?.data?.products?.pageInfo||{};
    console.log(`[products/all] fetched ${edges.length} products`);
    const activeNodes = edges.map(e=>e.node).filter(p=>!p.status || p.status.toLowerCase()==='active');
    res.json({
      products: activeNodes.map(p=>({
        id: p.id.replace('gid://shopify/Product/',''),
        title: p.title,
        image: p.images?.edges?.[0]?.node?.url||null,
        price: p.variants?.edges?.[0]?.node?.price||'0',
        options: (p.options||[]).map(o=>({name:o.name,position:o.position})),
        variants: (p.variants?.edges||[]).map(e=>e.node).map(v=>({
          id: v.id.replace('gid://shopify/ProductVariant/',''),
          title: v.title,
          price: v.price,
          option1: v.selectedOptions?.[0]?.value||null,
          option2: v.selectedOptions?.[1]?.value||null,
          option3: v.selectedOptions?.[2]?.value||null,
          available: true
        }))
      })),
      hasNextPage: pageInfo.hasNextPage||false,
      endCursor: pageInfo.endCursor||null
    });
  } catch(e){ console.error('[products/all]',e.message); res.json({products:[],hasNextPage:false,endCursor:null}); }
});

// Search route MUST be before /:product_id to avoid Express matching 'search' as a product_id
app.get('/api/products/search', async (req,res)=>{ const { q }=req.query; if(!q)return res.json({ products:[] }); try {
  // Use title:TERM* prefix search — most reliable for Shopify Admin API product title matching
  const gql = await graphql(`{products(first:12,query:${JSON.stringify('title:'+q+'*')}){edges{node{id title images(first:1){edges{node{url}}} options{name position} variants(first:100){edges{node{id title price selectedOptions{name value}}}}}}}}`);
  if (gql?.errors?.length) console.error('[product search] GQL errors:', JSON.stringify(gql.errors).slice(0,300));
  const nodes=(gql?.data?.products?.edges||[]).map(e=>e.node);
  res.json({ products: nodes.map(p=>({
    id: p.id.replace('gid://shopify/Product/',''),
    title: p.title,
    image: p.images?.edges?.[0]?.node?.url||null,
    price: p.variants?.edges?.[0]?.node?.price||'0',
    options: (p.options||[]).map(o=>({ name:o.name, position:o.position })),
    variants: (p.variants?.edges||[]).map(e=>e.node).map(v=>({
      id: v.id.replace('gid://shopify/ProductVariant/',''),
      title: v.title,
      price: v.price,
      option1: v.selectedOptions?.[0]?.value||null,
      option2: v.selectedOptions?.[1]?.value||null,
      option3: v.selectedOptions?.[2]?.value||null,
      available: true
    }))
  })) });
} catch(e){ console.error('[product search]',e.message); res.json({ products:[] }); } });

app.get('/api/products/:product_id', async (req,res)=>{
  try {
    const d = await shopifyREST('GET', `products/${req.params.product_id}.json?fields=id,title,images,options,variants`);
    const p = d.product;
    if (!p) return res.status(404).json({ error:'Not found' });
    res.json({
      id: p.id, title: p.title,
      image: p.images?.[0]?.src || null,
      options: p.options || [],
      variants: (p.variants||[]).map(v=>({
        id: v.id, title: v.title,
        option1: v.option1, option2: v.option2, option3: v.option3,
        price: v.price,
        available: (v.inventory_quantity||1) > 0
      }))
    });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// MANUAL REFUND / STORE CREDIT / EXCHANGE
// ══════════════════════════════════════════
app.post('/api/shopify/refund/:order_id', async (req,res)=>{
  const { order_id }=req.params; const { refund_method, note, line_item_ids, req_id }=req.body;
  try {
    const fo=await shopifyREST('GET',`orders/${order_id}.json?fields=id,line_items,financial_status`);
    const lines=fo?.order?.line_items||[]; if(!lines.length)return res.status(400).json({ error:'No line items' });
    const useLines=line_item_ids?.length?lines.filter(li=>line_item_ids.map(String).includes(String(li.id))):lines;
    let locId2=null; try { const l2=await shopifyREST('GET','locations.json?active=true&limit=1'); locId2=l2?.locations?.[0]?.id||null; } catch(e){}
    const rli=useLines.map(li=>{ const item={line_item_id:li.id,quantity:li.quantity,restock_type:locId2?'return':'no_restock'}; if(locId2)item.location_id=locId2; return item; });
    const calc=await shopifyREST('POST',`orders/${order_id}/refunds/calculate.json`,{ refund:{ refund_line_items:rli } });
    if (calc.errors)return res.status(400).json({ error:'Calc failed:'+JSON.stringify(calc.errors) });
    let txns=calc?.refund?.transactions||[];
    if (parseFloat(RESTOCKING_FEE_PCT)>0)txns=txns.map(t=>({...t,amount:(parseFloat(t.amount||0)*(1-parseFloat(RESTOCKING_FEE_PCT)/100)).toFixed(2)}));
    if (RETURN_SHIPPING_FEE>0)txns=txns.map(t=>({...t,amount:Math.max(0,parseFloat(t.amount||0)-RETURN_SHIPPING_FEE).toFixed(2)}));
    const result=await shopifyREST('POST',`orders/${order_id}/refunds.json`,{ refund:{ notify:refund_method==='store_credit'?false:true,note:note||'Return approved',refund_line_items:rli,transactions:refund_method==='store_credit'?[]:txns.map(t=>({ parent_id:t.parent_id,amount:t.amount,kind:'refund',gateway:t.gateway })) }});
    if (result?.refund?.id) {
      const amount=result.refund.transactions?.[0]?.amount||'0';
      await updateOrderTags(order_id,['return-refunded'],[]);
      if (req_id)await supabase.from('requests').update({ refund_id:String(result.refund.id),refund_amount:parseFloat(amount),status:'refunded' }).eq('req_id',req_id);
      await auditLog(order_id,req_id||null,'refund_manual','merchant',`₹${amount}`);
      res.json({ success:true,refund_id:result.refund.id,amount });
    } else { res.status(400).json({ error:'Refund failed:'+(JSON.stringify(result?.errors||result)).slice(0,300) }); }
  } catch(e){ res.status(500).json({ error:e.message }); }
});

app.post('/api/shopify/store-credit/:order_id', async (req,res)=>{
  const { order_id }=req.params; const { amount,apply_bonus,customer_email,note,req_id }=req.body;
  try {
    const base=parseFloat(amount||0); const total=base.toFixed(2); // No bonus
    const code=`CREDIT-${String(order_id).slice(-6)}-${uid().toUpperCase().slice(0,6)}`;
    let customerId=null;
    try { const cd=await shopifyREST('GET',`orders/${order_id}.json?fields=customer`); customerId=cd?.order?.customer?.id||null; } catch(e){}
    let gcr=null; try { const gcp={ gift_card:{ initial_value:total,code,note:note||`Store credit #${order_id}` } }; if(customerId)gcp.gift_card.customer_id=customerId; gcr=await shopifyREST('POST','gift_cards.json',gcp); } catch(e){}
    if (!gcr?.gift_card?.id) {
      const en=(await shopifyREST('GET',`orders/${order_id}.json?fields=note`))?.order?.note||'';
      await shopifyREST('PUT',`orders/${order_id}.json`,{ order:{ id:order_id,note:(en+`\n[STORE CREDIT] Code:${code} ₹${total} ${new Date().toISOString()}`).slice(0,5000) }});
    }
    await updateOrderTags(order_id,['store-credit-issued'],[]);
    if (req_id)await supabase.from('requests').update({ refund_amount:parseFloat(total),status:'refunded' }).eq('req_id',req_id);
    await auditLog(order_id,req_id||null,'store_credit','merchant',`${code} ₹${total}`);
    res.json({ success:true,method:gcr?.gift_card?.id?'gift_card':'manual_note',code,amount:total });
  } catch(e){ res.status(500).json({ error:e.message }); }
});

app.post('/api/shopify/exchange/:order_id', async (req,res)=>{
  const { order_id }=req.params; const { exchange_items,customer_address,order_number,req_id }=req.body;
  try {
    // Guard: prevent duplicate exchange orders
    if (req_id) {
      const { data:existReq } = await supabase.from('requests').select('exchange_order_id,status,awb').eq('req_id',req_id).single();
      if (existReq?.exchange_order_id) return res.status(400).json({ error:`Exchange order already exists: ${existReq.exchange_order_id}` });
      // Block: exchange order must not be created before item is picked up
      const allowedStatuses = ['picked_up','delivered','refunded','exchange_fulfilled'];
      if (existReq && !allowedStatuses.includes(existReq.status)) {
        return res.status(400).json({ error:`Cannot create exchange order yet — item has not been picked up. Current status: ${existReq.status}. Exchange order will be auto-created once Delhivery picks up the return.` });
      }
    }
    const orig=await shopifyREST('GET',`orders/${order_id}.json?fields=id,email,shipping_address,billing_address,customer`);
    const o=orig?.order; if(!o)return res.status(400).json({ error:'Order not found' });
    const addr=customer_address||o.shipping_address||o.billing_address;
    const valid=(exchange_items||[]).filter(i=>i.variant_id); if(!valid.length)return res.status(400).json({ error:'No variant_id' });
    const excNum=await getNextExcNumber(); const excTag=`EXC${excNum}`;
    const draft=await shopifyREST('POST','draft_orders.json',{ draft_order:{ line_items:valid.map(i=>({ variant_id:parseInt(i.variant_id)||i.variant_id,quantity:i.quantity||1,applied_discount:{ description:'Exchange',value_type:'percentage',value:'100',amount:String(i.price||'0'),title:'Exchange' } })),customer:o.customer?{ id:o.customer.id }:undefined,shipping_address:addr,billing_address:o.billing_address||addr,email:o.email,note:`Exchange for #${order_number||order_id}${req_id?' ('+req_id+')':''}`,tags:`exchange-order,${excTag}`,send_invoice:false }});
    if (!draft?.draft_order?.id)return res.status(400).json({ error:'Draft failed:'+(JSON.stringify(draft?.errors||draft)).slice(0,200) });
    const done=await shopifyREST('PUT',`draft_orders/${draft.draft_order.id}/complete.json`);
    await updateOrderTags(order_id,['exchange-fulfilled'],[]);
    if (req_id)await supabase.from('requests').update({ exchange_order_id:String(done?.draft_order?.order_id||''),exchange_order_name:done?.draft_order?.name||excTag,exchange_order_number:excTag,status:'exchange_fulfilled' }).eq('req_id',req_id);
    await auditLog(order_id,req_id||null,'exchange_manual','merchant',`${done?.draft_order?.name||excTag}`);
    res.json({ success:true,new_order_id:done?.draft_order?.order_id,new_order_name:done?.draft_order?.name||excTag,exc_number:excTag });
  } catch(e){ res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// BULK ACTIONS
// ══════════════════════════════════════════
app.post('/api/bulk/approve', async (req,res)=>{
  const { order_ids,actor }=req.body; if(!order_ids?.length)return res.status(400).json({ error:'No IDs' });
  const results=[];
  for (const oid of order_ids) {
    try {
      const { data:reqs }=await supabase.from('requests').select('request_type').eq('order_id',String(oid)).eq('status','pending');
      const type=reqs?.[0]?.request_type||'return';
      const at=type==='exchange'?'exchange-approved':type==='mixed'?'mixed-approved':'return-approved';
      const rt=type==='exchange'?'exchange-requested':type==='mixed'?'mixed-requested':'return-requested';
      await updateOrderTags(oid,[at],[rt]);
      await supabase.from('requests').update({ status:'approved',approved_at:new Date().toISOString() }).eq('order_id',String(oid)).eq('status','pending');
      await auditLog(oid,null,'bulk_approved',actor||'merchant','');
      results.push({ order_id:oid,success:true });
    } catch(e){ results.push({ order_id:oid,success:false,error:e.message }); }
  }
  res.json({ results,succeeded:results.filter(r=>r.success).length,failed:results.filter(r=>!r.success).length });
});

app.post('/api/bulk/reject', async (req,res)=>{
  const { order_ids,reason,actor }=req.body; if(!order_ids?.length)return res.status(400).json({ error:'No IDs' });
  const results=[];
  for (const oid of order_ids) {
    try {
      const { data:reqs }=await supabase.from('requests').select('request_type').eq('order_id',String(oid)).limit(1);
      const type=reqs?.[0]?.request_type||'return';
      const rt=type==='exchange'?'exchange-rejected':type==='mixed'?'mixed-rejected':'return-rejected';
      const rem=type==='exchange'?'exchange-requested':type==='mixed'?'mixed-requested':'return-requested';
      await updateOrderTags(oid,[rt],[rem]);
      await supabase.from('requests').update({ status:'rejected' }).eq('order_id',String(oid)).in('status',['pending','approved']);
      await auditLog(oid,null,'bulk_rejected',actor||'merchant',reason||'');
      results.push({ order_id:oid,success:true });
    } catch(e){ results.push({ order_id:oid,success:false,error:e.message }); }
  }
  res.json({ results,succeeded:results.filter(r=>r.success).length,failed:results.filter(r=>!r.success).length });
});

// ══════════════════════════════════════════
// ANALYTICS + AUDIT
// ══════════════════════════════════════════
// ── DELETE REQUEST ──
app.delete('/api/returns/:req_id', async (req,res)=>{
  const { req_id }=req.params;
  const order_id = req.body?.order_id || null; // fallback if req_id not in DB
  try {
    const TAGS_TO_REMOVE=['return-requested','return-approved','return-rejected','return-refunded','exchange-requested','exchange-approved','exchange-rejected','exchange-fulfilled','mixed-requested','mixed-approved','mixed-rejected','pickup-scheduled','pickup-scan','return-received','flagged-review','keep-it-rule','store-credit-issued','pickup-failed'];

    // Try Supabase first
    const { data:request } = await supabase.from('requests').select('order_id,request_type').eq('req_id',req_id).single();

    const oid = request?.order_id || order_id;
    if (!oid) return res.status(404).json({ error:'No order ID found. Pass order_id in request body.' });

    // Clear Shopify tags regardless of whether in DB
    try { await updateOrderTags(oid,[],TAGS_TO_REMOVE); } catch(e){ console.error('[delete tags]',e.message); }

    // Delete from Supabase if exists
    if (request) await supabase.from('requests').delete().eq('req_id',req_id);

    await auditLog(oid,req_id,'request_deleted',req.body?.actor||'merchant','Deleted — customer can resubmit');
    res.json({ success:true });
  } catch(e){ res.status(500).json({ error:e.message }); }
});

// ── REJECT REQUEST (final) ──
app.post('/api/returns/:req_id/reject-final', async (req,res)=>{
  const { req_id }=req.params;
  try {
    const { data:request }=await supabase.from('requests').select('order_id,request_type').eq('req_id',req_id).single();
    if (!request) return res.status(404).json({ error:'Not found' });
    const rt=request.request_type;
    const rejTag=rt==='exchange'?'exchange-rejected':rt==='mixed'?'mixed-rejected':'return-rejected';
    const remTag=rt==='exchange'?'exchange-requested':rt==='mixed'?'mixed-requested':'return-requested';
    await updateOrderTags(request.order_id,[rejTag],[remTag]);
    await supabase.from('requests').update({ status:'rejected' }).eq('req_id',req_id);
    await auditLog(request.order_id,req_id,'rejected',req.body?.actor||'merchant',req.body?.reason||'');
    res.json({ success:true });
  } catch(e){ res.status(500).json({ error:e.message }); }
});


// ── ATTACH MANUAL AWB ──
app.post('/api/returns/attach-awb', async (req,res)=>{
  const { order_id, req_id, awb } = req.body;
  if (!awb||!order_id) return res.status(400).json({ error:'Missing awb or order_id' });
  try {
    // Update Supabase if req_id exists
    if (req_id) {
      await supabase.from('requests').update({
        awb, awb_status:'Manually Attached', awb_final:false,
        status:'pickup_scheduled', pickup_created_at:new Date().toISOString()
      }).eq('req_id',req_id);
    } else {
      // Find most recent request for this order
      const { data:reqs } = await supabase.from('requests').select('req_id')
        .eq('order_id',String(order_id)).order('created_at',{ascending:false}).limit(1);
      if (reqs?.length) {
        await supabase.from('requests').update({
          awb, awb_status:'Manually Attached', awb_final:false,
          status:'pickup_scheduled', pickup_created_at:new Date().toISOString()
        }).eq('req_id',reqs[0].req_id);
      }
    }
    // Update Shopify order tags + note
    await updateOrderTags(order_id,['pickup-scheduled'],[]);
    const fn = await shopifyREST('GET',`orders/${order_id}.json?fields=note`);
    const en = fn?.order?.note||'';
    await shopifyREST('PUT',`orders/${order_id}.json`,{
      order:{ id:order_id, note:(en+`\nMANUAL AWB: ${awb} | ${new Date().toISOString()}`).slice(0,5000) }
    });
    await auditLog(order_id, req_id||'manual', 'awb_attached', 'merchant', `AWB:${awb} (manually attached)`);
    console.log(`[AWB] Manually attached ${awb} to order ${order_id}`);
    res.json({ success:true, awb });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

app.get('/api/analytics', async (_req,res)=>{
  try {
    const { data:reqs }=await supabase.from('requests').select('*').neq('status','archived');
    const total=(reqs||[]).length;
    const approved=(reqs||[]).filter(r=>!['pending','rejected'].includes(r.status)).length;
    const refunded=(reqs||[]).filter(r=>r.status==='refunded').length;
    const revenue=(reqs||[]).reduce((s,r)=>s+parseFloat(r.total_price||0),0);
    res.json({ total_requests:total,approved,refunded,revenue_at_risk:revenue,approval_rate:total>0?Math.round(approved/total*100):0 });
  } catch(e){ res.status(500).json({ error:e.message }); }
});

app.get('/api/audit', async (req,res)=>{
  try {
    let q=supabase.from('audit_log').select('*').order('created_at',{ ascending:false }).limit(parseInt(req.query.limit)||100);
    if (req.query.order_id)q=q.eq('order_id',String(req.query.order_id));
    const { data }=await q;
    res.json({ logs:data||[] });
  } catch(e){ res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// SETTINGS
// ══════════════════════════════════════════
app.get('/api/settings', (_req,res)=>res.json({ return_window_days:RETURN_WINDOW_DAYS, exchange_window_days:EXCHANGE_WINDOW_DAYS, restocking_fee_pct:RESTOCKING_FEE_PCT, warehouse:WAREHOUSE_CONFIG, delhivery:{ configured:!!DELHIVERY_TOKEN, warehouse:DELHIVERY_WAREHOUSE, mode:'production' } }));
app.post('/api/settings', async (req,res)=>{
  const { return_window_days, exchange_window_days, restocking_fee_pct, warehouse } = req.body;
  if (return_window_days   != null) RETURN_WINDOW_DAYS   = parseInt(return_window_days);
  if (exchange_window_days != null) EXCHANGE_WINDOW_DAYS = parseInt(exchange_window_days);
  if (restocking_fee_pct   != null) RESTOCKING_FEE_PCT   = parseFloat(restocking_fee_pct);
  if (warehouse) WAREHOUSE_CONFIG = { ...WAREHOUSE_CONFIG, ...warehouse };
  await supabase.from('settings').upsert({ key:'app_settings', value:{ return_window_days:RETURN_WINDOW_DAYS, exchange_window_days:EXCHANGE_WINDOW_DAYS, restocking_fee_pct:RESTOCKING_FEE_PCT, warehouse:WAREHOUSE_CONFIG } });
  console.log(`[Settings] Saved — ReturnWindow:${RETURN_WINDOW_DAYS}d ExchangeWindow:${EXCHANGE_WINDOW_DAYS}d`);
  res.json({ success:true, return_window_days:RETURN_WINDOW_DAYS, exchange_window_days:EXCHANGE_WINDOW_DAYS });
});

app.get('/api/delhivery/config',  (_req,res)=>res.json({ configured:!!DELHIVERY_TOKEN,warehouse:DELHIVERY_WAREHOUSE,mode:'production' }));
app.get('/api/delhivery/track/:waybill', async (req,res)=>{ try { const d=await delhiveryAPI('GET',`/api/v1/packages/json/?waybill=${req.params.waybill}`); const pkg=d?.ShipmentData?.[0]?.Shipment; if(!pkg)return res.json({ found:false }); res.json({ found:true,waybill:pkg.AWB,status:pkg.Status?.Status,status_code:pkg.Status?.StatusCode,scans:(pkg.Scans||[]).reverse().map(s=>({ status:s.ScanDetail?.Scan,detail:s.ScanDetail?.Instructions,location:s.ScanDetail?.ScannedLocation,date:s.ScanDetail?.ScanDateTime,code:s.ScanDetail?.StatusCode })) }); } catch(e){ res.status(500).json({ error:e.message }); } });
app.get('/api/delhivery/serviceability/:pincode', async (req,res)=>{ try { const d=await delhiveryAPI('GET',`/c/api/pin-codes/json/?filter_codes=${req.params.pincode}`); const pin=d?.delivery_codes?.[0]; res.json({ serviceable:!!pin,pickup:pin?.pickup?.toLowerCase()==='y',pincode:req.params.pincode }); } catch(e){ res.status(500).json({ error:e.message }); } });
app.post('/api/delhivery/create-pickup', async (req,res)=>{
  try {
    const { req_id, order_id, order_number, customer_name, customer_phone,
            customer_address, customer_city, customer_state, customer_pincode,
            products_desc, total_amount, quantity, weight } = req.body;

    let request = null;

    // If req_id provided — look up from Supabase
    if (req_id) {
      const { data } = await supabase.from('requests').select('*').eq('req_id',req_id).single();
      request = data;
    }

    // If called from dashboard with order details directly (no req_id)
    // Build a synthetic request object
    if (!request && order_id) {
      // Try to find existing request for this order in Supabase
      const { data:existing } = await supabase.from('requests').select('*')
        .eq('order_id',String(order_id)).order('created_at',{ascending:false}).limit(1);

      if (existing?.length) {
        request = existing[0];
      } else {
        // Build synthetic request from dashboard params
        request = {
          req_id: `${order_number}_MAN001`,
          order_id: String(order_id),
          order_number: String(order_number||order_id),
          items: [],
          address: {
            name:     customer_name||'Customer',
            address1: customer_address||'',
            city:     customer_city||'',
            province: customer_state||'',
            zip:      String(customer_pincode||''),
            phone:    String(customer_phone||'')
          },
          total_price: total_amount||'0',
          request_type: 'return'
        };
      }
      // Always use the address from dashboard call (most up to date)
      if (customer_pincode) {
        request.address = {
          name:     customer_name||request.address?.name||'Customer',
          address1: customer_address||request.address?.address1||'',
          city:     customer_city||request.address?.city||'',
          province: customer_state||request.address?.province||'',
          zip:      String(customer_pincode),
          phone:    String(customer_phone||request.address?.phone||'')
        };
      }
    }

    if (!request) return res.status(404).json({ error:'Request not found — submit a return request first or provide order details' });

    const waybill = await createDelhiveryPickup(request);
    res.json({ success:!!waybill, waybill });
  } catch(e) {
    const isPincode = e.code==='NON_SERVICEABLE' || e.message?.toLowerCase().includes('non serviceable') || e.message?.toLowerCase().includes('pincode');
    res.status(isPincode?422:500).json({ error: e.message, pincode_error: isPincode });
  }
});

app.get('/api/store/export', async (_req,res)=>{ const { data:requests }=await supabase.from('requests').select('*'); res.setHeader('Content-Disposition',`attachment; filename=returns-backup-${Date.now()}.json`); res.json({ exported_at:new Date().toISOString(),shop:SHOP_DOMAIN,requests:requests||[] }); });

// ══════════════════════════════════════════
// PICKUP SCHEDULER  (every 5 min)
// Approves → creates pickup 2hrs later
// ══════════════════════════════════════════
async function runPickupScheduler() {
  try {
    const twoHoursAgo=new Date(Date.now()-2*60*60*1000).toISOString();
    const { data:allApproved, error:se }=await supabase.from('requests').select('*').eq('status','approved').is('awb',null);
    if (se) { console.error('[Scheduler] Supabase error:', se.message); return; }
    // Only process requests approved 2+ hours ago (or with no approved_at — legacy)
    const eligible=(allApproved||[]).filter(r=>!r.approved_at||new Date(r.approved_at)<new Date(twoHoursAgo));
    console.log(`[Scheduler] ${eligible.length}/${allApproved?.length||0} eligible for pickup`);
    if (!eligible.length) return;
    for (const request of eligible) {
      // Check if same order already has AWB — combine
      const { data:sameOrder }=await supabase.from('requests').select('awb').eq('order_id',request.order_id).not('awb','is',null).neq('req_id',request.req_id).limit(1);
      if (sameOrder?.length&&sameOrder[0].awb) {
        await supabase.from('requests').update({ awb:sameOrder[0].awb,status:'pickup_scheduled',awb_status:'Combined with existing pickup' }).eq('req_id',request.req_id);
        await auditLog(request.order_id,request.req_id,'pickup_combined','system',`Reusing AWB ${sameOrder[0].awb}`);
      } else {
        try { await createDelhiveryPickup(request); } catch(pe){ console.error('[Scheduler pickup]', pe.message); }
      }
    }
  } catch(e){ console.error('[Scheduler]',e.message); }
}
setInterval(runPickupScheduler, 5*60*1000);
setTimeout(runPickupScheduler,  30*1000);

// ══════════════════════════════════════════
// TRACKING POLLER  (every 12 hours, twice/day)
// ══════════════════════════════════════════
async function pollTracking() {
  try {
    const { data:active }=await supabase.from('requests').select('*').not('awb','is',null).eq('awb_final',false).not('status','in','("archived","rejected","pickup_failed")');
    if (!active?.length)return;
    console.log(`[Poll] Checking ${active.length} AWBs`);

    // Deduplicate AWBs
    const uniqueAwbs=[...new Set(active.map(r=>r.awb))];

    // Batch in groups of 10
    for (let i=0;i<uniqueAwbs.length;i+=10) {
      const batch=uniqueAwbs.slice(i,i+10).join(',');
      try {
        const d=await delhiveryAPI('GET',`/api/v1/packages/json/?waybill=${batch}`);
        const shipments=d?.ShipmentData||[];
        for (const { Shipment:pkg } of shipments) {
          if (!pkg)continue;
          const waybill=pkg.AWB;
          const statusCode=pkg.Status?.StatusCode||'';
          const statusText=pkg.Status?.Status||'';
          const statusType=pkg.Status?.StatusType||'';

          const reqs=active.filter(r=>r.awb===waybill);
          for (const request of reqs) {
            if (request.awb_status_code===statusCode)continue;

            await supabase.from('requests').update({ awb_status:statusText,awb_status_code:statusCode,awb_last_scan:pkg.Status,awb_last_checked:new Date().toISOString() }).eq('req_id',request.req_id);
            console.log(`[Poll] ${waybill} → ${statusCode} (${statusText}) for ${request.req_id}`);

            // ── PICKUP DETECTED ──
            // StatusType 'PU' covers all pickup scan events from Delhivery
            const isPickup = statusType==='PU' || statusCode==='EOD-77' || statusCode==='X-UCI-PU' || statusCode==='PKT-AR';
            if (isPickup && !['picked_up','delivered','refunded','exchange_fulfilled','archived'].includes(request.status)) {
              // Email 3: In transit — sent after exchange order is created (a few lines below)
              await updateOrderTags(request.order_id,['pickup-scan'],['pickup-scheduled']);
              await supabase.from('requests').update({ status:'picked_up' }).eq('req_id',request.req_id);
              await auditLog(request.order_id,request.req_id,'carrier_pickup','poll',`AWB ${waybill} code:${statusCode}`);
              // Create exchange order on pickup for exchange/mixed — keep status as picked_up
              // (exchange_fulfilled is only set on warehouse delivery, so the delivery block still runs)
              let pickedUpExchName = null;
              if (request.request_type==='exchange'||request.request_type==='mixed') {
                const latestReq = (await supabase.from('requests').select('*').eq('req_id',request.req_id).single()).data||request;
                if (!latestReq.exchange_order_id) {
                  await createExchangeOrder(latestReq);
                }
                pickedUpExchName = latestReq.exchange_order_name || null;
              }
              // Email 3: In transit
              if (request.customer_email) {
                sendEmail(request.customer_email, `We've Picked Up Your Package — Order #${request.order_number}`,
                  emailInTransit({ name:(request.customer_name||'').split(' ')[0], orderNumber:request.order_number, reqId:request.req_id, requestType:request.request_type, exchangeOrderName:pickedUpExchName }))
                  .catch(()=>{});
              }
            }

            // ── DELIVERED TO WAREHOUSE ──
            // Covers: RD-AC, DTO, StatusType=RD (Return Delivered), RTO-OFD delivered,
            // and text-based fallback for any "return delivered" / "delivered to origin" variants
            const isWarehouseDelivery =
              statusCode==='RD-AC' ||
              statusCode==='DTO'   ||
              statusCode==='RTO'   ||
              statusType==='RD'    ||
              statusType==='RTD'   ||
              (statusType==='DL' && (statusText.toLowerCase().includes('return')||statusText.toLowerCase().includes('origin'))) ||
              statusText.toLowerCase().includes('delivered to origin') ||
              statusText.toLowerCase().includes('return delivered') ||
              statusText.toLowerCase().includes('return delivery accepted');
            if (isWarehouseDelivery && !['refunded','archived'].includes(request.status)) {
              await updateOrderTags(request.order_id,['return-received'],['pickup-scan','pickup-scheduled']);
              await supabase.from('requests').update({ status:'delivered', awb_final:true }).eq('req_id',request.req_id);
              await auditLog(request.order_id, request.req_id, 'carrier_delivered', 'poll', `AWB ${waybill} code:${statusCode}`);

              const latestReq = (await supabase.from('requests').select('*').eq('req_id',request.req_id).single()).data || request;
              const custEmail  = latestReq.customer_email || '';
              const custFirst  = (latestReq.customer_name||'').split(' ')[0] || '';

              // Email 4: Delivered to warehouse
              if (custEmail) {
                sendEmail(custEmail, `We've Received Your Item — Order #${latestReq.order_number}`,
                  emailDelivered({ name:custFirst, orderNumber:latestReq.order_number, reqId:latestReq.req_id, requestType:latestReq.request_type, refundMethod:latestReq.refund_method }))
                  .catch(()=>{});
              }

              // Block refund if any item is non-returnable (watch/wallet)
              const hasNonReturnable = (latestReq.items||[]).some(i => isNonReturnable(i.title||''));
              if (hasNonReturnable) {
                console.warn(`[Poll] BLOCKED refund for ${latestReq.req_id} — non-returnable item detected`);
                await auditLog(latestReq.order_id, latestReq.req_id, 'refund_blocked', 'system', 'Non-returnable item (watch/wallet)');
                await supabase.from('requests').update({ status:'archived' }).eq('req_id',latestReq.req_id);
                continue;
              }

              if (latestReq.request_type==='return') {
                let refundResult = null;
                try { refundResult = await processRefund(latestReq); } catch(refErr) { console.error('[Poll refund]',refErr.message); }
                await supabase.from('requests').update({ status:'refunded' }).eq('req_id',request.req_id);
                await updateOrderTags(request.order_id,['return-refunded'],['return-approved','return-requested','return-received','pickup-scan']);
                // Email 5: Refund processed
                if (custEmail && refundResult) {
                  sendEmail(custEmail, `Refund Processed — Order #${latestReq.order_number}`,
                    emailRefundProcessed({ name:custFirst, orderNumber:latestReq.order_number, reqId:latestReq.req_id, amount:refundResult.amount, method:refundResult.method }))
                    .catch(()=>{});
                }
              } else if (latestReq.request_type==='exchange') {
                await supabase.from('requests').update({ status:'exchange_fulfilled' }).eq('req_id',request.req_id);
                await updateOrderTags(request.order_id,['exchange-fulfilled'],['exchange-approved','exchange-requested','pickup-scan']);
              } else if (latestReq.request_type==='mixed') {
                if (!latestReq.exchange_order_id) await createExchangeOrder(latestReq);
                let refundResult = null;
                try { refundResult = await processRefund(latestReq); } catch(refErr) { console.error('[Poll mixed refund]',refErr.message); }
                await supabase.from('requests').update({ status:'exchange_fulfilled' }).eq('req_id',request.req_id);
                await updateOrderTags(request.order_id,['exchange-fulfilled'],['mixed-approved','mixed-requested','pickup-scan']);
                if (custEmail && refundResult) {
                  sendEmail(custEmail, `Refund Processed — Order #${latestReq.order_number}`,
                    emailRefundProcessed({ name:custFirst, orderNumber:latestReq.order_number, reqId:latestReq.req_id, amount:refundResult.amount, method:refundResult.method }))
                    .catch(()=>{});
                }
              }

              // Archive immediately on warehouse delivery
              await archiveRequest(request.req_id, request.order_id);
            }
          }
        }
      } catch(e){ console.error('[Poll batch]',e.message); }
      await new Promise(r=>setTimeout(r,1000)); // 1s between batches
    }
  } catch(e){ console.error('[pollTracking]',e.message); }
}
setInterval(pollTracking, 2*60*60*1000); // every 2 hours
setTimeout(pollTracking,  2*60*1000);    // 2 min after startup

// ══════════════════════════════════════════
// REPAIR: Auto-refund archived returns with missing refund_id
// ══════════════════════════════════════════
async function repairPendingRefunds() {
  try {
    // 1. Find archived returns with no refund_id → refund + re-archive
    const { data:stuck } = await supabase.from('requests')
      .select('*').eq('status','archived').is('refund_id',null).eq('request_type','return');
    for (const req of stuck||[]) {
      try {
        if ((req.items||[]).some(i => isNonReturnable(i.title||''))) {
          console.warn(`[repair] BLOCKED non-returnable ${req.req_id}`);
          await auditLog(req.order_id, req.req_id, 'refund_blocked', 'system', 'Non-returnable item (watch/wallet)');
          continue;
        }
        await processRefund(req);
        await supabase.from('requests').update({ status:'archived' }).eq('req_id',req.req_id);
        console.log(`[repair] ✅ Refunded+archived ${req.req_id}`);
      } catch(e) { console.error(`[repair] ❌ ${req.req_id}:`, e.message); }
    }
    // 2. Find refunded (not yet archived) returns that came from the old archived state
    const { data:needsArchive } = await supabase.from('requests')
      .select('*').eq('status','refunded').not('refund_id','is',null).eq('request_type','return');
    for (const req of needsArchive||[]) {
      try {
        await supabase.from('requests').update({ status:'archived', archived_at:new Date().toISOString() }).eq('req_id',req.req_id);
        console.log(`[repair] ✅ Archived ${req.req_id}`);
      } catch(e) { console.error(`[repair-archive] ❌ ${req.req_id}:`, e.message); }
    }
    if ((stuck?.length||0)+(needsArchive?.length||0) > 0)
      console.log(`[repair] Done — refunded:${stuck?.length||0} archived:${needsArchive?.length||0}`);
  } catch(e) { console.error('[repairPendingRefunds]', e.message); }
}
setTimeout(repairPendingRefunds, 3*60*1000); // 3 min after startup

// ── Manual archive/unarchive ──
app.post('/api/requests/:reqId/archive', async (req,res)=>{
  const { reqId } = req.params;
  try {
    const { data:r } = await supabase.from('requests').select('order_id').eq('req_id',reqId).single();
    await supabase.from('requests').update({ status:'archived', archived_at:new Date().toISOString() }).eq('req_id',reqId);
    if (r?.order_id) await auditLog(r.order_id, reqId, 'archived', 'manual', 'Manually archived');
    res.json({ success:true });
  } catch(e){ res.status(500).json({ error:e.message }); }
});
app.post('/api/requests/:reqId/unarchive', async (req,res)=>{
  const { reqId } = req.params;
  try {
    const { data:r } = await supabase.from('requests').select('order_id').eq('req_id',reqId).single();
    await supabase.from('requests').update({ status:'delivered' }).eq('req_id',reqId);
    if (r?.order_id) await auditLog(r.order_id, reqId, 'unarchived', 'manual', 'Manually unarchived');
    res.json({ success:true });
  } catch(e){ res.status(500).json({ error:e.message }); }
});

// ── Process all archived-but-unrefunded returns, then archive them ──
app.post('/api/admin/process-pending-refunds', async (req,res)=>{
  try {
    await repairPendingRefunds();
    res.json({ success:true });
  } catch(e){ res.status(500).json({ error:e.message }); }
});

// ── Manual Delhivery sync (triggers pollTracking immediately) ──
app.post('/api/delhivery/sync-now', async (req,res)=>{
  try {
    await pollTracking();
    res.json({ success:true, message:'AWB sync complete' });
  } catch(e){ res.status(500).json({ error:e.message }); }
});

// ── Repair Shopify tags to match Supabase status (fixes tag/status mismatch) ──
app.post('/api/admin/repair-tags', async (req,res)=>{
  try {
    // Find requests where Supabase says refunded/archived but Shopify tags may lag
    const { data:reqs } = await supabase.from('requests')
      .select('req_id,order_id,status,request_type')
      .in('status',['refunded','archived','exchange_fulfilled']);
    let fixed=0;
    for (const r of reqs||[]) {
      try {
        if (r.request_type==='return') {
          await updateOrderTags(r.order_id,['return-refunded'],['return-approved','return-requested','return-received','pickup-scan','mixed-approved']);
        } else {
          await updateOrderTags(r.order_id,['exchange-fulfilled'],['exchange-approved','exchange-requested','mixed-approved','mixed-requested','pickup-scan']);
        }
        fixed++;
      } catch(e){ console.error('[repair-tags]',r.req_id,e.message); }
    }
    res.json({ success:true, fixed, total:reqs?.length||0 });
  } catch(e){ res.status(500).json({ error:e.message }); }
});

// ══════════════════════════════════════════
// EASEBUZZ PAYMENT ROUTES
// ══════════════════════════════════════════

// Popup callback HTML helpers
const _payHtml = (ok, txnid) => `<!DOCTYPE html><html><head><meta charset="utf-8">
<style>body{font-family:sans-serif;display:flex;flex-direction:column;align-items:center;justify-content:center;height:100vh;margin:0;background:${ok?'#f0fdf4':'#fef2f2'};gap:14px}
.icon{font-size:60px}.msg{font-size:18px;font-weight:700;color:${ok?'#16a34a':'#dc2626'}}.sub{font-size:13px;color:#6b7280}</style></head>
<body><div class="icon">${ok?'✅':'❌'}</div>
<div class="msg">${ok?'Payment Successful!':'Payment Failed'}</div>
<div class="sub">Closing window…</div>
<script>
  try{if(window.opener)window.opener.postMessage({type:'${ok?'PAYMENT_SUCCESS':'PAYMENT_FAILED'}',txnid:${JSON.stringify(txnid)}},'*');}catch(e){}
  setTimeout(()=>window.close(),1200);
</script></body></html>`;

app.post('/api/payments/initiate', async (req,res)=>{
  try {
    const { amount, order_id, firstname, email, phone } = req.body;
    if (!EASEBUZZ_KEY||!EASEBUZZ_SALT) return res.json({ success:false, error:'Payment not configured' });
    const txnid  = `BLK${String(order_id).slice(-6)}_${Date.now()}`;
    const amtStr = parseFloat(amount).toFixed(2);
    const p = {
      key: EASEBUZZ_KEY, txnid, amount: amtStr,
      productinfo: `BLAKC Exchange Upgrade Order ${order_id}`,
      firstname: (firstname||'Customer').replace(/[^a-zA-Z\s]/g,'').trim().slice(0,50)||'Customer',
      email: email && email.includes('@') ? email : 'customer@blakc.store',
      phone: String(phone||'').replace(/\D/g,'').slice(-10).padStart(10,'0'),
      surl: `${BACKEND_URL}/api/payments/callback/success`,
      furl: `${BACKEND_URL}/api/payments/callback/failure`,
      udf1: String(order_id), udf2:'', udf3:'', udf4:'', udf5:''
    };
    if (EASEBUZZ_MID) p.mid = EASEBUZZ_MID;
    p.hash = ebHash(p);
    console.log(`[payments/initiate] txnid=${txnid} amt=${amtStr} order=${order_id}`);
    const r = await fetch(`${EASEBUZZ_BASE}/payment/initiateLink`,{
      method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'},
      body: new URLSearchParams(p).toString()
    });
    const data = await r.json();
    if (data.status === 1) {
      await supabase.from('payments').insert({ txnid, order_id:String(order_id), amount:parseFloat(amtStr), status:'pending' }).throwOnError();
      console.log(`[payments/initiate] ✅ access_key obtained for ${txnid}`);
      res.json({ success:true, txnid, payment_url:`${EASEBUZZ_BASE}/pay/${data.data}` });
    } else {
      console.error('[payments/initiate] ❌ EB error:', JSON.stringify(data));
      res.json({ success:false, error: data.error_desc || data.message || data.error || JSON.stringify(data) });
    }
  } catch(e) { console.error('[payments/initiate]',e.message); res.json({ success:false, error:e.message }); }
});

app.post('/api/payments/callback/success', express.urlencoded({extended:true}), async (req,res)=>{
  const p = req.body;
  console.log('[payments/callback/success]', p.txnid, p.status);
  try {
    const expected = ebVerify(p);
    const valid = expected === p.hash;
    await supabase.from('payments').update({ status: valid?'paid':'hash_mismatch', txn_response:p }).eq('txnid', p.txnid||'');
  } catch(e) { console.error('[payments/callback/success]', e.message); }
  res.send(_payHtml(true, p.txnid||''));
});

app.post('/api/payments/callback/failure', express.urlencoded({extended:true}), async (req,res)=>{
  const p = req.body;
  console.log('[payments/callback/failure]', p.txnid, p.status);
  try { await supabase.from('payments').update({ status:'failed', txn_response:p }).eq('txnid', p.txnid||''); }
  catch(e) { console.error('[payments/callback/failure]', e.message); }
  res.send(_payHtml(false, p.txnid||''));
});

app.get('/api/payments/status/:txnid', async (req,res)=>{
  const { data } = await supabase.from('payments').select('status,amount').eq('txnid',req.params.txnid).single();
  res.json({ status: data?.status||'unknown', amount: data?.amount||0 });
});

const PORT=process.env.PORT||3000;
loadPersistedSettings().then(()=>{
  app.listen(PORT,()=>console.log(`[Server] ✅ Returns Manager v4 on :${PORT} | Auth:${!!ACCESS_TOKEN} | Shop:${SHOP_DOMAIN||'none'} | ReturnWindow:${RETURN_WINDOW_DAYS}d`));
});
