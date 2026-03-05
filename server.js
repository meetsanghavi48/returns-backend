const express = require('express');
const cors    = require('cors');
const fetch   = require('node-fetch');
const crypto  = require('crypto');
const path    = require('path');
const fs      = require('fs');

const app = express();
app.use(cors({ origin:'*', methods:['GET','POST','PUT','DELETE','OPTIONS'], allowedHeaders:['Content-Type','Authorization','X-Shopify-Access-Token'] }));
app.use(express.json({ limit:'10mb' }));
app.options('*', cors());
app.use((req,res,next)=>{ res.setHeader('Content-Security-Policy',"frame-ancestors 'self' https://*.myshopify.com https://admin.shopify.com"); res.setHeader('Access-Control-Allow-Origin','*'); next(); });

// ── DATA PERSISTENCE & HELPERS ──
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

function dataPath(name) { return path.join(DATA_DIR, name + '.json'); }

function loadJSON(name, def) {
  try {
    if (fs.existsSync(dataPath(name))) {
      return JSON.parse(fs.readFileSync(dataPath(name, 'utf8')));
    }
  } catch(e) { console.error(`[DB Error] Loading ${name}:`, e); }
  return def;
}

function saveJSON(name, data) {
  try { fs.writeFileSync(dataPath(name), JSON.stringify(data, null, 2)); }
  catch(e) { console.error(`[DB Error] Saving ${name}:`, e); }
}

const _timeouts = {};
function saveDeferred(name, data) {
  if (_timeouts[name]) clearTimeout(_timeouts[name]);
  _timeouts[name] = setTimeout(() => saveJSON(name, data), 2000);
}

// ── SHOPIFY CONFIG ──
const SHOPIFY_API_KEY    = process.env.SHOPIFY_API_KEY    || 'c1542c4ed17151e558edc3f37ceb9fd2';
const SHOPIFY_API_SECRET = process.env.SHOPIFY_API_SECRET || '';
const BACKEND_URL        = process.env.BACKEND_URL        || 'https://returns-backend.onrender.com';
const SCOPES = 'read_orders,write_orders,read_products,write_products,read_draft_orders,write_draft_orders,read_customers,write_customers,read_inventory,write_inventory';

const _auth = loadJSON('auth', {});
let ACCESS_TOKEN = process.env.ACCESS_TOKEN || _auth.access_token || '';
let SHOP_DOMAIN  = process.env.SHOP_DOMAIN  || _auth.shop_domain  || '';

// ── DELHIVERY CONFIG ──
const _delv = loadJSON('delhivery', {});
let DELHIVERY_TOKEN     = process.env.DELHIVERY_TOKEN     || _delv.token     || '';
let DELHIVERY_WAREHOUSE = process.env.DELHIVERY_WAREHOUSE || _delv.warehouse || '';
let DELHIVERY_MODE      = process.env.DELHIVERY_MODE      || _delv.mode      || 'staging';

// ── IN-MEMORY STORES & RULES ──
let RULES = [
  { id:'rule_1', name:'VIP Size Exchange (Auto-Approve)', enabled:true, priority:1, category: 'on_submit', match_mode: 'all',
    conditions:[{field:'return_reason',op:'contains',value:'size'}, {field:'request_type',op:'eq',value:'exchange'}],
    action:'auto_approve', action_params:{message:'Size exchange approved automatically! Your new item will be shipped upon pickup.'} },
  { id:'rule_2', name:'Keep It (Defective & Low Value)', enabled:true, priority:2, category: 'on_submit', match_mode: 'all',
    conditions:[{field:'product_price',op:'lt',value:'400'},{field:'return_reason',op:'contains',value:'defective'}],
    action:'keep_it', action_params:{message:'No need to send this back. We are processing your replacement/refund right away.'} },
  { id:'rule_3', name:'Incentivize Store Credit', enabled:true, priority:3, category: 'on_submit', match_mode: 'all',
    conditions:[{field:'refund_method',op:'eq',value:'store_credit'}],
    action:'auto_approve', action_params:{message:'Thanks for choosing store credit! We have added a 10% bonus to your wallet.'} },
  { id:'rule_4', name:'Flag High-Value Returns', enabled:true, priority:4, category: 'on_submit', match_mode: 'all',
    conditions:[{field:'order_value',op:'gt',value:'4000'}],
    action:'flag_review', action_params:{note:'High-value return. Inspect carefully before refunding.'} },
  { id:'rule_5', name:'On Pickup Scan → Auto Exchange', enabled:true, priority:5, category: 'on_carrier', match_mode: 'all',
    conditions:[{field:'carrier_event',op:'eq',value:'pickup_scan'},{field:'request_type',op:'eq',value:'exchange'}],
    action:'auto_exchange', action_params:{} }
];

let RETURN_REQUESTS = loadJSON('requests', {});
let AUDIT_LOG = [];
let ANALYTICS       = loadJSON('analytics', { total_requests:0, approved:0, rejected:0, auto_approved:0, refunded_amount:0, store_credits_issued:0, exchanges_created:0, reasons:{}, products_returned:{} });
const _settings = loadJSON('settings', {});
let WAREHOUSE_CONFIG   = _settings.warehouse      || { name:process.env.WAREHOUSE_NAME||'', address:process.env.WAREHOUSE_ADDRESS||'', city:process.env.WAREHOUSE_CITY||'', state:process.env.WAREHOUSE_STATE||'', pincode:process.env.WAREHOUSE_PINCODE||'', phone:process.env.WAREHOUSE_PHONE||'' };
let RETURN_WINDOW_DAYS = _settings.return_window_days !== undefined ? _settings.return_window_days : (parseInt(process.env.RETURN_WINDOW_DAYS)||30);
let STORE_CREDIT_BONUS = _settings.store_credit_bonus !== undefined ? _settings.store_credit_bonus : (parseFloat(process.env.STORE_CREDIT_BONUS)||10);
let RESTOCKING_FEE_PCT = _settings.restocking_fee_pct !== undefined ? _settings.restocking_fee_pct : (parseFloat(process.env.RESTOCKING_FEE_PCT)||0);

// ── CORE HELPERS ──
function uid(){ return Date.now().toString(36)+Math.random().toString(36).slice(2,7); }
function gidToId(gid){ return typeof gid==='string'?gid.replace(/^gid:\/\/shopify\/\w+\//,''):String(gid||''); }

function auditLog(order_id,action,actor,details){
  const e={id:uid(),timestamp:new Date().toISOString(),order_id:String(order_id),action,actor:actor||'system',details:details||''};
  AUDIT_LOG.unshift(e); if(AUDIT_LOG.length>2000)AUDIT_LOG=AUDIT_LOG.slice(0,2000);
  console.log(`[AUDIT] #${order_id} | ${action} | ${actor} | ${details}`); return e;
}

async function shopifyREST(method,endpoint,body){
  if(!ACCESS_TOKEN||!SHOP_DOMAIN) throw new Error('Not authenticated');
  const opts={method:method||'GET',headers:{'X-Shopify-Access-Token':ACCESS_TOKEN,'Content-Type':'application/json'}};
  if(body)opts.body=JSON.stringify(body);
  const r=await fetch(`https://${SHOP_DOMAIN}/admin/api/2024-01/${endpoint}`,opts);
  const text=await r.text();
  if(!r.ok) console.error(`[Shopify ERROR] ${text.slice(0,300)}`);
  try{return JSON.parse(text);}catch(e){return{error:text};}
}

async function graphql(query,variables){
  if(!ACCESS_TOKEN||!SHOP_DOMAIN) throw new Error('Not authenticated');
  const r=await fetch(`https://${SHOP_DOMAIN}/admin/api/2024-01/graphql.json`,{
    method:'POST',headers:{'X-Shopify-Access-Token':ACCESS_TOKEN,'Content-Type':'application/json'},
    body:JSON.stringify({query,variables})
  });
  return r.json();
}

async function updateOrderTags(order_id,addTags,removeTags=[]){
  const d=await shopifyREST('GET',`orders/${order_id}.json?fields=tags`);
  let tags=(d?.order?.tags||'').split(',').map(t=>t.trim()).filter(Boolean);
  tags=tags.filter(t=>!removeTags.includes(t));
  addTags.forEach(t=>{if(!tags.includes(t))tags.push(t);});
  await shopifyREST('PUT',`orders/${order_id}.json`,{order:{id:order_id,tags:tags.join(', ')}});
  return tags;
}

function runRulesEngine(requestData){
  const sorted=[...RULES].filter(r=>r.enabled).sort((a,b)=>a.priority-b.priority);
  const matched=[];
  for(const rule of sorted){
    const mode=rule.match_mode||'all';
    const match=mode==='any'
      ?rule.conditions.some(cond=>evalCond(getFieldValue(requestData,cond.field),cond.op,cond.value))
      :rule.conditions.every(cond=>evalCond(getFieldValue(requestData,cond.field),cond.op,cond.value));
    if(match){matched.push({rule_id:rule.id,rule_name:rule.name,action:rule.action,params:rule.action_params});if(!rule.continue_on_match)break;}
  }
  return matched;
}

function getFieldValue(req,field){
  switch(field){
    case 'return_reason':     return(req.items||[]).map(i=>i.reason||'').join(' ').toLowerCase();
    case 'product_price':     return parseFloat(req.items?.[0]?.price||0);
    case 'order_value':       return parseFloat(req.total_price||0);
    case 'shipping_cost':     return parseFloat(req.shipping_cost||0);
    case 'customer_return_count': return parseInt(req.customer_return_count||0);
    case 'request_type':      return(req.request_type||'return').toLowerCase();
    case 'request_stage':     return(req.status||req.return_status||'pending').toLowerCase();
    case 'carrier_event':     return(req.carrier_event||'').toLowerCase();
    case 'refund_method':     return(req.refund_method||'').toLowerCase();
    case 'payment_method':    return(req.is_cod?'cod':'prepaid');
    case 'order_tags':        return((req.tags||[]).join(',')).toLowerCase();
    case 'days_since_order':  return req.days_since_order||0;
    case 'item_count':        return(req.items||[]).length;
    default: return '';
  }
}

function evalCond(val,op,target){
  const n=parseFloat(val),t=parseFloat(target);
  switch(op){
    case 'eq':       return String(val).toLowerCase()===String(target).toLowerCase();
    case 'neq':      return String(val).toLowerCase()!==String(target).toLowerCase();
    case 'contains': return String(val).toLowerCase().includes(String(target).toLowerCase());
    case 'gt':       return!isNaN(n)&&!isNaN(t)&&n>t;
    case 'lt':       return!isNaN(n)&&!isNaN(t)&&n<t;
    case 'gte':      return!isNaN(n)&&!isNaN(t)&&n>=t;
    case 'lte':      return!isNaN(n)&&!isNaN(t)&&n<=t;
    default: return false;
  }
}

function delhiveryBase(){ return DELHIVERY_MODE==='production'?'https://track.delhivery.com':'https://staging-express.delhivery.com'; }
async function delhiveryAPI(method,path,body,isForm){
  if(!DELHIVERY_TOKEN) throw new Error('Delhivery token not configured');
  const url=delhiveryBase()+path;
  const headers={'Authorization':`Token ${DELHIVERY_TOKEN}`};
  const opts={method:method||'GET',headers};
  if(body){
    if(isForm){headers['Content-Type']='application/x-www-form-urlencoded';opts.body=`format=json&data=${encodeURIComponent(JSON.stringify(body))}`;}
    else{headers['Content-Type']='application/json';opts.body=JSON.stringify(body);}
  }
  const r=await fetch(url,opts);
  const text=await r.text();
  try{return JSON.parse(text);}catch(e){return{raw:text};}
}

// ── SERVE FRONTEND ──
app.get('/dashboard',(req,res)=>{const f=path.join(__dirname,'index.html');if(fs.existsSync(f))res.sendFile(f);else res.send('<h2>Upload index.html to same directory as server.js</h2>');});
app.get('/portal',(req,res)=>{const f=path.join(__dirname,'portal.html');if(fs.existsSync(f))res.sendFile(f);else res.send('<h2>Upload portal.html to same directory as server.js</h2>');});
app.get('/',(req,res)=>res.redirect('/dashboard'));

// ── AUTH ──
app.get('/api/status',(req,res)=>res.json({connected:!!ACCESS_TOKEN,shop:SHOP_DOMAIN||null,return_window:RETURN_WINDOW_DAYS,store_credit_bonus:STORE_CREDIT_BONUS}));
app.get('/auth',(req,res)=>{const shop=req.query.shop||SHOP_DOMAIN;if(!shop)return res.status(400).send('Missing ?shop=');res.redirect(`https://${shop}/admin/oauth/authorize?client_id=${SHOPIFY_API_KEY}&scope=${SCOPES}&redirect_uri=${BACKEND_URL}/auth/callback&state=state123`);});
app.get('/auth/callback',async(req,res)=>{
  const{shop,code}=req.query;
  if(!shop||!code)return res.status(400).send('Missing params');
  try{
    const r=await fetch(`https://${shop}/admin/oauth/access_token`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({client_id:SHOPIFY_API_KEY,client_secret:SHOPIFY_API_SECRET,code})});
    const d=await r.json();
    if(d.access_token){ACCESS_TOKEN=d.access_token;SHOP_DOMAIN=shop;saveJSON('auth', { access_token: ACCESS_TOKEN, shop_domain: SHOP_DOMAIN }); auditLog('system','auth_connected','system',`Connected to ${shop}`);res.send(`<html><body style="font-family:sans-serif;padding:40px;background:#0f0f11;color:#f0f0f5"><h2 style="color:#7effa0">Connected!</h2><p>Token saved. <a href="/dashboard" style="color:#5ee8ff">Open Dashboard</a></p></body></html>`);}
    else res.status(400).send(JSON.stringify(d));
  }catch(e){res.status(500).send(e.message);}
});

// ── DASHBOARD APIs ──
app.get('/api/orders',async(req,res)=>{
  try{
    const result=await graphql(`{
      orders(first:250,reverse:true){
        edges{node{
          id name createdAt tags note
          displayFinancialStatus displayFulfillmentStatus
          totalPriceSet{shopMoney{amount currencyCode}}
          email phone
          customer{id displayName email phone}
          shippingAddress{name address1 address2 city province zip country phone}
          lineItems(first:20){edges{node{
            id title quantity
            originalUnitPriceSet{shopMoney{amount}}
            variant{id image{url}}
            product{id images(first:1){edges{node{url}}}}
          }}}
        }}
      }
    }`);
    if(result.errors) return res.status(400).json({error:result.errors[0].message});
    const orders=result.data.orders.edges.map(({node:o})=>{
      const tags=o.tags||[];
      const allTags=Array.isArray(tags)?tags:tags.split(',').map(t=>t.trim());
      const has=t=>allTags.includes(t);
      const hasAny=arr=>arr.some(t=>allTags.includes(t));
      const return_status=
        has('return-refunded')?'refunded':
        has('exchange-fulfilled')?'fulfilled':
        has('return-inspected')||has('exchange-inspected')?'inspected':
        has('pickup-scheduled')||has('pickup-scan')?'received':
        hasAny(['return-approved','exchange-approved','mixed-approved'])?'approved':
        hasAny(['return-rejected','exchange-rejected','mixed-rejected'])?'rejected':
        has('return-requested')?'pending':
        has('exchange-requested')?'exchange-pending':
        has('mixed-requested')?'pending':null;
      const request_type=
        hasAny(['exchange-requested','exchange-approved','exchange-rejected'])?'exchange':
        hasAny(['mixed-requested','mixed-approved','mixed-rejected'])?'mixed':'return';
      const oid=gidToId(o.id);
      return{
        id:oid, gid:o.id, order_number:o.name.replace('#',''), created_at:o.createdAt,
        financial_status:o.displayFinancialStatus.toLowerCase(), fulfillment_status:o.displayFulfillmentStatus.toLowerCase(),
        total_price:o.totalPriceSet.shopMoney.amount, currency:o.totalPriceSet.shopMoney.currencyCode,
        tags:allTags, note:o.note||'',
        customer_name:o.customer?.displayName||o.shippingAddress?.name||'',
        customer_email:o.customer?.email||o.email||'', customer_phone:o.customer?.phone||o.phone||o.shippingAddress?.phone||'',
        customer_id:o.customer?.id?gidToId(o.customer.id):null, shipping_address:o.shippingAddress||null,
        line_items:o.lineItems.edges.map(({node:li})=>({
          id:gidToId(li.id), gid:li.id, title:li.title, quantity:li.quantity, price:li.originalUnitPriceSet?.shopMoney?.amount||'0',
          variant_id:li.variant?.id?gidToId(li.variant.id):null, image_url:li.variant?.image?.url||li.product?.images?.edges?.[0]?.node?.url||null, product_id:li.product?.id?gidToId(li.product.id):null
        })),
        return_status, request_type, requests: RETURN_REQUESTS[oid]||[]
      };
    });
    res.json({orders, return_requests:orders.filter(o=>o.return_status).length});
  }catch(e){res.status(500).json({error:e.message});}
});

// ── PORTAL LOOKUP ──
app.get('/api/lookup',async(req,res)=>{
  const{order_number,contact}=req.query;
  if(!order_number)return res.status(400).json({error:'Missing order_number'});
  try{
    const cleanNum=String(order_number).replace(/^#+/,'').trim();
    const data=await shopifyREST('GET',`orders.json?name=%23${cleanNum}&status=any&fields=id,order_number,created_at,financial_status,fulfillment_status,total_price,currency,line_items,tags,note,email,phone,customer,shipping_address,billing_address,payment_gateway`);
    if(!data.orders?.length)return res.json({found:false});
    const o=data.orders[0];
    if(contact&&contact.trim()){
      const inp=contact.toLowerCase().trim().replace(/\D/g,'');
      const oEmail=(o.email||'').toLowerCase();
      const oPhone=(o.phone||o.shipping_address?.phone||'').replace(/\D/g,'');
      if(oEmail||oPhone){
        const emailOk=oEmail&&oEmail.includes(contact.toLowerCase().trim());
        const phoneOk=oPhone&&inp.length>=6&&oPhone.includes(inp.slice(-8));
        if(!emailOk&&!phoneOk)return res.json({found:false,mismatch:true});
      }
    }
    const orderDate=new Date(o.created_at);
    const daysDiff=(Date.now()-orderDate)/(1000*60*60*24);
    const deadline=new Date(orderDate.getTime()+RETURN_WINDOW_DAYS*24*60*60*1000).toISOString();
    const productIds=[...new Set((o.line_items||[]).map(li=>li.product_id).filter(Boolean))].join(',');
    let productData={};
    if(productIds){
      const prods=await shopifyREST('GET',`products.json?ids=${productIds}&fields=id,images,options,variants&limit=20`);
      (prods.products||[]).forEach(p=>{productData[p.id]={image:p.images?.[0]?.src||null,options:p.options||[],variants:p.variants||[],non_returnable:(p.tags||'').includes('non-returnable')};});
    }
    const tags=o.tags||'';
    const tagArr=tags.split(',').map(t=>t.trim());
    const has=t=>tagArr.includes(t);
    const return_status=
      has('return-refunded')?'refunded':
      has('exchange-fulfilled')?'fulfilled':
      has('return-inspected')||has('exchange-inspected')?'inspected':
      has('pickup-scheduled')||has('pickup-scan')?'received':
      (has('return-approved')||has('exchange-approved')||has('mixed-approved'))?'approved':
      (has('return-rejected')||has('exchange-rejected')||has('mixed-rejected'))?'rejected':
      has('return-requested')?'pending':
      has('exchange-requested')?'exchange-pending':
      has('mixed-requested')?'pending':null;
    const request_type=has('exchange-requested')||has('exchange-approved')||has('exchange-rejected')?'exchange':has('mixed-requested')||has('mixed-approved')||has('mixed-rejected')?'mixed':'return';
    
    res.json({
      found:true,
      order:{
        id:o.id, order_number:o.order_number, created_at:o.created_at, financial_status:o.financial_status,
        fulfillment_status:o.fulfillment_status, total_price:o.total_price, currency:o.currency,
        has_return:!!return_status, return_status, request_type, note:o.note, return_deadline:deadline,
        within_window:daysDiff<=RETURN_WINDOW_DAYS, days_remaining:Math.max(0,RETURN_WINDOW_DAYS-Math.floor(daysDiff)),
        address:o.shipping_address||o.billing_address||null, store_credit_bonus:STORE_CREDIT_BONUS,
        customer_name:o.customer?`${o.customer.first_name||''} ${o.customer.last_name||''}`.trim():o.shipping_address?.name||'',
        customer_email:o.email||'', customer_phone:o.phone||o.shipping_address?.phone||o.customer?.phone||'',
        payment_gateway:o.payment_gateway||'', is_cod:['cash_on_delivery','cod','manual'].includes((o.payment_gateway||'').toLowerCase()),
        requests:RETURN_REQUESTS[String(o.id)]||[],
        line_items:(o.line_items||[]).map(li=>({
          id:li.id, title:li.title, variant_title:li.variant_title||'', variant_id:li.variant_id, product_id:li.product_id,
          quantity:li.quantity, price:li.price, fulfillment_status:li.fulfillment_status,
          image_url:productData[li.product_id]?.image||null, non_returnable:productData[li.product_id]?.non_returnable||false,
          product_options:productData[li.product_id]?.options||[], product_variants:productData[li.product_id]?.variants||[]
        })).filter(li=>li.fulfillment_status==='fulfilled'||o.fulfillment_status==='fulfilled')
      }
    });
  }catch(e){res.status(500).json({error:e.message});}
});

// ── PRODUCTS ──
app.get('/api/products/featured',async(req,res)=>{try{const d=await shopifyREST('GET','products.json?fields=id,title,images,variants,options&limit=8');res.json({products:(d.products||[]).map(p=>({id:p.id,title:p.title,image:p.images?.[0]?.src||null,price:p.variants?.[0]?.price||'0'}))});}catch(e){res.json({products:[]});}});
app.get('/api/products/search',async(req,res)=>{const{q}=req.query;if(!q)return res.json({products:[]});try{const d=await shopifyREST('GET',`products.json?title=${encodeURIComponent(q)}&fields=id,title,images,variants,options&limit=8`);res.json({products:(d.products||[]).map(p=>({id:p.id,title:p.title,image:p.images?.[0]?.src||null,price:p.variants?.[0]?.price||'0',options:p.options||[],variants:(p.variants||[]).map(v=>({id:v.id,title:v.title,option1:v.option1,option2:v.option2,option3:v.option3,price:v.price,available:(v.inventory_quantity||1)>0}))}))});}catch(e){res.json({products:[]});}});
app.get('/api/products/:product_id',async(req,res)=>{try{const d=await shopifyREST('GET',`products/${req.params.product_id}.json?fields=id,title,images,options,variants`);const p=d.product;if(!p)return res.status(404).json({error:'Not found'});res.json({id:p.id,title:p.title,image:p.images?.[0]?.src||null,options:p.options||[],variants:(p.variants||[]).map(v=>({id:v.id,title:v.title,option1:v.option1,option2:v.option2,option3:v.option3,price:v.price,available:(v.inventory_quantity||1)>0}))});}catch(e){res.status(500).json({error:e.message});}});

// ── SUBMIT REQUEST ──
app.post('/api/returns/request',async(req,res)=>{
  const{order_id,order_number,items,refund_method,customer_note,address,media_urls,shipping_preference}=req.body;
  if(!order_id||!items?.length)return res.status(400).json({error:'Missing required fields'});
  try{
    const returns=items.filter(i=>i.action==='return');
    const exchanges=items.filter(i=>i.action==='exchange');
    const hasBoth=returns.length>0&&exchanges.length>0;
    const hasExchange=exchanges.length>0;
    const requestData={
      order_id,order_number,items,refund_method,
      total_price:items.reduce((s,i)=>s+parseFloat(i.price||0)*(i.qty||1),0),
      request_type:hasBoth?'mixed':hasExchange?'exchange':'return',
      shipping_preference:shipping_preference||'self_ship', media_urls:media_urls||[]
    };
    const ruleMatches=runRulesEngine(requestData);
    let autoAction=ruleMatches.length?ruleMatches[0]:null;
    if(autoAction)auditLog(order_id,`rule_matched:${autoAction.action}`,'rules_engine',`Rule: ${autoAction.rule_name}`);

    const req_id=`REQ-${order_number}-${uid()}`;
    const itemLines=items.map(i=>{
      let l=`[${i.action.toUpperCase()}] ${i.title}${i.variant_title?' - '+i.variant_title:''} x${i.qty||1}`;
      if(i.reason)l+=` | Reason: ${i.reason}`;
      if(i.action==='exchange'){
        if(i.exchange_variant_title)l+=` | Exchange for: ${i.exchange_variant_title}`;
        if(i.exchange_product_title)l+=` | New Product: ${i.exchange_product_title}`;
        if(i.exchange_variant_id)l+=` | ExchVarID: ${i.exchange_variant_id}`;
      }
      return l;
    }).join('\n');

    const existing=await shopifyREST('GET',`orders/${order_id}.json?fields=note,tags`);
    const existingNote=existing?.order?.note||'';
    const existingTags=(existing?.order?.tags||'').split(',').map(t=>t.trim()).filter(Boolean);
    const existingRequests=RETURN_REQUESTS[String(order_id)]||[];
    const reqNum=existingRequests.length+1;
    const tag=hasBoth?'mixed-requested':hasExchange?'exchange-requested':'return-requested';
    
    const newBlock=`\n---REQUEST ${reqNum} (${req_id})---\nItems:\n${itemLines}\nRefund Method: ${refund_method||'Store Credit'}\nCustomer Note: ${customer_note||'None'}\nRules: ${autoAction?autoAction.rule_name:'None'}\nSubmitted: ${new Date().toISOString()}\n---END REQUEST ${reqNum}---`;
    const newNote=(existingNote+newBlock).slice(0,5000);

    let initialStatus='pending';
    let addTags=[tag];
    if(autoAction?.action==='auto_approve'){addTags=[tag.replace('-requested','-approved')];initialStatus='approved';ANALYTICS.auto_approved++;}
    else if(autoAction?.action==='auto_reject'){addTags=[tag.replace('-requested','-rejected')];initialStatus='rejected';ANALYTICS.rejected++;}
    else if(autoAction?.action==='flag_review'){addTags=['flagged-review'];initialStatus='pending';}
    else if(autoAction?.action==='keep_it'){addTags=['return-approved','keep-it-rule'];initialStatus='keep_it';}

    const newTags=[...new Set([...existingTags,...addTags])];
    await shopifyREST('PUT',`orders/${order_id}.json`,{order:{id:order_id,tags:newTags.join(', '),note:newNote}});

    if(!RETURN_REQUESTS[String(order_id)])RETURN_REQUESTS[String(order_id)]=[];
    RETURN_REQUESTS[String(order_id)].push({
      req_id, req_num:reqNum, ...requestData, status:initialStatus,
      submitted_at:new Date().toISOString(), auto_action:autoAction?.action||null, awb:null
    });

    ANALYTICS.total_requests++;
    saveDeferred('requests',RETURN_REQUESTS);
    saveDeferred('analytics',ANALYTICS);
    
    res.json({ success:true, req_id, status:initialStatus, auto_action:autoAction?.action||null, keep_it_message:autoAction?.action==='keep_it'?autoAction.params?.message:null });
  }catch(e){res.status(500).json({error:e.message});}
});

// ── APPROVE / REJECT ──
app.post('/api/returns/:order_id/approve',async(req,res)=>{
  const{order_id}=req.params; const{type}=req.body;
  try{
    const approvedTag=type==='exchange'?'exchange-approved':type==='mixed'?'mixed-approved':'return-approved';
    const removeTag=type==='exchange'?'exchange-requested':type==='mixed'?'mixed-requested':'return-requested';
    await updateOrderTags(order_id,[approvedTag],[removeTag]);
    const reqs=RETURN_REQUESTS[String(order_id)]||[];
    if(reqs.length) reqs[reqs.length-1].status='approved';
    ANALYTICS.approved++; saveDeferred('analytics',ANALYTICS);
    auditLog(order_id,'approved','merchant',`${type} request approved`);
    res.json({success:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/returns/:order_id/reject',async(req,res)=>{
  const{order_id}=req.params; const{type,reason}=req.body;
  try{
    const rejectedTag=type==='exchange'?'exchange-rejected':type==='mixed'?'mixed-rejected':'return-rejected';
    const removeTag=type==='exchange'?'exchange-requested':type==='mixed'?'mixed-requested':'return-requested';
    await updateOrderTags(order_id,[rejectedTag],[removeTag]);
    const reqs=RETURN_REQUESTS[String(order_id)]||[];
    if(reqs.length) reqs[reqs.length-1].status='rejected';
    ANALYTICS.rejected++; saveDeferred('analytics',ANALYTICS);
    auditLog(order_id,'rejected','merchant',`Rejected`);
    res.json({success:true});
  }catch(e){res.status(500).json({error:e.message});}
});

// ── ORDER DETAILS ──
app.get('/api/orders/:order_id/details',async(req,res)=>{
  try{
    const d=await shopifyREST('GET',`orders/${req.params.order_id}.json?fields=id,order_number,email,phone,customer,shipping_address,billing_address,line_items,note,tags,total_price,financial_status,payment_gateway`);
    const o=d?.order;
    if(!o)return res.status(404).json({error:'Not found'});
    const rawAddr=o.shipping_address||o.billing_address||null;
    const addr=rawAddr?{...rawAddr, name:rawAddr.name||(o.customer?`${o.customer.first_name||''} ${o.customer.last_name||''}`.trim():null)||'', phone:rawAddr.phone||o.phone||o.customer?.phone||''}:null;
    res.json({
      id:o.id, order_number:o.order_number, email:o.email||'', phone:o.phone||o.shipping_address?.phone||'',
      customer_name:o.customer?`${o.customer.first_name||''} ${o.customer.last_name||''}`.trim():rawAddr?.name||'',
      customer_email:o.email||o.customer?.email||'', customer_phone:o.phone||o.customer?.phone||o.shipping_address?.phone||'',
      payment_gateway:o.payment_gateway||'', is_cod:['cash_on_delivery','cod','manual'].includes((o.payment_gateway||'').toLowerCase()),
      address:addr, line_items:(o.line_items||[]).map(li=>({id:li.id, title:li.title, variant_id:li.variant_id, variant_title:li.variant_title||'', quantity:li.quantity, price:li.price})),
      total_price:o.total_price, financial_status:o.financial_status, note:o.note, tags:o.tags, requests:RETURN_REQUESTS[String(req.params.order_id)]||[]
    });
  }catch(e){res.status(500).json({error:e.message});}
});

// ── REFUND / CREDIT / EXCHANGE ──
app.post('/api/shopify/refund/:order_id',async(req,res)=>{
  const{order_id}=req.params; const{refund_method,note,line_item_ids}=req.body;
  try{
    const freshOrder=await shopifyREST('GET',`orders/${order_id}.json?fields=id,line_items,financial_status`);
    const freshLines=freshOrder?.order?.line_items||[];
    const useLines=line_item_ids?.length?freshLines.filter(li=>line_item_ids.map(String).includes(String(li.id))):freshLines;
    const refundLineItems=useLines.map(li=>({line_item_id:li.id, quantity:li.quantity||1, restock_type:'return'}));

    const calc=await shopifyREST('POST',`orders/${order_id}/refunds/calculate.json`,{refund:{shipping:{full_refund:false},refund_line_items:refundLineItems}});
    let transactions=calc?.refund?.transactions||[];
    
    const fee=parseFloat(RESTOCKING_FEE_PCT);
    if(fee>0&&transactions.length)transactions=transactions.map(t=>({...t,amount:(parseFloat(t.amount||0)*(1-fee/100)).toFixed(2)}));

    const refundPayload={refund:{notify:true, note:note||'Return approved', shipping:{full_refund:false}, refund_line_items:refundLineItems, transactions:refund_method==='store_credit'?[]:transactions.map(t=>({parent_id:t.parent_id,amount:t.amount,kind:'refund',gateway:t.gateway}))}};
    const result=await shopifyREST('POST',`orders/${order_id}/refunds.json`,refundPayload);

    if(result?.refund?.id){
      await updateOrderTags(order_id,['return-refunded'],[]);
      const amount=result.refund.transactions?.[0]?.amount||'0';
      ANALYTICS.refunded_amount+=parseFloat(amount);
      auditLog(order_id,'refund_created','merchant',`Refund ${result.refund.id} — ₹${amount}`);
      res.json({success:true,refund_id:result.refund.id,amount});
    }else{ res.status(400).json({error:'Refund failed'}); }
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/shopify/store-credit/:order_id',async(req,res)=>{
  const{order_id}=req.params; const{amount,apply_bonus,note}=req.body;
  try{
    const finalAmount=(parseFloat(amount||0)+(apply_bonus?(parseFloat(amount||0)*STORE_CREDIT_BONUS/100):0)).toFixed(2);
    const code=`CREDIT-${String(order_id).slice(-6)}-${uid().toUpperCase().slice(0,6)}`;
    
    let giftCardResult=null;
    try{ giftCardResult=await shopifyREST('POST','gift_cards.json',{gift_card:{initial_value:finalAmount,code,note:note||`Store credit`}}); }catch(e){}

    if(giftCardResult?.gift_card?.id){
      await updateOrderTags(order_id,['store-credit-issued'],[]);
      ANALYTICS.store_credits_issued+=parseFloat(finalAmount);
      auditLog(order_id,'store_credit_issued','merchant',`Gift card ${code} — ₹${finalAmount}`);
      res.json({success:true,method:'gift_card',gift_card_id:giftCardResult.gift_card.id,code,amount:finalAmount});
    }else{
      const freshOrder=await shopifyREST('GET',`orders/${order_id}.json?fields=note`);
      const creditNote=(freshOrder?.order?.note||'')+`\n\n[STORE CREDIT ISSUED]\nCode: ${code}\nAmount: ₹${finalAmount}`;
      await shopifyREST('PUT',`orders/${order_id}.json`,{order:{id:order_id,note:creditNote.slice(0,5000)}});
      await updateOrderTags(order_id,['store-credit-issued'],[]);
      ANALYTICS.store_credits_issued+=parseFloat(finalAmount);
      auditLog(order_id,'store_credit_issued_manual','merchant',`Manual credit ${code}`);
      res.json({success:true,method:'manual_note',code,amount:finalAmount});
    }
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/shopify/exchange/:order_id',async(req,res)=>{
  const{order_id}=req.params; const{exchange_items,customer_address,order_number}=req.body;
  try{
    const orig=await shopifyREST('GET',`orders/${order_id}.json?fields=id,email,shipping_address,billing_address,customer`);
    const o=orig?.order;
    const addr=customer_address||o.shipping_address||o.billing_address;
    const validItems=(exchange_items||[]).filter(i=>i.variant_id);

    const draftPayload={ draft_order:{ line_items:validItems.map(item=>({variant_id:parseInt(item.variant_id),quantity:item.quantity||1,applied_discount:{description:'Exchange',value_type:'percentage',value:'100',amount:item.price||'0',title:'Exchange'}})), customer:o.customer?{id:o.customer.id}:undefined, shipping_address:addr, billing_address:o.billing_address||addr, email:o.email, note:`Exchange for order #${order_number||order_id}`, tags:'exchange-order', send_invoice:false } };
    const draft=await shopifyREST('POST','draft_orders.json',draftPayload);
    const draftId=draft?.draft_order?.id;
    if(!draftId)return res.status(400).json({error:'Draft order creation failed'});

    const completed=await shopifyREST('PUT',`draft_orders/${draftId}/complete.json`);
    await updateOrderTags(order_id,['exchange-fulfilled'],[]);
    ANALYTICS.exchanges_created++;
    auditLog(order_id,'exchange_order_created','merchant',`New order ${completed?.draft_order?.name}`);
    res.json({success:true,new_order_id:completed?.draft_order?.order_id});
  }catch(e){res.status(500).json({error:e.message});}
});

// ── RULES ENGINE CRUD ──
app.get('/api/rules',(req,res)=>res.json({rules:RULES}));
app.post('/api/rules',(req,res)=>{const r={id:'rule_'+uid(),enabled:true,...req.body};RULES.push(r);saveDeferred('rules',RULES);res.json({success:true,rule:r});});
app.put('/api/rules/:id',(req,res)=>{const i=RULES.findIndex(r=>r.id===req.params.id);if(i<0)return res.status(404).json({error:'Not found'});RULES[i]={...RULES[i],...req.body};saveDeferred('rules',RULES);res.json({success:true,rule:RULES[i]});});
app.delete('/api/rules/:id',(req,res)=>{const i=RULES.findIndex(r=>r.id===req.params.id);if(i<0)return res.status(404).json({error:'Not found'});RULES.splice(i,1);saveDeferred('rules',RULES);res.json({success:true});});

// ── STATUS & EXPORT ──
app.get('/api/store/status',(req,res)=>{
  const files=['auth','settings','delhivery','rules','requests','analytics'];
  const status={data_dir:DATA_DIR,is_persistent:fs.existsSync(DATA_DIR),files:{}};
  files.forEach(f=>{
    const p=dataPath(f);
    if(fs.existsSync(p)){ const stat=fs.statSync(p); status.files[f]={exists:true,size_kb:Math.round(stat.size/1024*10)/10,modified:stat.mtime}; }
    else status.files[f]={exists:false};
  });
  status.memory={rules:RULES.length,return_requests:Object.keys(RETURN_REQUESTS).length,delhivery_configured:!!DELHIVERY_TOKEN,warehouse_configured:!!WAREHOUSE_CONFIG.address,return_window_days:RETURN_WINDOW_DAYS,store_credit_bonus:STORE_CREDIT_BONUS};
  res.json(status);
});

app.get('/api/store/export',(req,res)=>{
  const backup={exported_at:new Date().toISOString(),shop:SHOP_DOMAIN,settings:{return_window_days:RETURN_WINDOW_DAYS,store_credit_bonus:STORE_CREDIT_BONUS,restocking_fee_pct:RESTOCKING_FEE_PCT,warehouse:WAREHOUSE_CONFIG},delhivery:{warehouse:DELHIVERY_WAREHOUSE,mode:DELHIVERY_MODE},rules:RULES,analytics:ANALYTICS,return_requests:RETURN_REQUESTS};
  res.setHeader('Content-Disposition',`attachment; filename=returns-backup-${Date.now()}.json`);
  res.json(backup);
});

// ── AUDIT & ANALYTICS ──
app.get('/api/audit',(req,res)=>{res.json({logs:AUDIT_LOG.slice(0,100)});});
app.get('/api/analytics',async(req,res)=>{res.json({...ANALYTICS,approval_rate:ANALYTICS.total_requests>0?Math.round(ANALYTICS.approved/ANALYTICS.total_requests*100):0,auto_approve_rate:ANALYTICS.total_requests>0?Math.round(ANALYTICS.auto_approved/ANALYTICS.total_requests*100):0});});

// ── SETTINGS & DELHIVERY ──
app.get('/api/settings',(req,res)=>res.json({return_window_days:RETURN_WINDOW_DAYS,store_credit_bonus:STORE_CREDIT_BONUS,restocking_fee_pct:RESTOCKING_FEE_PCT,warehouse:WAREHOUSE_CONFIG,delhivery:{configured:!!DELHIVERY_TOKEN,warehouse:DELHIVERY_WAREHOUSE,mode:DELHIVERY_MODE}}));
app.post('/api/settings',(req,res)=>{
  const{return_window_days,store_credit_bonus,restocking_fee_pct,warehouse}=req.body;
  if(return_window_days!==undefined)RETURN_WINDOW_DAYS=parseInt(return_window_days);
  if(store_credit_bonus!==undefined)STORE_CREDIT_BONUS=parseFloat(store_credit_bonus);
  if(restocking_fee_pct!==undefined)RESTOCKING_FEE_PCT=parseFloat(restocking_fee_pct);
  if(warehouse)WAREHOUSE_CONFIG={...WAREHOUSE_CONFIG,...warehouse};
  saveJSON('settings',{return_window_days:RETURN_WINDOW_DAYS,store_credit_bonus:STORE_CREDIT_BONUS,restocking_fee_pct:RESTOCKING_FEE_PCT,warehouse:WAREHOUSE_CONFIG});
  res.json({success:true});
});

app.post('/api/delhivery/config',(req,res)=>{
  const{token,warehouse,mode}=req.body;
  if(token)DELHIVERY_TOKEN=token;
  if(warehouse)DELHIVERY_WAREHOUSE=warehouse;
  if(mode)DELHIVERY_MODE=mode;
  saveJSON('delhivery',{token:DELHIVERY_TOKEN,warehouse:DELHIVERY_WAREHOUSE,mode:DELHIVERY_MODE});
  res.json({success:true});
});
app.get('/api/delhivery/config',(req,res)=>res.json({configured:!!DELHIVERY_TOKEN,warehouse:DELHIVERY_WAREHOUSE,mode:DELHIVERY_MODE}));

app.post('/api/delhivery/create-pickup',async(req,res)=>{
  const{order_id,order_number,customer_name,customer_phone,customer_address,customer_city,customer_state,customer_pincode}=req.body;
  if(!DELHIVERY_TOKEN)return res.status(400).json({error:'Delhivery not configured'});
  const rvpOrderId=`RVP-${order_number}-${Date.now().toString().slice(-6)}`;
  const payload={pickup_location:{name:DELHIVERY_WAREHOUSE},shipments:[{name:customer_name,add:customer_address,pin:String(customer_pincode),city:customer_city,state:customer_state,country:'India',phone:String(customer_phone).replace(/\D/g,'').slice(-10),order:rvpOrderId,payment_mode:'Pickup',products_desc:'Return Shipment',hsn_code:'62034200',cod_amount:'0',order_date:new Date().toISOString().split('T')[0],total_amount:'0',seller_name:DELHIVERY_WAREHOUSE,seller_inv:`INV-${order_number}`,quantity:1,weight:0.5}]};
  try{
    const data=await delhiveryAPI('POST','/api/cmu/create.json',payload,true);
    const waybill=data?.packages?.[0]?.waybill||data?.waybill;
    if(waybill){
      const curr=await shopifyREST('GET',`orders/${order_id}.json?fields=note`);
      await shopifyREST('PUT',`orders/${order_id}.json`,{order:{id:order_id,note:(curr?.order?.note||'')+'\nDELHIVERY AWB: '+waybill}});
      await updateOrderTags(order_id,['pickup-scheduled'],[]);
      const reqs=RETURN_REQUESTS[String(order_id)]||[];
      if(reqs.length)reqs[reqs.length-1].awb=waybill;
      saveDeferred('requests',RETURN_REQUESTS);
    }
    res.json({success:!!waybill,waybill,raw:data});
  }catch(e){res.status(500).json({error:e.message});}
});

const PORT=process.env.PORT||3000;
app.listen(PORT,()=>console.log(`Returns Manager v3 on :${PORT}`));
