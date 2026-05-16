'use strict';

require('dotenv').config();
const express = require('express');
const { McpServer } = require('@modelcontextprotocol/sdk/server/mcp.js');
const { StreamableHTTPServerTransport } = require('@modelcontextprotocol/sdk/server/streamableHttp.js');
const { z } = require('zod');
const SFConnection = require('./sfConnection.js');

const PORT = parseInt(process.env.PORT || '8008', 10);
const sf = new SFConnection();

function ok(data) {
  return { content: [{ type: 'text', text: JSON.stringify(data) }] };
}

function sfErr(e, extra = {}) {
  return ok({ error: e?.message || String(e), ...extra });
}

async function soql(query) {
  if (!sf.isConnected) throw new Error('Salesforce not connected');
  const result = await sf.apiCall((conn) => conn.query(query));
  return result ? result.records : [];
}

function esc(val) {
  return String(val)
    .replace(/\\/g, '\\\\')  // backslash first, then quote
    .replace(/'/g, "\\'");
}

// Reusable SOQL field lists
const _SF_ID   = 'Id, Master_UUID__c, First_Name__c, Last_Name__c, Email__c';
const _SF_CORE = `${_SF_ID}, User_Type__c, Status__c, Company_Name__c`;
const _SF_WALLET = 'Wallet_Balance__c, Wallet_Status__c, Last_Lease_ID__c, Last_Lease_At__c, Last_Sync_At__c';
const _SF_INVOICE = 'Last_Invoice_URL__c, Last_Invoice_Due_Date__c, Last_Invoice_Number__c, Payment_Status__c';
const _SF_FULL = `${_SF_CORE}, Birthdate__c, VAT_Number__c, Company_ID__c, Badge_ID__c, Street__c, House_Number__c, Postal_Code__c, City__c, Country_Code__c, ${_SF_WALLET}, ${_SF_INVOICE}`;

// ─────────────────────────────────────────────
//  Tool registration
// ─────────────────────────────────────────────

function createMcpServer() {
  const server = new McpServer({ name: 'crm', version: '1.0.0' });

  // ── MEMBERS ──────────────────────────────────────────────────────

  server.tool(
    'list_members',
    "List CRM members (Member__c). Optionally filter by search term, user type ('Bedrijf'/'Particulier'), or status. Authoritative source for person identity — use for any question about who a person is.",
    {
      limit: z.number().int().min(1).max(200).optional().default(50),
      search: z.string().optional(),
      user_type: z.enum(['Bedrijf', 'Particulier']).optional(),
      status: z.string().optional(),
    },
    async ({ limit, search, user_type, status }) => {
      try {
        const where = [];
        if (user_type) where.push(`User_Type__c = '${esc(user_type)}'`);
        if (status) where.push(`Status__c = '${esc(status)}'`);
        if (search) {
          const s = esc(search);
          where.push(`(Email__c LIKE '%${s}%' OR First_Name__c LIKE '%${s}%' OR Last_Name__c LIKE '%${s}%' OR Company_Name__c LIKE '%${s}%')`);
        }
        const w = where.length ? `WHERE ${where.join(' AND ')}` : '';
        const records = await soql(
          `SELECT ${_SF_CORE}, Badge_ID__c, Wallet_Balance__c, Wallet_Status__c, Payment_Status__c FROM Member__c ${w} LIMIT ${limit}`
        );
        return ok({ members: records, count: records.length });
      } catch (e) { return sfErr(e); }
    }
  );

  server.tool(
    'get_member',
    'Get full details for a CRM member by their Master_UUID (identity UUID from the identity service). Authoritative source for person identity and full member profile.',
    { master_uuid: z.string() },
    async ({ master_uuid }) => {
      try {
        const records = await soql(
          `SELECT ${_SF_FULL} FROM Member__c WHERE Master_UUID__c = '${esc(master_uuid)}' LIMIT 1`
        );
        if (!records.length) return ok({ error: `No member found with Master_UUID: ${master_uuid}` });
        return ok(records[0]);
      } catch (e) { return sfErr(e); }
    }
  );

  server.tool(
    'get_member_by_email',
    'Find a CRM member by their exact email address. Returns the full Member__c profile. Primary tool for any person lookup by email.',
    { email: z.string() },
    async ({ email }) => {
      try {
        const records = await soql(
          `SELECT ${_SF_CORE}, Wallet_Balance__c, Wallet_Status__c, Payment_Status__c FROM Member__c WHERE Email__c = '${esc(email.toLowerCase())}' LIMIT 1`
        );
        if (!records.length) return ok({ error: `No member found with email: ${email}` });
        return ok(records[0]);
      } catch (e) { return sfErr(e); }
    }
  );

  server.tool(
    'search_members',
    'Search CRM members by partial name, email, or company name. Primary tool for person search.',
    {
      query: z.string(),
      limit: z.number().int().min(1).max(100).optional().default(25),
    },
    async ({ query, limit }) => {
      try {
        const q = esc(query);
        const records = await soql(
          `SELECT ${_SF_ID}, User_Type__c, Company_Name__c, Status__c FROM Member__c WHERE Email__c LIKE '%${q}%' OR First_Name__c LIKE '%${q}%' OR Last_Name__c LIKE '%${q}%' OR Company_Name__c LIKE '%${q}%' LIMIT ${limit}`
        );
        return ok({ members: records, count: records.length, query });
      } catch (e) { return sfErr(e); }
    }
  );

  server.tool(
    'get_members_by_type',
    "Get members filtered by type. user_type: 'Bedrijf' (company) or 'Particulier' (individual).",
    {
      user_type: z.enum(['Bedrijf', 'Particulier']),
      limit: z.number().int().min(1).max(200).optional().default(100),
    },
    async ({ user_type, limit }) => {
      try {
        const records = await soql(
          `SELECT Id, Master_UUID__c, First_Name__c, Last_Name__c, Email__c, Company_Name__c, VAT_Number__c, Status__c FROM Member__c WHERE User_Type__c = '${esc(user_type)}' LIMIT ${limit}`
        );
        return ok({ members: records, count: records.length, user_type });
      } catch (e) { return sfErr(e); }
    }
  );

  server.tool(
    'get_member_stats',
    'Aggregate statistics about CRM members: counts by type and status, badge and wallet coverage.',
    {},
    async () => {
      try {
        const [total, byType, byStatus, withBadge, withWallet] = await Promise.all([
          soql('SELECT COUNT() FROM Member__c'),
          soql('SELECT User_Type__c, COUNT(Id) FROM Member__c GROUP BY User_Type__c'),
          soql('SELECT Status__c, COUNT(Id) FROM Member__c GROUP BY Status__c'),
          soql('SELECT COUNT() FROM Member__c WHERE Badge_ID__c != null'),
          soql('SELECT COUNT() FROM Member__c WHERE Wallet_Balance__c != null'),
        ]);
        return ok({
          total_members: total[0]?.expr0 ?? 0,
          members_with_badge: withBadge[0]?.expr0 ?? 0,
          members_with_wallet: withWallet[0]?.expr0 ?? 0,
          by_type: byType.map((r) => ({ type: r.User_Type__c, count: r.expr0 })),
          by_status: byStatus.map((r) => ({ status: r.Status__c, count: r.expr0 })),
        });
      } catch (e) { return sfErr(e); }
    }
  );

  // ── WALLET ───────────────────────────────────────────────────────

  server.tool(
    'get_member_wallet',
    "Get wallet balance, status, and lease info for a member by their Master_UUID. CRITICAL: if Wallet_Status__c='Leased', the Wallet_Balance__c field is STALE — Kassa holds the live balance for the duration of the lease. ALWAYS also call kassa__get_wallet_by_master_uuid when Wallet_Status__c='Leased' and report the Kassa value as the live balance, with the CRM cached value and Last_Lease_ID__c for traceability.",
    { master_uuid: z.string() },
    async ({ master_uuid }) => {
      try {
        const records = await soql(
          `SELECT ${_SF_ID}, ${_SF_WALLET} FROM Member__c WHERE Master_UUID__c = '${esc(master_uuid)}' LIMIT 1`
        );
        if (!records.length) return ok({ error: `No member found: ${master_uuid}` });
        return ok(records[0]);
      } catch (e) { return sfErr(e); }
    }
  );

  server.tool(
    'list_active_leases',
    "List all members whose wallet is currently 'Leased' — wallet control has been transferred to Kassa for on-site spending. For each member returned here, the LIVE balance is in Kassa (use kassa__get_wallet_by_master_uuid), not in the CRM record shown.",
    { limit: z.number().int().min(1).max(200).optional().default(100) },
    async ({ limit }) => {
      try {
        const records = await soql(
          `SELECT ${_SF_ID}, Wallet_Balance__c, Last_Lease_ID__c, Last_Lease_At__c FROM Member__c WHERE Wallet_Status__c = 'Leased' LIMIT ${limit}`
        );
        return ok({ leased_wallets: records, count: records.length });
      } catch (e) { return sfErr(e); }
    }
  );

  server.tool(
    'get_wallet_stats',
    'Aggregate wallet statistics: total balance in system, average, max, count per wallet status. Numbers are based on CRM cached balances — leased wallets reflect the last-synced value, not live spending. For an exact live total during an event reconcile with kassa__get_all_wallets.',
    {},
    async () => {
      try {
        const [byStatus, totals] = await Promise.all([
          soql('SELECT Wallet_Status__c, COUNT(Id), SUM(Wallet_Balance__c) FROM Member__c WHERE Wallet_Status__c != null GROUP BY Wallet_Status__c'),
          soql('SELECT COUNT(Id), SUM(Wallet_Balance__c), AVG(Wallet_Balance__c), MAX(Wallet_Balance__c) FROM Member__c WHERE Wallet_Balance__c != null'),
        ]);
        return ok({
          total_members_with_wallet: totals[0]?.expr0 ?? 0,
          total_balance_eur: totals[0]?.expr1 ?? 0,
          avg_balance_eur: totals[0]?.expr2 ?? 0,
          max_balance_eur: totals[0]?.expr3 ?? 0,
          by_status: byStatus.map((r) => ({
            status: r.Wallet_Status__c,
            count: r.expr0,
            total_balance_eur: r.expr1,
          })),
        });
      } catch (e) { return sfErr(e); }
    }
  );

  // ── INVOICES ─────────────────────────────────────────────────────

  server.tool(
    'get_member_invoice_info',
    'Returns the LAST invoice URL/number/due-date cached on the CRM member record. May be stale. For the current and complete invoice history use facturatie__get_client_invoices (look up the FossBilling client_id first via facturatie__get_client_by_email or via facturatie__get_company_billing_account).',
    { master_uuid: z.string() },
    async ({ master_uuid }) => {
      try {
        const records = await soql(
          `SELECT ${_SF_ID}, ${_SF_INVOICE} FROM Member__c WHERE Master_UUID__c = '${esc(master_uuid)}' LIMIT 1`
        );
        if (!records.length) return ok({ error: `No member found: ${master_uuid}` });
        return ok(records[0]);
      } catch (e) { return sfErr(e); }
    }
  );

  server.tool(
    'get_members_with_cancelled_payment',
    "List members whose payment has been cancelled (Payment_Status__c = 'Cancelled').",
    { limit: z.number().int().min(1).max(200).optional().default(100) },
    async ({ limit }) => {
      try {
        const records = await soql(
          `SELECT Master_UUID__c, Email__c, First_Name__c, Last_Name__c, Last_Invoice_Number__c, Last_Invoice_Due_Date__c FROM Member__c WHERE Payment_Status__c = 'Cancelled' LIMIT ${limit}`
        );
        return ok({ members: records, count: records.length });
      } catch (e) { return sfErr(e); }
    }
  );

  // ── CONSUMPTIONS ─────────────────────────────────────────────────

  server.tool(
    'list_consumptions',
    'List Consumption__c records (bar/catering items ordered at events, linked to members). The CRM master record of consumption items, populated post-event. For LIVE POS orders during the event use kassa__get_recent_orders; for items pending invoicing use facturatie__get_pending_consumptions.',
    { limit: z.number().int().min(1).max(200).optional().default(50) },
    async ({ limit }) => {
      try {
        const records = await soql(
          `SELECT Id, Consumption_ID__c, Product_Name__c, Quantity__c, Total_Amount__c, Price_Per_Unit__c, Product_SKU__c, VAT_Rate__c, Member__c FROM Consumption__c LIMIT ${limit}`
        );
        return ok({ consumptions: records, count: records.length });
      } catch (e) { return sfErr(e); }
    }
  );

  server.tool(
    'get_member_consumptions',
    'Get all consumption items linked to a specific member. Provide the Salesforce Member Id (Id field, e.g. from get_member).',
    {
      member_sf_id: z.string(),
      limit: z.number().int().min(1).max(200).optional().default(50),
    },
    async ({ member_sf_id, limit }) => {
      try {
        const records = await soql(
          `SELECT Id, Consumption_ID__c, Product_Name__c, Quantity__c, Total_Amount__c, Price_Per_Unit__c, Product_SKU__c, VAT_Rate__c FROM Consumption__c WHERE Member__c = '${esc(member_sf_id)}' LIMIT ${limit}`
        );
        return ok({ consumptions: records, count: records.length, member_sf_id });
      } catch (e) { return sfErr(e); }
    }
  );

  server.tool(
    'get_consumption_stats',
    'Aggregate consumption statistics: total items, total revenue, top products by revenue.',
    {},
    async () => {
      try {
        const [totals, byProduct] = await Promise.all([
          soql('SELECT COUNT(Id), SUM(Total_Amount__c), SUM(Quantity__c) FROM Consumption__c'),
          soql('SELECT Product_Name__c, COUNT(Id), SUM(Quantity__c), SUM(Total_Amount__c) FROM Consumption__c GROUP BY Product_Name__c ORDER BY SUM(Total_Amount__c) DESC LIMIT 20'),
        ]);
        return ok({
          total_items: totals[0]?.expr0 ?? 0,
          total_revenue_eur: totals[0]?.expr1 ?? 0,
          total_quantity: totals[0]?.expr2 ?? 0,
          by_product: byProduct.map((r) => ({
            product: r.Product_Name__c,
            count: r.expr0,
            total_qty: r.expr1,
            total_revenue_eur: r.expr2,
          })),
        });
      } catch (e) { return sfErr(e); }
    }
  );

  // ── TASKS / ACTIVITY LOG ──────────────────────────────────────────

  server.tool(
    'get_recent_tasks',
    'Get recent Salesforce Task records — the CURATED human-readable CRM activity log covering check-ins, payments, session registrations, invoices, refunds, and badge scans. For the raw event stream of the same events use monitoring__get_logs_by_action.',
    { limit: z.number().int().min(1).max(100).optional().default(20) },
    async ({ limit }) => {
      try {
        const records = await soql(
          `SELECT Id, Subject, Description, Status, ActivityDate, CreatedDate FROM Task ORDER BY CreatedDate DESC LIMIT ${limit}`
        );
        return ok({ tasks: records, count: records.length });
      } catch (e) { return sfErr(e); }
    }
  );

  server.tool(
    'get_tasks_by_subject',
    "Filter Task records by a keyword in the Subject. Useful keywords: 'Check-in', 'Payment registered', 'Invoice', 'Session', 'Badge', 'Refund', 'Sessie Inschrijving'.",
    {
      keyword: z.string(),
      limit: z.number().int().min(1).max(100).optional().default(25),
    },
    async ({ keyword, limit }) => {
      try {
        const records = await soql(
          `SELECT Id, Subject, Description, Status, ActivityDate, CreatedDate FROM Task WHERE Subject LIKE '%${esc(keyword)}%' ORDER BY CreatedDate DESC LIMIT ${limit}`
        );
        return ok({ tasks: records, count: records.length, keyword });
      } catch (e) { return sfErr(e); }
    }
  );

  server.tool(
    'get_checkin_tasks',
    "Get all check-in activity records (Tasks with Subject starting with 'Check-in:').",
    { limit: z.number().int().min(1).max(100).optional().default(50) },
    async ({ limit }) => {
      try {
        const records = await soql(
          `SELECT Id, Subject, Description, ActivityDate, CreatedDate FROM Task WHERE Subject LIKE 'Check-in:%' ORDER BY CreatedDate DESC LIMIT ${limit}`
        );
        return ok({ checkins: records, count: records.length });
      } catch (e) { return sfErr(e); }
    }
  );

  // ── OVERVIEW & HEALTH ─────────────────────────────────────────────

  server.tool(
    'check_salesforce_status',
    'Check whether the Salesforce connection is active and credentials are valid.',
    {},
    async () => {
      try {
        const healthy = await sf.healthCheck();
        return ok({
          status: healthy ? 'connected' : 'disconnected',
          instance_url: sf.getInstanceUrl(),
          auth_method: sf.authMethod,
        });
      } catch (e) {
        return ok({ status: 'error', error: e?.message || String(e) });
      }
    }
  );

  server.tool(
    'get_crm_overview',
    'Full CRM overview: member counts by type, wallet stats, active leases, recent activity. Single call for an admin dashboard.',
    {},
    async () => {
      try {
        const [membersByType, walletByStatus, walletTotals, activeLeases, recentTasks] = await Promise.all([
          soql('SELECT User_Type__c, COUNT(Id) FROM Member__c GROUP BY User_Type__c'),
          soql('SELECT Wallet_Status__c, COUNT(Id), SUM(Wallet_Balance__c) FROM Member__c WHERE Wallet_Status__c != null GROUP BY Wallet_Status__c'),
          soql('SELECT COUNT(Id), SUM(Wallet_Balance__c) FROM Member__c WHERE Wallet_Balance__c != null'),
          soql("SELECT COUNT() FROM Member__c WHERE Wallet_Status__c = 'Leased'"),
          soql('SELECT Subject, ActivityDate, CreatedDate FROM Task ORDER BY CreatedDate DESC LIMIT 5'),
        ]);

        return ok({
          total_members: membersByType.reduce((s, r) => s + (r.expr0 ?? 0), 0),
          members_by_type: membersByType.map((r) => ({ type: r.User_Type__c, count: r.expr0 })),
          wallet: {
            total_balance_eur: walletTotals[0]?.expr1 ?? 0,
            active_leases: activeLeases[0]?.expr0 ?? 0,
            by_status: walletByStatus.map((r) => ({
              status: r.Wallet_Status__c,
              count: r.expr0,
              total_balance_eur: r.expr1,
            })),
          },
          recent_activity: recentTasks,
          salesforce_connected: sf.isConnected,
          instance_url: sf.getInstanceUrl(),
        });
      } catch (e) { return sfErr(e); }
    }
  );

  return server;
}

// ── HTTP server ───────────────────────────────────────────────────

const app = express();
app.use(express.json());

app.post('/mcp', async (req, res) => {
  const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
  const server = createMcpServer();
  res.on('close', async () => {
    try { await transport.close(); } catch (_) { /* ignore */ }
    try { await server.close(); } catch (_) { /* ignore */ }
  });
  await server.connect(transport);
  await transport.handleRequest(req, res, req.body);
});

app.get('/health', (_req, res) => res.send('OK'));

async function main() {
  await sf.init();
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`[crm-mcp] MCP server listening on :${PORT}`);
  });
}

main().catch((e) => {
  console.error('[crm-mcp] Fatal startup error:', e);
  process.exit(1);
});
