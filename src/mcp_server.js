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
    "List CRM members (Member__c), newest first. Optionally filter by search term, user type ('Bedrijf'/'Particulier'), or status. Default limit 50. Use limit:5 for 'show latest members'. Authoritative source for person identity. Returns: id, Master_UUID__c, name, email, User_Type__c, Status__c, CreatedDate.",
    {
      limit: z.number().int().min(1).max(200).optional().default(50).describe("Max members to return (default 50). Use 5 for 'show latest members'."),
      search: z.string().optional().describe("Partial name, email, or company name to filter by. Case-insensitive LIKE match."),
      user_type: z.enum(['Bedrijf', 'Particulier']).optional().describe("Filter by type: 'Bedrijf' = company, 'Particulier' = individual."),
      status: z.string().optional().describe("Filter by status, e.g. 'Active', 'Inactive', 'Pending'."),
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
          `SELECT ${_SF_CORE}, Badge_ID__c, Wallet_Balance__c, Wallet_Status__c, Payment_Status__c, CreatedDate FROM Member__c ${w} ORDER BY CreatedDate DESC LIMIT ${limit}`
        );
        return ok({ members: records, count: records.length });
      } catch (e) { return sfErr(e); }
    }
  );

  server.tool(
    'get_member',
    'Get full details for a CRM member by their Master_UUID. Use search_members or list_members first to find the UUID — never guess it. Returns: full profile, wallet, invoice info, address.',
    { master_uuid: z.string().describe("The member's Master_UUID__c from Salesforce (format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx). Get it via search_members or list_members — never invent a value.") },
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
    'Find a CRM member by their exact email address. Primary tool for any person lookup by email. Returns full Member__c profile including wallet status.',
    { email: z.string().describe("The member's exact email address (e.g. 'john@example.com'). Must match exactly — use search_members for partial matches.") },
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
    'Search CRM members by partial name, email, or company name. Primary tool for person search. Use this when you have a name fragment and need the master_uuid.',
    {
      query: z.string().describe("Name fragment, email, or company name to search. Case-insensitive partial match. Example: 'jan', 'smith', 'acme'."),
      limit: z.number().int().min(1).max(100).optional().default(25).describe("Max results (default 25, max 100)."),
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
    "Get members filtered by type, newest first. Returns: id, name, email, company, VAT, status, CreatedDate.",
    {
      user_type: z.enum(['Bedrijf', 'Particulier']).describe("'Bedrijf' = company members, 'Particulier' = individual/private members."),
      limit: z.number().int().min(1).max(200).optional().default(100).describe("Max results (default 100, max 200)."),
    },
    async ({ user_type, limit }) => {
      try {
        const records = await soql(
          `SELECT Id, Master_UUID__c, First_Name__c, Last_Name__c, Email__c, Company_Name__c, VAT_Number__c, Status__c, CreatedDate FROM Member__c WHERE User_Type__c = '${esc(user_type)}' ORDER BY CreatedDate DESC LIMIT ${limit}`
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
          soql('SELECT COUNT(Id) FROM Member__c'),
          soql('SELECT User_Type__c, COUNT(Id) FROM Member__c GROUP BY User_Type__c'),
          soql('SELECT Status__c, COUNT(Id) FROM Member__c GROUP BY Status__c'),
          soql('SELECT COUNT(Id) FROM Member__c WHERE Badge_ID__c != null'),
          soql('SELECT COUNT(Id) FROM Member__c WHERE Wallet_Balance__c != null'),
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
    "Get wallet balance, status, and lease info for a member. CRITICAL: if Wallet_Status__c='Leased', Wallet_Balance__c is STALE — you MUST also call kassa__get_wallet_by_master_uuid and report the Kassa value as the live balance.",
    { master_uuid: z.string().describe("The member's Master_UUID__c. Get it via search_members or list_members first — never guess.") },
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
    "List all members whose wallet is currently 'Leased' — wallet control is in Kassa. For each member here, use kassa__get_wallet_by_master_uuid to get the live balance.",
    { limit: z.number().int().min(1).max(200).optional().default(100).describe("Max leased wallets to return (default 100).") },
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
    'Returns the last invoice URL/number/due-date CACHED on the CRM record — may be stale. For full invoice history use facturatie__get_invoices_by_email instead.',
    { master_uuid: z.string().describe("The member's Master_UUID__c. Get it via search_members first — never guess.") },
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
    "List members whose payment has been cancelled (Payment_Status__c = 'Cancelled'). Returns: name, email, last invoice number, due date.",
    { limit: z.number().int().min(1).max(200).optional().default(100).describe("Max results (default 100).") },
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
    'List Consumption__c records (bar/catering items, post-event master data). For live POS orders use kassa__get_recent_orders. For pending invoicing use facturatie__get_pending_consumptions.',
    { limit: z.number().int().min(1).max(200).optional().default(50).describe("Max consumption records to return (default 50, newest first).") },
    async ({ limit }) => {
      try {
        const records = await soql(
          `SELECT Id, Consumption_ID__c, Product_Name__c, Quantity__c, Total_Amount__c, Price_Per_Unit__c, Product_SKU__c, VAT_Rate__c, Member__c, CreatedDate FROM Consumption__c ORDER BY CreatedDate DESC LIMIT ${limit}`
        );
        return ok({ consumptions: records, count: records.length });
      } catch (e) { return sfErr(e); }
    }
  );

  server.tool(
    'get_member_consumptions',
    'Get all consumption items for a specific member by Salesforce Id. Get the Id from get_member (the Id field, starting with a 3-character prefix like "a0B").',
    {
      member_sf_id: z.string().describe("Salesforce record Id of the Member__c object (the 'Id' field from get_member, NOT Master_UUID__c). Example format: 'a0Bxx000000xxxx'."),
      limit: z.number().int().min(1).max(200).optional().default(50).describe("Max consumption records (default 50, newest first)."),
    },
    async ({ member_sf_id, limit }) => {
      try {
        const records = await soql(
          `SELECT Id, Consumption_ID__c, Product_Name__c, Quantity__c, Total_Amount__c, Price_Per_Unit__c, Product_SKU__c, VAT_Rate__c, CreatedDate FROM Consumption__c WHERE Member__c = '${esc(member_sf_id)}' ORDER BY CreatedDate DESC LIMIT ${limit}`
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
    'Get recent Salesforce Task records — curated human-readable activity log (check-ins, payments, registrations, invoices, badge scans). For raw event stream use monitoring__get_logs_by_action.',
    { limit: z.number().int().min(1).max(100).optional().default(20).describe("Max tasks to return (default 20, newest first).") },
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
    "Filter Task records by keyword in Subject. Useful values: 'Check-in', 'Payment registered', 'Invoice', 'Session', 'Badge', 'Refund', 'Sessie Inschrijving'.",
    {
      keyword: z.string().describe("Text to search in Task Subject. Partial match. Examples: 'Check-in', 'Refund', 'Badge'."),
      limit: z.number().int().min(1).max(100).optional().default(25).describe("Max tasks (default 25, newest first)."),
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
    "Get all check-in activity records (Tasks with Subject starting with 'Check-in:'). Returns: subject, description, date, created.",
    { limit: z.number().int().min(1).max(100).optional().default(50).describe("Max check-in records (default 50, newest first).") },
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
    'discover_salesforce_schema',
    'List available Salesforce objects and the actual fields on Member__c and Consumption__c. Use this to debug why CRM queries return no results — confirms whether the expected custom objects and fields exist in this Salesforce org.',
    {},
    async () => {
      try {
        if (!sf.isConnected) throw new Error('Salesforce not connected');
        // Query Member__c for one record to see what fields come back
        const memberSample = await soql('SELECT FIELDS(ALL) FROM Member__c LIMIT 1').catch(() => null);
        // If FIELDS(ALL) is not available (non-Enterprise), fall back to known fields
        const memberCheck = await soql(`SELECT Id, Master_UUID__c, Email__c, First_Name__c, Last_Name__c, Wallet_Balance__c, Wallet_Status__c FROM Member__c LIMIT 1`).catch((e) => ({ error: e.message }));
        const consumptionCheck = await soql('SELECT Id, Consumption_ID__c, Product_Name__c FROM Consumption__c LIMIT 1').catch((e) => ({ error: e.message }));
        const taskCheck = await soql('SELECT Id, Subject FROM Task LIMIT 1').catch((e) => ({ error: e.message }));
        const memberCount = await soql('SELECT COUNT(Id) FROM Member__c').catch(() => [{ expr0: 'error' }]);

        return ok({
          member_object_accessible: !Array.isArray(memberCheck) || memberCheck.length >= 0,
          consumption_object_accessible: !consumptionCheck?.error,
          task_object_accessible: !taskCheck?.error,
          member_count: memberCount[0]?.expr0 ?? 'error',
          member_field_check: memberCheck,
          consumption_field_check: consumptionCheck,
          member_full_record_sample: memberSample,
        });
      } catch (e) { return sfErr(e); }
    }
  );

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
          soql("SELECT COUNT(Id) FROM Member__c WHERE Wallet_Status__c = 'Leased'"),
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
