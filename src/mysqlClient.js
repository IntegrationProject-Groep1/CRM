'use strict';

require('dotenv').config();
const mysql = require('mysql2/promise');

class MySQLService {
  constructor() {
    this.pool = null;
    this.userTable = 'crm_user_sync';
  }

  init() {
    const host = process.env.MYSQL_HOST;
    const port = Number(process.env.MYSQL_PORT || 3306);
    const database = process.env.MYSQL_DATABASE;
    const user = process.env.MYSQL_USER;
    const password = process.env.MYSQL_PASSWORD;

    if (!host || !database || !user || !password) {
      console.log('[mysql] Missing MYSQL_HOST / MYSQL_DATABASE / MYSQL_USER / MYSQL_PASSWORD — MySQL disabled');
      return;
    }

    this.pool = mysql.createPool({
      host,
      port,
      database,
      user,
      password,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
    });

    console.log('[mysql] Pool initialized');
  }

  get isConnected() {
    return this.pool !== null;
  }

  async query(sql, params = []) {
    if (!this.isConnected) return [[], null];

    try {
      const [rows] = await this.pool.execute(sql, params);
      return [rows, null];
    } catch (error) {
      return [null, error];
    }
  }

  toMySqlDateTime(value) {
    if (!value) return null;

    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) return null;

    const pad = (n) => String(n).padStart(2, '0');
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
  }

  // ── People ────────────────────────────────────────────────────────────────

  async upsertPerson(data) {
    if (!this.isConnected) return null;

    if (!data.external_user_id) {
      console.log('[mysql] Error upserting person: missing external_user_id');
      return null;
    }

    const normalizedPaymentStatus = data.payment_status === 'paid' ? 'paid' : 'pending';
    const normalizedAmount = Number.isFinite(Number(data.amount)) ? Number(data.amount) : 0;

    const payload = {
      User_ID__c: data.external_user_id,
      User_Type__c: data.person_type || 'Particulier',
      Company_Name__c: data.company_name || null,
      BTW_Number__c: data.vat_number || null,
      Language__c: data.language || 'NL',
      Salutation__c: data.salutation || null,
      First_Name__c: data.first_name || '',
      Last_Name__c: data.last_name || '',
      Email__c: data.email || '',
      Birthdate__c: data.date_of_birth || '1970-01-01',
      Amount__c: normalizedAmount,
      Street__c: data.street || '',
      House_Number__c: data.house_number || '',
      Postal_Code__c: data.postal_code || '',
      City__c: data.city || '',
      Country_Code__c: data.country || 'BE',
      Payment_Status__c: normalizedPaymentStatus,
      Badge_ID__c: data.badge_id || null,
      sync_status: data.sync_status || null,
      sync_log: data.sync_log || null,
      auth_user_id: data.auth_user_id || null,
    };

    const sql = `
      INSERT INTO crm_user_sync (
        User_ID__c,
        User_Type__c,
        Company_Name__c,
        BTW_Number__c,
        Language__c,
        Salutation__c,
        First_Name__c,
        Last_Name__c,
        Email__c,
        Birthdate__c,
        Amount__c,
        Street__c,
        House_Number__c,
        Postal_Code__c,
        City__c,
        Country_Code__c,
        Payment_Status__c,
        Badge_ID__c,
        sync_status,
        sync_log,
        auth_user_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        User_Type__c = VALUES(User_Type__c),
        Company_Name__c = VALUES(Company_Name__c),
        BTW_Number__c = VALUES(BTW_Number__c),
        Language__c = VALUES(Language__c),
        Salutation__c = VALUES(Salutation__c),
        First_Name__c = VALUES(First_Name__c),
        Last_Name__c = VALUES(Last_Name__c),
        Email__c = VALUES(Email__c),
        Birthdate__c = VALUES(Birthdate__c),
        Amount__c = VALUES(Amount__c),
        Street__c = VALUES(Street__c),
        House_Number__c = VALUES(House_Number__c),
        Postal_Code__c = VALUES(Postal_Code__c),
        City__c = VALUES(City__c),
        Country_Code__c = VALUES(Country_Code__c),
        Payment_Status__c = VALUES(Payment_Status__c),
        Badge_ID__c = VALUES(Badge_ID__c),
        sync_status = VALUES(sync_status),
        sync_log = VALUES(sync_log),
        auth_user_id = VALUES(auth_user_id)
    `;

    const params = [
      payload.User_ID__c,
      payload.User_Type__c,
      payload.Company_Name__c,
      payload.BTW_Number__c,
      payload.Language__c,
      payload.Salutation__c,
      payload.First_Name__c,
      payload.Last_Name__c,
      payload.Email__c,
      payload.Birthdate__c,
      payload.Amount__c,
      payload.Street__c,
      payload.House_Number__c,
      payload.Postal_Code__c,
      payload.City__c,
      payload.Country_Code__c,
      payload.Payment_Status__c,
      payload.Badge_ID__c,
      payload.sync_status,
      payload.sync_log,
      payload.auth_user_id,
    ];

    const [, error] = await this.query(sql, params);
    if (error) {
      console.log(`[mysql] Error upserting person: ${error.message}`);
      return null;
    }

    return payload.User_ID__c;
  }

  async findPersonByExternalId(externalUserId) {
    if (!this.isConnected) return null;

    const [rows, error] = await this.query(
      `SELECT User_ID__c FROM ${this.userTable} WHERE User_ID__c = ? LIMIT 1`,
      [externalUserId]
    );

    if (error) {
      console.log(`[mysql] Error finding person: ${error.message}`);
      return null;
    }

    return rows.length > 0 ? rows[0].User_ID__c : null;
  }

  async findPersonByEmail(email) {
    if (!this.isConnected) return null;

    const [rows, error] = await this.query(
      `SELECT User_ID__c FROM ${this.userTable} WHERE Email__c = ? LIMIT 1`,
      [email]
    );

    if (error) {
      console.log(`[mysql] Error finding person by email: ${error.message}`);
      return null;
    }

    return rows.length > 0 ? rows[0].User_ID__c : null;
  }

  async findPersonByEmailForCheckIn(email) {
    if (!this.isConnected) {
      return { personId: null, error: null };
    }

    const [rows, error] = await this.query(
      `SELECT User_ID__c FROM ${this.userTable} WHERE Email__c = ? LIMIT 1`,
      [email]
    );

    if (error) {
      return { personId: null, error };
    }

    return {
      personId: rows.length > 0 ? rows[0].User_ID__c : null,
      error: null,
    };
  }

  async updatePersonSalesforceId(externalUserId, salesforceContactId) {
    if (!this.isConnected) return;

    const [, error] = await this.query(
      `UPDATE ${this.userTable} SET Member__c = ? WHERE User_ID__c = ?`,
      [salesforceContactId, externalUserId]
    );

    if (error) {
      console.log(`[mysql] Error updating Member__c: ${error.message}`);
    }
  }

  // ── Companies ─────────────────────────────────────────────────────────────

  async upsertCompany(data) {
    if (!this.isConnected) return null;

    const sql = `
      INSERT INTO companies (
        company_name,
        vat_number,
        salesforce_account_id
      ) VALUES (?, ?, ?)
      ON DUPLICATE KEY UPDATE
        company_name = VALUES(company_name),
        salesforce_account_id = VALUES(salesforce_account_id),
        id = LAST_INSERT_ID(id)
    `;

    const params = [
      data.company_name || null,
      data.vat_number,
      data.salesforce_account_id || null,
    ];

    const [result, error] = await this.query(sql, params);
    if (error) {
      console.log(`[mysql] Error upserting company: ${error.message}`);
      return null;
    }

    return result.insertId || null;
  }

  async updateCompanySalesforceId(vatNumber, salesforceAccountId) {
    if (!this.isConnected) return;

    const [, error] = await this.query(
      `UPDATE companies SET salesforce_account_id = ? WHERE vat_number = ?`,
      [salesforceAccountId, vatNumber]
    );

    if (error) {
      console.log(`[mysql] Error updating salesforce_account_id: ${error.message}`);
    }
  }

  // ── Event Attendees ───────────────────────────────────────────────────────

  async findEventAttendeeByPersonId(personId) {
    if (!this.isConnected) return null;

    const [rows, error] = await this.query(
      `SELECT id FROM event_attendees WHERE person_id = ? ORDER BY created_at DESC LIMIT 1`,
      [personId]
    );

    if (error) {
      console.log(`[mysql] Error finding event_attendee: ${error.message}`);
      return null;
    }

    return rows.length > 0 ? rows[0].id : null;
  }

  async updateEventAttendeeCheckIn(personId) {
    if (!this.isConnected) return;

    const [, error] = await this.query(
      `UPDATE ${this.userTable} SET iot_last_scan = ? WHERE User_ID__c = ?`,
      [this.toMySqlDateTime(new Date()), personId]
    );

    if (error) {
      console.log(`[mysql] Error updating iot_last_scan: ${error.message}`);
    }
  }

  async syncSalesforceStatus(personId, status) {
    if (!this.isConnected) return;

    const [, error] = await this.query(
      `UPDATE ${this.userTable} SET sync_status = ? WHERE User_ID__c = ?`,
      [status, personId]
    );

    if (error) {
      console.log(`[mysql] Error updating sync_status: ${error.message}`);
    }
  }

  // ── Payments ──────────────────────────────────────────────────────────────

  async insertPayment(data) {
    if (!this.isConnected) return null;

    const sql = `
      INSERT INTO payments (
        event_attendee_id,
        amount,
        payment_type,
        status,
        payment_method,
        paid_at
      ) VALUES (?, ?, ?, ?, ?, ?)
    `;

    const params = [
      data.event_attendee_id || null,
      Number.isFinite(Number(data.amount)) ? Number(data.amount) : 0,
      data.payment_type || null,
      data.status || null,
      data.payment_method || null,
      this.toMySqlDateTime(data.paid_at || new Date()),
    ];

    const [result, error] = await this.query(sql, params);
    if (error) {
      console.log(`[mysql] Error inserting payment: ${error.message}`);
      return null;
    }

    return result.insertId || null;
  }

  // ── Consumptions ──────────────────────────────────────────────────────────

  async insertConsumption(data) {
    if (!this.isConnected) return null;

    const sql = `
      INSERT INTO consumptions (
        event_attendee_id,
        item_name,
        quantity,
        unit_price,
        total_price,
        paid
      ) VALUES (?, ?, ?, ?, ?, ?)
    `;

    const params = [
      data.event_attendee_id || null,
      data.item_name,
      parseInt(data.quantity, 10) || 1,
      Number.isFinite(Number(data.unit_price)) ? Number(data.unit_price) : 0,
      Number.isFinite(Number(data.total_price)) ? Number(data.total_price) : 0,
      data.paid ? 1 : 0,
    ];

    const [result, error] = await this.query(sql, params);
    if (error) {
      console.log(`[mysql] Error inserting consumption: ${error.message}`);
      return null;
    }

    return result.insertId || null;
  }
}

module.exports = MySQLService;