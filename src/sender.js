'use strict';

require('dotenv').config();
const amqp = require('amqplib');
const { getAmqpOptions } = require('./amqpUrl');
const { create } = require('xmlbuilder2');
const { v4: uuidv4 } = require('uuid');

const USER_UNREGISTERED_EXCHANGE = 'frontend.user.unregistered';
const USER_UNREGISTERED_QUEUES = ['crm.salesforce', 'planning.outlook', 'mailing.sendgrid'];

class CRMSender {
  constructor() {
    this.connection = null;
    this.channel = null;
    this.rabbitmqOptions = getAmqpOptions();
  }

  async init() {
    try {
      this.connection = await amqp.connect(this.rabbitmqOptions);
      this.channel = await this.connection.createChannel();
      console.log('CRM Sender initialized');
    } catch (error) {
      console.log(`Failed to initialize CRM Sender: ${error}`);
      throw error;
    }
  }

  // ── invoice_request (CRM → Facturatie, section 11.1) ────────────────────────
  // Body: identity_uuid + invoice_data{contact, email, address, company_name?, vat_number?}
  // correlation_id is REQUIRED (links to consumption_order message_id)
  buildInvoiceRequestXml(data) {
    const messageId = uuidv4();
    const timestamp = new Date().toISOString();

    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(messageId);
    header.ele('timestamp').txt(timestamp);
    header.ele('source').txt('crm');
    header.ele('type').txt('invoice_request');
    header.ele('version').txt('2.0');
    header.ele('correlation_id').txt(data.correlation_id || uuidv4());

    const body = root.ele('body');
    body.ele('identity_uuid').txt(data.identity_uuid);

    const invoiceData = body.ele('invoice_data');
    const contact = invoiceData.ele('contact');
    contact.ele('first_name').txt(data.invoice_data?.first_name || '');
    contact.ele('last_name').txt(data.invoice_data?.last_name || '');
    invoiceData.ele('email').txt(data.invoice_data?.email || '');

    const address = invoiceData.ele('address');
    address.ele('street').txt(data.invoice_data?.address?.street || '');
    address.ele('number').txt(data.invoice_data?.address?.number || '');
    address.ele('postal_code').txt(data.invoice_data?.address?.postal_code || '');
    address.ele('city').txt(data.invoice_data?.address?.city || '');
    address.ele('country').txt(data.invoice_data?.address?.country || '');

    if (data.invoice_data?.company_name) invoiceData.ele('company_name').txt(data.invoice_data.company_name);
    if (data.invoice_data?.vat_number)   invoiceData.ele('vat_number').txt(data.invoice_data.vat_number);

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  // ── send_mailing (CRM → Mailing, section 12.1) ──────────────────────────────
  // Body: campaign_id, subject, mail_type, recipients[]{email, identity_uuid, contact}
  // correlation_id is REQUIRED per contract XSD
  buildMailingSendXml(data) {
    const messageId = uuidv4();
    const timestamp = new Date().toISOString();
    const mailing = data.mailing || data;

    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(messageId);
    header.ele('timestamp').txt(timestamp);
    header.ele('source').txt('crm');
    header.ele('type').txt('send_mailing');
    header.ele('version').txt('2.0');
    header.ele('correlation_id').txt(data.correlation_id || uuidv4());

    const body = root.ele('body');
    body.ele('campaign_id').txt(data.campaign_id || '');
    body.ele('subject').txt(data.subject || '');
    body.ele('mail_type').txt(data.mail_type || 'general_announcement');

    const recipients = body.ele('recipients');
    for (const r of (data.recipients || [])) {
      const recipientElem = recipients.ele('recipient');
      recipientElem.ele('email').txt(r.email);
      recipientElem.ele('identity_uuid').txt(r.identity_uuid || r.user_id || '');
      const contact = recipientElem.ele('contact');
      contact.ele('first_name').txt(r.first_name);
      contact.ele('last_name').txt(r.last_name);
    }

    if (data.template_data) body.ele('template_data').txt(data.template_data);
    if (data.body_html)     body.ele('body_html').txt(data.body_html);

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  buildLogXml({ level, action, message }) {
    const validLevels = new Set(['info', 'warning', 'error']);
    const validActions = new Set([
      'registration', 'user', 'payment', 'invoice', 'session', 'calendar',
      'email', 'wallet', 'refund', 'identity', 'xml_validation', 'system_error', 'badge',
    ]);

    if (!validLevels.has(level)) throw new Error(`Invalid log level: ${level}`);
    if (!validActions.has(action)) throw new Error(`Invalid log action: ${action}`);

    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(uuidv4());
    header.ele('timestamp').txt(new Date().toISOString());
    header.ele('source').txt('crm');
    header.ele('type').txt('log');
    header.ele('version').txt('2.0');

    const body = root.ele('body');
    body.ele('level').txt(level);
    body.ele('action').txt(action);
    body.ele('message').txt(message);

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  // ── new_registration (CRM → Kassa, section 10.1) ────────────────────────────
  // Body: customer{identity_uuid, email, date_of_birth, contact, type, ..., session_id, payment_due}
  // session_id and payment_due are inside customer; correlation_id REQUIRED
  buildNewRegistrationForKassaXml(data) {
    const messageId = uuidv4();
    const timestamp = new Date().toISOString();

     const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

     const header = root.ele('header');
    header.ele('message_id').txt(messageId);
    header.ele('timestamp').txt(timestamp);
    header.ele('source').txt('crm');
    header.ele('type').txt('new_registration');
    header.ele('version').txt('2.0');
    header.ele('correlation_id').txt(data.correlation_id || uuidv4());

    const body = root.ele('body');
    const customer = body.ele('customer');
    customer.ele('identity_uuid').txt(data.customer.identity_uuid);
    customer.ele('email').txt(data.customer.email);
    customer.ele('date_of_birth').txt(data.customer.date_of_birth);

    const contact = customer.ele('contact');
    contact.ele('first_name').txt(data.customer.first_name);
    contact.ele('last_name').txt(data.customer.last_name);

    customer.ele('type').txt(data.customer.type || 'private');
    if (data.customer.company_name) customer.ele('company_name').txt(data.customer.company_name);
    if (data.customer.vat_number)   customer.ele('vat_number').txt(data.customer.vat_number);
    if (data.customer.company_id)   customer.ele('company_id').txt(data.customer.company_id);
    if (data.customer.badge_id)     customer.ele('badge_id').txt(data.customer.badge_id);

    customer.ele('session_id').txt(data.customer.session_id || '');
    if (data.customer.session_title) customer.ele('session_title').txt(data.customer.session_title);

    const paymentDue = customer.ele('payment_due');
    paymentDue.ele('amount').att('currency', 'eur').txt(String(data.customer.payment_due?.amount || '0.00'));
    paymentDue.ele('status').txt(data.customer.payment_due?.status || 'unpaid');

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  async sendNewRegistrationToKassa(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const xmlPayload = this.buildNewRegistrationForKassaXml(data);
      const queue = 'kassa.incoming';
      await this.channel.assertQueue(queue, { durable: true, arguments: { 'x-dead-letter-exchange': 'kassa.dlx', 'x-dead-letter-routing-key': 'kassa.incoming.dlq' } });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`New registration forwarded to queue "${queue}"`);
      return { success: true, queue, payload: xmlPayload };
    } catch (error) {
      console.log(`Failed to send new registration to Kassa: ${error}`);
      throw error;
    }
  }

  // ── profile_update (CRM → Kassa, section 10.2) ──────────────────────────────
  // Body: identity_uuid, email, date_of_birth?, contact, type?, company_name?, vat_number?, company_id?, payment_due?
  buildProfileUpdateXml(data) {
    const messageId = uuidv4();
    const timestamp = new Date().toISOString();

    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(messageId);
    header.ele('timestamp').txt(timestamp);
    header.ele('source').txt('crm');
    header.ele('type').txt('profile_update');
    header.ele('version').txt('2.0');
    if (data.correlation_id) header.ele('correlation_id').txt(data.correlation_id);

    const body = root.ele('body');
    body.ele('identity_uuid').txt(data.identity_uuid);
    body.ele('email').txt(data.email);
    if (data.date_of_birth) body.ele('date_of_birth').txt(data.date_of_birth);

    const contact = body.ele('contact');
    contact.ele('first_name').txt(data.first_name || '');
    contact.ele('last_name').txt(data.last_name || '');

    if (data.type)         body.ele('type').txt(data.type);
    if (data.company_name) body.ele('company_name').txt(data.company_name);
    if (data.vat_number)   body.ele('vat_number').txt(data.vat_number);
    if (data.company_id)   body.ele('company_id').txt(data.company_id);

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  async sendProfileUpdateToKassa(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const xmlPayload = this.buildProfileUpdateXml(data);
      const queue = 'kassa.incoming';
      await this.channel.assertQueue(queue, { durable: true, arguments: { 'x-dead-letter-exchange': 'kassa.dlx', 'x-dead-letter-routing-key': 'kassa.incoming.dlq' } });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`Profile update forwarded to queue "${queue}"`);
      return { success: true, queue, payload: xmlPayload };
    } catch (error) {
      console.log(`Failed to send profile update to Kassa: ${error}`);
      throw error;
    }
  }

  // ── cancel_registration (CRM → Kassa & Planning, section 10.3) ──────────────
  // Body: identity_uuid, session_id, reason?
  buildCancelRegistrationXml(data) {
    const messageId = uuidv4();
    const timestamp = new Date().toISOString();

    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(messageId);
    header.ele('timestamp').txt(timestamp);
    header.ele('source').txt('crm');
    header.ele('type').txt('cancel_registration');
    header.ele('version').txt('2.0');
    if (data.correlation_id) header.ele('correlation_id').txt(data.correlation_id);

    const body = root.ele('body');
    body.ele('identity_uuid').txt(data.identity_uuid);
    body.ele('session_id').txt(data.session_id);
    if (data.reason) body.ele('reason').txt(data.reason);

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  async sendCancelRegistrationToKassa(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const xmlPayload = this.buildCancelRegistrationXml(data);
      const queue = 'kassa.incoming';
      await this.channel.assertQueue(queue, { durable: true, arguments: { 'x-dead-letter-exchange': 'kassa.dlx', 'x-dead-letter-routing-key': 'kassa.incoming.dlq' } });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`Cancel registration forwarded to queue "${queue}"`);

      // Also notify Planning via calendar.exchange (section 10.3)
      await this.channel.assertExchange('calendar.exchange', 'topic', { durable: true });
      const planningOk = this.channel.publish(
        'calendar.exchange',
        'crm.to.planning.cancel_registration',
        Buffer.from(xmlPayload),
        { contentType: 'application/xml', deliveryMode: 2 }
      );
      if (!planningOk) console.log('[sender] Warning: write buffer full for calendar.exchange');
      console.log('Cancel registration also forwarded to calendar.exchange (Planning)');

      return { success: true, queue, payload: xmlPayload };
    } catch (error) {
      console.log(`Failed to send cancel registration to Kassa: ${error}`);
      throw error;
    }
  }

  async sendInvoiceRequest(data) {
    if (!this.channel) {
      throw new Error('CRM Sender not initialized. Call init() first.');
    }
    try {
      const xmlPayload = this.buildInvoiceRequestXml(data);
      const queue = 'facturatie.incoming';
      await this.channel.assertQueue(queue, { durable: true });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`Invoice request sent to queue "${queue}"`);
      return { success: true, queue, payload: xmlPayload };
    } catch (error) {
      console.log(`Failed to send invoice request: ${error}`);
      throw error;
    }
  }

  async sendMailingSend(data) {
    if (!this.channel) {
      throw new Error('CRM Sender not initialized. Call init() first.');
    }
    try {
      const xmlPayload = this.buildMailingSendXml(data);
      const queue = 'crm.to.mailing';
      await this.channel.assertQueue(queue, { durable: true });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`Mailing send request sent to queue "${queue}"`);
      return { success: true, queue, payload: xmlPayload };
    } catch (error) {
      console.log(`Failed to send mailing send request: ${error}`);
      throw error;
    }
  }

  // ── payment_registered (CRM → Frontend/Facturatie passthrough) ──────────────
  async sendPaymentRegisteredToFrontend(xml) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const queue = 'frontend.incoming';
      await this.channel.assertQueue(queue, { durable: true });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xml), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`Payment registered forwarded to Frontend queue "${queue}"`);
    } catch (error) {
      console.log(`Failed to forward payment to Frontend: ${error}`);
    }
  }

  async sendPaymentRegisteredToFacturatie(xml) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const queue = 'facturatie.incoming';
      await this.channel.assertQueue(queue, { durable: true });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xml), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`Payment registered forwarded to Facturatie queue "${queue}"`);
    } catch (error) {
      console.log(`Failed to forward payment to Facturatie: ${error}`);
    }
  }

  // ── new_registration (CRM → Facturatie, section 10.1 passthrough/enrichment) ─
  async sendNewRegistrationToFacturatie(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const messageId = uuidv4();
      const timestamp = new Date().toISOString();

      const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');
      const header = root.ele('header');
      header.ele('message_id').txt(messageId);
      header.ele('timestamp').txt(timestamp);
      header.ele('source').txt('crm');
      header.ele('type').txt('new_registration');
      header.ele('version').txt('2.0');
      if (data.correlation_id) header.ele('correlation_id').txt(data.correlation_id);

      const body = root.ele('body');
      body.ele('master_uuid').txt(data.master_uuid);
      
      const customer = body.ele('customer');
      customer.ele('first_name').txt(data.customer.first_name);
      customer.ele('last_name').txt(data.customer.last_name);
      customer.ele('email').txt(data.customer.email);
      customer.ele('type').txt(data.customer.type || 'private');
      if (data.customer.company_name) customer.ele('company_name').txt(data.customer.company_name);
      if (data.customer.vat_number)   customer.ele('vat_number').txt(data.customer.vat_number);

      const address = body.ele('address');
      address.ele('street').txt(data.address.street || '');
      address.ele('number').txt(data.address.number || '');
      address.ele('postal_code').txt(data.address.postal_code || '');
      address.ele('city').txt(data.address.city || '');
      address.ele('country').txt(data.address.country || 'BE');

      const paymentDue = body.ele('payment_due');
      paymentDue.ele('amount').txt(String(data.payment_due.amount || '0.00'));
      paymentDue.ele('status').txt(data.payment_due.status || 'unpaid');

      const xmlPayload = root.doc().end({ prettyPrint: true, indent: '  ' });
      const queue = 'facturatie.incoming';
      await this.channel.assertQueue(queue, { durable: true });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`New registration forwarded to Facturatie queue "${queue}"`);
    } catch (error) {
      console.log(`Failed to forward registration to Facturatie: ${error}`);
    }
  }

  // ── user_unregistered (CRM → Fanout, section 5.2) ──────────────────────────
  async sendUserUnregisteredFanout(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const exchange = USER_UNREGISTERED_EXCHANGE;
      await this.channel.assertExchange(exchange, 'fanout', { durable: true });

      const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');
      const header = root.ele('header');
      header.ele('message_id').txt(data.message_id || uuidv4());
      header.ele('timestamp').txt(data.timestamp || new Date().toISOString());
      header.ele('source').txt(data.source || 'crm');
      header.ele('type').txt('user.unregistered');
      header.ele('version').txt('1.0');
      header.ele('receiver').txt(data.receiver || '');
      if (data.correlation_id) header.ele('correlation_id').txt(data.correlation_id);

      const body = root.ele('body');
      body.ele('master_uuid').txt(data.master_uuid);
      body.ele('session_id').txt(data.session_id);
      body.ele('timestamp').txt(data.body_timestamp || data.timestamp || new Date().toISOString());

      const xmlPayload = root.doc().end({ prettyPrint: true, indent: '  ' });
      const ok = this.channel.publish(exchange, '', Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for exchange "${exchange}"`);
      console.log(`User unregistered broadcast via exchange "${exchange}"`);
    } catch (error) {
      console.log(`Failed to broadcast user unregistered: ${error}`);
    }
  }

  async close() {
    try {
      if (this.connection) await this.connection.close();
      console.log('CRM Sender connection closed');
    } catch (error) {
      console.log(`Error closing connection: ${error}`);
    }
  }
}

module.exports = CRMSender;
