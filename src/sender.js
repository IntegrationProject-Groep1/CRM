'use strict';

require('dotenv').config();
const amqp = require('amqplib');
const { getAmqpOptions } = require('./amqpUrl');
const { create } = require('xmlbuilder2');
const { v4: uuidv4 } = require('uuid');

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

  buildInvoiceCancelledXml(data) {
    const messageId = `cnl-inv-${uuidv4()}`;
    const timestamp = new Date().toISOString();

    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(messageId);
    header.ele('master_uuid').txt(data.master_uuid);
    header.ele('type').txt('invoice_cancelled');
    header.ele('source').txt('crm');
    header.ele('timestamp').txt(timestamp);
    header.ele('version').txt('2.0');

    const body = root.ele('body');
    body.ele('invoice_number').txt(data.invoice_number);
    if (data.reason) {
      body.ele('reason').txt(data.reason);
    }

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  async sendInvoiceCancelledToFacturatie(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized');
    try {
      const xmlPayload = this.buildInvoiceCancelledXml(data);
      const queue = 'crm.to.facturatie'; // De queue van FossBilling
      await this.channel.assertQueue(queue, { durable: true });
      this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      console.log(`Invoice cancellation sent to Facturatie for invoice: ${data.invoice_number}`);
      return { success: true, payload: xmlPayload };
    } catch (error) {
      console.log(`Failed to send invoice cancellation: ${error}`);
      throw error;
    }
  }

  buildInvoiceRequestXml(data) {
    const messageId = `inv-crm-${uuidv4()}`;
    const timestamp = new Date().toISOString();

    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(messageId);
    header.ele('master_uuid').txt(data.header?.master_uuid || ''); // NIEUW
    header.ele('version').txt('2.0');
    header.ele('type').txt('invoice_request');
    header.ele('timestamp').txt(timestamp);
    header.ele('source').txt('crm');
    
    // Hersteld: correlation_id
    if (data.correlation_id) {
      header.ele('correlation_id').txt(data.correlation_id);
    }

    const body = root.ele('body');

    const customer = body.ele('customer');
    customer.ele('email').txt(data.customer.email);
    customer.ele('first_name').txt(data.customer.first_name);
    customer.ele('last_name').txt(data.customer.last_name);
    if (data.customer.phone) {
      customer.ele('phone').txt(data.customer.phone);
    }
    
    // Nieuw: Adresgegevens voor FossBilling
    const address = customer.ele('address');
    address.ele('street').txt(data.address?.street || '');
    address.ele('number').txt(data.address?.number || '');
    address.ele('postal_code').txt(data.address?.postal_code || '');
    address.ele('city').txt(data.address?.city || '');

    const invoice = body.ele('invoice');
    invoice.ele('description').txt(data.invoice.description);
    invoice.ele('amount').att('currency', data.invoice.currency || 'eur').txt(String(data.invoice.amount));
    invoice.ele('due_date').txt(data.invoice.due_date);
    
    // Hersteld: invoice_number
    if (data.invoice.invoice_number) {
      invoice.ele('invoice_number').txt(data.invoice.invoice_number);
    }

    const items = body.ele('items');
    for (const item of (data.items || [])) {
      const itemElem = items.ele('item');
      itemElem.ele('description').txt(item.description);
      itemElem.ele('quantity').txt(String(item.quantity));
      itemElem.ele('unit_price').att('currency', item.currency || 'eur').txt(String(item.unit_price));
      itemElem.ele('vat_rate').txt(String(item.vat_rate ?? 21));
      if (item.sku) {
        itemElem.ele('sku').txt(item.sku);
      }
    }

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  buildMailingSendXml(data) {
    const messageId = `mail-crm-${uuidv4()}`;
    const timestamp = new Date().toISOString();

    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(messageId);
    header.ele('master_uuid').txt(data.master_uuid);
    header.ele('version').txt('2.0');
    header.ele('type').txt('mailing_status');
    header.ele('timestamp').txt(timestamp);
    header.ele('source').txt('crm');
    if (data.correlation_id) {
      header.ele('correlation_id').txt(data.correlation_id);
    }

    const body = root.ele('body');

    const mailing = body.ele('mailing');
    mailing.ele('subject').txt(data.mailing.subject);
    mailing.ele('template_id').txt(data.mailing.template_id);
    if (data.mailing.from_address) {
      mailing.ele('from_address').txt(data.mailing.from_address);
    }
    if (data.mailing.reply_to) {
      mailing.ele('reply_to').txt(data.mailing.reply_to);
    }

    const recipients = body.ele('recipients');
    for (const recipient of data.recipients) {
      const recipientElem = recipients.ele('recipient');
      recipientElem.ele('email').txt(recipient.email);
      recipientElem.ele('first_name').txt(recipient.first_name);
      recipientElem.ele('last_name').txt(recipient.last_name);
      if (recipient.language) {
        recipientElem.ele('language').txt(recipient.language);
      }
    }

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  async sendInvoiceRequest(data) {
    if (!this.channel) {
      throw new Error('CRM Sender not initialized. Call init() first.');
    }
    try {
      const xmlPayload = this.buildInvoiceRequestXml(data);
      const queue = 'crm.to.facturatie';
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

  // ── Kassa outbound flows ────────────────────────────────────────────────────

  buildNewRegistrationForKassaXml(data) {
     const messageId = `reg-crm-${uuidv4()}`;
     const timestamp = new Date().toISOString();

     const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

     const header = root.ele('header');
      header.ele('message_id').txt(messageId);
      header.ele('master_uuid').txt(data.master_uuid);
      header.ele('type').txt('new_registration');
      header.ele('source').txt('crm');
      header.ele('timestamp').txt(timestamp);
      header.ele('version').txt('2.0');
    
    // --> DIT MOET ERBIJ <--
    if (data.header && data.header.master_uuid) {
      header.ele('master_uuid').txt(data.header.master_uuid);
    }
    // ----------------------

    const body = root.ele('body');
    const customer = body.ele('customer');
    customer.ele('email').txt(data.customer.email);

    const contact = customer.ele('contact');
    contact.ele('first_name').txt(data.customer.first_name);
    contact.ele('last_name').txt(data.customer.last_name);

    if (data.customer.company_name) {
      customer.ele('company_name').txt(data.customer.company_name);
    }
    customer.ele('type').txt(data.customer.type || 'private');
    if (data.customer.vat_number) {
      customer.ele('vat_number').txt(data.customer.vat_number);
    }
    customer.ele('master_uuid').txt(data.header?.master_uuid || '');
    customer.ele('date_of_birth').txt(data.customer.date_of_birth);

    const paymentDue = body.ele('payment_due');
    paymentDue.ele('amount').txt(String(data.payment_due.amount));
    const normalizedStatus = data.payment_due.status === 'paid' ? 'paid' : 'unpaid';
    paymentDue.ele('status').txt(normalizedStatus);

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  async sendNewRegistrationToKassa(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const xmlPayload = this.buildNewRegistrationForKassaXml(data);
      const queue = 'kassa.incoming';
      await this.channel.assertQueue(queue, { durable: true });
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

  async sendNewRegistrationToFacturatie(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized.');
    try {
      const xmlPayload = this.buildNewRegistrationForFacturatieXml(data);
      const queue = 'crm.to.facturatie'; // De queue waar FossBilling op luistert
      await this.channel.assertQueue(queue, { durable: true });
      this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      console.log(`New registration sent to Facturatie queue "${queue}"`);
      return { success: true, xmlPayload };
    } catch (error) {
      console.log(`Failed to send registration to Facturatie: ${error}`);
      throw error;
    }
  }

  buildProfileUpdateXml(data) {
    const messageId = `prof-crm-${uuidv4()}`;
    const timestamp = new Date().toISOString();

    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(messageId);
    header.ele('master_uuid').txt(data.master_uuid);
    header.ele('type').txt('profile_update');
    header.ele('source').txt('crm');
    header.ele('timestamp').txt(timestamp);
    header.ele('version').txt('2.0');
    if (data.correlation_id) header.ele('correlation_id').txt(data.correlation_id);

    const body = root.ele('body');
    body.ele('master_uuid').txt(data.master_uuid);
    body.ele('email').txt(data.email);
    body.ele('date_of_birth').txt(data.date_of_birth);
    body.ele('type').txt(data.type || 'private');

    const contact = body.ele('contact');
    contact.ele('first_name').txt(data.first_name || '');
    contact.ele('last_name').txt(data.last_name || '');

    if (data.company_name) body.ele('company_name').txt(data.company_name);
    if (data.vat_number) body.ele('vat_number').txt(data.vat_number);

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  async sendProfileUpdateToKassa(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const xmlPayload = this.buildProfileUpdateXml(data);
      const queue = 'kassa.incoming';
      await this.channel.assertQueue(queue, { durable: true });
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

  buildNewRegistrationForFacturatieXml(data) {
    const messageId = `reg-foss-${uuidv4()}`;
    const timestamp = new Date().toISOString();

    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(messageId);
    header.ele('master_uuid').txt(data.master_uuid); // De nieuwe standaard
    header.ele('type').txt('new_registration');
    header.ele('source').txt('crm');
    header.ele('timestamp').txt(timestamp);
    header.ele('version').txt('2.0');

    const body = root.ele('body');
    const customer = body.ele('customer');
    customer.ele('first_name').txt(data.customer.first_name);
    customer.ele('last_name').txt(data.customer.last_name);
    customer.ele('email').txt(data.customer.email);
    customer.ele('type').txt(data.customer.type);
    if (data.customer.company_name) customer.ele('company_name').txt(data.customer.company_name);
    if (data.customer.vat_number) customer.ele('vat_number').txt(data.customer.vat_number);

    const address = body.ele('address');
    address.ele('street').txt(data.address.street || '');
    address.ele('number').txt(data.address.number || '');
    address.ele('postal_code').txt(data.address.postal_code || '');
    address.ele('city').txt(data.address.city || '');
    address.ele('country').txt(data.address.country || 'BE');

    const fee = body.ele('registration_fee');
    fee.ele('amount').txt(String(data.registration_fee.amount));
    fee.ele('status').txt(data.registration_fee.status);
    fee.ele('trigger_invoice').txt(String(data.registration_fee.trigger_invoice));

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  buildCancelRegistrationXml(data) {
    const messageId = `cancel-crm-${uuidv4()}`;
    const timestamp = new Date().toISOString();

    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(messageId);
    header.ele('master_uuid').txt(data.master_uuid);
    header.ele('type').txt('cancel_registration');
    header.ele('source').txt('crm');
    header.ele('timestamp').txt(timestamp);
    header.ele('version').txt('2.0');
    if (data.correlation_id) header.ele('correlation_id').txt(data.correlation_id);

    const body = root.ele('body');
    body.ele('master_uuid').txt(data.master_uuid);
    body.ele('session_id').txt(data.session_id || '');

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  async sendCancelRegistrationToKassa(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const xmlPayload = this.buildCancelRegistrationXml(data);
      const queue = 'kassa.incoming';
      await this.channel.assertQueue(queue, { durable: true });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`Cancel registration forwarded to queue "${queue}"`);
      return { success: true, queue, payload: xmlPayload };
    } catch (error) {
      console.log(`Failed to send cancel registration to Kassa: ${error}`);
      throw error;
    }
  }

  

  // ────────────────────────────────────────────────────────────────────────────

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
