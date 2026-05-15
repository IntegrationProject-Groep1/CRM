'use strict';

require('dotenv').config();
const amqp = require('amqplib');
const { getAmqpOptions } = require('./amqpUrl');
const { create } = require('xmlbuilder2');
const { v4: uuidv4 } = require('uuid');
const { validateXml } = require('./validator');

const USER_UNREGISTERED_EXCHANGE = 'frontend.user.unregistered';

class CRMSender {
  constructor() {
    this.connection = null;
    this.channel = null;
    this.rabbitmqOptions = getAmqpOptions();
    this.xsdMapping = {
      'new_registration': 'new_registration_kassa.xsd',
      'profile_update': 'profile_update.xsd',
      'cancel_registration': 'cancel_registration.xsd',
      'invoice_request': 'invoice_request_facturatie.xsd',
      'invoice_cancelled': 'invoice_cancelled_facturatie.xsd',
      'send_mailing': 'send_mailing.xsd',
      'log': 'log.xsd',
      'session_registration_confirmed': 'session_registration_confirmed.xsd',
      'user.unregistered': 'user_unregistered.xsd',
      'event_ended': 'event_ended.xsd',
      'payment_registered': 'payment_registered_facturatie.xsd',
      'consumption_order': 'consumption_order.xsd',
      'wallet_lease_grant': 'wallet_lease_grant.xsd',
      'wallet_remote_topup': 'wallet_remote_topup.xsd',
    };
    this.messageTypeToLogAction = {
      'new_registration': 'registration',
      'profile_update': 'user',
      'cancel_registration': 'registration',
      'invoice_request': 'invoice',
      'invoice_cancelled': 'invoice',
      'send_mailing': 'email',
      'session_registration_confirmed': 'session',
      'user.unregistered': 'user',
      'event_ended': 'session',
      'payment_registered': 'payment',
      'consumption_order': 'payment',
      'invoice_status': 'invoice',
      'refund_processed': 'refund',
      'badge_scanned': 'badge',
      'badge_assigned': 'badge',
      'wallet_lease_grant': 'wallet',
      'wallet_remote_topup': 'wallet',
    };
  }

  _validate(xml, type) {
    const xsdFile = this.xsdMapping[type];
    if (!xsdFile) {
      console.log(`[sender] Warning: No XSD mapping for outgoing type "${type}"`);
      return;
    }

    const { valid, errors } = validateXml(xml, xsdFile);
    if (!valid) {
      const errorMsg = `Outgoing XML validation failed for "${type}" (${xsdFile}): ${errors.join('; ')}`;
      console.error(`[sender] ${errorMsg}`);
      throw new Error(errorMsg);
    }
    console.log(`[sender] XSD validation passed for outgoing "${type}"`);
  }

  async _logOutbound(type, destination, correlationId) {
    const action = this.messageTypeToLogAction[type] || 'system_error';
    const message = `Published ${type} to ${destination}. CorrelationID: ${correlationId || 'N/A'}.`;
    try {
      await this.sendLog({ level: 'info', action, message });
    } catch (err) {
      console.error(`[sender] Failed to send outbound log: ${err.message}`);
    }
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
  customer.ele('identity_uuid').txt(data.customer.master_uuid || data.customer.user_id || data.customer.identity_uuid || '');
  customer.ele('email').txt(data.customer.email);
  if (data.customer.date_of_birth) customer.ele('date_of_birth').txt(data.customer.date_of_birth);

  const contact = customer.ele('contact');
  contact.ele('first_name').txt(data.customer.first_name);
  contact.ele('last_name').txt(data.customer.last_name);

  customer.ele('type').txt(data.customer.type || 'private');
  if (data.customer.company_name) customer.ele('company_name').txt(data.customer.company_name);
  if (data.customer.vat_number)   customer.ele('vat_number').txt(data.customer.vat_number);
  if (data.customer.company_id)   customer.ele('company_id').txt(data.customer.company_id);
  if (data.customer.badge_id)     customer.ele('badge_id').txt(data.customer.badge_id);

  if (data.customer.session_title) customer.ele('session_title').txt(data.customer.session_title);

  const pd = data.payment_due || data.customer.payment_due;
  const rawStatus = pd?.status || 'unpaid';
  const normalizedStatus = rawStatus === 'pending' ? 'unpaid' : rawStatus;
  const paymentDue = customer.ele('payment_due');
  paymentDue.ele('amount').att('currency', 'eur').txt(String(pd?.amount || '0.00'));
  paymentDue.ele('status').txt(normalizedStatus);

  return root.doc().end({ prettyPrint: true, indent: '  ' });
}

  async sendNewRegistrationToKassa(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const xmlPayload = this.buildNewRegistrationForKassaXml(data);
      this._validate(xmlPayload, 'new_registration');
      const queue = 'kassa.incoming';
      await this.channel.assertQueue(queue, { durable: true, arguments: { 'x-dead-letter-exchange': 'kassa.dlx', 'x-dead-letter-routing-key': 'kassa.incoming.dlq' } });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`New registration forwarded to queue "${queue}"`);
      await this._logOutbound('new_registration', queue, data.correlation_id);
      return { success: true, queue, payload: xmlPayload };
    } catch (error) {
      console.log(`Failed to send new registration to Kassa: ${error}`);
      throw error;
    }
  }

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

    const body = root.ele('body');
    body.ele('identity_uuid').txt(data.user_id || data.identity_uuid || '');
    body.ele('email').txt(data.email);
    if (data.date_of_birth) body.ele('date_of_birth').txt(data.date_of_birth);

    const contact = body.ele('contact');
    contact.ele('first_name').txt(data.first_name || '');
    contact.ele('last_name').txt(data.last_name || '');

    if (data.type)         body.ele('type').txt(data.type);
    if (data.company_name) body.ele('company_name').txt(data.company_name);
    if (data.vat_number)   body.ele('vat_number').txt(data.vat_number);
    if (data.company_id)   body.ele('company_id').txt(data.company_id);

    if (data.payment_due) {
      const paymentDue = body.ele('payment_due');
      paymentDue.ele('amount').att('currency', 'eur').txt(String(data.payment_due.amount || '0.00'));
      paymentDue.ele('status').txt(data.payment_due.status || 'unpaid');
    }

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  async sendProfileUpdateToKassa(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const xmlPayload = this.buildProfileUpdateXml(data);
      this._validate(xmlPayload, 'profile_update');
      const queue = 'kassa.incoming';
      await this.channel.assertQueue(queue, { durable: true, arguments: { 'x-dead-letter-exchange': 'kassa.dlx', 'x-dead-letter-routing-key': 'kassa.incoming.dlq' } });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`Profile update forwarded to queue "${queue}"`);
      await this._logOutbound('profile_update', queue, 'N/A');
      return { success: true, queue, payload: xmlPayload };
    } catch (error) {
      console.log(`Failed to send profile update to Kassa: ${error}`);
      throw error;
    }
  }

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

    const body = root.ele('body');
    body.ele('identity_uuid').txt(data.user_id || data.identity_uuid || '');
    body.ele('session_id').txt(data.session_id);
    if (data.reason) body.ele('reason').txt(data.reason);

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  async sendCancelRegistrationToKassa(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const xmlPayload = this.buildCancelRegistrationXml(data);
      this._validate(xmlPayload, 'cancel_registration');
      const queue = 'kassa.incoming';
      await this.channel.assertQueue(queue, { durable: true, arguments: { 'x-dead-letter-exchange': 'kassa.dlx', 'x-dead-letter-routing-key': 'kassa.incoming.dlq' } });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`Cancel registration forwarded to queue "${queue}"`);
      await this._logOutbound('cancel_registration', queue, 'N/A');
      return { success: true, queue, payload: xmlPayload };
    } catch (error) {
      console.log(`Failed to send cancel registration to Kassa: ${error}`);
      throw error;
    }
  }

  async sendCancelRegistrationToPlanning(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const xmlPayload = this.buildCancelRegistrationXml(data);
      this._validate(xmlPayload, 'cancel_registration');
      const exchange = 'calendar.exchange';
      const routingKey = 'crm.to.planning.cancel_registration';
      await this.channel.assertExchange(exchange, 'topic', { durable: true });
      const ok = this.channel.publish(exchange, routingKey, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for exchange "${exchange}"`);
      console.log(`Cancel registration forwarded to Planning via "${exchange}" [${routingKey}]`);
      await this._logOutbound('cancel_registration', exchange, 'N/A');
      return { success: true, exchange, payload: xmlPayload };
    } catch (error) {
      console.log(`Failed to send cancel registration to Planning: ${error}`);
      throw error;
    }
  }

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
  body.ele('identity_uuid').txt(data.master_uuid || data.user_id || data.identity_uuid || '');

  const invoiceData = body.ele('invoice_data');
  const contact = invoiceData.ele('contact');
  contact.ele('first_name').txt(data.customer?.first_name || '');
  contact.ele('last_name').txt(data.customer?.last_name || '');

  invoiceData.ele('email').txt(data.customer?.email || '');

  const address = invoiceData.ele('address');
  address.ele('street').txt(data.address?.street || '');
  address.ele('number').txt(data.address?.number || '');
  address.ele('postal_code').txt(data.address?.postal_code || '');
  address.ele('city').txt(data.address?.city || '');
  address.ele('country').txt(data.address?.country || '');

  if (data.customer?.company_name) invoiceData.ele('company_name').txt(data.customer.company_name);
  if (data.customer?.vat_number)   invoiceData.ele('vat_number').txt(data.customer.vat_number);
  
  return root.doc().end({ prettyPrint: true, indent: '  ' });
}

  async sendInvoiceRequest(data) {
    if (!this.channel) {
      throw new Error('CRM Sender not initialized. Call init() first.');
    }
    try {
      const xmlPayload = this.buildInvoiceRequestXml(data);
      this._validate(xmlPayload, 'invoice_request');
      const queue = 'facturatie.incoming';
      await this.channel.assertQueue(queue, { durable: true });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`Invoice request sent to queue "${queue}"`);
      await this._logOutbound('invoice_request', queue, data.correlation_id);
      return { success: true, queue, payload: xmlPayload };
    } catch (error) {
      console.log(`Failed to send invoice request: ${error}`);
      throw error;
    }
  }

  buildWalletLeaseGrantXml(data) {
    const messageId = uuidv4();
    const timestamp = new Date().toISOString();

    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(messageId);
    header.ele('timestamp').txt(timestamp);
    header.ele('source').txt('crm');
    header.ele('type').txt('wallet_lease_grant');
    header.ele('version').txt('2.0');
    header.ele('correlation_id').txt(data.correlation_id);

    const body = root.ele('body');
    body.ele('identity_uuid').txt(data.identity_uuid);
    body.ele('current_balance', { currency: 'eur' })
      .txt(Number(data.current_balance).toFixed(2));
      
    body.ele('lease_id').txt(data.leaseId || `LSE-${Date.now()}`);

    return root.doc().end({ prettyPrint: true, indent: '  ' });
}

async sendWalletLeaseGrant(data) {
  if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
  
  try {
    const xmlPayload = this.buildWalletLeaseGrantXml(data);
    this._validate(xmlPayload, 'wallet_lease_grant');
    const queue = 'kassa.incoming';
    await this.channel.assertQueue(queue, {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': 'kassa.dlx',
        'x-dead-letter-routing-key': 'kassa.incoming.dlq',
      },
    });
    const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
      contentType: 'application/xml',
      deliveryMode: 2,
    });
    if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
    console.log(`[sender] Wallet Lease Grant verstuurd voor ${data.identity_uuid}`);
    await this._logOutbound('wallet_lease_grant', queue, data.correlation_id);
    return { success: true, payload: xmlPayload };
  } catch (error) {
    console.error(`[sender] Failed to send wallet lease grant: ${error.message}`);
    throw error;
  }
}

  buildWalletRemoteTopupXml(data) {
    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(uuidv4());
    header.ele('timestamp').txt(new Date().toISOString());
    header.ele('source').txt('crm');
    header.ele('type').txt('wallet_remote_topup');
    header.ele('version').txt('2.0');
    header.ele('correlation_id').txt(data.correlation_id || uuidv4());

    const body = root.ele('body');
    body.ele('identity_uuid').txt(data.identity_uuid);
    body.ele('add_amount', { currency: 'eur' }).txt(Number(data.add_amount || 0).toFixed(2));
    body.ele('reason').txt(data.reason || 'online_topup');

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  async sendWalletRemoteTopup(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const xmlPayload = this.buildWalletRemoteTopupXml(data);
      this._validate(xmlPayload, 'wallet_remote_topup');

      const queue = 'kassa.incoming';
      await this.channel.assertQueue(queue, {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': 'kassa.dlx',
          'x-dead-letter-routing-key': 'kassa.incoming.dlq',
        },
      });

      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });

      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`[sender] Wallet remote topup sent to Kassa for ${data.identity_uuid}: +€${data.add_amount}`);
      await this._logOutbound('wallet_remote_topup', queue, data.correlation_id);
      return { success: true, queue, payload: xmlPayload };
    } catch (error) {
      console.error(`[sender] Failed to send wallet remote topup: ${error.message}`);
      throw error;
    }
  }

  async sendConsumptionOrderToFacturatie(xml) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      this._validate(xml, 'consumption_order');
      const queue = 'facturatie.incoming';
      await this.channel.assertQueue(queue, { durable: true });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xml), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`Consumption order forwarded to queue "${queue}"`);
      await this._logOutbound('consumption_order', queue, 'PASSTHROUGH');
      return { success: true, queue, payload: xml };
    } catch (error) {
      console.log(`Failed to forward consumption order to Facturatie: ${error}`);
      throw error;
    }
  }

  buildMailingSendXml(data) {
    const messageId = uuidv4();
    const timestamp = new Date().toISOString();
    const mailing = data.mailing || {};

    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(messageId);
    header.ele('timestamp').txt(timestamp);
    header.ele('source').txt('crm');
    header.ele('type').txt('send_mailing');
    header.ele('version').txt('2.0');
    const correlationId = data.correlation_id || uuidv4();
    header.ele('correlation_id').txt(correlationId);

    const body = root.ele('body');
    body.ele('campaign_id').txt(mailing.campaign_id || data.campaign_id || '');
    body.ele('subject').txt(mailing.subject || data.subject || '');
    if (mailing.template_id || data.template_id) {
      body.ele('template_id').txt(mailing.template_id || data.template_id);
    }
    body.ele('mail_type').txt(mailing.mail_type || data.mail_type || 'general_announcement');

    const recipients = body.ele('recipients');
    for (const r of (data.recipients || [])) {
      const recipientElem = recipients.ele('recipient');
      recipientElem.ele('email').txt(r.email);
      recipientElem.ele('identity_uuid').txt(r.identity_uuid || r.user_id || '');
      const contact = recipientElem.ele('contact');
      contact.ele('first_name').txt(r.first_name || r.contact?.first_name || '');
      contact.ele('last_name').txt(r.last_name || r.contact?.last_name || '');
    }

    if (data.template_data) {
      const templateStr = typeof data.template_data === 'object'
        ? JSON.stringify(data.template_data)
        : data.template_data;
      body.ele('template_data').txt(templateStr);
    }
    if (data.body_html) body.ele('body_html').txt(data.body_html);
    if (data.attachment) {
      const att = body.ele('attachment');
      att.ele('filename').txt(data.attachment.filename || '');
      att.ele('content_type').txt(data.attachment.content_type || '');
      att.ele('base64_data').txt(data.attachment.base64_data || '');
    }

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  async sendMailingSend(data) {
    if (!this.channel) {
      throw new Error('CRM Sender not initialized. Call init() first.');
    }
    try {
      const xmlPayload = this.buildMailingSendXml(data);
      this._validate(xmlPayload, 'send_mailing');
      const queue = 'crm.to.mailing';
      await this.channel.assertQueue(queue, { durable: true });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`Mailing send request sent to queue "${queue}"`);
      await this._logOutbound('send_mailing', queue, data.correlation_id);
      return { success: true, queue, payload: xmlPayload };
    } catch (error) {
      console.log(`Failed to send mailing send request: ${error}`);
      throw error;
    }
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

  async sendLog(data) {
    if (!this.channel) {
      console.error('[sender] sendLog failed: Channel not initialized');
      return;
    }
    try {
      const xmlPayload = this.buildLogXml(data);
      const queue = 'logs';
      await this.channel.assertQueue(queue, { durable: true });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      return { success: true, queue, payload: xmlPayload };
    } catch (error) {
      console.error(`[sender] sendLog error: ${error.message}`);
      return { success: false, error: error.message };
    }
  }
 buildPaymentRegisteredXml(data) {
  const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

  const header = root.ele('header');
  header.ele('message_id').txt(uuidv4());
  header.ele('timestamp').txt(new Date().toISOString());
  header.ele('source').txt('facturatie');
  header.ele('type').txt('payment_registered');
  header.ele('version').txt('2.0');
  if (data.correlation_id) header.ele('correlation_id').txt(data.correlation_id);

  const body = root.ele('body');
  body.ele('identity_uuid').txt(data.identity_uuid || '');

  const invoice = body.ele('invoice');
  invoice.ele('id').txt(data.invoice_id || '');
  invoice.ele('amount_paid', { currency: 'eur' }).txt(String(data.amount_paid || '0.00'));
  invoice.ele('status').txt(data.status || 'paid');

  body.ele('payment_context').txt(data.payment_context || 'consumption');

  if (data.transaction_id && data.payment_method) {
    const trans = body.ele('transaction');
    trans.ele('id').txt(data.transaction_id);
    trans.ele('payment_method').txt(data.payment_method);
  }
  return root.doc().end({ prettyPrint: true, indent: '  ' });
}
async sendPaymentRegisteredToFrontend(data) {
  if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
  try {
    const xmlPayload = this.buildPaymentRegisteredXml(data);
    this._validate(xmlPayload, 'payment_registered');
    const queue = 'frontend.incoming';
    await this.channel.assertQueue(queue, { durable: true });
    const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
      contentType: 'application/xml',
      deliveryMode: 2,
    });
    if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
    console.log(`Payment registered forwarded to Frontend queue "${queue}"`);
    await this._logOutbound('payment_registered', queue, data.correlation_id);
    return { success: true, queue, payload: xmlPayload };
  } catch (error) {
    console.log(`Failed to forward payment to Frontend: ${error}`);
    throw error;
  }
}

async sendPaymentRegisteredToFacturatie(data) {
  if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
  try {
    const xmlPayload = this.buildPaymentRegisteredXml(data);
    this._validate(xmlPayload, 'payment_registered');
    const queue = 'facturatie.incoming';
    await this.channel.assertQueue(queue, { durable: true });
    const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
      contentType: 'application/xml',
      deliveryMode: 2,
    });
    if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
    console.log(`Payment registered forwarded to Facturatie queue "${queue}"`);
    await this._logOutbound('payment_registered', queue, data.correlation_id);
    return { success: true, queue, payload: xmlPayload };
  } catch (error) {
    console.log(`Failed to forward payment to Facturatie: ${error}`);
    throw error;
  }
}

  async sendEventEndedToFacturatie(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const messageId = uuidv4();
      const timestamp = new Date().toISOString();

      const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');
      const header = root.ele('header');
      header.ele('message_id').txt(messageId);
      header.ele('timestamp').txt(timestamp);
      header.ele('source').txt('frontend');
      header.ele('type').txt('event_ended');
      header.ele('version').txt('2.0');

      const body = root.ele('body');
      body.ele('session_id').txt(data.session_id);
      body.ele('ended_at').txt(data.ended_at || timestamp);

      const xmlPayload = root.doc().end({ prettyPrint: true, indent: '  ' });
      this._validate(xmlPayload, 'event_ended');
      const queue = 'facturatie.incoming';
      await this.channel.assertQueue(queue, { durable: true });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`Event ended notification sent to Facturatie queue "${queue}"`);
      await this._logOutbound('event_ended', queue, messageId);
    } catch (error) {
      console.log(`Failed to send event ended to Facturatie: ${error}`);
    }
  }

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
      const correlationId = data.correlation_id || uuidv4();
      header.ele('correlation_id').txt(correlationId);

      const body = root.ele('body');
      const customer = body.ele('customer');
      customer.ele('identity_uuid').txt(data.master_uuid || '');
      customer.ele('email').txt(data.customer.email);
      customer.ele('date_of_birth').txt(data.customer.date_of_birth || '');
      const contact = customer.ele('contact');
      contact.ele('first_name').txt(data.customer.first_name || '');
      contact.ele('last_name').txt(data.customer.last_name || '');
      customer.ele('type').txt(data.customer.type || 'private');
      if (data.customer.company_name) customer.ele('company_name').txt(data.customer.company_name);
      if (data.customer.vat_number)   customer.ele('vat_number').txt(data.customer.vat_number);
      if (data.customer.company_id)   customer.ele('company_id').txt(data.customer.company_id);
      if (data.customer.badge_id)     customer.ele('badge_id').txt(data.customer.badge_id);
      const paymentDue = customer.ele('payment_due');
      paymentDue.ele('amount').att('currency', 'eur').txt(String(data.payment_due.amount || '0.00'));
      paymentDue.ele('status').txt(data.payment_due.status || 'unpaid');

      const xmlPayload = root.doc().end({ prettyPrint: true, indent: '  ' });
      this._validate(xmlPayload, 'new_registration');
      const queue = 'facturatie.incoming';
      await this.channel.assertQueue(queue, { durable: true });
      const ok = this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for queue "${queue}"`);
      console.log(`New registration forwarded to Facturatie queue "${queue}"`);
      await this._logOutbound('new_registration', queue, correlationId);
    } catch (error) {
      console.log(`Failed to forward registration to Facturatie: ${error}`);
    }
  }

  async sendSessionRegistrationConfirmed(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    try {
      const messageId = uuidv4();
      const timestamp = new Date().toISOString();

      const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');
      const header = root.ele('header');
      header.ele('message_id').txt(messageId);
      header.ele('timestamp').txt(timestamp);
      header.ele('source').txt('crm');
      header.ele('type').txt('session_registration_confirmed');
      header.ele('version').txt('2.0');
      const correlationId = data.correlation_id || uuidv4();
      header.ele('correlation_id').txt(correlationId);

      const body = root.ele('body');
      body.ele('session_id').txt(data.session_id);
      body.ele('identity_uuid').txt(data.identity_uuid);

      const xmlPayload = root.doc().end({ prettyPrint: true, indent: '  ' });
      this._validate(xmlPayload, 'session_registration_confirmed');
      const exchange = 'calendar.exchange';
      const routingKey = 'crm.to.planning.session_registration_confirmed';

      await this.channel.assertExchange(exchange, 'topic', { durable: true });
      const ok = this.channel.publish(exchange, routingKey, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      if (!ok) console.log(`[sender] Warning: write buffer full for exchange "${exchange}"`);
      console.log(`Session registration confirmation sent to Planning via "${exchange}" [${routingKey}]`);
      await this._logOutbound('session_registration_confirmed', exchange, correlationId);
    } catch (error) {
      console.log(`Failed to send session registration confirmation: ${error}`);
    }
  }

  buildUserUnregisteredXml(data) {
    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('type').txt('user.unregistered');
    header.ele('source').txt('crm');
    header.ele('version').txt('1.0');
    header.ele('timestamp').txt(new Date().toISOString());

    const body = root.ele('body');
    body.ele('identity_uuid').txt(data.identity_uuid || data.master_uuid);
    if (data.email) body.ele('email').txt(data.email);
    if (data.reason) body.ele('reason').txt(data.reason);

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  async sendUserUnregisteredFanout(data) {
    if (!this.channel) throw new Error('CRM Sender not initialized. Call init() first.');
    const exchange = USER_UNREGISTERED_EXCHANGE;
    const queues = ['crm.salesforce', 'planning.outlook', 'mailing.sendgrid'];

    await this.channel.assertExchange(exchange, 'fanout', { durable: true });
    for (const q of queues) {
      await this.channel.bindQueue(q, exchange, '');
    }

    const xmlPayload = this.buildUserUnregisteredXml(data);
    this._validate(xmlPayload, 'user.unregistered');
    const ok = this.channel.publish(exchange, '', Buffer.from(xmlPayload), {
      contentType: 'application/xml',
      deliveryMode: 2,
    });
    if (!ok) console.log(`[sender] Warning: write buffer full for exchange "${exchange}"`);
    console.log(`User unregistered broadcast via exchange "${exchange}"`);
    return { success: true, exchange, queues };
  }

  async close() {
    try {
      if (this.connection) await this.connection.close();
      console.log('CRM Sender connection closed');
    } catch (error) {
      /* already closed */
    }
  }
}

module.exports = CRMSender;
