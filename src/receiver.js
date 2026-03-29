'use strict';

require('dotenv').config();
const amqp = require('amqplib');
const { XMLParser } = require('fast-xml-parser');
const SFConnection = require('./sfConnection');

const QUEUE_NAME = 'crm.incoming';
const DEAD_LETTER_QUEUE = 'crm.dead-letter';

const MESSAGE_TYPES = {
  NEW_REGISTRATION: 'new_registration',
  PAYMENT_REGISTERED: 'payment_registered',
  BADGE_SCANNED: 'badge_scanned',
  SESSION_UPDATE: 'session_update',
  INVOICE_STATUS: 'invoice_status',
  MAILING_STATUS: 'mailing_status',
};

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '',
  textNodeName: '#text',
  parseTagValue: false,
  parseAttributeValue: false,
});

class ReceiverV2 {
  constructor() {
    this.connection = null;
    this.channel = null;
    this.sf = new SFConnection();
    this.running = true;
  }

  async start() {
    await this.sf.init();
    await this.connectRabbitMQ();
  }

  async connectRabbitMQ() {
    const maxRetries = 5;
    let retryCount = 0;

    while (retryCount < maxRetries && this.running) {
      try {
        this.connection = await amqp.connect(
          process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost/'
        );
        this.channel = await this.connection.createChannel();

        await this.channel.assertQueue(QUEUE_NAME, { durable: true });
        await this.channel.assertQueue(DEAD_LETTER_QUEUE, { durable: true });
        await this.channel.prefetch(1);

        this.channel.consume(QUEUE_NAME, async (msg) => {
          if (msg) {
            try {
              await this.handleMessage(msg);
            } catch (err) {
              console.log(`[receiver] Unhandled error in message handler: ${err}`);
            }
          }
        }, { noAck: false });

        console.log(`[receiver] Connected to RabbitMQ, listening on queue: ${QUEUE_NAME}`);

        await new Promise((resolve, reject) => {
          this.connection.on('error', reject);
          this.connection.on('close', resolve);
        });
      } catch (err) {
        retryCount++;
        console.log(`[receiver] RabbitMQ connection error: ${err}`);
        if (retryCount < maxRetries) {
          await new Promise(r => setTimeout(r, 5000));
        }
      }
    }
  }

  validateXmlMessage(parsed) {
    if (!parsed.message) {
      return [false, 'Missing message root element'];
    }
    const msg = parsed.message;
    const header = msg.header;
    if (!header) {
      return [false, 'Missing header element'];
    }

    const requiredFields = ['message_id', 'version', 'type', 'timestamp', 'source'];
    const missingFields = requiredFields.filter(f => header[f] === undefined || header[f] === null);
    if (missingFields.length > 0) {
      return [false, `Missing required header fields: ${missingFields.join(', ')}`];
    }

    if (String(header.version) !== '2.0') {
      return [false, `Invalid version: expected 2.0, got ${header.version}`];
    }

    const validTypes = Object.values(MESSAGE_TYPES);
    if (!validTypes.includes(header.type)) {
      return [false, `Invalid message type: ${header.type}`];
    }

    return [true, null];
  }

  async handleMessage(msg) {
    try {
      const xmlContent = msg.content.toString('utf8');
      console.log(`[receiver] Received message: ${msg.fields.deliveryTag}`);

      let parsed;
      try {
        parsed = parser.parse(xmlContent);
      } catch (err) {
        console.log(`[receiver] XML parse error: ${err}`);
        this.sendToDeadLetter(msg.content, 'XML_PARSE_ERROR');
        this.channel.nack(msg, false, false);
        return;
      }

      const [valid, error] = this.validateXmlMessage(parsed);
      if (!valid) {
        console.log(`[receiver] Validation error: ${error}`);
        this.sendToDeadLetter(msg.content, `VALIDATION_ERROR: ${error}`);
        this.channel.nack(msg, false, false);
        return;
      }

      const header = parsed.message.header;
      const body = parsed.message.body;
      const messageId = header.message_id;
      const messageType = header.type;

      console.log(`[receiver] Processing message type: ${messageType}, ID: ${messageId}`);
      await this.routeMessage(header, body);

      this.channel.ack(msg);
      console.log(`[receiver] Message processed successfully: ${messageId}`);
    } catch (err) {
      console.log(`[receiver] Unexpected error: ${err}`);
      this.channel.nack(msg, false, false);
    }
  }

  async routeMessage(header, body) {
    const msgType = header.type;
    const handlers = {
      [MESSAGE_TYPES.NEW_REGISTRATION]: () => this.handleNewRegistration(header, body),
      [MESSAGE_TYPES.PAYMENT_REGISTERED]: () => this.handlePaymentRegistered(header, body),
      [MESSAGE_TYPES.BADGE_SCANNED]: () => this.handleBadgeScanned(header, body),
      [MESSAGE_TYPES.SESSION_UPDATE]: () => this.handleSessionUpdate(header, body),
      [MESSAGE_TYPES.INVOICE_STATUS]: () => this.handleInvoiceStatus(header, body),
      [MESSAGE_TYPES.MAILING_STATUS]: () => this.handleMailingStatus(header, body),
    };
    const handler = handlers[msgType];
    if (handler) {
      await handler();
    } else {
      console.log(`[receiver] Unknown message type: ${msgType}`);
    }
  }

  async _findContactByEmail(email) {
    const result = await this.sf.apiCall(
      conn => conn.query(`SELECT Id FROM Contact WHERE Email = '${email.replace(/'/g, "\\'")}' LIMIT 1`)
    );
    if (result && result.records && result.records.length > 0) {
      return result.records[0].Id;
    }
    return null;
  }

  async handleNewRegistration(header, body) {
    try {
      const customer = body ? body.customer : null;
      if (!customer) {
        console.log('[receiver] Missing customer element in body');
        return;
      }

      const address = customer.address || null;
      const regFee = customer.registration_fee || null;

      const street = ReceiverV2.getElementText(address, 'street');
      const number = ReceiverV2.getElementText(address, 'number');
      const mailingStreet = street ? `${street} ${number || ''}`.trim() : null;

      const descriptionParts = [
        `user_id: ${ReceiverV2.getElementText(customer, 'id')}`,
        `type_user: ${ReceiverV2.getElementText(customer, 'type_user')}`,
        `badge_id: ${ReceiverV2.getElementText(customer, 'badge_id')}`,
        `registration_date: ${ReceiverV2.getElementText(customer, 'registration_date')}`,
      ];
      if (regFee) {
        const amountVal = regFee.amount;
        const amount = typeof amountVal === 'object' ? amountVal['#text'] : amountVal;
        const currency = typeof amountVal === 'object' ? (amountVal.currency || 'eur') : 'eur';
        const paid = ReceiverV2.getElementText(regFee, 'paid');
        descriptionParts.push(`registration_fee: ${amount} ${currency} - paid: ${paid}`);
      }

      const rawContactData = {
        FirstName: ReceiverV2.getElementText(customer, 'first_name'),
        LastName: ReceiverV2.getElementText(customer, 'last_name'),
        Email: ReceiverV2.getElementText(customer, 'email'),
        Phone: ReceiverV2.getElementText(customer, 'phone'),
        Birthdate: ReceiverV2.getElementText(customer, 'date_of_birth'),
        MailingStreet: mailingStreet,
        MailingCity: address ? ReceiverV2.getElementText(address, 'city') : null,
        MailingPostalCode: address ? ReceiverV2.getElementText(address, 'postal_code') : null,
        MailingCountryCode: address
          ? (ReceiverV2.getElementText(address, 'country') || '').toUpperCase() || null
          : null,
        Description: descriptionParts.join('\n'),
      };
      const contactData = Object.fromEntries(
        Object.entries(rawContactData).filter(([, v]) => v !== null && v !== '')
      );

      if (!this.sf.isConnected) {
        console.log(`[receiver] DRY RUN: Would upsert Contact: ${JSON.stringify(contactData)}`);
        return;
      }

      const email = contactData.Email;
      const existingId = email ? await this._findContactByEmail(email) : null;

      let contactId;
      if (existingId) {
        await this.sf.apiCall(conn => conn.sobject('Contact').update({ Id: existingId, ...contactData }));
        contactId = existingId;
        console.log(`[receiver] Updated Contact: ${contactId}`);
      } else {
        const contactResult = await this.sf.apiCall(conn => conn.sobject('Contact').create(contactData));
        if (!contactResult) throw new Error('apiCall returned null creating Contact');
        contactId = contactResult.id;
        console.log(`[receiver] Created Contact: ${contactId}`);
      }

      const isCompanyLinked = ReceiverV2.getElementText(customer, 'is_company_linked');
      if (isCompanyLinked === 'true') {
        const btwNumber = ReceiverV2.getElementText(customer, 'btw_number');
        const rawAccountData = {
          Name: ReceiverV2.getElementText(customer, 'company_name'),
          Description: btwNumber ? `btw_number: ${btwNumber}` : null,
        };
        const accountData = Object.fromEntries(
          Object.entries(rawAccountData).filter(([, v]) => v !== null)
        );

        const accountResult = await this.sf.apiCall(conn => conn.sobject('Account').create(accountData));
        if (!accountResult) throw new Error('apiCall returned null creating Account');
        const accountId = accountResult.id;
        console.log(`[receiver] Created Account: ${accountId}`);

        await this.sf.apiCall(conn => conn.sobject('Contact').update({ Id: contactId, AccountId: accountId }));
        console.log('[receiver] Linked Contact to Account');
      }
    } catch (err) {
      console.log(`[receiver] Error in handleNewRegistration: ${err}`);
      throw err;
    }
  }

  async handlePaymentRegistered(header, body) {
    try {
      const invoice = body ? body.invoice : null;
      const transaction = body ? body.transaction : null;

      const taskData = {
        Subject: `Payment registered for invoice: ${ReceiverV2.getElementText(invoice, 'id')}`,
        Description: [
          `Payment Method: ${ReceiverV2.getElementText(transaction, 'payment_method')}`,
          `Amount Paid: ${ReceiverV2.getElementText(invoice, 'amount_paid')}`,
          `Due Date: ${ReceiverV2.getElementText(invoice, 'due_date')}`,
          `Status: ${ReceiverV2.getElementText(invoice, 'status')}`,
        ].join('\n'),
        Status: 'Completed',
        Type: 'Payment',
        ActivityDate: new Date().toISOString().split('T')[0],
      };

      if (!this.sf.isConnected) {
        console.log(`[receiver] DRY RUN: Would create Task: ${JSON.stringify(taskData)}`);
        return;
      }

      const email = ReceiverV2.getElementText(body, 'email');
      if (email) {
        const contactId = await this._findContactByEmail(email);
        if (contactId) taskData.WhoId = contactId;
      }

      const result = await this.sf.apiCall(conn => conn.sobject('Task').create(taskData));
      console.log(`[receiver] Created Task for payment: ${result?.id}`);
    } catch (err) {
      console.log(`[receiver] Error in handlePaymentRegistered: ${err}`);
      throw err;
    }
  }

  async handleBadgeScanned(header, body) {
    try {
      const taskData = {
        Subject: `Badge scanned: ${ReceiverV2.getElementText(body, 'badge_id')}`,
        Description: [
          `Scan Type: ${ReceiverV2.getElementText(body, 'scan_type')}`,
          `Location: ${ReceiverV2.getElementText(body, 'location')}`,
          `Email: ${ReceiverV2.getElementText(body, 'email')}`,
        ].join('\n'),
        Status: 'Completed',
        Type: 'Other',
        ActivityDate: new Date().toISOString().split('T')[0],
      };

      if (!this.sf.isConnected) {
        console.log(`[receiver] DRY RUN: Would create Task: ${JSON.stringify(taskData)}`);
        return;
      }

      const email = ReceiverV2.getElementText(body, 'email');
      if (email) {
        const contactId = await this._findContactByEmail(email);
        if (contactId) taskData.WhoId = contactId;
      }

      const result = await this.sf.apiCall(conn => conn.sobject('Task').create(taskData));
      console.log(`[receiver] Created Task for badge scan: ${result?.id}`);
    } catch (err) {
      console.log(`[receiver] Error in handleBadgeScanned: ${err}`);
      throw err;
    }
  }

  async handleSessionUpdate(header, body) {
    try {
      const taskData = {
        Subject: `Session update: ${ReceiverV2.getElementText(body, 'session_name')}`,
        Description: [
          `Speaker: ${ReceiverV2.getElementText(body, 'speaker')}`,
          `Start Time: ${ReceiverV2.getElementText(body, 'start_time')}`,
          `End Time: ${ReceiverV2.getElementText(body, 'end_time')}`,
          `Status: ${ReceiverV2.getElementText(body, 'status')}`,
        ].join('\n'),
        Status: 'Completed',
        Type: 'Other',
        ActivityDate: new Date().toISOString().split('T')[0],
      };

      if (!this.sf.isConnected) {
        console.log(`[receiver] DRY RUN: Would create Task: ${JSON.stringify(taskData)}`);
        return;
      }

      const result = await this.sf.apiCall(conn => conn.sobject('Task').create(taskData));
      console.log(`[receiver] Created Task for session update: ${result?.id}`);
    } catch (err) {
      console.log(`[receiver] Error in handleSessionUpdate: ${err}`);
      throw err;
    }
  }

  async handleInvoiceStatus(header, body) {
    try {
      const invoice = body ? body.invoice : null;

      const taskData = {
        Subject: `Invoice status update: ${ReceiverV2.getElementText(invoice, 'id')}`,
        Description: [
          `Status: ${ReceiverV2.getElementText(invoice, 'status')}`,
          `Amount Paid: ${ReceiverV2.getElementText(invoice, 'amount_paid')}`,
        ].join('\n'),
        Status: 'Completed',
        Type: 'Other',
        ActivityDate: new Date().toISOString().split('T')[0],
      };

      if (!this.sf.isConnected) {
        console.log(`[receiver] DRY RUN: Would create Task: ${JSON.stringify(taskData)}`);
        return;
      }

      const email = ReceiverV2.getElementText(body, 'email');
      if (email) {
        const contactId = await this._findContactByEmail(email);
        if (contactId) taskData.WhoId = contactId;
      }

      const result = await this.sf.apiCall(conn => conn.sobject('Task').create(taskData));
      console.log(`[receiver] Created Task for invoice status: ${result?.id}`);
    } catch (err) {
      console.log(`[receiver] Error in handleInvoiceStatus: ${err}`);
      throw err;
    }
  }

  async handleMailingStatus(header, body) {
    try {
      const taskData = {
        Subject: `Mailing status: ${ReceiverV2.getElementText(body, 'mailing_id')}`,
        Description: [
          `Status: ${ReceiverV2.getElementText(body, 'status')}`,
          `Delivered: ${ReceiverV2.getElementText(body, 'delivered')}`,
          `Bounced: ${ReceiverV2.getElementText(body, 'bounced')}`,
        ].join('\n'),
        Status: 'Completed',
        Type: 'Other',
        ActivityDate: new Date().toISOString().split('T')[0],
      };

      if (!this.sf.isConnected) {
        console.log(`[receiver] DRY RUN: Would create Task: ${JSON.stringify(taskData)}`);
        return;
      }

      const result = await this.sf.apiCall(conn => conn.sobject('Task').create(taskData));
      console.log(`[receiver] Created Task for mailing status: ${result?.id}`);
    } catch (err) {
      console.log(`[receiver] Error in handleMailingStatus: ${err}`);
      throw err;
    }
  }

  sendToDeadLetter(originalBody, reason) {
    try {
      const deadLetterMessage = JSON.stringify({
        original_message: originalBody.toString('utf8'),
        error_reason: reason,
        timestamp: new Date().toISOString(),
      });
      this.channel.sendToQueue(
        DEAD_LETTER_QUEUE,
        Buffer.from(deadLetterMessage),
        { deliveryMode: 2 }
      );
      console.log(`[receiver] Message sent to dead letter queue: ${reason}`);
    } catch (err) {
      console.log(`[receiver] Error sending to dead letter queue: ${err}`);
    }
  }

  static getElementText(parent, tagName) {
    if (!parent) return null;
    const val = parent[tagName];
    if (val === undefined || val === null) return null;
    if (typeof val === 'object' && '#text' in val) return String(val['#text']);
    if (typeof val === 'object') return null;
    return String(val);
  }

  async shutdown() {
    console.log('[receiver] Signal received, shutting down gracefully...');
    this.running = false;
    if (this.channel) await this.channel.close();
    if (this.connection) await this.connection.close();
    process.exit(0);
  }
}

async function main() {
  const receiver = new ReceiverV2();

  process.on('SIGINT', () => receiver.shutdown());
  process.on('SIGTERM', () => receiver.shutdown());

  try {
    await receiver.start();
  } catch (err) {
    console.log(`[receiver] Failed to start receiver: ${err}`);
    process.exit(1);
  }
}

main();
