'use strict';

require('dotenv').config();
const amqp = require('amqplib');
const { XMLParser } = require('fast-xml-parser');
const SFConnection = require('./sfConnection');
const CRMSender = require('./sender');
const SupabaseService = require('./supabaseClient');

const QUEUE_NAME = 'crm.incoming';
const KASSA_QUEUE = 'kassa.payments';
const DEAD_LETTER_QUEUE = 'crm.dead-letter';

const MESSAGE_TYPES = {
  NEW_REGISTRATION: 'new_registration',
  PAYMENT_REGISTERED: 'payment_registered',
  BADGE_SCANNED: 'badge_scanned',
  SESSION_UPDATE: 'session_update',
  INVOICE_STATUS: 'invoice_status',
  MAILING_STATUS: 'mailing_status',
  // Kassa → CRM (via kassa.payments)
  CONSUMPTION_ORDER: 'consumption_order',
  BADGE_ASSIGNED: 'badge_assigned',
  REFUND_PROCESSED: 'refund_processed',
  INVOICE_REQUEST: 'invoice_request',
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
    this.sender = new CRMSender();
    this.db = new SupabaseService();
    this.running = true;
  }

  async start() {
    await this.sf.init();
    await this.sender.init();
    this.db.init();
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
        await this.channel.assertQueue(KASSA_QUEUE, { durable: true });
        await this.channel.assertQueue(DEAD_LETTER_QUEUE, { durable: true });
        await this.channel.prefetch(1);

        const consume = async (msg) => {
          if (msg) {
            try {
              await this.handleMessage(msg);
            } catch (err) {
              console.log(`[receiver] Unhandled error in message handler: ${err}`);
            }
          }
        };

        this.channel.consume(QUEUE_NAME, consume, { noAck: false });
        this.channel.consume(KASSA_QUEUE, consume, { noAck: false });

        console.log(`[receiver] Connected to RabbitMQ, listening on: ${QUEUE_NAME}, ${KASSA_QUEUE}`);

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
      [MESSAGE_TYPES.CONSUMPTION_ORDER]: () => this.handleConsumptionOrder(header, body),
      [MESSAGE_TYPES.BADGE_ASSIGNED]: () => this.handleBadgeAssigned(header, body),
      [MESSAGE_TYPES.REFUND_PROCESSED]: () => this.handleRefundProcessed(header, body),
      [MESSAGE_TYPES.INVOICE_REQUEST]: () => this.handleInvoiceRequestFromKassa(header, body),
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

      // Upsert person into Supabase
      const externalUserId = ReceiverV2.getElementText(customer, 'id');
      const personPayload = {
        external_user_id: externalUserId,
        first_name: contactData.FirstName,
        last_name: contactData.LastName,
        email: contactData.Email,
        phone: contactData.Phone || null,
        date_of_birth: contactData.Birthdate || null,
        street: address ? ReceiverV2.getElementText(address, 'street') : null,
        house_number: address ? ReceiverV2.getElementText(address, 'number') : null,
        postal_code: address ? ReceiverV2.getElementText(address, 'postal_code') : null,
        city: address ? ReceiverV2.getElementText(address, 'city') : null,
        country: address ? (ReceiverV2.getElementText(address, 'country') || 'Belgium') : 'Belgium',
        person_type: ReceiverV2.getElementText(customer, 'type_user') || 'private',
        salesforce_contact_id: contactId,
        updated_at: new Date().toISOString(),
      };
      const sbPersonId = await this.db.upsertPerson(
        Object.fromEntries(Object.entries(personPayload).filter(([, v]) => v !== null))
      );
      if (sbPersonId) {
        console.log(`[supabase] Upserted person: ${sbPersonId}`);
        await this.db.syncSalesforceStatus(sbPersonId, 'synced');
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

        // Upsert company into Supabase
        const companyPayload = {
          name: rawAccountData.Name,
          vat_number: btwNumber || null,
          salesforce_account_id: accountId,
          updated_at: new Date().toISOString(),
        };
        const sbCompanyId = await this.db.upsertCompany(
          Object.fromEntries(Object.entries(companyPayload).filter(([, v]) => v !== null))
        );
        if (sbCompanyId) console.log(`[supabase] Upserted company: ${sbCompanyId}`);
      }

      // Forward new_registration to kassa.incoming in Kassa's required format
      const userId = ReceiverV2.getElementText(customer, 'id');
      const typeUser = ReceiverV2.getElementText(customer, 'type_user');
      const dateOfBirth = ReceiverV2.getElementText(customer, 'date_of_birth');
      const age = dateOfBirth
        ? new Date().getFullYear() - new Date(dateOfBirth).getFullYear()
        : null;

      if (userId && age !== null) {
        const kassaPayload = {
          customer: {
            email: ReceiverV2.getElementText(customer, 'email'),
            first_name: ReceiverV2.getElementText(customer, 'first_name'),
            last_name: ReceiverV2.getElementText(customer, 'last_name'),
            user_id: userId,
            type: isCompanyLinked === 'true' ? 'company' : (typeUser || 'private'),
            company_name: ReceiverV2.getElementText(customer, 'company_name'),
            vat_number: ReceiverV2.getElementText(customer, 'btw_number'),
            age,
          },
          payment_due: {
            amount: regFee ? (typeof regFee.amount === 'object' ? regFee.amount['#text'] : regFee.amount) : '0',
            status: regFee && ReceiverV2.getElementText(regFee, 'paid') === 'true' ? 'paid' : 'unpaid',
          },
          correlation_id: header.message_id,
        };
        await this.sender.sendNewRegistrationToKassa(kassaPayload);
      } else {
        console.log('[receiver] Skipping Kassa forward: missing user_id or date_of_birth for age calculation');
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
      const paymentContext = ReceiverV2.getElementText(body, 'payment_context') || 'unknown';
      const userId = ReceiverV2.getElementText(body, 'user_id');

      const amountVal = invoice ? invoice.amount_paid : null;
      const amountPaid = typeof amountVal === 'object' ? amountVal['#text'] : amountVal;

      const taskData = {
        Subject: `Payment registered [${paymentContext}] invoice: ${ReceiverV2.getElementText(invoice, 'id') || 'N/A'}`,
        Description: [
          `Context: ${paymentContext}`,
          `Payment Method: ${ReceiverV2.getElementText(transaction, 'payment_method')}`,
          `Amount Paid: ${amountPaid}`,
          `Due Date: ${ReceiverV2.getElementText(invoice, 'due_date')}`,
          `Status: ${ReceiverV2.getElementText(invoice, 'status')}`,
          userId ? `User ID: ${userId}` : null,
        ].filter(Boolean).join('\n'),
        Status: 'Completed',
        Type: 'Payment',
        ActivityDate: new Date().toISOString().split('T')[0],
      };

      if (!this.sf.isConnected) {
        console.log(`[receiver] DRY RUN: Would create Task: ${JSON.stringify(taskData)}`);
        return;
      }

      // Try to link to contact via email (crm.incoming) or user_id lookup (kassa.payments)
      const email = ReceiverV2.getElementText(body, 'email');
      if (email) {
        const contactId = await this._findContactByEmail(email);
        if (contactId) taskData.WhoId = contactId;
      } else if (userId) {
        const contactId = await this._findContactByUserId(userId);
        if (contactId) taskData.WhoId = contactId;
      }

      const result = await this.sf.apiCall(conn => conn.sobject('Task').create(taskData));
      console.log(`[receiver] Created Task for payment [${paymentContext}]: ${result?.id}`);

      // Insert payment into Supabase
      let eventAttendeeId = null;
      if (userId) {
        const personId = await this.db.findPersonByExternalId(userId);
        if (personId) eventAttendeeId = await this.db.findEventAttendeeByPersonId(personId);
      }
      const paymentPayload = {
        amount: parseFloat(amountPaid) || 0,
        payment_type: paymentContext === 'consumption' ? 'consumption' : 'registration',
        status: 'completed',
        payment_method: ReceiverV2.getElementText(transaction, 'payment_method') || null,
        paid_at: new Date().toISOString(),
      };
      if (eventAttendeeId) paymentPayload.event_attendee_id = eventAttendeeId;
      const sbPaymentId = await this.db.insertPayment(paymentPayload);
      if (sbPaymentId) console.log(`[supabase] Inserted payment: ${sbPaymentId}`);
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

      // Record check-in time in Supabase
      const scanEmail = ReceiverV2.getElementText(body, 'email');
      if (scanEmail) {
        const { data: person } = await (this.db.isConnected
          ? this.db.client.from('people').select('id').eq('email', scanEmail).maybeSingle()
          : Promise.resolve({ data: null }));
        if (person) await this.db.updateEventAttendeeCheckIn(person.id);
      }
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

  async _findContactByUserId(userId) {
    const safeId = userId.replace(/'/g, "\\'");
    const result = await this.sf.apiCall(
      conn => conn.query(`SELECT Id FROM Contact WHERE user_id__c = '${safeId}' LIMIT 1`)
    );
    if (result && result.records && result.records.length > 0) {
      return result.records[0].Id;
    }
    return null;
  }

  async handleConsumptionOrder(header, body) {
    try {
      const isAnonymous = ReceiverV2.getElementText(body, 'is_anonymous') === 'true';
      const customer = body ? body.customer : null;
      const items = body ? body.items : null;

      const itemList = items
        ? (Array.isArray(items.item) ? items.item : [items.item]).filter(Boolean)
        : [];

      const itemSummary = itemList.map(item => {
        const unitPriceVal = item.unit_price;
        const price = typeof unitPriceVal === 'object' ? unitPriceVal['#text'] : unitPriceVal;
        const currency = typeof unitPriceVal === 'object' ? (unitPriceVal.currency || 'eur') : 'eur';
        return `${item.quantity}x ${item.description} @ ${price} ${currency}`;
      }).join(', ');

      const taskData = {
        Subject: `Consumption order${isAnonymous ? ' (anonymous)' : ''}: ${itemList.length} item(s)`,
        Description: [
          `Anonymous: ${isAnonymous}`,
          `Items: ${itemSummary}`,
          customer ? `Customer email: ${ReceiverV2.getElementText(customer, 'email')}` : null,
          customer ? `User ID: ${ReceiverV2.getElementText(customer, 'user_id')}` : null,
        ].filter(Boolean).join('\n'),
        Status: 'Completed',
        Type: 'Other',
        ActivityDate: new Date().toISOString().split('T')[0],
      };

      if (!this.sf.isConnected) {
        console.log(`[receiver] DRY RUN: Would create Task: ${JSON.stringify(taskData)}`);
        return;
      }

      if (!isAnonymous && customer) {
        const email = ReceiverV2.getElementText(customer, 'email');
        const userId = ReceiverV2.getElementText(customer, 'user_id');
        const contactId = email
          ? await this._findContactByEmail(email)
          : (userId ? await this._findContactByUserId(userId) : null);
        if (contactId) taskData.WhoId = contactId;
      }

      const result = await this.sf.apiCall(conn => conn.sobject('Task').create(taskData));
      console.log(`[receiver] Created Task for consumption order: ${result?.id}`);

      // Insert each item into Supabase consumptions (requires event_attendee_id)
      if (!isAnonymous && customer) {
        const userId = ReceiverV2.getElementText(customer, 'user_id');
        let eventAttendeeId = null;
        if (userId) {
          const personId = await this.db.findPersonByExternalId(userId);
          if (personId) eventAttendeeId = await this.db.findEventAttendeeByPersonId(personId);
        }
        if (eventAttendeeId) {
          for (const item of itemList) {
            const unitPriceVal = item.unit_price;
            const unitPrice = parseFloat(typeof unitPriceVal === 'object' ? unitPriceVal['#text'] : unitPriceVal) || 0;
            const qty = parseInt(item.quantity, 10) || 1;
            await this.db.insertConsumption({
              event_attendee_id: eventAttendeeId,
              item_name: String(item.description),
              quantity: qty,
              unit_price: unitPrice,
              total_price: unitPrice * qty,
              paid: false,
            });
          }
          console.log(`[supabase] Inserted ${itemList.length} consumption(s) for attendee: ${eventAttendeeId}`);
        }
      }
    } catch (err) {
      console.log(`[receiver] Error in handleConsumptionOrder: ${err}`);
      throw err;
    }
  }

  async handleBadgeAssigned(header, body) {
    try {
      const badgeId = ReceiverV2.getElementText(body, 'badge_id');
      const userId = ReceiverV2.getElementText(body, 'user_id');
      const assignedAt = ReceiverV2.getElementText(body, 'assigned_at');

      if (!this.sf.isConnected) {
        console.log(`[receiver] DRY RUN: Would update Contact badge_id=${badgeId} for user_id=${userId}`);
        return;
      }

      const contactId = userId ? await this._findContactByUserId(userId) : null;
      if (contactId) {
        await this.sf.apiCall(conn => conn.sobject('Contact').update({
          Id: contactId,
          Description: `badge_id: ${badgeId} assigned_at: ${assignedAt}`,
        }));
        console.log(`[receiver] Updated Contact ${contactId} with badge_id: ${badgeId}`);
      } else {
        console.log(`[receiver] Badge assigned but no Contact found for user_id: ${userId}`);
      }
    } catch (err) {
      console.log(`[receiver] Error in handleBadgeAssigned: ${err}`);
      throw err;
    }
  }

  async handleRefundProcessed(header, body) {
    try {
      const userId = ReceiverV2.getElementText(body, 'user_id');
      const refund = body ? body.refund : null;
      const refundType = ReceiverV2.getElementText(body, 'refund_type');
      const originalTxId = ReceiverV2.getElementText(body, 'original_transaction_id');

      const amountVal = refund ? refund.amount : null;
      const amount = typeof amountVal === 'object' ? amountVal['#text'] : amountVal;
      const currency = typeof amountVal === 'object' ? (amountVal.currency || 'eur') : 'eur';

      const walletVal = body ? body.new_wallet_balance : null;
      const newWallet = typeof walletVal === 'object' ? walletVal['#text'] : walletVal;

      const taskData = {
        Subject: `Refund processed [${refundType}]: ${amount} ${currency}`,
        Description: [
          `Type: ${refundType}`,
          `Amount: ${amount} ${currency}`,
          `Method: ${ReceiverV2.getElementText(refund, 'method')}`,
          `Reason: ${ReceiverV2.getElementText(refund, 'reason')}`,
          `Description: ${ReceiverV2.getElementText(refund, 'description')}`,
          `Original Transaction: ${originalTxId}`,
          newWallet ? `New Wallet Balance: ${newWallet} ${currency}` : null,
          userId ? `User ID: ${userId}` : null,
        ].filter(Boolean).join('\n'),
        Status: 'Completed',
        Type: 'Other',
        ActivityDate: new Date().toISOString().split('T')[0],
      };

      if (!this.sf.isConnected) {
        console.log(`[receiver] DRY RUN: Would create Task: ${JSON.stringify(taskData)}`);
        return;
      }

      if (userId) {
        const contactId = await this._findContactByUserId(userId);
        if (contactId) taskData.WhoId = contactId;
      }

      const result = await this.sf.apiCall(conn => conn.sobject('Task').create(taskData));
      console.log(`[receiver] Created Task for refund: ${result?.id}`);
    } catch (err) {
      console.log(`[receiver] Error in handleRefundProcessed: ${err}`);
      throw err;
    }
  }

  async handleInvoiceRequestFromKassa(header, body) {
    try {
      const userId = ReceiverV2.getElementText(body, 'user_id');
      const invoiceData = body ? body.invoice_data : null;
      const address = invoiceData ? invoiceData.address : null;

      const taskData = {
        Subject: `Invoice request from Kassa for user: ${userId}`,
        Description: [
          `User ID: ${userId}`,
          `Name: ${ReceiverV2.getElementText(invoiceData, 'first_name')} ${ReceiverV2.getElementText(invoiceData, 'last_name')}`,
          `Email: ${ReceiverV2.getElementText(invoiceData, 'email')}`,
          `VAT: ${ReceiverV2.getElementText(invoiceData, 'vat_number')}`,
          address ? `Address: ${ReceiverV2.getElementText(address, 'street')} ${ReceiverV2.getElementText(address, 'number')}, ${ReceiverV2.getElementText(address, 'postal_code')} ${ReceiverV2.getElementText(address, 'city')}` : null,
          `Correlation ID: ${header.correlation_id || 'N/A'}`,
        ].filter(Boolean).join('\n'),
        Status: 'Not Started',
        Type: 'Other',
        ActivityDate: new Date().toISOString().split('T')[0],
      };

      if (!this.sf.isConnected) {
        console.log(`[receiver] DRY RUN: Would create Task: ${JSON.stringify(taskData)}`);
        return;
      }

      const email = ReceiverV2.getElementText(invoiceData, 'email');
      if (email) {
        const contactId = await this._findContactByEmail(email);
        if (contactId) taskData.WhoId = contactId;
      }

      const result = await this.sf.apiCall(conn => conn.sobject('Task').create(taskData));
      console.log(`[receiver] Created Task for Kassa invoice request: ${result?.id}`);

      // Forward to facturatie queue
      await this.sender.sendInvoiceRequest({
        customer: {
          email: ReceiverV2.getElementText(invoiceData, 'email'),
          first_name: ReceiverV2.getElementText(invoiceData, 'first_name'),
          last_name: ReceiverV2.getElementText(invoiceData, 'last_name'),
        },
        invoice: {
          description: `Invoice request for user ${userId}`,
          amount: 0,
          currency: 'eur',
          due_date: new Date().toISOString().split('T')[0],
        },
        items: [],
        correlation_id: header.correlation_id || header.message_id,
      });
      console.log('[receiver] Invoice request forwarded to facturatie queue');
    } catch (err) {
      console.log(`[receiver] Error in handleInvoiceRequestFromKassa: ${err}`);
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
    await this.sender.close();
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
