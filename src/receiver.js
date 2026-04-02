'use strict';

require('dotenv').config();
const http = require('http');
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

  startHealthServer() {
    const port = process.env.HEALTH_PORT || 3000;
    http.createServer((req, res) => {
      res.writeHead(200);
      res.end('OK');
    }).listen(port, '0.0.0.0', () => {
      console.log(`[receiver] Health check server listening on port ${port}`);
    });
  }

  async start() {
    this.startHealthServer();
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
        await this.channel.assertQueue(DEAD_LETTER_QUEUE, { durable: true, arguments: { 'x-dead-letter-exchange': '' } });
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

    if (!this.running) return;
    console.log('[receiver] Max retries reached, exiting process.');
    process.exit(1);
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

  async _findUserByEmail(email) {
    const records = await this.sf.apiCall(
      conn => conn.sobject('Member__c').find({ Email__c: email }, ['Id']).limit(1)
    );
    return records && records.length > 0 ? records[0].Id : null;
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
      const sessionId = ReceiverV2.getElementText(body, 'session_id');

      const descriptionParts = [
        `user_id: ${ReceiverV2.getElementText(customer, 'user_id')}`,
        `type: ${ReceiverV2.getElementText(customer, 'type')}`,
        `badge_id: ${ReceiverV2.getElementText(customer, 'badge_id')}`,
        `registration_date: ${ReceiverV2.getElementText(customer, 'registration_date')}`,
        sessionId ? `session_id: ${sessionId}` : null,
      ].filter(Boolean);
      if (regFee) {
        const amountVal = regFee.amount;
        const amount = typeof amountVal === 'object' ? amountVal['#text'] : amountVal;
        const currency = typeof amountVal === 'object' ? (amountVal.currency || 'eur') : 'eur';
        const paid = ReceiverV2.getElementText(regFee, 'paid');
        descriptionParts.push(`registration_fee: ${amount} ${currency} - paid: ${paid}`);
      }

      const externalUserId = ReceiverV2.getElementText(customer, 'user_id');
      const isCompanyLinked = ReceiverV2.getElementText(customer, 'is_company_linked');
      const rawType = ReceiverV2.getElementText(customer, 'type');
      const userType = (isCompanyLinked === 'true' || rawType === 'company') ? 'Bedrijf' : 'Particulier';

      const paymentStatus = regFee && ReceiverV2.getElementText(regFee, 'paid') === 'true' ? 'paid' : 'pending';
      const amountVal = regFee ? regFee.amount : null;
      const registrationAmount = amountVal !== null && typeof amountVal === 'object' ? amountVal['#text'] : (amountVal || null);

      const rawUserData = {
        User_ID__c: externalUserId,
        First_Name__c: ReceiverV2.getElementText(customer, 'first_name'),
        Last_Name__c: ReceiverV2.getElementText(customer, 'last_name'),
        Email__c: ReceiverV2.getElementText(customer, 'email'),
        Birthdate__c: ReceiverV2.getElementText(customer, 'date_of_birth'),
        User_Type__c: userType,
        Street__c: address ? ReceiverV2.getElementText(address, 'street') : null,
        House_Number__c: address ? ReceiverV2.getElementText(address, 'number') : null,
        Postal_Code__c: address ? ReceiverV2.getElementText(address, 'postal_code') : null,
        City__c: address ? ReceiverV2.getElementText(address, 'city') : null,
        Country_Code__c: address ? (ReceiverV2.getElementText(address, 'country') || '').toUpperCase() || null : null,
        Amount__c: registrationAmount ? parseFloat(registrationAmount) : null,
        Payment_Status__c: paymentStatus,
        Badge_ID__c: ReceiverV2.getElementText(customer, 'badge_id') || null,
        Company_Name__c: isCompanyLinked === 'true' ? ReceiverV2.getElementText(customer, 'company_name') : null,
        BTW_Number__c: isCompanyLinked === 'true' ? ReceiverV2.getElementText(customer, 'vat_number') : null,
      };
      const userData = Object.fromEntries(
        Object.entries(rawUserData).filter(([, v]) => v !== null && v !== '')
      );

      if (!this.sf.isConnected) {
        console.log(`[receiver] DRY RUN: Would upsert Member__c: ${JSON.stringify(userData)}`);
      } else {
        const sfResult = await this.sf.apiCall(
          conn => conn.sobject('Member__c').upsert(userData, 'User_ID__c')
        );
        console.log(`[receiver] Upserted Member__c: ${sfResult?.id || externalUserId}`);
      }

      // Upsert person into Supabase
      const personPayload = {
        external_user_id: externalUserId,
        company_name: rawUserData.Company_Name__c || null,
        vat_number: rawUserData.BTW_Number__c || null,
        language: 'NL',
        salutation: rawUserData.Salutation__c || null,
        first_name: rawUserData.First_Name__c,
        last_name: rawUserData.Last_Name__c,
        email: rawUserData.Email__c,
        date_of_birth: rawUserData.Birthdate__c || null,
        amount: rawUserData.Amount__c,
        street: rawUserData.Street__c || null,
        house_number: rawUserData.House_Number__c || null,
        postal_code: rawUserData.Postal_Code__c || null,
        city: rawUserData.City__c || null,
        country: rawUserData.Country_Code__c || null,
        payment_status: rawUserData.Payment_Status__c,
        badge_id: rawUserData.Badge_ID__c || null,
        person_type: userType,
      };
      const sbPersonId = await this.db.upsertPerson(
        Object.fromEntries(Object.entries(personPayload).filter(([, v]) => v !== null))
      );
      if (sbPersonId) {
        console.log(`[supabase] Upserted person: ${sbPersonId}`);
        await this.db.syncSalesforceStatus(sbPersonId, 'synced');
      }

      // Forward new_registration to kassa.incoming in Kassa's required format
      const userId = ReceiverV2.getElementText(customer, 'user_id');
      const typeUser = ReceiverV2.getElementText(customer, 'type');
      const dateOfBirth = ReceiverV2.getElementText(customer, 'date_of_birth');
      let age = null;
      if (dateOfBirth) {
        const today = new Date();
        const dob = new Date(dateOfBirth);
        age = today.getFullYear() - dob.getFullYear();
        const birthdayPassedThisYear =
          today.getMonth() > dob.getMonth() ||
          (today.getMonth() === dob.getMonth() && today.getDate() >= dob.getDate());
        if (!birthdayPassedThisYear) age -= 1;
      }

      if (userId && age !== null) {
        const kassaPayload = {
          customer: {
            email: ReceiverV2.getElementText(customer, 'email'),
            first_name: ReceiverV2.getElementText(customer, 'first_name'),
            last_name: ReceiverV2.getElementText(customer, 'last_name'),
            user_id: userId,
            type: isCompanyLinked === 'true' ? 'company' : (typeUser || 'private'),
            company_name: ReceiverV2.getElementText(customer, 'company_name'),
            vat_number: ReceiverV2.getElementText(customer, 'vat_number'),
            age,
          },
          payment_due: {
            amount: regFee ? (typeof regFee.amount === 'object' ? regFee.amount['#text'] : regFee.amount) : '0',
            status: regFee && ReceiverV2.getElementText(regFee, 'paid') === 'true' ? 'paid' : 'unpaid',
          },
          session_id: sessionId || undefined,
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
        const contactId = await this._findUserByEmail(email);
        if (contactId) taskData.WhoId = contactId;
      } else if (userId) {
        const contactId = await this._findUserById(userId);
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
      if (!body) {
        console.log('[receiver] Missing body in badge_scanned message');
        return;
      }
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
        const contactId = await this._findUserByEmail(email);
        if (contactId) taskData.WhoId = contactId;
      }

      const result = await this.sf.apiCall(conn => conn.sobject('Task').create(taskData));
      console.log(`[receiver] Created Task for badge scan: ${result?.id}`);

      // Record check-in time in Supabase
      const scanEmail = ReceiverV2.getElementText(body, 'email');
      if (scanEmail) {
        const { personId, error: sbError } = await this.db.findPersonByEmailForCheckIn(scanEmail);
        if (sbError) {
          console.log(`[supabase] Error looking up person for badge scan: ${sbError.message}`);
        } else if (personId) {
          await this.db.updateEventAttendeeCheckIn(personId);
        }
      }
    } catch (err) {
      console.log(`[receiver] Error in handleBadgeScanned: ${err}`);
      throw err;
    }
  }

  async handleSessionUpdate(header, body) {
    try {
      if (!body) {
        console.log('[receiver] Missing body in session_update message');
        return;
      }
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
        const contactId = await this._findUserByEmail(email);
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
      if (!body) {
        console.log('[receiver] Missing body in mailing_status message');
        return;
      }
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

  async _findUserById(userId) {
    const records = await this.sf.apiCall(
      conn => conn.sobject('Member__c').find({ User_ID__c: userId }, ['Id']).limit(1)
    );
    return records && records.length > 0 ? records[0].Id : null;
  }

  async handleConsumptionOrder(header, body) {
    try {
      const isAnonymous = ReceiverV2.getElementText(body, 'is_anonymous') === 'true';
      const customer = body ? body.customer : null;
      const items = body ? body.items : null;

      const itemList = items
        ? (Array.isArray(items.item) ? items.item : [items.item]).filter(Boolean)
        : [];

      // Resolve Salesforce Member__c record ID
      let memberId = null;
      if (!isAnonymous && customer) {
        const email = ReceiverV2.getElementText(customer, 'email');
        const userId = ReceiverV2.getElementText(customer, 'user_id');
        memberId = email
          ? await this._findUserByEmail(email)
          : (userId ? await this._findUserById(userId) : null);
      }

      if (!this.sf.isConnected) {
        console.log(`[receiver] DRY RUN: Would upsert ${itemList.length} Consumption__c record(s)`);
        return;
      }

      // Upsert each item as a Consumption__c record
      for (let i = 0; i < itemList.length; i++) {
        const item = itemList[i];
  
         // 1. Haal het unieke ID uit de XML (LINE-xxx)
        const lineId = item.id; // Het veld <id> uit de XSD v2.3
        const sku = item.sku;   // Het product-ID <sku>
  
        const unitPriceVal = item.unit_price;
        const unitPrice = parseFloat(typeof unitPriceVal === 'object' ? unitPriceVal['#text'] : unitPriceVal) || 0;
        const qty = parseInt(item.quantity, 10) || 1;

        const consumptionData = {
    // 2. Gebruik het ID van Kassa voor de Upsert
        Consumption_ID__c: lineId || `${header.message_id}-${i}`, 
        Product_Name__c: String(item.description),
        //Product_SKU__c: String(sku), // Optioneel: voeg SKU toe aan Salesforce
        Quantity__c: qty,
        Total_Amount__c: unitPrice * qty,
        Price_Per_Unit__c: unitPrice,
      };

      if (memberId) {
        consumptionData.Member__c = memberId;
      }

      // Salesforce Upsert
      await this.sf.apiCall(conn => conn.sobject('Consumption__c').upsert(consumptionData, 'Consumption_ID__c'));
    }

      // Insert each item into Supabase
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

      if (!this.sf.isConnected) {
        console.log(`[receiver] DRY RUN: Would update Member__c Badge_ID__c=${badgeId} for User_ID__c=${userId}`);
        return;
      }

      const sfUserId = userId ? await this._findUserById(userId) : null;
      if (sfUserId) {
        await this.sf.apiCall(conn => conn.sobject('Member__c').update({
          Id: sfUserId,
          Badge_ID__c: badgeId,
        }));
        console.log(`[receiver] Updated Member__c ${sfUserId} Badge_ID__c: ${badgeId}`);
      } else {
        console.log(`[receiver] Badge assigned but no Member__c found for User_ID__c: ${userId}`);
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
        const contactId = await this._findUserById(userId);
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
        const contactId = await this._findUserByEmail(email);
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
      const ok = this.channel.sendToQueue(
        DEAD_LETTER_QUEUE,
        Buffer.from(deadLetterMessage),
        { deliveryMode: 2 }
      );
      if (!ok) console.log('[receiver] Warning: write buffer full, dead letter message may be dropped');
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
    try { if (this.channel) await this.channel.close(); } catch { /* ignore */ }
    try { if (this.connection) await this.connection.close(); } catch { /* ignore */ }
    try { await this.sender.close(); } catch { /* ignore */ }
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
