'use strict';

require('dotenv').config();
const http = require('http');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');
const { parseStringPromise } = require('xml2js');
const { XMLParser } = require('fast-xml-parser');
const { getAmqpOptions } = require('./amqpUrl');
const SFConnection = require('./sfConnection');
const CRMSender = require('./sender');
const { create } = require('xmlbuilder2');
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
  INVOICE_CANCELLED: 'invoice_cancelled',
  DELETE_USER: 'delete_user',
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
    await this.connectRabbitMQ();
  }

  async connectRabbitMQ() {
    const maxRetries = 5;
    let retryCount = 0;

    while (retryCount < maxRetries && this.running) {
      try {
        this.connection = await amqp.connect(getAmqpOptions());
        this.channel = await this.connection.createChannel();

        await this.channel.assertQueue(QUEUE_NAME, { durable: true });
        await this.channel.assertQueue(KASSA_QUEUE, { durable: true });
        await this.channel.assertQueue(DEAD_LETTER_QUEUE, {
          durable: true,
          arguments: { 'x-dead-letter-exchange': '' },
        });
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
          await new Promise((r) => setTimeout(r, 5000));
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

    const requiredFields = ['message_id', 'version', 'type', 'timestamp', 'source', 'master_uuid'];
    const missingFields = requiredFields.filter((f) => header[f] === undefined || header[f] === null);
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

getOrCreateMasterUuid(email, sourceSystem = 'crm') {
  if (!this.channel) throw new Error('RabbitMQ channel not initialized');

  const correlationId = uuidv4();
  let consumerTag = null;
  let replyQueueName = null;

  // We verwijderen 'async' voor (resolve, reject) om de linter blij te maken
  return new Promise((resolve, reject) => {
    // 1. Setup Timeout
    const timeout = setTimeout(async () => {
      try {
        if (consumerTag) await this.channel.cancel(consumerTag);
        if (replyQueueName) await this.channel.deleteQueue(replyQueueName);
      } catch (err) {
        console.error("[identity] Cleanup error during timeout:", err);
      }
      reject(new Error(`Identity Service timeout voor ${email}`));
    }, 15000);

    // 2. Start de flow (we gebruiken .then om de async setup te doen)
    this.channel.assertQueue('', { exclusive: true })
      .then((replyQueue) => {
        replyQueueName = replyQueue.queue;

        return this.channel.consume(replyQueueName, async (msg) => {
          // De callback zelf MAG wel async zijn
          if (!msg) return;

          if (msg.properties.correlationId === correlationId) {
            clearTimeout(timeout);

            try {
              const responseXml = msg.content.toString();
              const parsed = await parseStringPromise(responseXml, { explicitArray: false });

              if (parsed.identity_response?.status === 'ok') {
                resolve(parsed.identity_response.user.master_uuid);
              } else {
                reject(new Error('Identity Service gaf een foutmelding terug'));
              }
            } catch (err) {
              reject(new Error(`Fout bij verwerken Identity antwoord: ${err.message}`));
            } finally {
              // Cleanup na verwerking
              try {
                if (consumerTag) await this.channel.cancel(consumerTag);
                await this.channel.deleteQueue(replyQueueName);
              } catch (cleanupErr) {
                console.error("[identity] Final cleanup error:", cleanupErr);
              }
            }
          }
        }, { noAck: true });
      })
      .then((consumeObj) => {
        consumerTag = consumeObj.consumerTag;

        // 3. Veilig XML bouwen en versturen
        const requestXml = create({ version: '1.0' })
          .ele('identity_request')
            .ele('email').txt(email).up()
            .ele('source_system').txt(sourceSystem).up()
          .end();

        this.channel.sendToQueue('identity.user.create.request', Buffer.from(requestXml), {
          correlationId: correlationId,
          replyTo: replyQueueName,
          contentType: 'application/xml'
        });
      })
      .catch((err) => {
        clearTimeout(timeout);
        reject(err);
      });
  });
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
      [MESSAGE_TYPES.INVOICE_CANCELLED]: () => this.handleInvoiceCancellationRequest(header, body),
      [MESSAGE_TYPES.DELETE_USER]: () => this.handleDeleteUser(header, body),
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
      (conn) => conn.sobject('Member__c').find({ Email__c: email }, ['Id']).limit(1)
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

    // --- STAP 1: IDENTITY SERVICE CHECK (NIEUW) ---
    // We pakken het emailadres, maken het schoon, en vragen de officiële UUID op
    const emailForIdentity = (ReceiverV2.getElementText(customer, 'email') || '').toLowerCase().trim();
    console.log(`[receiver] Requesting Master UUID from Identity Service for: ${emailForIdentity}`);
    
    // Roep de nieuwe RPC-functie aan
    const masterUuid = await this.getOrCreateMasterUuid(emailForIdentity, 'crm');
    if (!masterUuid) {
      throw new Error(`Could not retrieve Master UUID from Identity Service for ${emailForIdentity}`);
    }
    console.log(`[receiver] Official Master UUID obtained: ${masterUuid}`);


    // --- STAP 2: JOUW ORIGINELE DATA EXTRACTIE ---
    const contact = customer.contact || null;
    const getCustomerText = (key) =>
      ReceiverV2.getElementText(customer, key) ||
      ReceiverV2.getElementText(contact, key);

    const address = customer.address || null;
    const regFee = customer.registration_fee || (body ? body.payment_due : null) || null;
    const isCompanyLinked = getCustomerText('is_company_linked') === 'true';
    const rawType = getCustomerText('type');
    const userType = (isCompanyLinked || rawType === 'company') ? 'Bedrijf' : 'Particulier';

    const paymentFlag = ReceiverV2.getElementText(regFee, 'paid');
    const paymentState = ReceiverV2.getElementText(regFee, 'status');
    const paymentStatus = (paymentFlag === 'true' || paymentState === 'paid') ? 'paid' : 'pending';

    const amountVal = regFee ? regFee.amount : null;
    const registrationAmount = amountVal !== null && typeof amountVal === 'object' 
      ? amountVal['#text'] 
      : (amountVal || null);

    // Jouw volledige rawUserData (aangepast om de officiële masterUuid te gebruiken)
    const rawUserData = {
      Master_UUID__c: masterUuid, // Officiële UUID
      First_Name__c: getCustomerText('first_name'),
      Last_Name__c: getCustomerText('last_name'),
      Email__c: emailForIdentity, // Gebruik de genormaliseerde email
      Birthdate__c: getCustomerText('date_of_birth'),
      User_Type__c: userType,
      Street__c: address ? ReceiverV2.getElementText(address, 'street') : null,
      House_Number__c: address ? ReceiverV2.getElementText(address, 'number') : null,
      Postal_Code__c: address ? ReceiverV2.getElementText(address, 'postal_code') : null,
      City__c: address ? ReceiverV2.getElementText(address, 'city') : null,
      Country_Code__c: address ? (ReceiverV2.getElementText(address, 'country') || '').toUpperCase() || null : null,
      Amount__c: registrationAmount ? parseFloat(registrationAmount) : null,
      Payment_Status__c: paymentStatus,
      Badge_ID__c: getCustomerText('badge_id') || null,
    };

    const userData = Object.fromEntries(
      Object.entries(rawUserData).filter(([, value]) => value !== null && value !== undefined && value !== '')
    );


    // --- STAP 3: JOUW ORIGINELE ACCOUNT & COMPANY LOGICA ---
    let companyId = null;
    const companyData = body ? body.company : null;
    if ((isCompanyLinked || rawType === 'company') && companyData) {
      const companyName = ReceiverV2.getElementText(companyData, 'name');
      const companyVat = ReceiverV2.getElementText(companyData, 'vat_number');
      const companyEmail = ReceiverV2.getElementText(companyData, 'email');

      if (companyName && companyVat) {
        if (!this.sf.isConnected) {
          console.log(`[receiver] DRY RUN: Would upsert Account for company VAT=${companyVat}`);
        } else {
          const result = await this.sf.apiCall((conn) =>
            conn.sobject('Account').upsert({
              Master_UUID__c: masterUuid, // Officiële UUID
              Company_Name__c: companyName,
              VAT_Number__c: companyVat,
              Email__c: companyEmail || null,
              // Optioneel: voeg hier het billing address toe als Facturatie dat op het Account wil
              Billing_Street__c: address ? ReceiverV2.getElementText(address, 'street') : null,
              Billing_City__c: address ? ReceiverV2.getElementText(address, 'city') : null,
            }, 'VAT_Number__c')
          );
          companyId = result.id || null;
          console.log(`[receiver] Upserted Account: ${companyId} for VAT ${companyVat}`);
        }
      }
    }

    // KOPPELING MAKEN TUSSEN PERSOON EN BEDRIJF
    if (companyId) {
      userData.Account__c = companyId;
    }


    // --- STAP 4: JOUW ORIGINELE SALESFORCE MEMBER UPSERT ---
    if (!this.sf.isConnected) {
      console.log(`[receiver] DRY RUN: Would upsert Member__c: ${JSON.stringify(userData)}`);
    } else {
      const result = await this.sf.apiCall((conn) =>
        conn.sobject('Member__c').upsert(userData, 'Master_UUID__c')
      );
      console.log(`[receiver] Upserted Member__c via Master UUID: ${result.id}`);
    }

    // --- STAP 5: JOUW ORIGINELE KASSA PAYLOAD ---
    const kassaPayload = {
      header: { master_uuid: masterUuid }, // Officiële UUID
      customer: {
        email: emailForIdentity,
        first_name: getCustomerText('first_name'),
        last_name: getCustomerText('last_name'),
        master_uuid: masterUuid,
        type: (isCompanyLinked || rawType === 'company') ? 'company' : (rawType || 'private'),
        company_name: companyData ? ReceiverV2.getElementText(companyData, 'name') : null,
        vat_number: companyData ? ReceiverV2.getElementText(companyData, 'vat_number') : null,
        date_of_birth: getCustomerText('date_of_birth'),
      },
      payment_due: {
        amount: registrationAmount || '0',
        status: paymentStatus === 'paid' ? 'paid' : 'unpaid',
      },
    };

    await this.sender.sendNewRegistrationToKassa(kassaPayload);
    console.log(`[receiver] Forwarded new_registration to Kassa for Master UUID=${masterUuid}`);

    
    // --- STAP 6: JOUW ORIGINELE FOSSBILLING PAYLOAD ---
    const fossPayload = {
      master_uuid: masterUuid, // Officiële UUID
      customer: {
        first_name: getCustomerText('first_name'),
        last_name: getCustomerText('last_name'),
        email: emailForIdentity,
        type: (isCompanyLinked || rawType === 'company') ? 'company' : 'private',
        company_name: companyData ? ReceiverV2.getElementText(companyData, 'name') : null,
        vat_number: companyData ? ReceiverV2.getElementText(companyData, 'vat_number') : null,
      },
      address: {
        street: address ? ReceiverV2.getElementText(address, 'street') : null,
        number: address ? ReceiverV2.getElementText(address, 'number') : null,
        postal_code: address ? ReceiverV2.getElementText(address, 'postal_code') : null,
        city: address ? ReceiverV2.getElementText(address, 'city') : null,
        country: address ? (ReceiverV2.getElementText(address, 'country') || '').toUpperCase() : null,
      },
      registration_fee: {
        amount: registrationAmount ? parseFloat(registrationAmount) : 0,
        status: paymentStatus,
        trigger_invoice: true  
      }
    };

    await this.sender.sendNewRegistrationToFacturatie(fossPayload);
console.log(`[receiver] Forwarded to Facturatie voor Master UUID=${masterUuid}`);

  } catch (err) {
    console.log(`[receiver] Error in handleNewRegistration: ${err.message}`);
    throw err;
  }
}

/**
 * Verwerkt send_invoice bericht van FossBilling (inclusief PDF-link)
 */
async handleSendInvoice(header, body) {
  try {
    const masterUuid = header.master_uuid;
    const invoiceUrl = ReceiverV2.getElementText(body, 'pdf_url'); // Bevat de link naar de PDF
    const dueDate = ReceiverV2.getElementText(body, 'due_date');
    const invoiceNumber = ReceiverV2.getElementText(body, 'invoice_number');

    console.log(`[receiver] Processing send_invoice for ${masterUuid}, invoice: ${invoiceNumber}`);

    // 1. Update Salesforce (Member__c) met de PDF-link en vervaldatum
    if (this.sf.isConnected) {
      await this.sf.apiCall((conn) =>
        conn.sobject('Member__c').upsert({
          Master_UUID__c: masterUuid,
          Last_Invoice_URL__c: invoiceUrl,
          Last_Invoice_Due_Date__c: dueDate,
          Last_Invoice_Number__c: invoiceNumber
        }, 'Master_UUID__c')
      );
    }

  } catch (err) {
    console.error(`[receiver] Error in handleSendInvoice: ${err}`);
  }
}

/**
 * Verwerkt invoice_cancelled bericht van FossBilling
 */
async handleInvoiceCancelled(header, body) {
  try {
    const masterUuid = header.master_uuid;
    const invoiceNumber = ReceiverV2.getElementText(body, 'invoice_number');

    console.log(`[receiver] Processing invoice_cancelled for ${masterUuid}, invoice: ${invoiceNumber}`);

    // Update Salesforce status naar 'Cancelled'
    if (this.sf.isConnected) {
      // We zoeken de consumptie op basis van het factuurnummer en de UUID
      await this.sf.apiCall((conn) =>
        conn.sobject('Consumption__c').find({ 
          Invoice_Number__c: invoiceNumber,
          Master_UUID__c: masterUuid 
        }).update({ Status__c: 'Cancelled' })
      );
    }
  } catch (err) {
    console.error(`[receiver] Error in handleInvoiceCancelled: ${err}`);
  }
}

  async handlePaymentRegistered(header, body) {
    try {
      const invoice = body ? body.invoice : null;
      const transaction = body ? body.transaction : null;
      const paymentContext = ReceiverV2.getElementText(body, 'payment_context') || 'unknown';
      const masterUuid = ReceiverV2.getElementText(body, 'master_uuid');

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
          masterUuid ? `Master UUID: ${masterUuid}` : null,
        ].filter(Boolean).join('\n'),
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
        const contactId = await this._findUserByEmail(email);
        if (contactId) taskData.WhoId = contactId;
      } else if (masterUuid) {
        const contactId = await this._findUserByMasterUuid(masterUuid);
        if (contactId) taskData.WhoId = contactId;
      }

      const result = await this.sf.apiCall((conn) => conn.sobject('Task').create(taskData));
      console.log(`[receiver] Created Task for payment [${paymentContext}]: ${result?.id}`);
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

      const result = await this.sf.apiCall((conn) => conn.sobject('Task').create(taskData));
      console.log(`[receiver] Created Task for badge scan: ${result?.id}`);
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

      const result = await this.sf.apiCall((conn) => conn.sobject('Task').create(taskData));
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

      const result = await this.sf.apiCall((conn) => conn.sobject('Task').create(taskData));
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

      const result = await this.sf.apiCall((conn) => conn.sobject('Task').create(taskData));
      console.log(`[receiver] Created Task for mailing status: ${result?.id}`);
    } catch (err) {
      console.log(`[receiver] Error in handleMailingStatus: ${err}`);
      throw err;
    }
  }

  sendToDeadLetter(content, reason) {
    if (this.channel) {
      const deadLetterContent = Buffer.from(
        JSON.stringify({
          reason,
          originalContent: content.toString('utf8'),
          timestamp: new Date().toISOString(),
        })
      );
      this.channel.sendToQueue(DEAD_LETTER_QUEUE, deadLetterContent, { persistent: true });
      console.log(`[receiver] Message sent to dead-letter queue: ${reason}`);
    }
  }

  static getElementText(obj, key) {
    if (!obj || obj[key] === undefined || obj[key] === null) {
      return null;
    }
    const value = obj[key];
    if (typeof value === 'object' && value['#text'] !== undefined) {
      return value['#text'];
    }
    if (Array.isArray(value)) {
      const first = value[0];
      if (typeof first === 'object' && first['#text'] !== undefined) {
        return first['#text'];
      }
      return String(first);
    }
    return String(value);
  }

  async _findUserByMasterUuid(masterUuid) {
  const records = await this.sf.apiCall(
    (conn) => conn.sobject('Member__c').find({ Master_UUID__c: masterUuid }, ['Id']).limit(1)
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

      if (this.sf.isConnected) {
        let memberId = null;
        if (!isAnonymous && customer) {
          const email = ReceiverV2.getElementText(customer, 'email');
          const masterUuid = ReceiverV2.getElementText(customer, 'master_uuid'); // <--- NIEUWE MANIER
          
          memberId = email
            ? await this._findUserByEmail(email)
            : (masterUuid ? await this._findUserByMasterUuid(masterUuid) : null); // Update ook de functienaam
        }

        for (let i = 0; i < itemList.length; i++) {
          const item = itemList[i];
          const lineId = item.id;

          const unitPriceVal = item.unit_price;
          const unitPrice = parseFloat(typeof unitPriceVal === 'object' ? unitPriceVal['#text'] : unitPriceVal) || 0;
          const qty = parseInt(item.quantity, 10) || 1;

          const sku = item.sku || null;
          const vatRate = item.vat_rate ? parseFloat(item.vat_rate) : null;
          const totalAmountVal = item.total_amount;
          const providedTotal = parseFloat(typeof totalAmountVal === 'object' ? totalAmountVal['#text'] : totalAmountVal);
          const finalTotalAmount = isNaN(providedTotal) ? (unitPrice * qty) : providedTotal;

          const consumptionData = {
            Consumption_ID__c: lineId || `${header.message_id}-${i}`,
            Product_Name__c: String(item.description),
            Quantity__c: qty,
            Total_Amount__c: finalTotalAmount,
            Price_Per_Unit__c: unitPrice,
            SKU__c: sku,
            VAT_Rate__c: vatRate,
          };

          if (memberId) {
            consumptionData.Member__c = memberId;
          }

          await this.sf.apiCall((conn) =>
            conn.sobject('Consumption__c').upsert(consumptionData, 'Consumption_ID__c')
          );
          console.log(`[receiver] Upserted Consumption__c: ${consumptionData.Consumption_ID__c}`);
        }
      } else {
        console.log(`[receiver] DRY RUN: Would upsert ${itemList.length} Consumption__c record(s)`);
      }
    } catch (err) {
      console.log(`[receiver] Error in handleConsumptionOrder: ${err}`);
      throw err;
    }
  }

  async handleDeleteUser(header, body) {
  try {
    // Pak de UUID bij voorkeur uit de header, anders uit de body
    const masterUuid = header.master_uuid || ReceiverV2.getElementText(body, 'master_uuid');

    if (!masterUuid) {
      console.log('[receiver] handleDeleteUser: missing master_uuid');
      return;
    }

    console.log(`[receiver] Processing delete_user for Master UUID: ${masterUuid}`);

    // 1. MySQL Soft Delete (Zoek op UUID)
    await this.db.query(
      'UPDATE crm_user_sync SET is_deleted = true WHERE master_uuid = ?',
      [masterUuid]
    );

    // 2. Salesforce Update
    if (this.sf.isConnected) {
      // Zoek op het unieke External ID veld in Salesforce
      await this.sf.apiCall((conn) =>
        conn.sobject('Member__c')
            .find({ Master_UUID__c: masterUuid })
            .update({ Is_Deleted__c: true })
      );
    }

    console.log(`[receiver] User ${masterUuid} marked as deleted.`);
  } catch (err) {
    console.error(`[receiver] Error in handleDeleteUser: ${err}`);
    throw err;
  }
}

  async handleInvoiceCancellationRequest(header, body) {
    try {
      const masterUuid = header.master_uuid;
      const invoiceNumber = ReceiverV2.getElementText(body, 'invoice_number');

      if (!invoiceNumber || !masterUuid) {
        console.log('[receiver] Missing data for invoice cancellation');
        return;
      }

      console.log(`[receiver] Forwarding invoice_cancelled to Facturatie for UUID: ${masterUuid}`);

      // Stuur door naar Facturatie via de Sender
      await this.sender.sendInvoiceCancelledToFacturatie({
        master_uuid: masterUuid,
        invoice_number: invoiceNumber,
        reason: ReceiverV2.getElementText(body, 'reason') || 'Cancelled by user via frontend'
      });

    } catch (err) {
      console.error(`[receiver] Error in handleInvoiceCancellationRequest: ${err.message}`);
      throw err;
    }
  }

  async handleBadgeAssigned(header, body) {
  try {
    const badgeId = ReceiverV2.getElementText(body, 'badge_id');
    const masterUuid = ReceiverV2.getElementText(body, 'master_uuid') || header.master_uuid;

    if (!this.sf.isConnected) {
      console.log(`[receiver] DRY RUN: Update Badge_ID__c=${badgeId} for UUID=${masterUuid}`); 
      return;
    }

    // Gebruik de nieuwe helperfunctie die we eerder hebben gedefinieerd
    const sfMemberId = masterUuid ? await this._findUserByMasterUuid(masterUuid) : null;
    
    if (sfMemberId) {
      await this.sf.apiCall((conn) => conn.sobject('Member__c').update({
        Id: sfMemberId,
        Badge_ID__c: badgeId,
      }));
      console.log(`[receiver] Updated Member__c ${sfMemberId} Badge_ID__c: ${badgeId}`);
    }
  } catch (err) {
    console.log(`[receiver] Error in handleBadgeAssigned: ${err}`);
    throw err;
  }
}

  async handleRefundProcessed(header, body) {
    try {
      const masterUuid = header ? header.master_uuid : null;
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
          masterUuid ? `Master UUID: ${masterUuid}` : null,
          originalTxId ? `Original Transaction ID: ${originalTxId}` : null,
          newWallet ? `New Wallet Balance: ${newWallet}` : null,
          userId ? `User ID: ${userId}` : null,
        ].filter(Boolean).join('\n'),
        Status: 'Completed',
        Type: 'Other',
        ActivityDate: new Date().toISOString().split('T')[0],
      };

    // 4. De juiste persoon zoeken in Salesforce
    const email = ReceiverV2.getElementText(body, 'email');
    
    if (masterUuid) {
      // Prioriteit 1: Zoeken op de nieuwe Master UUID
      const contactId = await this._findUserByMasterUuid(masterUuid);
      if (contactId) taskData.WhoId = contactId;
    } 
    
    if (!taskData.WhoId && email) {
      // Prioriteit 2: Fallback op email als de UUID nog niet gekoppeld is
      const contactId = await this._findUserByEmail(email);
      if (contactId) taskData.WhoId = contactId;
    }

    // 5. Task aanmaken
    const result = await this.sf.apiCall((conn) => conn.sobject('Task').create(taskData));
    console.log(`[receiver] Created Task for refund_processed: ${result?.id}`);

  } catch (err) {
    console.log(`[receiver] Error in handleRefundProcessed: ${err}`);
    throw err;
  }
}

  async handleInvoiceRequestFromKassa(header, body) {
  try {
    const invoice = body ? body.invoice : null;
    
    // 1. Identificatie: Pak de UUID uit de header of de body
    const masterUuid = header.master_uuid || ReceiverV2.getElementText(body, 'master_uuid');

    const taskData = {
      Subject: `Invoice request [Kassa]: ${ReceiverV2.getElementText(invoice, 'id') || 'N/A'}`,
      Description: [
        `Invoice ID: ${ReceiverV2.getElementText(invoice, 'id')}`,
        `Amount Paid: ${ReceiverV2.getElementText(invoice, 'amount_paid')}`,
        `Status: ${ReceiverV2.getElementText(invoice, 'status')}`,
        `Due Date: ${ReceiverV2.getElementText(invoice, 'due_date')}`,
        masterUuid ? `Master UUID: ${masterUuid}` : null,
      ].filter(Boolean).join('\n'),
      Status: 'Completed',
      Type: 'Other',
      ActivityDate: new Date().toISOString().split('T')[0],
    };

    const email = ReceiverV2.getElementText(body, 'email');
    const amountPaidVal = invoice ? invoice.amount_paid : null;
    const amountPaid = amountPaidVal !== null && typeof amountPaidVal === 'object' ? amountPaidVal['#text'] : amountPaidVal;
    const currency = amountPaidVal !== null && typeof amountPaidVal === 'object' ? (amountPaidVal.currency || 'eur') : 'eur';

    // 2. Salesforce koppeling
    if (!this.sf.isConnected) {
      console.log(`[receiver] DRY RUN: Would create Task for UUID: ${masterUuid}`);
    } else {
      let contactId = null;

      if (masterUuid) {
        // Zoek eerst op de nieuwe UUID
        contactId = await this._findUserByMasterUuid(masterUuid);
      } 
      
      if (!contactId && email) {
        // Fallback op email
        contactId = await this._findUserByEmail(email);
      }

      if (contactId) {
        taskData.WhoId = contactId;
      }

      const sfResult = await this.sf.apiCall((conn) => conn.sobject('Task').create(taskData));
      console.log(`[receiver] Created Task for invoice_request: ${sfResult?.id}`);
    }

    // 3. Doorsturen naar Facturatie (Sender)
    // BELANGRIJK: We voegen master_uuid toe aan de payload voor de Facturatie-module
    await this.sender.sendInvoiceRequest({
      correlation_id: header.message_id,
      master_uuid: masterUuid, // De lijm voor FossBilling
      customer: {
        email: email || '',
        first_name: ReceiverV2.getElementText(body, 'first_name') || '',
        last_name: ReceiverV2.getElementText(body, 'last_name') || '',
      },
      invoice: {
        description: `Invoice ${ReceiverV2.getElementText(invoice, 'id') || 'N/A'}`,
        amount: parseFloat(amountPaid) || 0,
        currency,
        due_date: ReceiverV2.getElementText(invoice, 'due_date') || new Date().toISOString().split('T')[0],
        invoice_number: ReceiverV2.getElementText(invoice, 'id') || undefined,
      },
      items: [],
    });
    
    console.log(`[receiver] Forwarded invoice_request to facturatie for UUID: ${masterUuid}`);
  } catch (err) {
    console.log(`[receiver] Error in handleInvoiceRequestFromKassa: ${err}`);
    throw err;
  }
}

  async shutdown() {
    console.log('[receiver] Signal received, shutting down gracefully...');
    this.running = false;

    try {
      if (this.channel) await this.channel.close();
    } catch (err) {
      console.log(`[receiver] Error closing channel: ${err}`);
    }

    try {
      if (this.connection) await this.connection.close();
    } catch (err) {
      console.log(`[receiver] Error closing connection: ${err}`);
    }

    try {
      await this.sender.close();
    } catch (err) {
      console.log(`[receiver] Error closing sender connection: ${err}`);
    }

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

module.exports = ReceiverV2;

if (require.main === module) {
  main();
}



