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
const FACTURATIE_TO_CRM_QUEUE = 'facturatie.to.crm';
const DEAD_LETTER_QUEUE = 'crm.dead-letter';
const USER_REGISTERED_QUEUE = 'user.registered';
const USER_CREATED_QUEUE = 'user.created';
const IDENTITY_EVENTS_EXCHANGE = 'user.events';
const IDENTITY_EVENTS_QUEUE = 'crm.identity.user.events';
const PLANNING_EXCHANGE = 'planning.exchange';
const PLANNING_SESSION_QUEUE = 'planning.session.events';
const PLANNING_SESSION_ROUTING_KEYS = [
  'planning.session.created',
  'planning.session.updated',
  'planning.session.deleted',
];
const KASSA_EXCHANGE = 'kassa.exchange';
const KASSA_ROUTING_KEYS = ['kassa.payments.#', 'kassa.to.crm.#'];

const MESSAGE_TYPES = {
  USER_CREATED: 'user.created',
  USER_REGISTERED: 'user.registered',
  NEW_REGISTRATION: 'new_registration',
  USER_UNREGISTERED: 'user.unregistered',
  PAYMENT_REGISTERED: 'payment_registered',
  BADGE_SCANNED: 'badge_scanned',
  SESSION_UPDATE: 'session_updated',
  SESSION_CREATED: 'session.created',
  SESSION_UPDATED: 'session.updated',
  SESSION_DELETED: 'session.deleted',
  INVOICE_STATUS: 'invoice_status',
  SEND_INVOICE: 'send_invoice',
  MAILING_STATUS: 'mailing_status',
  CONSUMPTION_ORDER: 'consumption_order',
  BADGE_ASSIGNED: 'badge_assigned',
  REFUND_PROCESSED: 'refund_processed',
  INVOICE_REQUEST: 'invoice_request',
  INVOICE_CANCELLED: 'invoice_cancelled',
  USER_UPDATED: 'user.updated',
  DELETE_USER: 'delete_user',
  USER_DELETED: 'user_deleted',
  COMPANY_REGISTRATION: 'company_registration',
  COMPANY_UPDATE: 'company_update',
  COMPANY_DELETE: 'company_delete',
  CANCEL_REGISTRATION: 'cancel_registration',
};

const LAZY_MASTER_UUID_TYPES = new Set([
  MESSAGE_TYPES.USER_CREATED,
  MESSAGE_TYPES.USER_REGISTERED,
  MESSAGE_TYPES.NEW_REGISTRATION,
  MESSAGE_TYPES.PAYMENT_REGISTERED,
  MESSAGE_TYPES.BADGE_SCANNED,
  MESSAGE_TYPES.INVOICE_STATUS,
  MESSAGE_TYPES.SEND_INVOICE,
  MESSAGE_TYPES.CONSUMPTION_ORDER,
  MESSAGE_TYPES.BADGE_ASSIGNED,
  MESSAGE_TYPES.REFUND_PROCESSED,
  MESSAGE_TYPES.INVOICE_REQUEST,
  MESSAGE_TYPES.INVOICE_CANCELLED,
  MESSAGE_TYPES.USER_UPDATED,
  MESSAGE_TYPES.DELETE_USER,
  MESSAGE_TYPES.USER_DELETED,
]);

const PLANNING_SESSION_TYPES = new Set([
  MESSAGE_TYPES.SESSION_CREATED,
  MESSAGE_TYPES.SESSION_UPDATED,
  MESSAGE_TYPES.SESSION_DELETED,
]);

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '',
  textNodeName: '#text',
  parseTagValue: false,
  parseAttributeValue: false,
});

const TYPES_ACCEPTING_V1 = new Set([
  'user.unregistered',
  'user.created',
  'user.registered',
]);

const BASE_HEADER_FIELDS = ['message_id', 'version', 'type', 'timestamp', 'source'];

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
        await this.channel.assertExchange(KASSA_EXCHANGE, 'topic', { durable: true });
        await this.channel.assertQueue(KASSA_QUEUE, { durable: true });
        for (const rk of KASSA_ROUTING_KEYS) {
          await this.channel.bindQueue(QUEUE_NAME, KASSA_EXCHANGE, rk);
        }
        await this.channel.assertQueue(FACTURATIE_TO_CRM_QUEUE, { durable: true });
        await this.channel.assertQueue(USER_REGISTERED_QUEUE, { durable: true });
        await this.channel.assertQueue(USER_CREATED_QUEUE, { durable: true });
        await this.channel.assertQueue(DEAD_LETTER_QUEUE, {
          durable: true,
          arguments: { 'x-dead-letter-exchange': '' },
        });

        await this.channel.assertExchange(IDENTITY_EVENTS_EXCHANGE, 'fanout', { durable: true });
        await this.channel.assertQueue(IDENTITY_EVENTS_QUEUE, { durable: true });
        await this.channel.bindQueue(IDENTITY_EVENTS_QUEUE, IDENTITY_EVENTS_EXCHANGE, '');

        await this.channel.assertExchange(PLANNING_EXCHANGE, 'topic', { durable: true });
        await this.channel.assertQueue(PLANNING_SESSION_QUEUE, {
          durable: true,
          arguments: { 'x-dead-letter-exchange': 'planning.dlx' },
        });
        for (const routingKey of PLANNING_SESSION_ROUTING_KEYS) {
          await this.channel.bindQueue(PLANNING_SESSION_QUEUE, PLANNING_EXCHANGE, routingKey);
        }

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
        this.channel.consume(FACTURATIE_TO_CRM_QUEUE, consume, { noAck: false });
        this.channel.consume(USER_CREATED_QUEUE, consume, { noAck: false });
        this.channel.consume(USER_REGISTERED_QUEUE, consume, { noAck: false });
        this.channel.consume(PLANNING_SESSION_QUEUE, consume, { noAck: false });
        this.channel.consume(IDENTITY_EVENTS_QUEUE, (msg) => this.handleIdentityUserEvent(msg), { noAck: false });

        console.log(`[receiver] Connected to RabbitMQ, listening on: ${QUEUE_NAME}, ${KASSA_QUEUE}, ${FACTURATIE_TO_CRM_QUEUE}, ${PLANNING_SESSION_QUEUE}, ${IDENTITY_EVENTS_QUEUE}`);

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

    const messageType = header.type;
    const needsReceiver = messageType === MESSAGE_TYPES.USER_UNREGISTERED;
    const skipMasterUuid = needsReceiver || LAZY_MASTER_UUID_TYPES.has(messageType) || PLANNING_SESSION_TYPES.has(messageType);
    const requiredFields = needsReceiver
      ? [...BASE_HEADER_FIELDS, 'receiver']
      : skipMasterUuid
        ? BASE_HEADER_FIELDS
        : [...BASE_HEADER_FIELDS, 'master_uuid'];

    const missingFields = requiredFields.filter((f) => header[f] === undefined || header[f] === null);
    if (missingFields.length > 0) {
      return [false, `Missing required header fields: ${missingFields.join(', ')}`];
    }
    const validVersions = TYPES_ACCEPTING_V1.has(messageType) ? ['1.0', '2.0'] : ['2.0'];
    if (!validVersions.includes(String(header.version))) {
      return [false, `Invalid version: expected ${validVersions.join(' or ')}, got ${header.version}`];
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

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(async () => {
        try {
          if (consumerTag) await this.channel.cancel(consumerTag);
          if (replyQueueName) await this.channel.deleteQueue(replyQueueName);
        } catch (err) {
          console.error("[identity] Cleanup error during timeout:", err);
        }
        reject(new Error(`Identity Service timeout voor ${email}`));
      }, 15000);

      this.channel.assertQueue('', { exclusive: true })
        .then((replyQueue) => {
          replyQueueName = replyQueue.queue;

          return this.channel.consume(replyQueueName, async (msg) => {
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
      await this.routeMessage(header, body, xmlContent);

      this.channel.ack(msg);
      console.log(`[receiver] Message processed successfully: ${messageId}`);
    } catch (err) {
      console.log(`[receiver] Unexpected error: ${err}`);
      this.channel.nack(msg, false, false);
    }
  }

  async routeMessage(header, body, rawXml = null) {
    const msgType = header.type;
    const handlers = {
      [MESSAGE_TYPES.USER_CREATED]: () => this.handleUserCreated(header, body),
      [MESSAGE_TYPES.USER_REGISTERED]: () => this.handleUserRegistered(header, body),
      [MESSAGE_TYPES.NEW_REGISTRATION]: () => this.handleNewRegistration(header, body),
      [MESSAGE_TYPES.USER_UNREGISTERED]: () => this.handleUserUnregistered(header, body),
      [MESSAGE_TYPES.PAYMENT_REGISTERED]: () => this.handlePaymentRegistered(header, body, rawXml),
      [MESSAGE_TYPES.BADGE_SCANNED]: () => this.handleBadgeScanned(header, body),
      [MESSAGE_TYPES.SESSION_CREATED]: () => this.handlePlanningSessionEvent(header, body),
      [MESSAGE_TYPES.SESSION_UPDATED]: () => this.handlePlanningSessionEvent(header, body),
      [MESSAGE_TYPES.SESSION_DELETED]: () => this.handlePlanningSessionEvent(header, body),
      [MESSAGE_TYPES.INVOICE_STATUS]: () => this.handleInvoiceStatus(header, body),
      [MESSAGE_TYPES.SEND_INVOICE]: () => this.handleSendInvoice(header, body),
      [MESSAGE_TYPES.MAILING_STATUS]: () => this.handleMailingStatus(header, body),
      [MESSAGE_TYPES.CONSUMPTION_ORDER]: () => this.handleConsumptionOrder(header, body, rawXml),
      [MESSAGE_TYPES.BADGE_ASSIGNED]: () => this.handleBadgeAssigned(header, body),
      [MESSAGE_TYPES.REFUND_PROCESSED]: () => this.handleRefundProcessed(header, body),
      [MESSAGE_TYPES.INVOICE_REQUEST]: () => this.handleInvoiceRequestFromKassa(header, body),
      [MESSAGE_TYPES.INVOICE_CANCELLED]: () => this.handleReceivedInvoiceCancelled(header, body),
      [MESSAGE_TYPES.USER_UPDATED]: () => this.handleUserUpdated(header, body),
      [MESSAGE_TYPES.DELETE_USER]: () => this.handleDeleteUser(header, body),
      [MESSAGE_TYPES.USER_DELETED]: () => this.handleDeleteUser(header, body),
      [MESSAGE_TYPES.COMPANY_REGISTRATION]: () => this.handleCompanyRegistration(header, body),
      [MESSAGE_TYPES.COMPANY_UPDATE]: () => this.handleCompanyUpdate(header, body),
      [MESSAGE_TYPES.COMPANY_DELETE]: () => this.handleCompanyDelete(header, body),
      [MESSAGE_TYPES.CANCEL_REGISTRATION]: () => this.handleCancelRegistration(header, body),
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

  async _findUserByMasterUuid(masterUuid) {
    const records = await this.sf.apiCall(
      (conn) => conn.sobject('Member__c').find({ Master_UUID__c: masterUuid }, ['Id']).limit(1)
    );
    return records && records.length > 0 ? records[0].Id : null;
  }

  _getExistingMasterUuid(header, body) {
    return (header && header.master_uuid) ||
      ReceiverV2.getElementText(body, 'master_uuid') ||
      ReceiverV2.getElementText(body, 'user_id') ||
      ReceiverV2.getElementText(body?.user, 'master_uuid') ||
      ReceiverV2.getElementText(body?.user, 'user_id') ||
      ReceiverV2.getElementText(body?.customer, 'master_uuid') ||
      ReceiverV2.getElementText(body?.company, 'master_uuid') ||
      null;
  }

  _getFallbackEmail(body, explicitEmail = null) {
    return explicitEmail ||
      ReceiverV2.getElementText(body, 'email') ||
      ReceiverV2.getElementText(body?.user, 'email') ||
      ReceiverV2.getElementText(body?.customer, 'email') ||
      null;
  }

  async resolveMasterUuid(header, body, options = {}) {
    const existingMasterUuid = this._getExistingMasterUuid(header, body);
    if (existingMasterUuid) return existingMasterUuid;

    const email = (this._getFallbackEmail(body, options.email) || '').toLowerCase().trim();
    if (!email) return null;

    const sourceSystem = options.sourceSystem || (header && header.source) || 'crm';
    console.log(`[receiver] Lazy Master UUID lookup for ${email} via ${sourceSystem}`);
    return this.getOrCreateMasterUuid(email, sourceSystem);
  }

  async handleUserUnregistered(header, body) {
    try {
      const masterUuid = ReceiverV2.getElementText(body, 'master_uuid');
      const sessionId = ReceiverV2.getElementText(body, 'session_id');
      const bodyTimestamp = ReceiverV2.getElementText(body, 'timestamp');

      if (!masterUuid || !sessionId) {
        console.log('[receiver] Missing master_uuid or session_id in user.unregistered body');
        return;
      }

      await this.sender.sendUserUnregisteredFanout({
        message_id: header.message_id,
        timestamp: header.timestamp,
        source: header.source,
        receiver: header.receiver,
        correlation_id: ReceiverV2.getElementText(header, 'correlation_id') || '',
        master_uuid: masterUuid,
        session_id: sessionId,
        body_timestamp: bodyTimestamp || header.timestamp,
      });

      console.log(`[receiver] Forwarded user.unregistered for master_uuid=${masterUuid}, session_id=${sessionId}`);
    } catch (err) {
      console.log(`[receiver] Error in handleUserUnregistered: ${err}`);
      throw err;
    }
  }

  async handleNewRegistration(header, body) {
    try {
      const customer = body ? body.customer : null;
      const contact = customer ? customer.contact : null;
      const address = customer ? customer.address : null;
      const companyData = body ? body.company : null;
      const regFee = customer ? (customer.registration_fee || customer.payment_due) : (body ? body.payment_due : null);
      const sessionId = body ? ReceiverV2.getElementText(body, 'session_id') : null;

      const getCustomerText = (key) =>
        ReceiverV2.getElementText(customer, key) || ReceiverV2.getElementText(contact, key);

      const email = getCustomerText('email');
      const firstName = getCustomerText('first_name');
      const lastName = getCustomerText('last_name');
      const externalUserId = getCustomerText('identity_uuid') || getCustomerText('user_id');
      const isCompanyLinked = getCustomerText('is_company_linked') === 'true';
      const rawType = getCustomerText('type');
      const userType = (isCompanyLinked || rawType === 'company') ? 'Bedrijf' : 'Particulier';

      const paymentFlag = regFee ? ReceiverV2.getElementText(regFee, 'paid') : null;
      const paymentState = regFee ? ReceiverV2.getElementText(regFee, 'status') : null;
      const paymentStatus = (paymentFlag === 'true' || paymentState === 'paid') ? 'paid' : 'pending';

      const amountVal = regFee ? regFee.amount : null;
      const registrationAmount = amountVal !== null && typeof amountVal === 'object'
        ? amountVal['#text']
        : (amountVal || null);

      console.log(`[receiver] Processing new_registration for: ${email}`);
      const masterUuid = await this.getOrCreateMasterUuid(email, header.source || 'frontend.drupal');

      const userData = {
        Master_UUID__c: masterUuid,
        User_ID__c: externalUserId,
        First_Name__c: firstName,
        Last_Name__c: lastName,
        Email__c: email,
        Birthdate__c: getCustomerText('date_of_birth'),
        User_Type__c: userType,
        Street__c: address ? ReceiverV2.getElementText(address, 'street') : null,
        House_Number__c: address ? ReceiverV2.getElementText(address, 'number') : null,
        Postal_Code__c: address ? ReceiverV2.getElementText(address, 'postal_code') : null,
        City__c: address ? ReceiverV2.getElementText(address, 'city') : null,
        Country_Code__c: address ? (ReceiverV2.getElementText(address, 'country') || '').toUpperCase() || null : null,
        Badge_ID__c: getCustomerText('badge_id') || null,
      };

      let companyId = null;
      if ((isCompanyLinked || rawType === 'company') && companyData) {
        const companyName = ReceiverV2.getElementText(companyData, 'name');
        const companyVat = ReceiverV2.getElementText(companyData, 'vat_number');
        const companyEmail = ReceiverV2.getElementText(companyData, 'email');

        if (companyName && companyVat && this.sf.isConnected) {
          const result = await this.sf.apiCall((conn) =>
            conn.sobject('Account').upsert({
              Master_UUID__c: masterUuid,
              Company_Name__c: companyName,
              VAT_Number__c: companyVat,
              Email__c: companyEmail || null,
              Billing_Street__c: address ? ReceiverV2.getElementText(address, 'street') : null,
              Billing_City__c: address ? ReceiverV2.getElementText(address, 'city') : null,
            }, 'VAT_Number__c')
          );
          companyId = result.id || null;
        }
      }

      if (companyId) userData.Account__c = companyId;

      if (this.sf.isConnected) {
        await this.sf.apiCall((conn) => conn.sobject('Member__c').upsert(userData, 'Master_UUID__c'));
      }

      const kassaPayload = {
        customer: {
          identity_uuid: masterUuid,
          email: email,
          date_of_birth: getCustomerText('date_of_birth') || '',
          first_name: firstName || '',
          last_name: lastName || '',
          type: (isCompanyLinked || rawType === 'company') ? 'company' : 'private',
          company_name: companyData ? ReceiverV2.getElementText(companyData, 'name') : null,
          vat_number: companyData ? ReceiverV2.getElementText(companyData, 'vat_number') : null,
          session_id: sessionId || '',
          payment_due: {
            amount: registrationAmount || '0.00',
            status: paymentStatus === 'paid' ? 'paid' : 'unpaid',
          },
        },
      };
      await this.sender.sendNewRegistrationToKassa(kassaPayload);

      const fossPayload = {
        master_uuid: masterUuid,
        customer: {
          first_name: firstName,
          last_name: lastName,
          email: email,
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
        payment_due: {
          amount: registrationAmount ? parseFloat(registrationAmount) : 0,
          status: paymentStatus,
        }
      };
      await this.sender.sendNewRegistrationToFacturatie(fossPayload);

    } catch (err) {
      console.log(`[receiver] Error in handleNewRegistration: ${err.message}`);
      throw err;
    }
  }

  async handleUserCreated(header, body) {
    try {
      const user = body?.user;
      if (!user) throw new Error('Body missing user element');

      const email = (ReceiverV2.getElementText(user, 'email') || '').toLowerCase().trim();
      const firstName = ReceiverV2.getElementText(user, 'first_name');
      const lastName = ReceiverV2.getElementText(user, 'last_name');
      const isCompany = ReceiverV2.getElementText(user, 'is_company') === 'true';

      const masterUuid = await this.getOrCreateMasterUuid(email, 'frontend.drupal');

      if (this.sf.isConnected) {
        const sfData = {
          Master_UUID__c: masterUuid,
          First_Name__c: firstName,
          Last_Name__c: lastName,
          Email__c: email,
          User_Type__c: isCompany ? 'Bedrijf' : 'Particulier'
        };
        await this.sf.apiCall((conn) => conn.sobject('Member__c').upsert(sfData, 'Master_UUID__c'));
      }
    } catch (err) {
      console.error(`[receiver] Error in handleUserCreated: ${err.message}`);
      throw err;
    }
  }

  async handleUserRegistered(header, body) {
    try {
      const user = body?.user;
      const session = body?.session;
      if (!user || !session) throw new Error('Body missing user or session');

      const email = (ReceiverV2.getElementText(user, 'email') || '').toLowerCase().trim();
      const sessionId = ReceiverV2.getElementText(session, 'session_id');
      const sessionName = ReceiverV2.getElementText(session, 'session_name');
      const paymentStatus = ReceiverV2.getElementText(body, 'payment_status');

      const masterUuid = await this.getOrCreateMasterUuid(email, 'frontend.drupal');

      if (this.sf.isConnected) {
        await this.sf.apiCall((conn) =>
          conn.sobject('Member__c').upsert({
            Master_UUID__c: masterUuid,
            First_Name__c: ReceiverV2.getElementText(user, 'first_name'),
            Last_Name__c: ReceiverV2.getElementText(user, 'last_name'),
            Email__c: email
          }, 'Master_UUID__c')
        );

        await this.sf.apiCall((conn) =>
          conn.sobject('Task').create({
            Subject: `Sessie Inschrijving: ${sessionName}`,
            Description: `ID: ${sessionId} | Status: ${paymentStatus}`,
            Status: 'Completed',
            Master_UUID__c: masterUuid,
            ActivityDate: new Date().toISOString().split('T')[0]
          })
        );
      }
    } catch (err) {
      console.error(`[receiver] Error in handleUserRegistered: ${err.message}`);
      throw err;
    }
  }

  async handleCompanyRegistration(header, body) {
    try {
      const company = body?.company;
      if (!company) throw new Error('Body missing company element');

      const masterUuid = header.master_uuid || ReceiverV2.getElementText(company, 'master_uuid');
      const email = (ReceiverV2.getElementText(company, 'email') || '').toLowerCase().trim();

      const sfCompanyData = {
        Master_UUID__c: masterUuid,
        Company_Name__c: ReceiverV2.getElementText(company, 'name'),
        Email__c: email,
        VAT_Number__c: ReceiverV2.getElementText(company, 'vat_number'),
        VAT_Rate__c: parseFloat(ReceiverV2.getElementText(company, 'vat_rate') || 0),
        User_Type__c: 'Bedrijf',
        Company_ID__c: header.message_id
      };

      if (this.sf.isConnected) {
        await this.sf.apiCall((conn) => conn.sobject('Member__c').upsert(sfCompanyData, 'Master_UUID__c'));
      }
    } catch (err) {
      console.error(`[receiver] Error in handleCompanyRegistration: ${err.message}`);
      throw err;
    }
  }

  async handleCompanyUpdate(header, body) {
    try {
      const company = body?.company;
      const companyUuid = header.master_uuid;
      if (!company || !companyUuid) return;

      let accountId = null;
      if (this.sf.isConnected) {
        const result = await this.sf.apiCall((conn) =>
          conn.sobject('Account').upsert({
            Master_UUID__c: companyUuid,
            Company_Name__c: ReceiverV2.getElementText(company, 'name'),
            VAT_Number__c: ReceiverV2.getElementText(company, 'vat_number'),
            Email__c: ReceiverV2.getElementText(company, 'email'),
            User_Type__c: 'Bedrijf'
          }, 'Master_UUID__c')
        );
        accountId = result.id;
      }

      const membersNode = company.members?.member;
      if (membersNode && accountId) {
        const memberList = Array.isArray(membersNode) ? membersNode : [membersNode];
        for (const memberData of memberList) {
          const userUuid = ReceiverV2.getElementText(memberData, 'master_uuid');
          const action = memberData.action;
          if (!userUuid) continue;

          const memberSfId = await this._findUserByMasterUuid(userUuid);
          if (memberSfId) {
            await this.sf.apiCall((conn) =>
              conn.sobject('Member__c').update({
                Id: memberSfId,
                Account__c: action === 'add' ? accountId : null
              })
            );
          }
        }
      }
    } catch (err) {
      console.error(`[receiver] Error in handleCompanyUpdate: ${err.message}`);
      throw err;
    }
  }

  async handleCompanyDelete(header, body) {
    try {
      const companyUuid = header.master_uuid || ReceiverV2.getElementText(body?.company, 'master_uuid');
      if (!companyUuid) return;

      if (this.sf.isConnected) {
        const records = await this.sf.apiCall((conn) =>
          conn.sobject('Account').find({ Master_UUID__c: companyUuid }, ['Id']).limit(1)
        );
        const accountId = records && records.length > 0 ? records[0].Id : null;
        if (accountId) {
          await this.sf.apiCall((conn) =>
            conn.sobject('Account').update({ Id: accountId, Status__c: 'Inactive' })
          );
        }
      }
    } catch (err) {
      console.error(`[receiver] Error in handleCompanyDelete: ${err.message}`);
      throw err;
    }
  }

  async handleSendInvoice(header, body) {
    try {
      const customer = body ? body.customer : null;
      const invoice = body ? body.invoice : null;
      const email = ReceiverV2.getElementText(customer, 'email');
      const masterUuid = await this.resolveMasterUuid(header, body, { email, sourceSystem: 'facturatie' });
      const invoiceUrl = ReceiverV2.getElementText(invoice, 'pdf_url');
      const dueDate = ReceiverV2.getElementText(invoice, 'due_date');
      const invoiceNumber = ReceiverV2.getElementText(invoice, 'id');

      if (!this.sf.isConnected) return;

      let memberId = await this._findUserByMasterUuid(masterUuid);
      if (!memberId && email) memberId = await this._findUserByEmail(email);

      if (memberId) {
        await this.sf.apiCall((conn) =>
          conn.sobject('Member__c').update({
            Id: memberId,
            Last_Invoice_URL__c: invoiceUrl,
            Last_Invoice_Due_Date__c: dueDate,
            Last_Invoice_Number__c: invoiceNumber
          })
        );
      }
    } catch (err) {
      console.error(`[receiver] Error in handleSendInvoice: ${err}`);
      throw err;
    }
  }

  async handleReceivedInvoiceCancelled(header, body) {
    try {
      const masterUuid = await this.resolveMasterUuid(header, body);
      const invoiceId = ReceiverV2.getElementText(body, 'invoice_id') || ReceiverV2.getElementText(body, 'invoice_number');

      if (this.sf.isConnected) {
        const memberId = await this._findUserByMasterUuid(masterUuid);
        if (memberId) {
          await this.sf.apiCall((conn) =>
            conn.sobject('Member__c').update({
              Id: memberId,
              Status__c: 'Cancelled'
            })
          );
        }
      }
    } catch (err) {
      console.error(`[receiver] Error in handleReceivedInvoiceCancelled: ${err}`);
    }
  }

  async handlePaymentRegistered(header, body, rawXml = null) {
    try {
      const invoice = body ? body.invoice : null;
      const transaction = body ? body.transaction : null;
      const paymentContext = ReceiverV2.getElementText(body, 'payment_context') || 'unknown';
      const email = ReceiverV2.getElementText(body, 'email') || (body?.customer ? ReceiverV2.getElementText(body.customer, 'email') : null);
      const masterUuid = await this.resolveMasterUuid(header, body, { email });

      const amountVal = body?.amount_paid || (invoice ? invoice.amount_paid : null);
      const amountPaid = typeof amountVal === 'object' ? amountVal['#text'] : (amountVal || '0.00');
      const invoiceId = ReceiverV2.getElementText(invoice, 'id');
      const transactionId = transaction ? ReceiverV2.getElementText(transaction, 'id') : null;
      const paymentMethod = transaction ? ReceiverV2.getElementText(transaction, 'method') : 'unknown';
      const paidAt = transaction ? ReceiverV2.getElementText(transaction, 'timestamp') : null;

      const taskData = {
        Subject: `Payment registered [${paymentContext}] invoice: ${invoiceId || 'N/A'}`,
        Description: [
          `Context: ${paymentContext}`,
          `Payment Method: ${paymentMethod}`,
          transactionId ? `Transaction ID: ${transactionId}` : null,
          `Amount Paid: ${amountPaid}`,
          paidAt ? `Paid At: ${paidAt}` : null,
          masterUuid ? `Master UUID: ${masterUuid}` : null,
        ].filter(Boolean).join('\n'),
        Status: 'Completed',
        ActivityDate: new Date().toISOString().split('T')[0],
      };

      if (header.source === 'kassa' && rawXml) {
        await this.sender.sendPaymentRegisteredToFrontend(rawXml);
        await this.sender.sendPaymentRegisteredToFacturatie(rawXml);
      }

      // If this was a registration payment, notify Planning (section 21.1)
      if (paymentContext === 'registration' || paymentContext === 'session_registration') {
        const sessionId = ReceiverV2.getElementText(body, 'session_id') || (invoice ? ReceiverV2.getElementText(invoice, 'session_id') : null);
        if (sessionId && masterUuid) {
          await this.sender.sendSessionRegistrationConfirmed({
            session_id: sessionId,
            identity_uuid: masterUuid,
            correlation_id: header.message_id
          });
        }
      }

      if (this.sf.isConnected) {
        let contactId = await this._findUserByMasterUuid(masterUuid);
        if (!contactId && email) contactId = await this._findUserByEmail(email);
        if (contactId) taskData.WhoId = contactId;
        await this.sf.apiCall((conn) => conn.sobject('Task').create(taskData));
      }
    } catch (err) {
      console.log(`[receiver] Error in handlePaymentRegistered: ${err}`);
      throw err;
    }
  }

  async handleBadgeScanned(header, body) {
    try {
      const email = ReceiverV2.getElementText(body, 'email');
      const masterUuid = await this.resolveMasterUuid(header, body, { email });

      const taskData = {
        Subject: `Badge scanned: ${ReceiverV2.getElementText(body, 'badge_id')}`,
        Description: [
          `Scan Type: ${ReceiverV2.getElementText(body, 'scan_type')}`,
          `Location: ${ReceiverV2.getElementText(body, 'location')}`,
          `Email: ${email}`,
          masterUuid ? `Master UUID: ${masterUuid}` : null,
        ].filter(Boolean).join('\n'),
        Status: 'Completed',
        ActivityDate: new Date().toISOString().split('T')[0],
      };

      if (this.sf.isConnected) {
        let contactId = await this._findUserByMasterUuid(masterUuid);
        if (!contactId && email) contactId = await this._findUserByEmail(email);
        if (contactId) taskData.WhoId = contactId;
        await this.sf.apiCall((conn) => conn.sobject('Task').create(taskData));
      }
    } catch (err) {
      console.log(`[receiver] Error in handleBadgeScanned: ${err}`);
      throw err;
    }
  }

  async handlePlanningSessionEvent(header, body) {
    try {
      const sessionId = ReceiverV2.getElementText(body, 'session_id');
      if (!sessionId) return;

      if (header.type === MESSAGE_TYPES.SESSION_DELETED) {
        await this.sender.sendEventEndedToFacturatie({
          session_id: sessionId,
          ended_at: ReceiverV2.getElementText(body, 'end_time') || header.timestamp,
        });
      }
    } catch (err) {
      console.log(`[receiver] Error in handlePlanningSessionEvent: ${err}`);
      throw err;
    }
  }

  async handleInvoiceStatus(header, body) {
    try {
      const invoice = body ? body.invoice : null;
      const invoiceId = ReceiverV2.getElementText(body, 'invoice_id') || ReceiverV2.getElementText(invoice, 'id');
      const status = ReceiverV2.getElementText(body, 'status') || ReceiverV2.getElementText(invoice, 'status');
      const amount = ReceiverV2.getElementText(body, 'amount') || ReceiverV2.getElementText(invoice, 'amount_paid');
      const email = ReceiverV2.getElementText(body, 'email');
      const masterUuid = await this.resolveMasterUuid(header, body, { email });

      const taskData = {
        Subject: `Invoice status update: ${invoiceId}`,
        Description: `Status: ${status}\nAmount: ${amount}\nMaster UUID: ${masterUuid}`,
        Status: 'Completed',
        ActivityDate: new Date().toISOString().split('T')[0],
      };

      if (this.sf.isConnected) {
        let contactId = await this._findUserByMasterUuid(masterUuid);
        if (!contactId && email) contactId = await this._findUserByEmail(email);
        if (contactId) taskData.WhoId = contactId;
        await this.sf.apiCall((conn) => conn.sobject('Task').create(taskData));
      }
    } catch (err) {
      console.log(`[receiver] Error in handleInvoiceStatus: ${err}`);
      throw err;
    }
  }

  async handleMailingStatus(header, body) {
    try {
      const taskData = {
        Subject: `Mailing status: ${ReceiverV2.getElementText(body, 'mailing_id')}`,
        Description: `Status: ${ReceiverV2.getElementText(body, 'status')}\nDelivered: ${ReceiverV2.getElementText(body, 'delivered')}`,
        Status: 'Completed',
        ActivityDate: new Date().toISOString().split('T')[0],
      };
      if (this.sf.isConnected) {
        await this.sf.apiCall((conn) => conn.sobject('Task').create(taskData));
      }
    } catch (err) {
      console.log(`[receiver] Error in handleMailingStatus: ${err}`);
      throw err;
    }
  }

  async handleConsumptionOrder(header, body, rawXml = null) {
    try {
      const isAnonymous = ReceiverV2.getElementText(body, 'is_anonymous') === 'true';
      const customer = body ? body.customer : null;
      const items = body ? body.items : null;
      const itemList = items ? (Array.isArray(items.item) ? items.item : [items.item]).filter(Boolean) : [];

      let memberId = null;
      if (!isAnonymous && customer) {
        const email = ReceiverV2.getElementText(customer, 'email');
        const masterUuid = await this.resolveMasterUuid(header, body, { email });
        memberId = await this._findUserByMasterUuid(masterUuid);
        if (!memberId && email) memberId = await this._findUserByEmail(email);
      }

      if (this.sf.isConnected) {
        for (let i = 0; i < itemList.length; i++) {
          const item = itemList[i];
          const unitPrice = parseFloat(ReceiverV2.getElementText(item, 'unit_price')) || 0;
          const qty = parseInt(ReceiverV2.getElementText(item, 'quantity'), 10) || 1;

          const consumptionData = {
            Consumption_ID__c: ReceiverV2.getElementText(item, 'id') || `${header.message_id}-${i}`,
            Product_Name__c: String(ReceiverV2.getElementText(item, 'description')),
            Quantity__c: qty,
            Total_Amount__c: unitPrice * qty,
            Price_Per_Unit__c: unitPrice,
            Product_SKU__c: ReceiverV2.getElementText(item, 'sku'),
            VAT_Rate__c: parseFloat(ReceiverV2.getElementText(item, 'vat_rate')) || null,
          };
          if (memberId) consumptionData.Member__c = memberId;
          await this.sf.apiCall((conn) => conn.sobject('Consumption__c').upsert(consumptionData, 'Consumption_ID__c'));
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
      const email = ReceiverV2.getElementText(body, 'email');
      const masterUuid = await this.resolveMasterUuid(header, body, { email });

      if (this.sf.isConnected) {
        const memberId = await this._findUserByMasterUuid(masterUuid);
        if (memberId) {
          await this.sf.apiCall((conn) => conn.sobject('Member__c').update({ Id: memberId, Badge_ID__c: badgeId }));
        }
      }
    } catch (err) {
      console.log(`[receiver] Error in handleBadgeAssigned: ${err}`);
      throw err;
    }
  }

  async handleRefundProcessed(header, body) {
    try {
      const refund = body ? body.refund : null;
      const email = ReceiverV2.getElementText(body, 'email');
      const masterUuid = await this.resolveMasterUuid(header, body, { email });

      const taskData = {
        Subject: `Refund processed: ${ReceiverV2.getElementText(refund, 'amount')}`,
        Description: `Reason: ${ReceiverV2.getElementText(refund, 'reason')}\nMaster UUID: ${masterUuid}`,
        Status: 'Completed',
        ActivityDate: new Date().toISOString().split('T')[0],
      };

      if (this.sf.isConnected) {
        let contactId = await this._findUserByMasterUuid(masterUuid);
        if (!contactId && email) contactId = await this._findUserByEmail(email);
        if (contactId) taskData.WhoId = contactId;
        await this.sf.apiCall((conn) => conn.sobject('Task').create(taskData));
      }
    } catch (err) {
      console.log(`[receiver] Error in handleRefundProcessed: ${err}`);
      throw err;
    }
  }

  async handleInvoiceRequestFromKassa(header, body) {
    try {
      const invoiceData = body ? body.invoice_data : null;
      const contact = invoiceData ? invoiceData.contact : null;
      const email = ReceiverV2.getElementText(body, 'email') || (invoiceData ? ReceiverV2.getElementText(invoiceData, 'email') : null);
      const masterUuid = await this.resolveMasterUuid(header, body, { email });

      const taskData = {
        Subject: `Invoice request [Kassa]`,
        Description: `Master UUID: ${masterUuid}`,
        Status: 'Completed',
        ActivityDate: new Date().toISOString().split('T')[0],
      };

      if (this.sf.isConnected) {
        let contactId = await this._findUserByMasterUuid(masterUuid);
        if (!contactId && email) contactId = await this._findUserByEmail(email);
        if (contactId) taskData.WhoId = contactId;
        await this.sf.apiCall((conn) => conn.sobject('Task').create(taskData));
      }

      await this.sender.sendInvoiceRequest({
        identity_uuid: masterUuid,
        correlation_id: header.correlation_id || header.message_id,
        invoice_data: {
          first_name: contact ? ReceiverV2.getElementText(contact, 'first_name') : '',
          last_name: contact ? ReceiverV2.getElementText(contact, 'last_name') : '',
          email: email || '',
          address: invoiceData ? {
            street:      ReceiverV2.getElementText(invoiceData.address, 'street') || '',
            number:      ReceiverV2.getElementText(invoiceData.address, 'number') || '',
            postal_code: ReceiverV2.getElementText(invoiceData.address, 'postal_code') || '',
            city:        ReceiverV2.getElementText(invoiceData.address, 'city') || '',
            country:     ReceiverV2.getElementText(invoiceData.address, 'country') || '',
          } : { street: '', number: '', postal_code: '', city: '', country: '' },
          company_name: invoiceData ? ReceiverV2.getElementText(invoiceData, 'company_name') : null,
          vat_number:   invoiceData ? ReceiverV2.getElementText(invoiceData, 'vat_number') : null,
        },
      });
    } catch (err) {
      console.log(`[receiver] Error in handleInvoiceRequestFromKassa: ${err}`);
      throw err;
    }
  }

  async handleUserUpdated(header, body) { console.log('[receiver] user.updated received'); }
  async handleDeleteUser(header, body) { console.log('[receiver] delete_user received'); }
  async handleCancelRegistration(header, body) { console.log('[receiver] cancel_registration received'); }
  async handleIdentityUserEvent(msg) {
    try {
      const content = msg.content.toString();
      console.log(`[receiver] Received Identity Fanout event: ${content.substring(0, 100)}...`);
      this.channel.ack(msg);
    } catch (err) {
      console.error(`[receiver] Identity Fanout error: ${err.message}`);
      this.channel.nack(msg, false, false);
    }
  }

  sendToDeadLetter(content, reason) {
    if (this.channel) {
      const deadLetterContent = Buffer.from(JSON.stringify({
        reason,
        originalContent: content.toString('utf8'),
        timestamp: new Date().toISOString(),
      }));
      this.channel.sendToQueue(DEAD_LETTER_QUEUE, deadLetterContent, { persistent: true });
    }
  }

  static getElementText(obj, key) {
    if (!obj || obj[key] === undefined || obj[key] === null) return null;
    const value = obj[key];
    if (typeof value === 'object' && value['#text'] !== undefined) return value['#text'];
    if (Array.isArray(value)) {
      const first = value[0];
      return (typeof first === 'object' && first['#text'] !== undefined) ? first['#text'] : String(first);
    }
    return String(value);
  }

  async shutdown() {
    this.running = false;
    try { if (this.channel) await this.channel.close(); } catch (err) {}
    try { if (this.connection) await this.connection.close(); } catch (err) {}
    try { await this.sender.close(); } catch (err) {}
    process.exit(0);
  }
}

async function main() {
  const receiver = new ReceiverV2();
  process.on('SIGINT', () => receiver.shutdown());
  process.on('SIGTERM', () => receiver.shutdown());
  try { await receiver.start(); } catch (err) { process.exit(1); }
}

module.exports = ReceiverV2;
if (require.main === module) main();
