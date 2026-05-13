'use strict';

require('dotenv').config();
const http = require('http');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');
const { parseStringPromise } = require('xml2js');
const { XMLParser } = require('fast-xml-parser');
const { validateXml } = require('./validator');
const { getAmqpOptions } = require('./amqpUrl');
const SFConnection = require('./sfConnection');
const CRMSender = require('./sender');
const { create } = require('xmlbuilder2');

const QUEUE_NAME = 'crm.incoming';
const KASSA_QUEUE = 'kassa.payments';
const FACTURATIE_TO_CRM_QUEUE = 'facturatie.to.crm';
const DEAD_LETTER_EXCHANGE = 'crm.dlx';
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

const MESSAGE_TYPES = {
  USER_CREATED: 'user.created',
  USER_REGISTERED: 'user.registered',
  NEW_REGISTRATION: 'new_registration',
  USER_UNREGISTERED: 'user.unregistered',
  PAYMENT_REGISTERED: 'payment_registered',
  BADGE_SCANNED: 'badge_scanned',
  SESSION_CREATED: 'session_created',
  SESSION_UPDATED: 'session_updated',
  SESSION_DELETED: 'session_deleted',
  EVENT_ENDED: 'event_ended',
  INVOICE_STATUS: 'invoice_status',
  SEND_INVOICE: 'send_invoice',
  MAILING_STATUS: 'mailing_status',
  CONSUMPTION_ORDER: 'consumption_order',
  BADGE_ASSIGNED: 'badge_assigned',
  REFUND_PROCESSED: 'refund_processed',
  INVOICE_REQUEST: 'invoice_request',
  INVOICE_CANCELLED: 'invoice_cancelled',
  USER_UPDATED: 'user_updated',
  USER_CHECKIN: 'user_checkin',
  DELETE_USER: 'delete_user',
  USER_DELETED: 'user_deleted',
  COMPANY_REGISTRATION: 'company_registration',
  COMPANY_UPDATE: 'company_update',
  COMPANY_DELETE: 'company_delete',
  CANCEL_REGISTRATION: 'cancel_registration',
  WALLET_LEASE_REQUEST: 'wallet_lease_request',
  WALLET_LEASE_RETURN: 'wallet_lease_return',
  WALLET_TOPUP_REQUEST: 'wallet_topup_request',
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
  MESSAGE_TYPES.EVENT_ENDED,
]);

const TYPES_ACCEPTING_V1 = new Set([
  'user_unregistered',
  'user_created',
  'user_registered',
]);

const BASE_HEADER_FIELDS = ['message_id', 'version', 'type', 'timestamp', 'source'];

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

  async log(level, action, message) {
    try {
      await this.sender.sendLog({ level, action, message });
    } catch (err) {
      console.error(`[receiver] Failed to send log: ${err.message}`);
    }
  }

  async connectRabbitMQ() {
    const maxRetries = 5;
    let retryCount = 0;

    while (retryCount < maxRetries && this.running) {
      try {
        this.connection = await amqp.connect(getAmqpOptions());
        this.channel = await this.connection.createChannel();

        await this.channel.assertExchange(DEAD_LETTER_EXCHANGE, 'fanout', { durable: true });
        await this.channel.assertQueue(DEAD_LETTER_QUEUE, { durable: true });
        await this.channel.bindQueue(DEAD_LETTER_QUEUE, DEAD_LETTER_EXCHANGE, '');

        const crmQueueArgs = { 'x-dead-letter-exchange': DEAD_LETTER_EXCHANGE };

        await this.channel.assertQueue(QUEUE_NAME, { durable: true, arguments: crmQueueArgs });
        await this.channel.assertQueue(KASSA_QUEUE, { durable: true, arguments: crmQueueArgs });
        await this.channel.assertQueue(FACTURATIE_TO_CRM_QUEUE, { durable: true, arguments: crmQueueArgs });
        await this.channel.assertQueue(USER_REGISTERED_QUEUE, { durable: true, arguments: crmQueueArgs });
        await this.channel.assertQueue(USER_CREATED_QUEUE, { durable: true, arguments: crmQueueArgs });

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

        console.log(`[receiver] Connected to RabbitMQ with Auto-DLX, listening on: ${QUEUE_NAME}, ${KASSA_QUEUE}, ${FACTURATIE_TO_CRM_QUEUE}, ${PLANNING_SESSION_QUEUE}, ${IDENTITY_EVENTS_QUEUE}`);

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
    if (!parsed.message || !parsed.message.header || !parsed.message.header.type) {
      return [false, 'Missing required message root or header/type'];
    }

    const header = parsed.message.header;
    const presentBaseHeaderFields = BASE_HEADER_FIELDS.filter((field) =>
      ReceiverV2.getElementText(header, field) !== null
    );

    if (!presentBaseHeaderFields.includes('type')) {
      return [false, 'Missing required header/type'];
    }

    const version = ReceiverV2.getElementText(header, 'version');
    const type = ReceiverV2.getElementText(header, 'type');
    if (version === 'v1' && !TYPES_ACCEPTING_V1.has(type)) {
      return [false, `Unsupported v1 message type: ${type}`];
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
        await this.log('error', 'xml_validation', `Received invalid XML from RabbitMQ. Parse error: ${err.message}`);
        this.channel.nack(msg, false, false);
        return;
      }

      const [basicValid, basicError] = this.validateXmlMessage(parsed);
      if (!basicValid) {
        console.log(`[basic-val] error: ${basicError}`);
        await this.log('error', 'xml_validation', `Received message with invalid structure. Error: ${basicError}`);
        this.channel.nack(msg, false, false);
        return;
      }

      const header = parsed.message.header;
      const body = parsed.message.body;
      const messageId = header.message_id;
      const messageType = header.type;
      const source = header.source;

      const xsdMapping = {
        [MESSAGE_TYPES.USER_CREATED]: 'user_created.xsd',
        'user_created': 'user_created.xsd',
        [MESSAGE_TYPES.USER_REGISTERED]: 'user_registered.xsd',
        'user_registered': 'user_registered.xsd',
        [MESSAGE_TYPES.NEW_REGISTRATION]: source === 'kassa' ? 'new_registration_kassa.xsd' : 'new_registration_frontend.xsd',
        [MESSAGE_TYPES.USER_UNREGISTERED]: 'user_unregistered.xsd',
        [MESSAGE_TYPES.PAYMENT_REGISTERED]: source === 'kassa' ? 'payment_registered_kassa.xsd' : 'payment_registered_facturatie.xsd',
        [MESSAGE_TYPES.BADGE_SCANNED]: 'badge_scanned.xsd',
        [MESSAGE_TYPES.SESSION_CREATED]: 'session_created.xsd',
        [MESSAGE_TYPES.SESSION_UPDATED]: 'session_updated.xsd',
        [MESSAGE_TYPES.SESSION_DELETED]: 'session_deleted.xsd',
        [MESSAGE_TYPES.EVENT_ENDED]: 'event_ended.xsd',
        [MESSAGE_TYPES.INVOICE_STATUS]: 'invoice_status.xsd',
        [MESSAGE_TYPES.MAILING_STATUS]: 'mailing_status.xsd',
        [MESSAGE_TYPES.CONSUMPTION_ORDER]: 'consumption_order.xsd',
        [MESSAGE_TYPES.BADGE_ASSIGNED]: 'badge_assigned.xsd',
        [MESSAGE_TYPES.REFUND_PROCESSED]: 'refund_processed.xsd',
        [MESSAGE_TYPES.INVOICE_REQUEST]: 'invoice_request_kassa.xsd',
        [MESSAGE_TYPES.INVOICE_CANCELLED]: 'invoice_cancelled.xsd',
        [MESSAGE_TYPES.USER_UPDATED]: 'user_updated.xsd',
        [MESSAGE_TYPES.USER_DELETED]: 'user_deleted.xsd',
        [MESSAGE_TYPES.USER_CHECKIN]: 'user_checkin.xsd',
        [MESSAGE_TYPES.CANCEL_REGISTRATION]: 'cancel_registration.xsd',
        [MESSAGE_TYPES.WALLET_LEASE_REQUEST]: 'wallet_lease_request.xsd',
        [MESSAGE_TYPES.WALLET_LEASE_RETURN]: 'wallet_lease_return.xsd',
        [MESSAGE_TYPES.WALLET_TOPUP_REQUEST]: 'wallet_topup_request.xsd',
      };

      const xsdFile = xsdMapping[messageType];
      if (xsdFile) {
        const { valid, errors } = validateXml(xmlContent, xsdFile);
        if (!valid) {
          const reason = `XSD_VALIDATION_ERROR: ${errors.join('; ')}`;
          console.log(`[receiver] ${reason} for ${messageType}`);
          await this.log('error', 'xml_validation', `Received ${messageType} from ${source}. Validation: Failure. Details: ${errors.join('; ')}`);
          this.channel.nack(msg, false, false);
          return;
        }
        console.log(`[receiver] XSD validation passed for ${messageType}`);
        await this.log('info', 'xml_validation', `Received ${messageType} from ${source}. Validation: Success.`);
      } else {
        console.log(`[receiver] Warning: No XSD mapping found for message type: ${messageType}`);
        await this.log('info', 'xml_validation', `Received ${messageType} from ${source}. Validation: Skipped (No XSD).`);
      }

      console.log(`[receiver] Processing message type: ${messageType}, ID: ${messageId}`);
      await this.routeMessage(header, body, xmlContent);

      this.channel.ack(msg);
      console.log(`[receiver] Message processed successfully: ${messageId}`);
    } catch (err) {
      console.log(`[receiver] Unexpected error: ${err}`);
      await this.log('error', 'system_error', `Internal Error in Receiver: ${err.message}`);
      this.channel.nack(msg, false, false);
    }
  }

  async routeMessage(header, body, rawXml = null) {
    const msgType = header.type;
    if (PLANNING_SESSION_TYPES.has(msgType)) {
      await this.handlePlanningSessionEvent(header, body);
      return;
    }

    const handlers = {
      [MESSAGE_TYPES.USER_CREATED]: () => this.handleUserCreated(header, body),
      [MESSAGE_TYPES.USER_REGISTERED]: () => this.handleUserRegistered(header, body),
      'user_created': () => this.handleUserCreated(header, body),
      'user_registered': () => this.handleUserRegistered(header, body),
      [MESSAGE_TYPES.NEW_REGISTRATION]: () => this.handleNewRegistration(header, body),
      [MESSAGE_TYPES.USER_UNREGISTERED]: () => this.handleUserUnregistered(header, body),
      [MESSAGE_TYPES.PAYMENT_REGISTERED]: () => this.handlePaymentRegistered(header, body, rawXml),
      [MESSAGE_TYPES.BADGE_SCANNED]: () => this.handleBadgeScanned(header, body),
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
      [MESSAGE_TYPES.SESSION_CREATED]: () => this.handlePlanningSessionEvent(header, body),
      [MESSAGE_TYPES.SESSION_UPDATED]: () => this.handlePlanningSessionEvent(header, body),
      [MESSAGE_TYPES.SESSION_DELETED]: () => this.handlePlanningSessionEvent(header, body),
      [MESSAGE_TYPES.USER_CHECKIN]: () => this.handleUserCheckin(header, body),
      [MESSAGE_TYPES.WALLET_LEASE_REQUEST]: () => this.handleWalletLeaseRequest(header, body),
      [MESSAGE_TYPES.WALLET_LEASE_RETURN]: () => this.handleWalletLeaseReturn(header, body),
      [MESSAGE_TYPES.WALLET_TOPUP_REQUEST]: () => this.handleWalletTopupRequest(header, body),
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

    const messageType = options.messageType || (header && header.type);
    const lazyLookupConfigured = messageType ? LAZY_MASTER_UUID_TYPES.has(messageType) : false;

    const email = (this._getFallbackEmail(body, options.email) || '').toLowerCase().trim();
    if (!email) return null;

    const sourceSystem = options.sourceSystem || (header && header.source) || 'crm';
    console.log(`[receiver] Lazy Master UUID lookup for ${email} via ${sourceSystem} (type=${messageType || 'unknown'}, configured=${lazyLookupConfigured})`);
    return this.getOrCreateMasterUuid(email, sourceSystem);
  }

  async handleUserUnregistered(header, body) {
    try {
      const masterUuid = ReceiverV2.getElementText(body, 'identity_uuid') ||
        ReceiverV2.getElementText(body, 'master_uuid') ||
        (header && header.master_uuid);

      const email = ReceiverV2.getElementText(body, 'email');
      const reason = ReceiverV2.getElementText(body, 'reason');

      if (!masterUuid) {
        console.log('[receiver] Missing master_uuid in user.unregistered body');
        return;
      }

      await this.sender.sendUserUnregisteredFanout({
        identity_uuid: masterUuid,
        email: email || '',
        reason: reason || ''
      });

      console.log(`[receiver] Forwarded user.unregistered for identity_uuid=${masterUuid}`);
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

      const getCustomerText = (key) =>
        ReceiverV2.getElementText(customer, key) || ReceiverV2.getElementText(contact, key);

      const email = getCustomerText('email');
      const identityUuid = getCustomerText('identity_uuid') || getCustomerText('user_id');
      const sessionId = getCustomerText('session_id') || ReceiverV2.getElementText(body, 'session_id');
      const firstName = getCustomerText('first_name');
      const lastName = getCustomerText('last_name');
      const addressText = typeof address === 'string' ? address : null;
      
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
      const masterUuid = identityUuid || await this.getOrCreateMasterUuid(email, header.source || 'frontend.drupal');
      if (!masterUuid) throw new Error('new_registration missing identity_uuid/master_uuid');

      const userData = {
        Master_UUID__c: masterUuid,
        
        First_Name__c: firstName,
        Last_Name__c: lastName,
        Email__c: email,
        Birthdate__c: getCustomerText('date_of_birth'),
        User_Type__c: userType,
        Street__c: addressText || (address ? ReceiverV2.getElementText(address, 'street') : null),
        House_Number__c: address ? ReceiverV2.getElementText(address, 'number') : null,
        Postal_Code__c: address ? ReceiverV2.getElementText(address, 'postal_code') : null,
        City__c: address ? ReceiverV2.getElementText(address, 'city') : null,
        Country_Code__c: address ? (ReceiverV2.getElementText(address, 'country') || '').toUpperCase() || null : null,
        Badge_ID__c: getCustomerText('badge_id') || null,
      };

      let companyId = null;
      if (isCompanyLinked || rawType === 'company') {
        const companyName = getCustomerText('company_name') || ReceiverV2.getElementText(companyData, 'name');
        const companyVat = getCustomerText('vat_number') || ReceiverV2.getElementText(companyData, 'vat_number');
        const companyEmail = ReceiverV2.getElementText(companyData, 'email');

        if (companyName && companyVat && this.sf.isConnected) {
          const result = await this.sf.apiCall((conn) =>
            conn.sobject('Account').upsert({
              Master_UUID__c: masterUuid,
              Company_Name__c: companyName,
              VAT_Number__c: companyVat,
              Email__c: companyEmail || null,
              Billing_Street__c: addressText || (address ? ReceiverV2.getElementText(address, 'street') : null),
              Billing_City__c: address ? ReceiverV2.getElementText(address, 'city') : null,
            }, 'VAT_Number__c')
          );
          companyId = result.id || null;
        }
      }

      if (companyId) userData.Account__c = companyId;

      if (this.sf.isConnected) {
        console.log(`[receiver] Salesforce Member__c upsert fields: ${Object.keys(userData).join(', ')}`);
        await this.sf.apiCall((conn) => conn.sobject('Member__c').upsert(userData, 'Master_UUID__c'));
      }

      const kassaPayload = {
        customer: {
          master_uuid: masterUuid,
          email: email,
          date_of_birth: getCustomerText('date_of_birth') || '',
          first_name: firstName || '',
          last_name: lastName || '',
          type: (isCompanyLinked || rawType === 'company') ? 'company' : 'private',
          company_name: getCustomerText('company_name') || ReceiverV2.getElementText(companyData, 'name'),
          vat_number: getCustomerText('vat_number') || ReceiverV2.getElementText(companyData, 'vat_number'),
          session_id: sessionId || '',
        },
        session_id: sessionId || '',
        payment_due: {
          amount: registrationAmount || '0.00',
          status: paymentStatus === 'paid' ? 'paid' : 'unpaid',
        },
      };
      await this.sender.sendNewRegistrationToKassa(kassaPayload);

      const fossPayload = {
        master_uuid: masterUuid,
        customer: {
          first_name: firstName,
          last_name: lastName,
          email: email,
          date_of_birth: getCustomerText('date_of_birth') || '',
          type: (isCompanyLinked || rawType === 'company') ? 'company' : 'private',
          company_name: getCustomerText('company_name') || ReceiverV2.getElementText(companyData, 'name'),
          vat_number: getCustomerText('vat_number') || ReceiverV2.getElementText(companyData, 'vat_number'),
        },
        session_id: sessionId || '',
        payment_due: {
          amount: registrationAmount ? parseFloat(registrationAmount) : 0,
          status: paymentStatus === 'paid' ? 'paid' : 'unpaid',
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
      const userData = body?.customer || body?.user;
      if (!userData) throw new Error('Body missing user element');

      const identityUuid = ReceiverV2.getElementText(userData, 'identity_uuid') ||
        ReceiverV2.getElementText(userData, 'master_uuid');
      const email = (ReceiverV2.getElementText(userData, 'email') || '').toLowerCase().trim();
      const firstName = ReceiverV2.getElementText(userData, 'first_name');
      const lastName = ReceiverV2.getElementText(userData, 'last_name');
      const isCompany = ReceiverV2.getElementText(userData, 'is_company') === 'true';
      const sourceSystem = header?.source || 'frontend.drupal';

      const masterUuid = identityUuid || await this.getOrCreateMasterUuid(email, sourceSystem);

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
      const customerData = body?.customer || body?.user;
      const session = body?.session || customerData?.session;
      const sessionId = ReceiverV2.getElementText(session, 'session_id') ||
        ReceiverV2.getElementText(customerData, 'session_id');
      const sessionName = ReceiverV2.getElementText(session, 'session_name') ||
        ReceiverV2.getElementText(customerData, 'session_name');

      if (!customerData || !sessionId) throw new Error('Body missing user or session');

      const email = (ReceiverV2.getElementText(customerData, 'email') || '').toLowerCase().trim();
      const identityUuid = ReceiverV2.getElementText(customerData, 'identity_uuid') ||
        ReceiverV2.getElementText(customerData, 'master_uuid');
      const sourceSystem = header?.source || 'frontend.drupal';
      const masterUuid = identityUuid || await this.getOrCreateMasterUuid(email, sourceSystem);
      const paymentStatus = ReceiverV2.getElementText(body, 'payment_status');

      if (this.sf.isConnected) {
        await this.sf.apiCall((conn) =>
          conn.sobject('Member__c').upsert({
            Master_UUID__c: masterUuid,
            First_Name__c: ReceiverV2.getElementText(customerData, 'first_name'),
            Last_Name__c: ReceiverV2.getElementText(customerData, 'last_name'),
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

      if (!memberId) throw new Error(`No Member__c found for masterUuid=${masterUuid}, email=${email}`);

      await this.sf.apiCall((conn) =>
        conn.sobject('Member__c').update({
          Id: memberId,
          Last_Invoice_URL__c: invoiceUrl,
          Last_Invoice_Due_Date__c: dueDate,
          Last_Invoice_Number__c: invoiceNumber
        })
      );
    } catch (err) {
      console.error(`[receiver] Error in handleSendInvoice: ${err}`);
      throw err;
    }
  }

  async handleReceivedInvoiceCancelled(header, body) {
    try {
      const masterUuid = await this.resolveMasterUuid(header, body);

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
      const invoiceId = ReceiverV2.getElementText(body, 'invoice_id') || ReceiverV2.getElementText(invoice, 'id');
      const transactionId = transaction ? ReceiverV2.getElementText(transaction, 'id') : null;
      const paymentMethod = ReceiverV2.getElementText(body, 'payment_method') ||
        (transaction ? ReceiverV2.getElementText(transaction, 'method') : null) || 'unknown';
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
        await this.sender.sendPaymentRegisteredToFacturatie({
    identity_uuid: masterUuid,
    invoice_id: invoiceId,
    amount_paid: amountPaid,
    payment_context: paymentContext,
    transaction_id: transactionId,
    payment_method: paymentMethod,
    correlation_id: header.message_id
  });
      }

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
        return;
      }

      if (this.sf.isConnected) {
        const title = ReceiverV2.getElementText(body, 'title');
        const speaker = ReceiverV2.extractSpeaker(body);
        const taskData = {
          Subject: `${header.type === MESSAGE_TYPES.SESSION_CREATED ? 'Session created' : 'Session updated'}: ${title || sessionId}`,
          Description: ReceiverV2.buildSessionDescription(body, speaker),
          Status: 'Completed',
          ActivityDate: new Date().toISOString().split('T')[0],
        };

        if (speaker.identity_uuid) {
          taskData.Master_UUID__c = speaker.identity_uuid;
        }

        await this.sf.apiCall((conn) => conn.sobject('Task').create(taskData));
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

      if (rawXml) {
        await this.sender.sendConsumptionOrderToFacturatie(rawXml);
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

  async handleWalletLeaseRequest(header, body) {
    try {
      const masterUuid = ReceiverV2.getElementText(body, 'identity_uuid');
      const badgeId = ReceiverV2.getElementText(body, 'badge_id');

      console.log(`[lease] Aanvraag ontvangen voor User: ${masterUuid}`);

      if (!this.sf.isConnected) {
        throw new Error("Salesforce niet verbonden. Kan lease niet verstrekken.");
      }

      // 1. Haal huidige saldo en status op uit Salesforce
      const records = await this.sf.apiCall((conn) =>
        conn.sobject('Member__c').find({ Master_UUID__c: masterUuid }, ['Id', 'Wallet_Balance__c', 'Wallet_Status__c']).limit(1)
      );

      if (!records || records.length === 0) {
        throw new Error(`User met UUID ${masterUuid} niet gevonden in CRM.`);
      }

      const member = records[0];
      
      // Genereer een unieke Lease ID voor deze sessie (Verplicht voor XSD)
      const generatedLeaseId = `LEASE-${new Date().getFullYear()}-${uuidv4().substring(0, 8)}`.toUpperCase();

      // 2. "Bevries" de wallet in Salesforce
      const updateFields = {
        Id: member.Id,
        Wallet_Status__c: 'Leased',
        Last_Lease_ID__c: generatedLeaseId,
        Last_Lease_At__c: new Date().toISOString()
      };

      // Badge ID toevoegen (fixt linter error)
      if (badgeId) {
        updateFields.Badge_ID__c = badgeId;
      }

      await this.sf.apiCall((conn) =>
        conn.sobject('Member__c').update(updateFields)
      );

      // 3. Stuur het saldo terug naar de Kassa (Authority Transfer)
      const leaseData = {
        identity_uuid: masterUuid,
        current_balance: member.Wallet_Balance__c || 0.00,
        lease_id: generatedLeaseId,
        correlation_id: header.message_id 
      };

      await this.sender.sendWalletLeaseGrant(leaseData);

      console.log(`[lease] Macht overgedragen aan Kassa voor ${masterUuid}. Lease: ${generatedLeaseId}`);

    } catch (err) {
      console.error(`[receiver] Error in handleWalletLeaseRequest: ${err.message}`);
      throw err; 
    }
  }

async handleWalletLeaseReturn(header, body) {
    let leaseId = 'ONBEKEND'; 
    try {
      const masterUuid = ReceiverV2.getElementText(body, 'identity_uuid');
      const finalBalance = ReceiverV2.getElementText(body, 'final_balance');
      leaseId = ReceiverV2.getElementText(body, 'lease_id'); 
      const txCount = ReceiverV2.getElementText(body, 'transaction_count');

      console.log(`[lease-return] Ontvangen voor User: ${masterUuid}. Lease: ${leaseId}. Transacties: ${txCount}`);

      if (!this.sf.isConnected) {
        throw new Error("Salesforce niet verbonden. Kan lease-return niet verwerken.");
      }

      // 1. Zoek de gebruiker op in Salesforce
      const records = await this.sf.apiCall((conn) =>
        conn.sobject('Member__c').find({ Master_UUID__c: masterUuid }, ['Id']).limit(1)
      );

      if (!records || records.length === 0) {
        throw new Error(`User met UUID ${masterUuid} niet gevonden bij afsluiten lease.`);
      }

      const memberId = records[0].Id;

      // 2. Update Salesforce: Saldo bijwerken en status op 'Active' zetten
      await this.sf.apiCall((conn) =>
        conn.sobject('Member__c').update({
          Id: memberId,
          Wallet_Balance__c: parseFloat(finalBalance),
          Wallet_Status__c: 'Active',
          Last_Lease_ID__c: leaseId, 
          Last_Sync_At__c: new Date().toISOString()
        })
      );

      // 3. Log de succesvolle afhandeling
      await this.sender.sendLog({
        level: 'info',
        action: 'wallet',
        message: `Lease ${leaseId} succesvol beëindigd voor ${masterUuid}. Nieuw saldo: ${finalBalance} (${txCount} transacties).`
      });

      console.log(`[lease-return] Wallet succesvol vrijgegeven in CRM voor ${masterUuid}.`);

    } catch (err) {
      console.error(`[receiver] Fout bij verwerken wallet_lease_return: ${err.message}`);
      
      // Deze 'this' werkt nu omdat hij netjes binnen de catch van de class methode staat
      await this.sender.sendLog({
        level: 'error',
        action: 'wallet',
        message: `CRITIEK: Kon lease-return voor ${leaseId} niet verwerken! Error: ${err.message}`
      });
      throw err;
    }
  }

  async handleWalletTopupRequest(header, body) {
    try {
      const identityUuid = ReceiverV2.getElementText(body, 'identity_uuid');
      const topupAmount = parseFloat(ReceiverV2.getElementText(body, 'topup_amount') || 0);
      const transactionId = ReceiverV2.getElementText(body, 'transaction_id') || header.message_id;

      if (!identityUuid || isNaN(topupAmount) || topupAmount <= 0) {
        console.log('[receiver] Invalid wallet_topup_request: missing identity_uuid or invalid amount');
        return;
      }

      console.log(`[wallet-topup] Received topup request for ${identityUuid}: +€${topupAmount}`);

      if (!this.sf.isConnected) {
        throw new Error('Salesforce not connected. Cannot process wallet topup.');
      }

      const records = await this.sf.apiCall((conn) =>
        conn.sobject('Member__c').find({ Master_UUID__c: identityUuid }, ['Id', 'Wallet_Balance__c']).limit(1)
      );

      if (!records || records.length === 0) {
        throw new Error(`User with UUID ${identityUuid} not found in CRM.`);
      }

      const member = records[0];
      const currentBalance = parseFloat(member.Wallet_Balance__c || 0);
      const newBalance = Math.round((currentBalance + topupAmount) * 100) / 100;

      await this.sf.apiCall((conn) =>
        conn.sobject('Member__c').update({
          Id: member.Id,
          Wallet_Balance__c: newBalance,
        })
      );

      await this.sender.sendLog({
        level: 'info',
        action: 'wallet',
        message: `Wallet topup processed for ${identityUuid}: +€${topupAmount}. New balance: €${newBalance}. Transaction: ${transactionId}.`,
      });

      await this.sender.sendWalletRemoteTopup({
        identity_uuid: identityUuid,
        add_amount: topupAmount,
        reason: `online_topup:${transactionId}`,
        correlation_id: header.message_id,
      });

      console.log(`[wallet-topup] Wallet updated for ${identityUuid}. New balance: €${newBalance}`);
    } catch (err) {
      console.error(`[receiver] Error in handleWalletTopupRequest: ${err.message}`);
      await this.sender.sendLog({
        level: 'error',
        action: 'wallet',
        message: `Failed to process wallet_topup_request: ${err.message}`,
      });
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

      const amountPaidRaw = invoiceData ? ReceiverV2.getElementText(invoiceData, 'amount_paid') : null;
      const invoiceAmount = amountPaidRaw ? parseFloat(amountPaidRaw) : 0;

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
        master_uuid: masterUuid,
        correlation_id: header.correlation_id || header.message_id,
        customer: {
          email: email || '',
          first_name: contact ? ReceiverV2.getElementText(contact, 'first_name') : '',
          last_name: contact ? ReceiverV2.getElementText(contact, 'last_name') : '',
          company_name: invoiceData ? ReceiverV2.getElementText(invoiceData, 'company_name') : null,
          vat_number:   invoiceData ? ReceiverV2.getElementText(invoiceData, 'vat_number') : null,
        },
        invoice: {
          amount: invoiceAmount,
          id: invoiceData ? ReceiverV2.getElementText(invoiceData, 'id') : null,
        },
        address: invoiceData ? {
          street:      ReceiverV2.getElementText(invoiceData.address, 'street') || '',
          number:      ReceiverV2.getElementText(invoiceData.address, 'number') || '',
          postal_code: ReceiverV2.getElementText(invoiceData.address, 'postal_code') || '',
          city:        ReceiverV2.getElementText(invoiceData.address, 'city') || '',
          country:     ReceiverV2.getElementText(invoiceData.address, 'country') || '',
        } : { street: '', number: '', postal_code: '', city: '', country: '' },
      });
    } catch (err) {
      console.log(`[receiver] Error in handleInvoiceRequestFromKassa: ${err}`);
      throw err;
    }
  }

  async handleUserUpdated(header, body) {
    try {
      const customer = body?.customer;
      if (!customer) throw new Error('Body missing customer element');

      const identityUuid = ReceiverV2.getElementText(customer, 'identity_uuid');
      const email = (ReceiverV2.getElementText(customer, 'email') || '').toLowerCase().trim();
      const contact = customer.contact;
      const firstName = ReceiverV2.getElementText(contact, 'first_name');
      const lastName = ReceiverV2.getElementText(contact, 'last_name');
      const dateOfBirth = ReceiverV2.getElementText(customer, 'date_of_birth');
      const rawType = ReceiverV2.getElementText(customer, 'type');
      const userType = rawType === 'company' ? 'Bedrijf' : 'Particulier';

      if (!identityUuid) throw new Error('Missing identity_uuid in user_updated message');

      if (this.sf.isConnected) {
        await this.sf.apiCall((conn) =>
          conn.sobject('Member__c').upsert({
            Master_UUID__c: identityUuid,
            Email__c: email,
            First_Name__c: firstName,
            Last_Name__c: lastName,
            Birthdate__c: dateOfBirth || null,
            User_Type__c: userType,
          }, 'Master_UUID__c')
        );
      }

      await this.sender.sendProfileUpdateToKassa({
        identity_uuid: identityUuid,
        email,
        first_name: firstName,
        last_name: lastName,
        date_of_birth: dateOfBirth,
        type: rawType,
        company_name: ReceiverV2.getElementText(customer, 'company_name'),
        vat_number: ReceiverV2.getElementText(customer, 'vat_number'),
        company_id: ReceiverV2.getElementText(customer, 'company_id'),
      });

      console.log(`[receiver] User updated in Salesforce and forwarded to Kassa: ${identityUuid}`);
    } catch (err) {
      console.error(`[receiver] Error in handleUserUpdated: ${err.message}`);
      throw err;
    }
  }

  async handleUserCheckin(header, body) {
    try {
      const masterUuid = ReceiverV2.getElementText(body, 'identity_uuid');
      const sessionId = ReceiverV2.getElementText(body, 'session_id');
      const checkinAt = ReceiverV2.getElementText(body, 'checkin_at');

      if (!this.sf.isConnected) {
        throw new Error(`Salesforce niet verbonden. Check-in voor ${masterUuid} mislukt.`);
      }

      const result = await this.sf.apiCall((conn) =>
        conn.sobject('Task').create({
          Subject: `Check-in: ${sessionId}`,
          Description: `Sessie scan op ${checkinAt}`,
          Status: 'Completed',
          Master_UUID__c: masterUuid,
          ActivityDate: new Date().toISOString().split('T')[0]
        })
      );

      if (result && !result.success) {
        throw new Error(`SF Check-in mislukt: ${JSON.stringify(result.errors)}`);
      }
      console.log(`[salesforce] Check-in geregistreerd voor ${masterUuid}`);
    } catch (err) {
      console.error(`[receiver] Error in handleUserCheckin: ${err.message}`);
      throw err;
    }
  }

  async handleDeleteUser(header, body) {
    let masterUuid;
    try {
      // 1. UUID Extractie (nu inclusief identity_uuid voor XSD alignment)
      masterUuid = ReceiverV2.getElementText(body, 'identity_uuid') || 
                   ReceiverV2.getElementText(body, 'master_uuid') || 
                   ReceiverV2.getElementText(body, 'user_id') || 
                   header?.master_uuid;

      if (!masterUuid) {
        console.warn('[receiver] handleDeleteUser: Geen UUID gevonden.');
        return;
      }

      if (!this.sf.isConnected) {
        throw new Error("Salesforce niet verbonden.");
      }

      // 2. Zoek de record ID op basis van de Master_UUID__c
      const memberId = await this._findUserByMasterUuid(masterUuid);
      
      if (!memberId) {
        console.log(`[receiver] Delete overgeslagen: User ${masterUuid} bestaat niet in Salesforce.`);
        return;
      }

      // 3. De actie: HARD DELETE
      // Aangezien velden als Is_Deleted__c niet bestaan, verwijderen we het record echt.
      await this.sf.apiCall((conn) =>
        conn.sobject('Member__c').destroy(memberId)
      );

      console.log(`[receiver] User ${masterUuid} (ID: ${memberId}) succesvol verwijderd uit Salesforce.`);

      // 4. Bevestiging naar de sender
      await this.sender.sendLog({
        level: 'info',
        action: 'delete_user',
        message: `User ${masterUuid} definitief verwijderd uit CRM.`
      });

    } catch (err) {
      console.error(`[receiver] Fout bij handleDeleteUser voor ${masterUuid || 'onbekend'}: ${err.message}`);
      throw err;
    }
  }

  async handleCancelRegistration(header, body) {
    try {
      const userId = ReceiverV2.getElementText(body, 'user_id');
      const sessionId = ReceiverV2.getElementText(body, 'session_id');
      const reason = ReceiverV2.getElementText(body, 'reason');

      if (!userId || !sessionId) {
        console.log('[receiver] handleCancelRegistration: missing user_id or session_id');
        return;
      }

      const payload = { user_id: userId, session_id: sessionId };
      if (reason) payload.reason = reason;

      await this.sender.sendCancelRegistrationToKassa(payload);
      await this.sender.sendCancelRegistrationToPlanning(payload);

      if (this.sf.isConnected) {
        const memberId = await this._findUserByMasterUuid(userId);
        if (memberId) {
          await this.sf.apiCall((conn) =>
            conn.sobject('Member__c').update({ Id: memberId, Status__c: 'Cancelled' })
          );
        }
      }
    } catch (err) {
      console.log(`[receiver] Error in handleCancelRegistration: ${err}`);
      throw err;
    }
  }

  async handleIdentityUserEvent(msg) {
    try {
      const xmlContent = msg.content.toString();
      
      const { valid, errors } = validateXml(xmlContent, 'identity_user_created.xsd');
      if (!valid) {
        console.error(`[receiver] Identity event XSD Validation error: ${errors.join(', ')}`);
        await this.log('error', 'xml_validation', `Received UserCreated from identity-service. Validation: Failure. Details: ${errors.join('; ')}`);
        this.channel.nack(msg, false, false);
        return;
      }
      await this.log('info', 'xml_validation', `Received UserCreated from identity-service. Validation: Success.`);

      let parsed;
      try {
        parsed = parser.parse(xmlContent);
      } catch (parseErr) {
        console.error('[receiver] Identity event XML parse error:', parseErr.message);
        await this.log('error', 'xml_validation', `Received invalid XML from identity-service. Parse error: ${parseErr.message}`);
        this.channel.nack(msg, false, false);
        return;
      }

      const event = parsed && parsed.user_event;
      if (!event) {
        console.error('[receiver] Identity event missing user_event root');
        this.channel.nack(msg, false, false);
        return;
      }

      const eventType = ReceiverV2.getElementText(event, 'event');
      const masterUuid = ReceiverV2.getElementText(event, 'master_uuid');
      const email = ReceiverV2.getElementText(event, 'email');

      if (!masterUuid) {
        console.error('[receiver] Identity event missing master_uuid');
        this.channel.nack(msg, false, false);
        return;
      }

      if (eventType === 'UserCreated' && this.sf.isConnected) {
        await this.sf.apiCall((conn) =>
          conn.sobject('Member__c').upsert({ Master_UUID__c: masterUuid, Email__c: email }, 'Master_UUID__c')
        );
      }

      this.channel.ack(msg);
      console.log(`[receiver] Identity event processed: ${eventType} ${masterUuid}`);
    } catch (err) {
      console.error(`[receiver] Identity Fanout error: ${err.message}`);
      await this.log('error', 'system_error', `Internal Error in handleIdentityUserEvent: ${err.message}`);
      this.channel.nack(msg, false, false);
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

  static extractSpeaker(body) {
    const speaker = body?.speaker;
    const contact = speaker?.contact;

    return {
      identity_uuid: ReceiverV2.getElementText(speaker, 'identity_uuid'),
      first_name: ReceiverV2.getElementText(contact, 'first_name'),
      last_name: ReceiverV2.getElementText(contact, 'last_name'),
      organisation: ReceiverV2.getElementText(speaker, 'organisation'),
      email: ReceiverV2.getElementText(speaker, 'email'),
    };
  }

  static buildSessionDescription(body, speaker = ReceiverV2.extractSpeaker(body)) {
    const speakerName = [speaker.first_name, speaker.last_name].filter(Boolean).join(' ');
    return [
      `Session ID: ${ReceiverV2.getElementText(body, 'session_id')}`,
      `Title: ${ReceiverV2.getElementText(body, 'title')}`,
      `Start: ${ReceiverV2.getElementText(body, 'start_datetime')}`,
      `End: ${ReceiverV2.getElementText(body, 'end_datetime')}`,
      `Location: ${ReceiverV2.getElementText(body, 'location')}`,
      `Type: ${ReceiverV2.getElementText(body, 'session_type')}`,
      `Status: ${ReceiverV2.getElementText(body, 'status')}`,
      `Attendees: ${ReceiverV2.getElementText(body, 'current_attendees')}/${ReceiverV2.getElementText(body, 'max_attendees')}`,
      ReceiverV2.getElementText(body, 'change_reason')
        ? `Change reason: ${ReceiverV2.getElementText(body, 'change_reason')}`
        : null,
      speakerName ? `Speaker: ${speakerName}` : null,
      speaker.identity_uuid ? `Speaker UUID: ${speaker.identity_uuid}` : null,
      speaker.organisation ? `Speaker organisation: ${speaker.organisation}` : null,
      speaker.email ? `Speaker email: ${speaker.email}` : null,
    ].filter(Boolean).join('\n');
  }

  async shutdown() {
    this.running = false;
    try { if (this.channel) await this.channel.close(); } catch (_err) { /* already closed */ }
    try { if (this.connection) await this.connection.close(); } catch (_err) { /* already closed */ }
    try { await this.sender.close(); } catch (_err) { /* already closed */ }
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
