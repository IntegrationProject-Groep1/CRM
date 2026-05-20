'use strict';

jest.mock('../src/sfConnection', () => {
  return jest.fn().mockImplementation(() => ({
    init: jest.fn().mockResolvedValue(undefined),
    isConnected: false,
    apiCall: jest.fn().mockResolvedValue([]),
  }));
});

jest.mock('../src/validator', () => ({
  validateXml: jest.fn(() => ({ valid: true, errors: [] }))
}));

jest.mock('../src/sender', () => {
  return jest.fn().mockImplementation(() => ({
    init: jest.fn().mockResolvedValue(undefined),
    close: jest.fn().mockResolvedValue(undefined),
    sendNewRegistrationToKassa: jest.fn().mockResolvedValue({ success: true }),
    sendNewRegistrationToFacturatie: jest.fn().mockResolvedValue({ success: true }),
    sendInvoiceCancelledToFacturatie: jest.fn().mockResolvedValue({ success: true }),
    sendInvoiceRequest: jest.fn().mockResolvedValue({ success: true }),
    sendConsumptionOrderToFacturatie: jest.fn().mockResolvedValue({ success: true }),
    sendPaymentRegisteredToFrontend: jest.fn().mockResolvedValue({ success: true }),
    sendUserUnregisteredFanout: jest.fn().mockResolvedValue({ success: true }),
    sendEventEndedToFacturatie: jest.fn().mockResolvedValue({ success: true }),
    sendSessionRegistrationConfirmed: jest.fn().mockResolvedValue({ success: true }),
  }));
});

jest.mock('amqplib');
jest.mock('../src/amqpUrl', () => ({ getAmqpOptions: jest.fn().mockReturnValue({}) }));

beforeAll(() => {
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'error').mockImplementation(() => {});
});

afterAll(() => {
  console.log.mockRestore();
  console.error.mockRestore();
});

const ReceiverV2 = require('../src/receiver');

function buildXml(type, bodyXml = '', extra = {}) {
  const correlationId = extra.correlation_id
    ? `<correlation_id>${extra.correlation_id}</correlation_id>`
    : '';

  return `<?xml version="1.0" encoding="UTF-8"?>
<message>
  <header>
    <message_id>msg-${Math.random().toString(36).slice(2)}</message_id>
    <version>2.0</version>
    <type>${type}</type>
    <timestamp>${new Date().toISOString()}</timestamp>
    <source>test</source>
    <master_uuid>test-master-uuid-1234</master_uuid>
    ${correlationId}
  </header>
  <body>${bodyXml}</body>
</message>`;
}

function withoutMasterUuid(xmlString) {
  return xmlString.replace(/\s*<master_uuid>test-master-uuid-1234<\/master_uuid>/, '');
}

function buildMsg(xmlString) {
  return {
    content: Buffer.from(xmlString, 'utf8'),
    fields: { deliveryTag: 1 },
  };
}

function buildFrontendUserUnregisteredXml(overrides = {}) {
  const headerTimestamp = overrides.headerTimestamp || new Date().toISOString();

  return `<?xml version="1.0" encoding="UTF-8"?>
<message>
  <header>
    <message_id>${overrides.messageId || `msg-${Math.random().toString(36).slice(2)}`}</message_id>
    <timestamp>${headerTimestamp}</timestamp>
    <source>${overrides.source || 'frontend.drupal'}</source>
    <receiver>${overrides.receiver || 'crm.salesforce planning.outlook mailing.sendgrid'}</receiver>
    <type>user.unregistered</type>
    <version>1.0</version>
    <correlation_id>${overrides.correlationId || ''}</correlation_id>
  </header>
  <body>
    <identity_uuid>${overrides.identityUuid || 'test-master-uuid-1234'}</identity_uuid>
    <email>${overrides.email || 'test@example.com'}</email>
    <reason>${overrides.reason || 'User requested unregistration'}</reason>
  </body>
</message>`;
}

function buildSendInvoiceXml() {
  return `<?xml version="1.0" encoding="UTF-8"?>
<message>
  <header>
    <message_id>c91df23a-47be-6789-d012-3e25f5a6b702</message_id>
    <version>2.0</version>
    <type>send_invoice</type>
    <timestamp>2026-03-29T18:36:00Z</timestamp>
    <source>facturatie_system_01</source>
    <correlation_id>f47ac10b-58cc-4372-a567-0e02b2c3d479</correlation_id>
  </header>
  <body>
    <customer>
      <id>12345</id>
      <email>info@bedrijf.be</email>
    </customer>
    <invoice>
      <id>INV-2026-001</id>
      <status>paid</status>
      <amount_paid currency="eur">15.00</amount_paid>
      <due_date>2026-03-06</due_date>
      <pdf_url>https://example.test/invoice.pdf</pdf_url>
    </invoice>
  </body>
</message>`;
}

/** Create a fresh ReceiverV2 instance with a mocked channel. */
function makeReceiver() {
  const receiver = new ReceiverV2();
  receiver.channel = {
    ack: jest.fn(),
    nack: jest.fn(),
    sendToQueue: jest.fn().mockReturnValue(true),
    assertQueue: jest.fn().mockResolvedValue({ queue: 'test-reply-queue' }),
    consume: jest.fn(),
    deleteQueue: jest.fn(),
  };
  receiver.getOrCreateMasterUuid = jest.fn().mockResolvedValue('test-master-uuid-1234');
  return receiver;
}

describe('ReceiverV2.getElementText', () => {
  test('haalt primitive en #text waarden correct op', () => {
    expect(ReceiverV2.getElementText({ name: 'Jan' }, 'name')).toBe('Jan');
    expect(ReceiverV2.getElementText({ amount: { '#text': '9.99' } }, 'amount')).toBe('9.99');
    expect(ReceiverV2.getElementText(null, 'x')).toBeNull();
  });
});

describe('validateXmlMessage', () => {
  let receiver;

  beforeEach(() => { receiver = makeReceiver(); });

  function validParsed(overrides = {}) {
    return {
      message: {
        header: {
          message_id: 'id-1',
          version: '2.0',
          type: 'new_registration',
          timestamp: new Date().toISOString(),
          source: 'test',
          master_uuid: 'uuid-1',
          ...overrides,
        },
      },
    };
  }

  test('geldig bericht geeft true terug', () => {
    const [valid, err] = receiver.validateXmlMessage({
      message: {
        header: {
          message_id: 'id-1',
          version: '2.0',
          type: 'new_registration',
          timestamp: new Date().toISOString(),
          source: 'test',
          master_uuid: 'uuid-1',
        },
      },
    });

    expect(valid).toBe(true);
    expect(err).toBeNull();
  });

  test('ongeldig bericht zonder root geeft fout', () => {
    const [valid, err] = receiver.validateXmlMessage({});
    expect(valid).toBe(false);
    expect(err).toMatch(/Missing required message root/);
  });

  test('ontbrekende header geeft fout', () => {
    const [valid, err] = receiver.validateXmlMessage({ message: {} });
    expect(valid).toBe(false);
    expect(err).toMatch(/Missing required message root/);
  });

  test('ontbrekende type geeft fout', () => {
    const [valid, err] = receiver.validateXmlMessage({ message: { header: {} } });
    expect(valid).toBe(false);
    expect(err).toMatch(/Missing required message root/);
  });

  test('all valid message types with standard header are accepted', () => {
    // Types that require the standard header fields (message_id, version, type, timestamp, source, master_uuid).
    // user.unregistered and send_invoice have different required fields and are covered by dedicated tests below.
    const validTypes = [
      'user.created', 'user.registered', 'new_registration', 'payment_registered',
      'badge_scanned', 'session_created', 'session_updated', 'session_deleted',
      'event_ended',
      'invoice_status', 'mailing_status',
      'consumption_order', 'badge_assigned', 'refund_processed', 'invoice_request',
      'invoice_cancelled', 'user.updated', 'delete_user', 'user_deleted',
    ];
    for (const type of validTypes) {
      const [valid] = receiver.validateXmlMessage(validParsed({ type }));
      expect(valid).toBe(true);
    }
  });

  test('send_invoice zonder master_uuid en met version 2.0 is geldig', () => {
    const parsed = validParsed({ type: 'send_invoice' });
    delete parsed.message.header.master_uuid;

    const [valid, err] = receiver.validateXmlMessage(parsed);

    expect(valid).toBe(true);
    expect(err).toBeNull();
  });

  test('planning session events zonder master_uuid zijn geldig', () => {
    for (const type of ['session_created', 'session_updated', 'session_deleted']) {
      const parsed = validParsed({ type, source: 'planning' });
      delete parsed.message.header.master_uuid;

      const [valid, err] = receiver.validateXmlMessage(parsed);

      expect(valid).toBe(true);
      expect(err).toBeNull();
    }
  });

  test('event_ended zonder master_uuid is geldig', () => {
    const parsed = validParsed({ type: 'event_ended', source: 'frontend' });
    delete parsed.message.header.master_uuid;

    const [valid, err] = receiver.validateXmlMessage(parsed);

    expect(valid).toBe(true);
    expect(err).toBeNull();
  });

  test('lazy types zonder master_uuid zijn geldig', () => {
    const lazyTypes = [
      'new_registration',
      'payment_registered',
      'badge_scanned',
      'consumption_order',
      'invoice_request',
      'user.updated',
    ];

    for (const type of lazyTypes) {
      const parsed = validParsed({ type });
      delete parsed.message.header.master_uuid;

      const [valid, err] = receiver.validateXmlMessage(parsed);

      expect(valid).toBe(true);
      expect(err).toBeNull();
    }
  });

  test('frontend user.unregistered met version 1.0 en zonder master_uuid is geldig', () => {
    const parsed = {
      message: {
        header: {
          message_id: 'id-frontend-1',
          version: '1.0',
          type: 'user.unregistered',
          timestamp: new Date().toISOString(),
          source: 'frontend.drupal',
          receiver: 'crm.salesforce planning.outlook mailing.sendgrid',
        },
      },
    };

    const [valid, err] = receiver.validateXmlMessage(parsed);
    expect(valid).toBe(true);
    expect(err).toBeNull();
  });

  test('user.created zonder master_uuid en version 1.0 is geldig', () => {
    const parsed = {
      message: {
        header: {
          message_id: 'id-uc-1',
          version: '1.0',
          type: 'user.created',
          timestamp: new Date().toISOString(),
          source: 'frontend.drupal',
        },
      },
    };

    const [valid, err] = receiver.validateXmlMessage(parsed);
    expect(valid).toBe(true);
    expect(err).toBeNull();
  });

  test('user.registered zonder master_uuid en version 1.0 is geldig', () => {
    const parsed = {
      message: {
        header: {
          message_id: 'id-ur-1',
          version: '1.0',
          type: 'user.registered',
          timestamp: new Date().toISOString(),
          source: 'frontend.drupal',
        },
      },
    };

    const [valid, err] = receiver.validateXmlMessage(parsed);
    expect(valid).toBe(true);
    expect(err).toBeNull();
  });
});

describe('handleMessage', () => {
  test('user_created met customer tag wordt naar handleUserCreated gerouteerd', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    const upsert = jest.fn().mockResolvedValue({});
    receiver.sf.apiCall.mockImplementation(async (callback) => callback({
      sobject: () => ({ upsert }),
    }));

    const xml = buildXml('user_created', `
      <customer>
        <identity_uuid>test-identity-1234</identity_uuid>
        <email>john.doe@example.com</email>
        <contact>
          <first_name>John</first_name>
          <last_name>Doe</last_name>
        </contact>
        <type>company</type>
        <company_name>Test Company NV</company_name>
        <vat_number>BE0123456789</vat_number>
      </customer>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(upsert).toHaveBeenCalledWith(expect.objectContaining({
      Company_Name__c: 'Test Company NV',
      VAT_Number__c: 'BE0123456789',
    }), 'Master_UUID__c');
  });

  test('user.created met user tag blijft achterwaarts compatibel', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({});

    const xml = buildXml('user.created', `
      <user>
        <master_uuid>test-master-uuid-1234</master_uuid>
        <email>jane.doe@example.com</email>
        <first_name>Jane</first_name>
        <last_name>Doe</last_name>
      </user>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sf.apiCall).toHaveBeenCalled();
  });

  test('user_registered met customer en session tag wordt naar handleUserRegistered gerouteerd', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({});

    const xml = buildXml('user_registered', `
      <customer>
        <identity_uuid>test-identity-5678</identity_uuid>
        <email>alice@example.com</email>
        <first_name>Alice</first_name>
        <last_name>Smith</last_name>
      </customer>
      <session>
        <session_id>sess-001</session_id>
        <session_name>Test Session</session_name>
      </session>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sf.apiCall).toHaveBeenCalled();
  });

  test('user_registered met customerData.session_id fallback wordt naar handleUserRegistered gerouteerd', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({});

    const xml = buildXml('user_registered', `
      <customer>
        <identity_uuid>test-identity-9012</identity_uuid>
        <email>bob@example.com</email>
        <first_name>Bob</first_name>
        <last_name>Jones</last_name>
        <session_id>sess-002</session_id>
      </customer>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sf.apiCall).toHaveBeenCalled();
  });

  test('ongeldige XML stuurt naar dead-letter en nackt', async () => {
    const receiver = makeReceiver();
    const msg = buildMsg('geen xml');

    await receiver.handleMessage(msg);

    expect(receiver.channel.nack).toHaveBeenCalledWith(msg, false, false);
    // Auto-DLX: handmatige sendToQueue is weg
    expect(receiver.channel.sendToQueue).not.toHaveBeenCalledWith('crm.dead-letter', expect.any(Buffer), expect.any(Object));
  });

  test('geldig bericht wordt geackt', async () => {
    const receiver = makeReceiver();
    const xml = buildXml('mailing_status', `
      <mailing_id>mail-1</mailing_id>
      <status>delivered</status>
      <delivered>10</delivered>
      <bounced>0</bounced>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.channel.ack).toHaveBeenCalled();
  });

  test('tijdelijke Salesforce timeout wordt naar retry queue gezet en geackt', async () => {
    const receiver = makeReceiver();
    const msg = buildMsg(buildXml('mailing_status', `
      <mailing_id>mail-1</mailing_id>
      <status>delivered</status>
      <delivered>10</delivered>
      <bounced>0</bounced>
    `));
    msg.fields.routingKey = 'crm.incoming';
    receiver.routeMessage = jest.fn().mockRejectedValue(Object.assign(
      new Error('Salesforce request timeout'),
      { isSalesforceError: true }
    ));

    await receiver.handleMessage(msg);

    expect(receiver.channel.sendToQueue).toHaveBeenCalledWith(
      'crm.incoming.retry',
      msg.content,
      expect.objectContaining({
        headers: expect.objectContaining({
          'x-crm-original-queue': 'crm.incoming',
          'x-crm-retry-count': 1,
        }),
      })
    );
    expect(receiver.channel.ack).toHaveBeenCalledWith(msg);
    expect(receiver.channel.nack).not.toHaveBeenCalled();
  });

  test('tijdelijke fout gaat na max retries naar dead-letter', async () => {
    const receiver = makeReceiver();
    const msg = buildMsg(buildXml('mailing_status', `
      <mailing_id>mail-1</mailing_id>
      <status>delivered</status>
      <delivered>10</delivered>
      <bounced>0</bounced>
    `));
    msg.properties = {
      headers: {
        'x-crm-original-queue': 'crm.incoming',
        'x-crm-retry-count': 288,
      },
    };
    receiver.routeMessage = jest.fn().mockRejectedValue(Object.assign(
      new Error('Salesforce request timeout'),
      { isSalesforceError: true }
    ));

    await receiver.handleMessage(msg);

    expect(receiver.channel.nack).toHaveBeenCalledWith(msg, false, false);
  });
});

describe('handleSendInvoice', () => {
  test('routeert send_invoice naar handleSendInvoice', async () => {
    const receiver = makeReceiver();
    receiver.handleSendInvoice = jest.fn().mockResolvedValue(undefined);
    const header = { type: 'send_invoice' };
    const body = {};

    await receiver.routeMessage(header, body);

    expect(receiver.handleSendInvoice).toHaveBeenCalledWith(header, body);
  });

  test('parset nested XML en update alleen de factuurvelden op Member__c', async () => {
    const receiver = makeReceiver();
    const update = jest.fn().mockResolvedValue({ id: 'sf-member-1' });
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockImplementation(async (callback) => callback({
      sobject: jest.fn().mockReturnValue({ update }),
    }));
    jest.spyOn(receiver, '_findUserByMasterUuid').mockResolvedValue('sf-member-1');
    jest.spyOn(receiver, '_findUserByEmail').mockResolvedValue(null);

    await receiver.handleMessage(buildMsg(buildSendInvoiceXml().replace(
      '<correlation_id>f47ac10b-58cc-4372-a567-0e02b2c3d479</correlation_id>',
      '<master_uuid>master-from-header</master_uuid><correlation_id>f47ac10b-58cc-4372-a567-0e02b2c3d479</correlation_id>'
    )));

    expect(receiver._findUserByMasterUuid).toHaveBeenCalledWith('master-from-header');
    expect(receiver._findUserByEmail).not.toHaveBeenCalled();
    expect(update).toHaveBeenCalledWith({
      Id: 'sf-member-1',
      Last_Invoice_URL__c: 'https://example.test/invoice.pdf',
      Last_Invoice_Due_Date__c: '2026-03-06',
      Last_Invoice_Number__c: 'INV-2026-001',
    });
    expect(receiver.channel.ack).toHaveBeenCalled();
  });

  test('vraagt lazy master_uuid op via customer.email wanneer master_uuid ontbreekt', async () => {
    const receiver = makeReceiver();
    const update = jest.fn().mockResolvedValue({ id: 'sf-member-2' });
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockImplementation(async (callback) => callback({
      sobject: jest.fn().mockReturnValue({ update }),
    }));
    jest.spyOn(receiver, '_findUserByMasterUuid').mockResolvedValue(null);
    jest.spyOn(receiver, '_findUserByEmail').mockResolvedValue('sf-member-2');

    await receiver.handleMessage(buildMsg(buildSendInvoiceXml()));

    expect(receiver.getOrCreateMasterUuid).toHaveBeenCalledWith('info@bedrijf.be', 'facturatie');
    expect(receiver._findUserByMasterUuid).toHaveBeenCalledWith('test-master-uuid-1234');
    expect(receiver._findUserByEmail).toHaveBeenCalledWith('info@bedrijf.be');
    expect(update).toHaveBeenCalledWith(expect.objectContaining({ Id: 'sf-member-2' }));
    expect(receiver.channel.ack).toHaveBeenCalled();
  });

  test('faalt expliciet wanneer geen Member__c gevonden wordt', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    jest.spyOn(receiver, '_findUserByMasterUuid').mockResolvedValue(null);
    jest.spyOn(receiver, '_findUserByEmail').mockResolvedValue(null);

    await expect(receiver.handleSendInvoice(
      { type: 'send_invoice' },
      {
        customer: { email: 'missing@example.com' },
        invoice: { id: 'INV-404', due_date: '2026-03-06', pdf_url: '' },
      }
    )).rejects.toThrow(/No Member__c found/);

    expect(receiver.sf.apiCall).not.toHaveBeenCalled();
  });
});

describe('handleNewRegistration', () => {
  test('stuurt registratie door naar Kassa en Facturatie', async () => {
    const receiver = makeReceiver();
    const xml = buildXml('new_registration', `
      <customer>
        <email>jan@example.com</email>
        <contact>
          <first_name>Jan</first_name>
          <last_name>Peeters</last_name>
        </contact>
        <type>private</type>
      </customer>
      <payment_due>
        <amount currency="eur">25.00</amount>
        <status>pending</status>
      </payment_due>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sender.sendNewRegistrationToKassa).toHaveBeenCalledWith(
      expect.objectContaining({
        customer: expect.objectContaining({ email: 'jan@example.com', master_uuid: 'test-master-uuid-1234' }),
        payment_due: expect.objectContaining({ status: 'unpaid' }),
      }),
    );
    expect(receiver.sender.sendNewRegistrationToFacturatie).toHaveBeenCalledWith(
      expect.objectContaining({
        master_uuid: 'test-master-uuid-1234',
        customer: expect.objectContaining({ email: 'jan@example.com' }),
      }),
    );
  });

  test('maakt Salesforce upsert als verbonden', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    const upsert = jest.fn().mockResolvedValue({ id: 'sf-member-1' });
    receiver.sf.apiCall.mockImplementation(async (callback) => callback({
      sobject: () => ({ upsert }),
    }));

    const xml = buildXml('new_registration', `
      <customer>
        <email>jan@example.com</email>
        <contact>
          <first_name>Jan</first_name>
          <last_name>Peeters</last_name>
        </contact>
        <type>private</type>
      </customer>
      <payment_due><amount>25</amount><status>paid</status></payment_due>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sf.apiCall).toHaveBeenCalled();
    expect(upsert).toHaveBeenCalledWith(
      expect.objectContaining({
        Master_UUID__c: 'test-master-uuid-1234',
        First_Name__c: 'Jan',
        Last_Name__c: 'Peeters',
        Email__c: 'jan@example.com',
      }),
      'Master_UUID__c',
    );
  });

  test('gebruikt identity_uuid uit contract als master uuid', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    const upsert = jest.fn().mockResolvedValue({ id: 'sf-member-1' });
    receiver.sf.apiCall.mockImplementation(async (callback) => callback({
      sobject: () => ({ upsert }),
    }));

    const xml = buildXml('new_registration', `
      <customer>
        <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-123456789abc</identity_uuid>
        <email>jan@example.com</email>
        <type>private</type>
        <is_company_linked>false</is_company_linked>
        <date_of_birth>1995-03-21</date_of_birth>
        <contact>
          <first_name>Jan</first_name>
          <last_name>Peeters</last_name>
        </contact>
        <address>Nijverheidskaai 170, 1070 Brussel</address>
        <session_id>sess-keynote-001</session_id>
        <payment_due><amount currency="eur">25</amount><status>unpaid</status></payment_due>
      </customer>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.getOrCreateMasterUuid).not.toHaveBeenCalled();
    expect(upsert).toHaveBeenCalledWith(
      expect.objectContaining({
        Master_UUID__c: 'e8b27c1d-4f2a-4b3e-9c5f-123456789abc',
        Street__c: 'Nijverheidskaai 170, 1070 Brussel',
      }),
      'Master_UUID__c',
    );
    expect(receiver.sender.sendNewRegistrationToKassa).toHaveBeenCalledWith(
      expect.objectContaining({
        customer: expect.objectContaining({
          master_uuid: 'e8b27c1d-4f2a-4b3e-9c5f-123456789abc',
          session_id: 'sess-keynote-001',
        }),
        session_id: 'sess-keynote-001',
      }),
    );
  });
});

describe('handlePaymentRegistered', () => {
  test('upsert Member__c in Salesforce bij payment_registered van kassa', async () => {

  const receiver = makeReceiver();

  receiver.sf.isConnected = true;

  const upsert = jest.fn().mockResolvedValue({ id: 'member-1' });

  receiver.sf.apiCall

    .mockResolvedValueOnce({ records: [] })                          // query: geen klantdata

    .mockImplementationOnce(async (callback) => callback({

      sobject: () => ({ upsert }),                                   // Member__c upsert

    }));

  const xml = buildXml('payment_registered', `

    <payment_context>registration</payment_context>

    <invoice>

      <id>INV-001</id>

      <amount_paid currency="eur">100.00</amount_paid>

      <status>paid</status>

    </invoice>

  `).replace('<source>test</source>', '<source>kassa</source>');

  await receiver.handleMessage(buildMsg(xml));

  expect(upsert).toHaveBeenCalledWith(

    expect.objectContaining({

      Master_UUID__c: 'test-master-uuid-1234',

      Payment_Status__c: 'paid',

      Amount__c: '100.00',

      Last_Invoice_Number__c: 'INV-001',

    }),

    'Master_UUID__c'

  );

});

 test('forwardt Kassa payment_registered naar Frontend en stuurt invoice_request naar Facturatie', async () => {
  const receiver = makeReceiver();
  receiver.sf.isConnected = true;
  receiver.sf.apiCall.mockResolvedValue({ records: [] });

  const xml = buildXml('payment_registered', `
    <payment_context>consumption</payment_context>
    <user_id>e8b27c1d-4f2a-4b3e-9c5f-123456789abc</user_id>
    <invoice>
      <status>paid</status>
      <amount_paid currency="eur">50.00</amount_paid>
    </invoice>
    <transaction>
      <id>TRX-2026-04150001</id>
      <payment_method>on_site</payment_method>
    </transaction>
  `).replace('<source>test</source>', '<source>kassa</source>');

  await receiver.handleMessage(buildMsg(xml));

  expect(receiver.sender.sendPaymentRegisteredToFrontend).toHaveBeenCalledWith(
    expect.objectContaining({ payment_context: 'consumption', amount_paid: '50.00' })
  );
  expect(receiver.sender.sendInvoiceRequest).toHaveBeenCalledWith(
    expect.objectContaining({ payment_context: 'consumption', amount_paid: '50.00' })
  );
  expect(receiver.channel.ack).toHaveBeenCalled();
});
});

describe('handleInvoiceStatus', () => {
  test('verwerkt Facturatie invoice_status v2.0 zonder master_uuid header', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    const createTask = jest.fn().mockResolvedValue({ id: 'task-1' });
    receiver.sf.apiCall.mockImplementation(async (callback) => callback({
      sobject: () => ({ create: createTask }),
    }));

    const xml = withoutMasterUuid(buildXml('invoice_status', `
      <invoice_id>foss-inv-00142</invoice_id>
      <user_id>e8b27c1d-4f2a-4b3e-9c5f-123456789abc</user_id>
      <status>paid</status>
      <amount currency="eur">150.00</amount>
      <due_date>2026-06-15</due_date>
    `));

    await receiver.handleMessage(buildMsg(xml));

    expect(createTask).toHaveBeenCalledWith(expect.objectContaining({
      Subject: expect.stringContaining('foss-inv-00142'),
      Description: expect.stringContaining('Status: paid'),
    }));
    expect(receiver.channel.ack).toHaveBeenCalled();
  });
});

describe('handleConsumptionOrder', () => {
  test('upsert consumpties in Salesforce als verbonden', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({ id: 'cons-1' });

    const xml = buildXml('consumption_order', `
      <is_anonymous>false</is_anonymous>
      <customer>
        <email>k@example.com</email>
        <master_uuid>test-master-uuid-1234</master_uuid>
      </customer>
      <items>
        <item>
          <id>line-1</id>
          <description>Koffie</description>
          <quantity>2</quantity>
          <unit_price currency="eur">3.50</unit_price>
        </item>
      </items>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sf.apiCall).toHaveBeenCalledWith(expect.any(Function));
    expect(receiver.sender.sendConsumptionOrderToFacturatie).toHaveBeenCalledWith(expect.stringContaining('<type>consumption_order</type>'));
  });
});

describe('handleBadgeScanned', () => {
  test('maakt lazy master_uuid aan wanneer alleen email aanwezig is', async () => {
    const receiver = makeReceiver();
    const xml = withoutMasterUuid(buildXml('badge_scanned', `
      <badge_id>BADGE-99</badge_id>
      <scan_type>entry</scan_type>
      <location>Main Hall</location>
      <email>badge@example.com</email>
    `));

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.getOrCreateMasterUuid).toHaveBeenCalledWith('badge@example.com', 'test');
    expect(receiver.channel.ack).toHaveBeenCalled();
  });

  test('maakt badge-scan task aan in Salesforce', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({ id: 'task-badge-1' });

    const xml = buildXml('badge_scanned', `
      <badge_id>BADGE-99</badge_id>
      <scan_type>entry</scan_type>
      <location>Main Hall</location>
      <email>badge@example.com</email>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sf.apiCall).toHaveBeenCalled();
  });
});

describe('handlePlanningSessionEvent', () => {
  test.each(['session_created', 'session_updated'])(
    'maakt Salesforce Task aan voor %s met speaker details',
    async (type) => {
      const receiver = makeReceiver();
      receiver.sf.isConnected = true;
      const createTask = jest.fn().mockResolvedValue({ id: 'task-session-1' });
      receiver.sf.apiCall.mockImplementation(async (callback) => callback({
        sobject: () => ({ create: createTask }),
      }));
      const xml = withoutMasterUuid(buildXml(type, `
        <session_id>sess-keynote-001</session_id>
        <title>Keynote: AI in Healthcare</title>
        <start_datetime>2026-05-15T14:00:00Z</start_datetime>
        <end_datetime>2026-05-15T15:00:00Z</end_datetime>
        <location>Aula A - Campus Jette</location>
        <session_type>keynote</session_type>
        <status>published</status>
        <max_attendees>120</max_attendees>
        <current_attendees>0</current_attendees>
        ${type === 'session_updated' ? '<change_reason>Spreker heeft 30 minuten vertraging door file</change_reason>' : ''}
        <speaker>
          <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-123456789abc</identity_uuid>
          <contact>
            <first_name>Sarah</first_name>
            <last_name>Leclercq</last_name>
          </contact>
          <organisation>UZ Brussel</organisation>
          <email>s.leclercq@uzbrussel.be</email>
        </speaker>
      `, { correlation_id: 'session-master-uuid-001' }));

      await receiver.handleMessage(buildMsg(xml));

      expect(receiver.channel.ack).toHaveBeenCalled();
      expect(receiver.channel.nack).not.toHaveBeenCalled();
      expect(createTask).toHaveBeenCalledWith(expect.objectContaining({
        Subject: expect.stringContaining('Keynote: AI in Healthcare'),
        Master_UUID__c: 'e8b27c1d-4f2a-4b3e-9c5f-123456789abc',
      }));
      expect(createTask).toHaveBeenCalledWith(expect.objectContaining({
        Description: expect.stringContaining('Speaker: Sarah Leclercq'),
      }));
      expect(createTask).toHaveBeenCalledWith(expect.objectContaining({
        Description: expect.stringContaining('Speaker organisation: UZ Brussel'),
      }));
      expect(createTask).toHaveBeenCalledWith(expect.objectContaining({
        Description: expect.stringContaining('Speaker email: s.leclercq@uzbrussel.be'),
      }));
    }
  );

  test('session_deleted behoudt forwarding naar Facturatie zonder Salesforce side effects', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    const xml = withoutMasterUuid(buildXml('session_deleted', `
      <session_id>sess-keynote-001</session_id>
      <reason>Sessie geannuleerd</reason>
    `, { correlation_id: 'session-master-uuid-001' }));

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.channel.ack).toHaveBeenCalled();
    expect(receiver.channel.nack).not.toHaveBeenCalled();
    expect(receiver.sender.sendEventEndedToFacturatie).toHaveBeenCalledWith(expect.objectContaining({
      session_id: 'sess-keynote-001',
    }));
    expect(receiver.sf.apiCall).not.toHaveBeenCalled();
  });

  test('negeert session event zonder session_id maar acked bericht', async () => {
    const receiver = makeReceiver();
    const xml = withoutMasterUuid(buildXml('session_updated', `
      <title>Keynote: AI in Healthcare</title>
    `));

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.channel.ack).toHaveBeenCalled();
    expect(receiver.sf.apiCall).not.toHaveBeenCalled();
  });

  test('accepteert event_ended zonder forwarding side effects', async () => {
    const receiver = makeReceiver();
    const xml = withoutMasterUuid(buildXml('event_ended', `
      <session_id>sess-keynote-001</session_id>
      <ended_at>2026-05-15T15:00:00Z</ended_at>
    `).replace('<source>test</source>', '<source>frontend</source>'));

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.channel.ack).toHaveBeenCalled();
    expect(receiver.channel.nack).not.toHaveBeenCalled();
    expect(receiver.sender.sendEventEndedToFacturatie).not.toHaveBeenCalled();
    expect(receiver.sf.apiCall).not.toHaveBeenCalled();
  });
});

describe('handleDeleteUser', () => {
  test('verwijdert record uit Salesforce als verbonden', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    receiver._findUserByMasterUuid = jest.fn().mockResolvedValue('sf-member-1');
    receiver.sf.apiCall.mockResolvedValue({});
    receiver.sender.sendLog = jest.fn().mockResolvedValue({});

    const xml = buildXml('delete_user', '<master_uuid>test-master-uuid-1234</master_uuid>');

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver._findUserByMasterUuid).toHaveBeenCalledWith('test-master-uuid-1234');
    expect(receiver.sf.apiCall).toHaveBeenCalledWith(expect.any(Function));
    expect(receiver.sender.sendLog).toHaveBeenCalledWith({
      level: 'info',
      action: 'delete_user',
      message: 'User test-master-uuid-1234 definitief verwijderd uit CRM.'
    });
  });

  test('gebruikt identity_uuid als master_uuid bij user_deleted berichten', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    receiver._findUserByMasterUuid = jest.fn().mockResolvedValue('sf-member-1');
    receiver.sf.apiCall.mockResolvedValue({});
    receiver.sender.sendLog = jest.fn().mockResolvedValue({});

    const xml = withoutMasterUuid(buildXml('user_deleted', `
      <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-123456789abc</identity_uuid>
      <email>jan.depeet@mail.com</email>
      <reason>Account op verzoek van gebruiker verwijderd</reason>
    `));

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver._findUserByMasterUuid).toHaveBeenCalledWith('e8b27c1d-4f2a-4b3e-9c5f-123456789abc');
    expect(receiver.sf.apiCall).toHaveBeenCalled();
    expect(receiver.sender.sendLog).toHaveBeenCalled();
  });
});

describe('handleInvoiceRequestFromKassa', () => {
  test('stuurt lazy master_uuid door naar facturatie als header master_uuid ontbreekt', async () => {
    const receiver = makeReceiver();
    const xml = withoutMasterUuid(buildXml('invoice_request', `
      <email>kassa@example.com</email>
      <invoice_data>
        <id>KINV-001</id>
        <amount_paid currency="eur">150.00</amount_paid>
        <status>pending</status>
        <due_date>2026-06-01</due_date>
      </invoice_data>
    `));

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.getOrCreateMasterUuid).toHaveBeenCalledWith('kassa@example.com', 'test');
    expect(receiver.sender.sendInvoiceRequest).toHaveBeenCalledWith(
      expect.objectContaining({ master_uuid: 'test-master-uuid-1234' }),
    );
  });

  test('stuurt factuurverzoek door via sender', async () => {
    const receiver = makeReceiver();
    const xml = buildXml('invoice_request', `
      <master_uuid>test-master-uuid-1234</master_uuid>
      <email>kassa@example.com</email>
      <invoice_data>
        <id>KINV-001</id>
        <amount_paid currency="eur">150.00</amount_paid>
        <status>pending</status>
        <due_date>2026-06-01</due_date>
      </invoice_data>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sender.sendInvoiceRequest).toHaveBeenCalledWith(
      expect.objectContaining({
        customer: expect.objectContaining({ email: 'kassa@example.com' }),
        invoice: expect.objectContaining({ amount: 150 }),
      }),
    );
  });
});

describe('handleReceivedInvoiceCancelled', () => {
  test('routeert invoice_cancelled naar de ontvang-handler', async () => {
    const receiver = makeReceiver();
    receiver.handleReceivedInvoiceCancelled = jest.fn().mockResolvedValue(undefined);

    const xml = buildXml('invoice_cancelled', `
      <invoice_number>INV-123</invoice_number>
      <reason>Cancelled in billing</reason>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.handleReceivedInvoiceCancelled).toHaveBeenCalled();
  });
});

describe('handleCancelRegistration', () => {
  let receiver;

  beforeEach(() => {
    receiver = makeReceiver();
    receiver.sender.sendCancelRegistrationToKassa = jest.fn().mockResolvedValue({ success: true });
    receiver.sender.sendCancelRegistrationToPlanning = jest.fn().mockResolvedValue({ success: true });
  });

  test('stuurt door naar Kassa en Planning bij geldig bericht', async () => {
    const xml = buildXml('cancel_registration', `
      <user_id>test-master-uuid-1234</user_id>
      <session_id>sess-keynote-001</session_id>
      <reason>Gebruiker gevraagd</reason>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sender.sendCancelRegistrationToKassa).toHaveBeenCalledWith(
      expect.objectContaining({ user_id: 'test-master-uuid-1234', session_id: 'sess-keynote-001' })
    );
    expect(receiver.sender.sendCancelRegistrationToPlanning).toHaveBeenCalledWith(
      expect.objectContaining({ user_id: 'test-master-uuid-1234', session_id: 'sess-keynote-001' })
    );
  });

  test('negeert bericht als user_id ontbreekt', async () => {
    const xml = buildXml('cancel_registration', `<session_id>sess-001</session_id>`);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sender.sendCancelRegistrationToKassa).not.toHaveBeenCalled();
    expect(receiver.sender.sendCancelRegistrationToPlanning).not.toHaveBeenCalled();
  });

  test('negeert bericht als session_id ontbreekt', async () => {
    const xml = buildXml('cancel_registration', `<user_id>test-master-uuid-1234</user_id>`);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sender.sendCancelRegistrationToKassa).not.toHaveBeenCalled();
  });

  test('update Member__c Status__c in Salesforce als verbonden', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({});
    receiver._findUserByMasterUuid = jest.fn().mockResolvedValue('sf-member-id-001');

    const xml = buildXml('cancel_registration', `
      <user_id>test-master-uuid-1234</user_id>
      <session_id>sess-keynote-001</session_id>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sf.apiCall).toHaveBeenCalled();
  });
});

describe('handleUserUnregistered', () => {
  let receiver;

  beforeEach(() => { receiver = makeReceiver(); });

  test('frontend user.unregistered wordt via sender naar fanout gestuurd', async () => {
    await receiver.handleMessage(buildMsg(buildFrontendUserUnregisteredXml()));

    expect(receiver.sender.sendUserUnregisteredFanout).toHaveBeenCalledWith({
      identity_uuid: 'test-master-uuid-1234',
      email: 'test@example.com',
      reason: 'User requested unregistration'
    });
  });

  test('user.unregistered met master_uuid wordt geaccepteerd met version 1.0', async () => {
    await receiver.handleMessage(buildMsg(buildFrontendUserUnregisteredXml()));

    expect(receiver.channel.ack).toHaveBeenCalled();
    expect(receiver.channel.nack).not.toHaveBeenCalled();
  });
});

describe('handleIdentityUserEvent', () => {
  let receiver;

  function buildIdentityEvent(overrides = {}) {
    return Buffer.from(`<?xml version="1.0" encoding="UTF-8"?>
<user_event>
  <event>${overrides.event || 'UserCreated'}</event>
  <master_uuid>${overrides.master_uuid || 'identity-uuid-001'}</master_uuid>
  <email>${overrides.email || 'jan.peeters@ehb.be'}</email>
  <source_system>${overrides.source_system || 'frontend'}</source_system>
  <timestamp>2026-05-04T10:00:00Z</timestamp>
</user_event>`);
  }

  function buildIdentityMsg(overrides = {}) {
    return { content: buildIdentityEvent(overrides), fields: { deliveryTag: 99 } };
  }

  beforeEach(() => { receiver = makeReceiver(); });

  test('upsert Member__c in Salesforce bij UserCreated als verbonden', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({ id: 'sf-001', success: true });

    await receiver.handleIdentityUserEvent(buildIdentityMsg());

    expect(receiver.sf.apiCall).toHaveBeenCalled();
    expect(receiver.channel.ack).toHaveBeenCalled();
  });

  test('ack zonder SF update in DRY RUN mode', async () => {
    receiver.sf.isConnected = false;

    await receiver.handleIdentityUserEvent(buildIdentityMsg());

    expect(receiver.sf.apiCall).not.toHaveBeenCalled();
    expect(receiver.channel.ack).toHaveBeenCalled();
  });

  test('nack bij onbekend event type', async () => {
    await receiver.handleIdentityUserEvent(buildIdentityMsg({ event: 'UserUpdated' }));

    expect(receiver.channel.ack).toHaveBeenCalled();
    expect(receiver.channel.nack).not.toHaveBeenCalled();
  });

  test('nack bij ontbrekende master_uuid', async () => {
    const msg = { content: Buffer.from('<user_event><event>UserCreated</event><email>x@x.com</email></user_event>'), fields: { deliveryTag: 99 } };

    await receiver.handleIdentityUserEvent(msg);

    expect(receiver.channel.nack).toHaveBeenCalled();
    expect(receiver.channel.ack).not.toHaveBeenCalled();
  });

  test('nack bij ongeldige XML', async () => {
    const msg = { content: Buffer.from('dit is geen xml'), fields: { deliveryTag: 99 } };

    await receiver.handleIdentityUserEvent(msg);

    expect(receiver.channel.nack).toHaveBeenCalled();
  });
});
