'use strict';

jest.mock('../src/sfConnection', () => {
  return jest.fn().mockImplementation(() => ({
    init: jest.fn().mockResolvedValue(undefined),
    isConnected: false,
    apiCall: jest.fn().mockResolvedValue([]),
  }));
});

jest.mock('../src/sender', () => {
  return jest.fn().mockImplementation(() => ({
    init: jest.fn().mockResolvedValue(undefined),
    close: jest.fn().mockResolvedValue(undefined),
    sendNewRegistrationToKassa: jest.fn().mockResolvedValue({ success: true }),
    sendNewRegistrationToFacturatie: jest.fn().mockResolvedValue({ success: true }),
    sendInvoiceCancelledToFacturatie: jest.fn().mockResolvedValue({ success: true }),
    sendInvoiceRequest: jest.fn().mockResolvedValue({ success: true }),
    sendUserUnregisteredFanout: jest.fn().mockResolvedValue({ success: true }),
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

function buildMsg(xmlString) {
  return {
    content: Buffer.from(xmlString, 'utf8'),
    fields: { deliveryTag: 1 },
  };
}

function buildFrontendUserUnregisteredXml(overrides = {}) {
  const headerTimestamp = overrides.headerTimestamp || new Date().toISOString();
  const bodyTimestamp = overrides.bodyTimestamp || headerTimestamp;

  return `<?xml version="1.0" encoding="UTF-8"?>
<message xmlns="urn:integration:planning:v1">
  <header>
    <message_id>${overrides.messageId || `msg-${Math.random().toString(36).slice(2)}`}</message_id>
    <timestamp>${headerTimestamp}</timestamp>
    <source>${overrides.source || 'frontend.drupal'}</source>
    <receiver>${overrides.receiver || 'crm.salesforce planning.outlook mailing.sendgrid'}</receiver>
    <type>user.unregistered</type>
    <version>${overrides.version || '1.0'}</version>
    <correlation_id>${overrides.correlationId || ''}</correlation_id>
  </header>
  <body>
    <user_id>${overrides.userId || 'user-001'}</user_id>
    <session_id>${overrides.sessionId || 'sess-42'}</session_id>
    <timestamp>${bodyTimestamp}</timestamp>
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
  test('geldig bericht geeft true terug', () => {
    const receiver = makeReceiver();
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
    const receiver = makeReceiver();
    const [valid, err] = receiver.validateXmlMessage({});
    expect(valid).toBe(false);
    expect(err).toMatch(/Missing message root/);
  });

  test('ontbrekende header geeft fout', () => {
    const [valid, err] = receiver.validateXmlMessage({ message: {} });
    expect(valid).toBe(false);
    expect(err).toMatch(/Missing header/);
  });

  test('ontbrekend verplicht veld geeft fout', () => {
    const parsed = validParsed();
    delete parsed.message.header.message_id;
    const [valid, err] = receiver.validateXmlMessage(parsed);
    expect(valid).toBe(false);
    expect(err).toMatch(/message_id/);
  });

  test('verkeerde versie geeft fout', () => {
    const [valid, err] = receiver.validateXmlMessage(validParsed({ version: '1.0' }));
    expect(valid).toBe(false);
    expect(err).toMatch(/Invalid version/);
  });

  test('onbekend message type geeft fout', () => {
    const [valid, err] = receiver.validateXmlMessage(validParsed({ type: 'unknown_type' }));
    expect(valid).toBe(false);
    expect(err).toMatch(/Invalid message type/);
  });

  test('alle geldige message types worden geaccepteerd', () => {
    const validTypes = [
      'new_registration', 'payment_registered', 'badge_scanned', 'session_update',
      'invoice_status', 'mailing_status', 'consumption_order', 'badge_assigned',
      'refund_processed', 'invoice_request',
    ];
    for (const type of validTypes) {
      const [valid] = receiver.validateXmlMessage(validParsed({ type }));
      expect(valid).toBe(true);
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
});

describe('handleMessage', () => {
  test('ongeldige XML stuurt naar dead-letter en nackt', async () => {
    const receiver = makeReceiver();
    const msg = buildMsg('geen xml');

    await receiver.handleMessage(msg);

    expect(receiver.channel.nack).toHaveBeenCalledWith(msg, false, false);
    expect(receiver.channel.sendToQueue).toHaveBeenCalledWith(
      'crm.dead-letter',
      expect.any(Buffer),
      expect.any(Object),
    );
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
        <user_id>u-1</user_id>
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
        customer: expect.objectContaining({ email: 'jan@example.com', user_id: 'u-1' }),
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
    receiver.sf.apiCall.mockResolvedValue({ id: 'sf-member-1' });

    const xml = buildXml('new_registration', `
      <customer>
        <email>jan@example.com</email>
        <contact>
          <first_name>Jan</first_name>
          <last_name>Peeters</last_name>
        </contact>
        <user_id>u-1</user_id>
        <type>private</type>
      </customer>
      <payment_due><amount>25</amount><status>paid</status></payment_due>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sf.apiCall).toHaveBeenCalled();
  });
});

describe('handlePaymentRegistered', () => {
  test('maakt Task aan in Salesforce als verbonden', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({ id: 'task-1' });

    const xml = buildXml('payment_registered', `
      <user_id>u-10</user_id>
      <email>pay@example.com</email>
      <payment_context>registration</payment_context>
      <invoice>
        <id>INV-001</id>
        <amount_paid currency="eur">100.00</amount_paid>
        <due_date>2026-12-31</due_date>
        <status>paid</status>
      </invoice>
      <transaction>
        <payment_method>card</payment_method>
      </transaction>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sf.apiCall).toHaveBeenCalled();
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
        <user_id>u-20</user_id>
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
  });
});

describe('handleBadgeScanned', () => {
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

describe('handleDeleteUser', () => {
  test('zet Is_Deleted__c in Salesforce als verbonden', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({});

    const xml = buildXml('delete_user', '<user_id>u-42</user_id>');

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sf.apiCall).toHaveBeenCalled();
  });
});

describe('handleInvoiceRequestFromKassa', () => {
  test('stuurt factuurverzoek door via sender', async () => {
    const receiver = makeReceiver();
    const xml = buildXml('invoice_request', `
      <user_id>u-50</user_id>
      <email>kassa@example.com</email>
      <invoice>
        <id>KINV-001</id>
        <amount_paid currency="eur">150.00</amount_paid>
        <status>pending</status>
        <due_date>2026-06-01</due_date>
      </invoice>
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

describe('handleUserUnregistered', () => {
  let receiver;

  beforeEach(() => { receiver = makeReceiver(); });

  test('frontend user.unregistered wordt via sender naar fanout gestuurd', async () => {
    await receiver.handleMessage(buildMsg(buildFrontendUserUnregisteredXml()));

    expect(receiver.sender.sendUserUnregisteredFanout).toHaveBeenCalledWith(
      expect.objectContaining({
        message_id: expect.any(String),
        source: 'frontend.drupal',
        receiver: 'crm.salesforce planning.outlook mailing.sendgrid',
        user_id: 'user-001',
        session_id: 'sess-42',
      }),
    );
  });

  test('user.unregistered zonder master_uuid wordt geaccepteerd met version 1.0', async () => {
    await receiver.handleMessage(buildMsg(buildFrontendUserUnregisteredXml()));

    expect(receiver.channel.ack).toHaveBeenCalled();
    expect(receiver.channel.nack).not.toHaveBeenCalled();
  });
});
