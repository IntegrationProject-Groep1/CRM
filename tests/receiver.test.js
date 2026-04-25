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
    <master_uuid>${overrides.masterUuid || 'test-master-uuid-1234'}</master_uuid>
    <session_id>${overrides.sessionId || 'sess-42'}</session_id>
    <timestamp>${bodyTimestamp}</timestamp>
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
      'refund_processed', 'invoice_request', 'invoice_cancelled', 'send_invoice',
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

  test('valt terug op customer.email wanneer master_uuid ontbreekt', async () => {
    const receiver = makeReceiver();
    const update = jest.fn().mockResolvedValue({ id: 'sf-member-2' });
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockImplementation(async (callback) => callback({
      sobject: jest.fn().mockReturnValue({ update }),
    }));
    jest.spyOn(receiver, '_findUserByMasterUuid').mockResolvedValue(null);
    jest.spyOn(receiver, '_findUserByEmail').mockResolvedValue('sf-member-2');

    await receiver.handleMessage(buildMsg(buildSendInvoiceXml()));

    expect(receiver._findUserByMasterUuid).not.toHaveBeenCalled();
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
    receiver.sf.apiCall.mockResolvedValue({ id: 'sf-member-1' });

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
  });
});

describe('handlePaymentRegistered', () => {
  test('maakt Task aan in Salesforce als verbonden en neemt transaction_id mee', async () => {
    const receiver = makeReceiver();
    receiver.sf.isConnected = true;
    receiver._findUserByEmail = jest.fn().mockResolvedValue('member-1');
    const createTask = jest.fn().mockResolvedValue({ id: 'task-1' });
    receiver.sf.apiCall.mockImplementation(async (callback) => callback({
      sobject: () => ({ create: createTask }),
    }));

    const xml = buildXml('payment_registered', `
      <master_uuid>test-master-uuid-1234</master_uuid>
      <email>pay@example.com</email>
      <payment_context>registration</payment_context>
      <invoice>
        <id>INV-001</id>
        <amount_paid currency="eur">100.00</amount_paid>
        <due_date>2026-12-31</due_date>
        <status>paid</status>
      </invoice>
      <transaction>
        <transaction_id>TX-12345</transaction_id>
        <payment_method>card</payment_method>
      </transaction>
    `);

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sf.apiCall).toHaveBeenCalled();
    expect(createTask).toHaveBeenCalledWith(expect.objectContaining({
      Description: expect.stringContaining('Transaction ID: TX-12345'),
    }));
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

    const xml = buildXml('delete_user', '<master_uuid>test-master-uuid-1234</master_uuid>');

    await receiver.handleMessage(buildMsg(xml));

    expect(receiver.sf.apiCall).toHaveBeenCalled();
  });
});

describe('handleInvoiceRequestFromKassa', () => {
  test('stuurt factuurverzoek door via sender', async () => {
    const receiver = makeReceiver();
    const xml = buildXml('invoice_request', `
      <master_uuid>test-master-uuid-1234</master_uuid>
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
        master_uuid: 'test-master-uuid-1234',
        session_id: 'sess-42',
      }),
    );
  });

  test('user.unregistered met master_uuid wordt geaccepteerd met version 1.0', async () => {
    await receiver.handleMessage(buildMsg(buildFrontendUserUnregisteredXml()));

    expect(receiver.channel.ack).toHaveBeenCalled();
    expect(receiver.channel.nack).not.toHaveBeenCalled();
  });
});
