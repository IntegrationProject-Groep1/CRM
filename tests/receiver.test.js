'use strict';

/**
 * Unit tests for ReceiverV2.
 *
 * All external dependencies are mocked:
 *  - SFConnection  (Salesforce)
 *  - MySQLService  (MySQL)
 *  - CRMSender     (RabbitMQ outbound)
 *  - amqplib       (RabbitMQ inbound)
 *
 * Flows covered:
 *  - validateXmlMessage        — header validation
 *  - handleMessage             — XML parse errors, validation errors, routing, ack/nack
 *  - handleNewRegistration     — registratie flow
 *  - handlePaymentRegistered   — betaling flow
 *  - handleConsumptionOrder    — consumptie flow
 *  - handleBadgeScanned        — badge flow
 *  - handleSessionUpdate       — sessie flow
 *  - handleInvoiceStatus       — factuur status flow
 *  - handleMailingStatus       — mailing flow
 *  - handleBadgeAssigned       — badge toewijzing flow
 *  - handleRefundProcessed     — terugbetaling flow
 *  - handleInvoiceRequestFromKassa — factuurverzoek flow
 *  - getElementText            — statische hulpfunctie
 *  - sendToDeadLetter          — dead-letter routing
 */

// ── Module mocks (must be before require of the module under test) ─────────────

jest.mock('../src/sfConnection', () => {
  return jest.fn().mockImplementation(() => ({
    init: jest.fn().mockResolvedValue(undefined),
    isConnected: false,
    apiCall: jest.fn().mockResolvedValue([]),
  }));
});

jest.mock('../src/mysqlClient', () => {
  return jest.fn().mockImplementation(() => ({
    init: jest.fn(),
    upsertPerson: jest.fn().mockResolvedValue(1),
    upsertCompany: jest.fn().mockResolvedValue(1),
    insertPayment: jest.fn().mockResolvedValue(1),
    insertConsumption: jest.fn().mockResolvedValue(1),
    findPersonByExternalId: jest.fn().mockResolvedValue(null),
    findPersonByEmailForCheckIn: jest.fn().mockResolvedValue({ personId: null, error: null }),
    findEventAttendeeByPersonId: jest.fn().mockResolvedValue(null),
    updateEventAttendeeCheckIn: jest.fn().mockResolvedValue(undefined),
    softDeletePerson: jest.fn().mockResolvedValue(true),
    softDeleteConsumptionsByUserId: jest.fn().mockResolvedValue(true),
  }));
});

jest.mock('../src/sender', () => {
  return jest.fn().mockImplementation(() => ({
    init: jest.fn().mockResolvedValue(undefined),
    close: jest.fn().mockResolvedValue(undefined),
    sendNewRegistrationToKassa: jest.fn().mockResolvedValue({ success: true }),
    sendInvoiceRequest: jest.fn().mockResolvedValue({ success: true }),
  }));
});

jest.mock('amqplib');
jest.mock('../src/amqpUrl', () => ({ getAmqpOptions: jest.fn().mockReturnValue({}) }));

// ── Imports ───────────────────────────────────────────────────────────────────

// Suppress console.log noise during tests
beforeAll(() => jest.spyOn(console, 'log').mockImplementation(() => {}));
afterAll(() => console.log.mockRestore());

const ReceiverV2 = require('../src/receiver');

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Build a minimal valid XML message string. */
function buildXml(type, bodyXml = '', extra = {}) {
  const correlationId = extra.correlation_id
    ? `<correlation_id>${extra.correlation_id}</correlation_id>` : '';
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

/** Build a RabbitMQ message object as amqplib would deliver it. */
function buildMsg(xmlString) {
  return {
    content: Buffer.from(xmlString, 'utf8'),
    fields: { deliveryTag: 1 },
  };
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
  // Mock identity service RPC — returns a fixed master UUID without hitting RabbitMQ
  receiver.getOrCreateMasterUuid = jest.fn().mockResolvedValue('test-master-uuid-1234');
  return receiver;
}

// ─────────────────────────────────────────────────────────────────────────────
// getElementText (static helper)
// ─────────────────────────────────────────────────────────────────────────────

describe('ReceiverV2.getElementText', () => {
  test('geeft null terug voor null object', () => {
    expect(ReceiverV2.getElementText(null, 'key')).toBeNull();
  });

  test('geeft null terug als sleutel niet bestaat', () => {
    expect(ReceiverV2.getElementText({ a: '1' }, 'b')).toBeNull();
  });

  test('geeft string terug voor primitive waarde', () => {
    expect(ReceiverV2.getElementText({ name: 'Jan' }, 'name')).toBe('Jan');
  });

  test('haalt #text op uit object waarde', () => {
    expect(ReceiverV2.getElementText({ amount: { '#text': '99.99', currency: 'eur' } }, 'amount')).toBe('99.99');
  });

  test('geeft eerste element terug van array', () => {
    expect(ReceiverV2.getElementText({ tags: ['first', 'second'] }, 'tags')).toBe('first');
  });

  test('haalt #text op uit eerste array-element object', () => {
    expect(ReceiverV2.getElementText({ val: [{ '#text': 'hello' }] }, 'val')).toBe('hello');
  });

  test('converteert nummer naar string', () => {
    expect(ReceiverV2.getElementText({ age: 30 }, 'age')).toBe('30');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// validateXmlMessage
// ─────────────────────────────────────────────────────────────────────────────

describe('validateXmlMessage', () => {
  let receiver;

  beforeEach(() => { receiver = makeReceiver(); });

  const validParsed = (overrides = {}) => ({
    message: {
      header: {
        message_id: 'id-1',
        version: '2.0',
        type: 'new_registration',
        timestamp: new Date().toISOString(),
        source: 'test',
        master_uuid: 'test-master-uuid-1234',
        ...overrides,
      },
    },
  });

  test('geldig bericht geeft [true, null]', () => {
    const [valid, err] = receiver.validateXmlMessage(validParsed());
    expect(valid).toBe(true);
    expect(err).toBeNull();
  });

  test('ontbrekend message root element geeft fout', () => {
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
});

// ─────────────────────────────────────────────────────────────────────────────
// handleMessage — routing & ack/nack
// ─────────────────────────────────────────────────────────────────────────────

describe('handleMessage', () => {
  let receiver;

  beforeEach(() => { receiver = makeReceiver(); });

  test('ongeldige XML stuurt naar dead-letter en nack', async () => {
    const msg = buildMsg('dit is geen xml!!!');
    await receiver.handleMessage(msg);
    expect(receiver.channel.nack).toHaveBeenCalledWith(msg, false, false);
    expect(receiver.channel.sendToQueue).toHaveBeenCalledWith(
      'crm.dead-letter', expect.any(Buffer), expect.any(Object),
    );
  });

  test('validatiefout stuurt naar dead-letter en nack', async () => {
    const xml = `<message><header><type>new_registration</type></header></message>`;
    const msg = buildMsg(xml);
    await receiver.handleMessage(msg);
    expect(receiver.channel.nack).toHaveBeenCalledWith(msg, false, false);
  });

  test('geldig bericht roept ack aan na verwerking', async () => {
    const xml = buildXml('mailing_status', `
      <mailing_id>mail-1</mailing_id>
      <status>delivered</status>
      <delivered>100</delivered>
      <bounced>2</bounced>
    `);
    await receiver.handleMessage(buildMsg(xml));
    expect(receiver.channel.ack).toHaveBeenCalled();
  });

  test('handler die een fout gooit roept nack aan', async () => {
    receiver.routeMessage = jest.fn().mockRejectedValue(new Error('handler crash'));
    const xml = buildXml('mailing_status');
    await receiver.handleMessage(buildMsg(xml));
    expect(receiver.channel.nack).toHaveBeenCalled();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// sendToDeadLetter
// ─────────────────────────────────────────────────────────────────────────────

describe('sendToDeadLetter', () => {
  test('stuurt naar crm.dead-letter met reden en timestamp', () => {
    const receiver = makeReceiver();
    const content = Buffer.from('<message/>', 'utf8');
    receiver.sendToDeadLetter(content, 'TEST_REASON');

    expect(receiver.channel.sendToQueue).toHaveBeenCalledWith(
      'crm.dead-letter',
      expect.any(Buffer),
      { persistent: true },
    );

    const sentBuffer = receiver.channel.sendToQueue.mock.calls[0][1];
    const parsed = JSON.parse(sentBuffer.toString('utf8'));
    expect(parsed.reason).toBe('TEST_REASON');
    expect(parsed.originalContent).toBe('<message/>');
    expect(parsed.timestamp).toBeDefined();
  });

  test('doet niets als channel null is', () => {
    const receiver = makeReceiver();
    receiver.channel = null;
    expect(() => receiver.sendToDeadLetter(Buffer.from('x'), 'R')).not.toThrow();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// REGISTRATIE FLOW — handleNewRegistration
// ─────────────────────────────────────────────────────────────────────────────

describe('handleNewRegistration', () => {
  let receiver;

  beforeEach(() => { receiver = makeReceiver(); });

  function registratieXml(overrides = {}) {
    return buildXml('new_registration', `
      <customer>
        <email>${overrides.email || 'jan@example.com'}</email>
        <contact>
          <first_name>Jan</first_name>
          <last_name>Peeters</last_name>
        </contact>
        <user_id>u-1</user_id>
        <type>private</type>
        <age>30</age>
      </customer>
      <payment_due>
        <amount currency="eur">25.00</amount>
        <status>${overrides.payment_status || 'pending'}</status>
      </payment_due>
    `);
  }

  test('stuurt new_registration door naar Kassa via sender', async () => {
    await receiver.handleMessage(buildMsg(registratieXml()));
    expect(receiver.sender.sendNewRegistrationToKassa).toHaveBeenCalledWith(
      expect.objectContaining({
        customer: expect.objectContaining({ email: 'jan@example.com' }),
        payment_due: expect.objectContaining({ status: 'unpaid' }),
      }),
    );
  });

  test('payment_status "paid" wordt als "paid" doorgestuurd', async () => {
    await receiver.handleMessage(buildMsg(registratieXml({ payment_status: 'paid' })));
    expect(receiver.sender.sendNewRegistrationToKassa).toHaveBeenCalledWith(
      expect.objectContaining({
        payment_due: expect.objectContaining({ status: 'paid' }),
      }),
    );
  });

  test('roept upsertPerson aan in MySQL', async () => {
    await receiver.handleMessage(buildMsg(registratieXml()));
    expect(receiver.db.upsertPerson).toHaveBeenCalledWith(
      expect.objectContaining({ email: 'jan@example.com', external_user_id: 'u-1' }),
    );
  });

  test('ontbrekend customer element logt en keert terug zonder fout', async () => {
    const xml = buildXml('new_registration', '<other>x</other>');
    await receiver.handleMessage(buildMsg(xml));
    expect(receiver.channel.ack).toHaveBeenCalled();
  });

  test('SF upsert wordt aangeroepen als SF verbonden is', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({ id: 'sf-id-1' });
    await receiver.handleMessage(buildMsg(registratieXml()));
    expect(receiver.sf.apiCall).toHaveBeenCalled();
  });

  test('bedrijfsdata wordt verwerkt als is_company_linked true is', async () => {
    const xml = buildXml('new_registration', `
      <customer>
        <email>co@example.com</email>
        <contact><first_name>Dirk</first_name><last_name>De Smet</last_name></contact>
        <user_id>u-2</user_id>
        <is_company_linked>true</is_company_linked>
        <type>company</type>
        <age>40</age>
      </customer>
      <company>
        <name>Acme NV</name>
        <vat_number>BE0123456789</vat_number>
        <email>info@acme.be</email>
      </company>
      <payment_due><amount>0</amount><status>pending</status></payment_due>
    `);
    await receiver.handleMessage(buildMsg(xml));
    expect(receiver.db.upsertCompany).toHaveBeenCalledWith(
      expect.objectContaining({ company_name: 'Acme NV', vat_number: 'BE0123456789' }),
    );
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// BETALING FLOW — handlePaymentRegistered
// ─────────────────────────────────────────────────────────────────────────────

describe('handlePaymentRegistered', () => {
  let receiver;

  beforeEach(() => { receiver = makeReceiver(); });

  function betalingXml(overrides = {}) {
    return buildXml('payment_registered', `
      <user_id>${overrides.user_id || 'u-10'}</user_id>
      <email>${overrides.email || ''}</email>
      <payment_context>${overrides.context || 'registration'}</payment_context>
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
  }

  test('slaat betaling op in MySQL', async () => {
    await receiver.handleMessage(buildMsg(betalingXml()));
    expect(receiver.db.insertPayment).toHaveBeenCalledWith(
      expect.objectContaining({ amount: 100, payment_type: 'registration', status: 'completed' }),
    );
  });

  test('consumptie context geeft payment_type "consumption"', async () => {
    await receiver.handleMessage(buildMsg(betalingXml({ context: 'consumption' })));
    expect(receiver.db.insertPayment).toHaveBeenCalledWith(
      expect.objectContaining({ payment_type: 'consumption' }),
    );
  });

  test('SF Task wordt aangemaakt als SF verbonden is', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({ id: 'task-sf-1' });
    await receiver.handleMessage(buildMsg(betalingXml({ email: 'k@example.com' })));
    expect(receiver.sf.apiCall).toHaveBeenCalled();
  });

  test('SF Task wordt NIET aangemaakt in dry-run mode', async () => {
    receiver.sf.isConnected = false;
    await receiver.handleMessage(buildMsg(betalingXml()));
    expect(receiver.sf.apiCall).not.toHaveBeenCalled();
  });

  test('event attendee wordt opgezocht via user_id voor MySQL koppeling', async () => {
    receiver.db.findPersonByExternalId.mockResolvedValue(5);
    receiver.db.findEventAttendeeByPersonId.mockResolvedValue(10);
    await receiver.handleMessage(buildMsg(betalingXml({ user_id: 'u-10' })));
    expect(receiver.db.findPersonByExternalId).toHaveBeenCalledWith('u-10');
    expect(receiver.db.insertPayment).toHaveBeenCalledWith(
      expect.objectContaining({ event_attendee_id: 10 }),
    );
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// CONSUMPTIE FLOW — handleConsumptionOrder
// ─────────────────────────────────────────────────────────────────────────────

describe('handleConsumptionOrder', () => {
  let receiver;

  beforeEach(() => { receiver = makeReceiver(); });

  function consumptieXml(overrides = {}) {
    return buildXml('consumption_order', `
      <is_anonymous>${overrides.anonymous || 'false'}</is_anonymous>
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
          <vat_rate>21</vat_rate>
          <sku>SKU-001</sku>
        </item>
      </items>
    `);
  }

  test('dry-run logt en returnt als SF niet verbonden is', async () => {
    receiver.sf.isConnected = false;
    await receiver.handleMessage(buildMsg(consumptieXml()));
    expect(receiver.sf.apiCall).not.toHaveBeenCalled();
    expect(receiver.channel.ack).toHaveBeenCalled();
  });

  test('SF upsert Consumption__c wordt aangeroepen per item als verbonden', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({ id: 'cons-sf-1' });
    await receiver.handleMessage(buildMsg(consumptieXml()));
    expect(receiver.sf.apiCall).toHaveBeenCalledWith(expect.any(Function));
  });

  test('consumptie wordt in MySQL opgeslagen als attendee gevonden', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({});
    receiver.db.findPersonByExternalId.mockResolvedValue(3);
    receiver.db.findEventAttendeeByPersonId.mockResolvedValue(7);
    await receiver.handleMessage(buildMsg(consumptieXml()));
    expect(receiver.db.insertConsumption).toHaveBeenCalledWith(
      expect.objectContaining({ item_name: 'Koffie', quantity: 2, event_attendee_id: 7 }),
    );
  });

  test('anonieme consumptie slaat geen MySQL data op', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({});
    await receiver.handleMessage(buildMsg(consumptieXml({ anonymous: 'true' })));
    expect(receiver.db.insertConsumption).not.toHaveBeenCalled();
  });

  test('meerdere items worden allemaal verwerkt', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({});
    receiver.db.findPersonByExternalId.mockResolvedValue(3);
    receiver.db.findEventAttendeeByPersonId.mockResolvedValue(7);

    const xml = buildXml('consumption_order', `
      <is_anonymous>false</is_anonymous>
      <customer><email>k@example.com</email><user_id>u-20</user_id></customer>
      <items>
        <item><id>l-1</id><description>Koffie</description><quantity>1</quantity><unit_price>3.50</unit_price></item>
        <item><id>l-2</id><description>Water</description><quantity>2</quantity><unit_price>2.00</unit_price></item>
      </items>
    `);
    await receiver.handleMessage(buildMsg(xml));
    expect(receiver.db.insertConsumption).toHaveBeenCalledTimes(2);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// handleBadgeScanned
// ─────────────────────────────────────────────────────────────────────────────

describe('handleBadgeScanned', () => {
  let receiver;

  beforeEach(() => { receiver = makeReceiver(); });

  const badgeXml = () => buildXml('badge_scanned', `
    <badge_id>BADGE-99</badge_id>
    <scan_type>entry</scan_type>
    <location>Main Hall</location>
    <email>badge@example.com</email>
  `);

  test('dry-run mode maakt geen SF call', async () => {
    receiver.sf.isConnected = false;
    await receiver.handleMessage(buildMsg(badgeXml()));
    expect(receiver.sf.apiCall).not.toHaveBeenCalled();
    expect(receiver.channel.ack).toHaveBeenCalled();
  });

  test('SF Task wordt aangemaakt als verbonden', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({ id: 'task-badge-1' });
    await receiver.handleMessage(buildMsg(badgeXml()));
    expect(receiver.sf.apiCall).toHaveBeenCalled();
  });

  test('check-in tijd wordt bijgewerkt in MySQL als persoon gevonden', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({ id: 't-1' });
    receiver.db.findPersonByEmailForCheckIn.mockResolvedValue({ personId: 5, error: null });
    await receiver.handleMessage(buildMsg(badgeXml()));
    expect(receiver.db.updateEventAttendeeCheckIn).toHaveBeenCalledWith(5);
  });

  test('ontbrekende body logt en returnt zonder fout', async () => {
    const xml = buildXml('badge_scanned', '');
    // Override: lege body is geen null, verwijder body volledig
    const rawXml = `<?xml version="1.0"?><message>
      <header><message_id>x</message_id><version>2.0</version><type>badge_scanned</type>
      <timestamp>${new Date().toISOString()}</timestamp><source>test</source><master_uuid>test-master-uuid-1234</master_uuid></header>
    </message>`;
    await receiver.handleMessage(buildMsg(rawXml));
    expect(receiver.channel.ack).toHaveBeenCalled();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// handleSessionUpdate
// ─────────────────────────────────────────────────────────────────────────────

describe('handleSessionUpdate', () => {
  let receiver;

  beforeEach(() => { receiver = makeReceiver(); });

  const sessionXml = () => buildXml('session_update', `
    <session_name>Keynote</session_name>
    <speaker>Jan Janssen</speaker>
    <start_time>10:00</start_time>
    <end_time>11:00</end_time>
    <status>active</status>
  `);

  test('dry-run maakt geen SF call', async () => {
    receiver.sf.isConnected = false;
    await receiver.handleMessage(buildMsg(sessionXml()));
    expect(receiver.sf.apiCall).not.toHaveBeenCalled();
    expect(receiver.channel.ack).toHaveBeenCalled();
  });

  test('SF Task wordt aangemaakt met sessienaam als verbonden', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({ id: 'task-sess-1' });
    await receiver.handleMessage(buildMsg(sessionXml()));
    expect(receiver.sf.apiCall).toHaveBeenCalledWith(expect.any(Function));
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// handleInvoiceStatus
// ─────────────────────────────────────────────────────────────────────────────

describe('handleInvoiceStatus', () => {
  let receiver;

  beforeEach(() => { receiver = makeReceiver(); });

  const invoiceStatusXml = () => buildXml('invoice_status', `
    <email>inv@example.com</email>
    <invoice>
      <id>INV-002</id>
      <status>paid</status>
      <amount_paid>200.00</amount_paid>
    </invoice>
  `);

  test('dry-run maakt geen SF call', async () => {
    receiver.sf.isConnected = false;
    await receiver.handleMessage(buildMsg(invoiceStatusXml()));
    expect(receiver.sf.apiCall).not.toHaveBeenCalled();
  });

  test('SF Task wordt aangemaakt als verbonden', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({ id: 'task-inv-1' });
    await receiver.handleMessage(buildMsg(invoiceStatusXml()));
    expect(receiver.sf.apiCall).toHaveBeenCalled();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// handleMailingStatus
// ─────────────────────────────────────────────────────────────────────────────

describe('handleMailingStatus', () => {
  let receiver;

  beforeEach(() => { receiver = makeReceiver(); });

  const mailingXml = () => buildXml('mailing_status', `
    <mailing_id>MAIL-001</mailing_id>
    <status>delivered</status>
    <delivered>500</delivered>
    <bounced>10</bounced>
  `);

  test('dry-run maakt geen SF call', async () => {
    receiver.sf.isConnected = false;
    await receiver.handleMessage(buildMsg(mailingXml()));
    expect(receiver.sf.apiCall).not.toHaveBeenCalled();
    expect(receiver.channel.ack).toHaveBeenCalled();
  });

  test('SF Task wordt aangemaakt als verbonden', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({ id: 'task-mail-1' });
    await receiver.handleMessage(buildMsg(mailingXml()));
    expect(receiver.sf.apiCall).toHaveBeenCalled();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// handleBadgeAssigned
// ─────────────────────────────────────────────────────────────────────────────

describe('handleBadgeAssigned', () => {
  let receiver;

  beforeEach(() => { receiver = makeReceiver(); });

  const badgeAssignedXml = () => buildXml('badge_assigned', `
    <badge_id>BADGE-42</badge_id>
    <user_id>u-30</user_id>
  `);

  test('dry-run logt en maakt geen SF call', async () => {
    receiver.sf.isConnected = false;
    await receiver.handleMessage(buildMsg(badgeAssignedXml()));
    expect(receiver.sf.apiCall).not.toHaveBeenCalled();
    expect(receiver.channel.ack).toHaveBeenCalled();
  });

  test('Member__c badge wordt bijgewerkt in SF als lid gevonden', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall
      .mockResolvedValueOnce([{ Id: 'sf-member-1' }]) // _findUserById
      .mockResolvedValueOnce({});                       // update
    await receiver.handleMessage(buildMsg(badgeAssignedXml()));
    expect(receiver.sf.apiCall).toHaveBeenCalledTimes(2);
  });

  test('logt melding als geen Member__c gevonden', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue([]); // geen resultaat
    await receiver.handleMessage(buildMsg(badgeAssignedXml()));
    expect(receiver.channel.ack).toHaveBeenCalled();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// handleRefundProcessed
// ─────────────────────────────────────────────────────────────────────────────

describe('handleRefundProcessed', () => {
  let receiver;

  beforeEach(() => { receiver = makeReceiver(); });

  const refundXml = () => buildXml('refund_processed', `
    <user_id>u-40</user_id>
    <email>refund@example.com</email>
    <refund_type>partial</refund_type>
    <original_transaction_id>TX-001</original_transaction_id>
    <refund>
      <amount currency="eur">50.00</amount>
      <method>bank_transfer</method>
      <reason>Duplicate payment</reason>
    </refund>
    <new_wallet_balance>0.00</new_wallet_balance>
  `);

  test('dry-run maakt geen SF call', async () => {
    receiver.sf.isConnected = false;
    await receiver.handleMessage(buildMsg(refundXml()));
    expect(receiver.sf.apiCall).not.toHaveBeenCalled();
    expect(receiver.channel.ack).toHaveBeenCalled();
  });

  test('SF Task wordt aangemaakt met bedrag en type als verbonden', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({ id: 'task-refund-1' });
    await receiver.handleMessage(buildMsg(refundXml()));
    expect(receiver.sf.apiCall).toHaveBeenCalled();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// handleInvoiceRequestFromKassa
// ─────────────────────────────────────────────────────────────────────────────

describe('handleInvoiceRequestFromKassa', () => {
  let receiver;

  beforeEach(() => { receiver = makeReceiver(); });

  const kassaInvoiceXml = () => buildXml('invoice_request', `
    <user_id>u-50</user_id>
    <email>kassa@example.com</email>
    <invoice>
      <id>KINV-001</id>
      <amount_paid currency="eur">150.00</amount_paid>
      <status>pending</status>
      <due_date>2026-06-01</due_date>
    </invoice>
  `);

  test('dry-run maakt geen SF call maar stuurt wel door naar sender', async () => {
    receiver.sf.isConnected = false;
    await receiver.handleMessage(buildMsg(kassaInvoiceXml()));
    expect(receiver.sf.apiCall).not.toHaveBeenCalled();
    expect(receiver.sender.sendInvoiceRequest).toHaveBeenCalled();
  });

  test('SF Task wordt aangemaakt en factuur wordt doorgestuurd als verbonden', async () => {
    receiver.sf.isConnected = true;
    receiver.sf.apiCall.mockResolvedValue({ id: 'task-kinv-1' });
    await receiver.handleMessage(buildMsg(kassaInvoiceXml()));
    expect(receiver.sf.apiCall).toHaveBeenCalled();
    expect(receiver.sender.sendInvoiceRequest).toHaveBeenCalled();
  });

  test('sendInvoiceRequest wordt aangeroepen met email en bedrag', async () => {
    receiver.sf.isConnected = false;
    await receiver.handleMessage(buildMsg(kassaInvoiceXml()));
    expect(receiver.sender.sendInvoiceRequest).toHaveBeenCalledWith(
      expect.objectContaining({
        customer: expect.objectContaining({ email: 'kassa@example.com' }),
        invoice: expect.objectContaining({ amount: 150 }),
      }),
    );
  });
});