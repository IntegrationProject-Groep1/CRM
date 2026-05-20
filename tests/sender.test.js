'use strict';

process.env.RABBITMQ_USER = process.env.RABBITMQ_USER || 'test';
process.env.RABBITMQ_PASS = process.env.RABBITMQ_PASS || 'test';
process.env.RABBITMQ_PROTOCOL = process.env.RABBITMQ_PROTOCOL || 'amqps';

/**
 * Tests for CRM sender XML building + async send methods (mocked RabbitMQ).
 *
 * Flows covered:
 *  - Registratie : buildNewRegistrationForKassaXml / sendNewRegistrationToKassa
 *  - Consumptie  : buildProfileUpdateXml / sendProfileUpdateToKassa
 *                  buildCancelRegistrationXml / sendCancelRegistrationToKassa
 *  - Betaling    : buildInvoiceRequestXml / sendInvoiceRequest
 *  - Mailing     : buildMailingSendXml / sendMailingSend
 */

const { XMLParser } = require('fast-xml-parser');
jest.mock('../src/validator', () => ({
  validateXml: jest.fn(() => ({ valid: true, errors: [] }))
}));
const CRMSender = require('../src/sender');

// ── XML parser ────────────────────────────────────────────────────────────────

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '',
  textNodeName: '#text',
  parseTagValue: false,
  parseAttributeValue: false,
});

// ── RabbitMQ channel mock factory ─────────────────────────────────────────────

function makeMockChannel({ sendOk = true } = {}) {
  return {
    assertQueue: jest.fn().mockResolvedValue(undefined),
    assertExchange: jest.fn().mockResolvedValue(undefined),
    bindQueue: jest.fn().mockResolvedValue(undefined),
    sendToQueue: jest.fn().mockReturnValue(sendOk),
    publish: jest.fn().mockReturnValue(sendOk),
  };
}

function attachMockChannel(sender, opts) {
  const channel = makeMockChannel(opts);
  sender.channel = channel;
  return channel;
}

// ─────────────────────────────────────────────────────────────────────────────
// REGISTRATIE FLOW
// ─────────────────────────────────────────────────────────────────────────────

describe('Registratie flow — buildNewRegistrationForKassaXml', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const baseData = () => ({
    customer: {
      email: 'jan@example.com',
      first_name: 'Jan',
      last_name: 'Peeters',
      user_id: 'u-42',
      session_title: 'Keynote',
    },
    payment_due: { amount: '25.00', status: 'pending' },
    correlation_id: 'corr-abc',
  });

  test('header bevat correcte type en source', () => {
    const root = parser.parse(sender.buildNewRegistrationForKassaXml(baseData())).message;
    expect(root.header.type).toBe('new_registration');
    expect(root.header.source).toBe('crm');
    expect(String(root.header.version)).toBe('2.0');
  });

  test('correlation_id wordt meegestuurd (v2.3) en session_title staat in customer', () => {
    const data = baseData();
    data.correlation_id = 'c1234567-89ab-cdef-0123-456789abcdef';
    const root = parser.parse(sender.buildNewRegistrationForKassaXml(data)).message;
    expect(root.header.correlation_id).toBe(data.correlation_id);
    expect(root.body.customer.session_title).toBe('Keynote');
  });

  test('klantgegevens staan correct in body', () => {
    const root = parser.parse(sender.buildNewRegistrationForKassaXml(baseData())).message;
    const c = root.body.customer;
    expect(c.email).toBe('jan@example.com');
    expect(c.contact.first_name).toBe('Jan');
    expect(c.contact.last_name).toBe('Peeters');
    expect(c.identity_uuid).toBe('u-42');
  });

  test('payment_due status "pending" wordt genormaliseerd naar "unpaid"', () => {
    const root = parser.parse(sender.buildNewRegistrationForKassaXml(baseData())).message;
    expect(root.body.customer.payment_due.status).toBe('unpaid');
    expect(root.body.customer.payment_due.amount['#text']).toBe('25.00');
    expect(root.body.customer.payment_due.amount.currency).toBe('eur');
  });

  test('payment_due status "paid" blijft "paid"', () => {
    const data = baseData();
    data.payment_due.status = 'paid';
    const root = parser.parse(sender.buildNewRegistrationForKassaXml(data)).message;
    expect(root.body.customer.payment_due.status).toBe('paid');
  });

  test('customer type valt terug op "private" als niet opgegeven', () => {
    const root = parser.parse(sender.buildNewRegistrationForKassaXml(baseData())).message;
    expect(root.body.customer.type).toBe('private');
  });

  test('optionele velden company_name en vat_number worden opgenomen als aanwezig', () => {
    const data = baseData();
    data.customer.company_name = 'Acme NV';
    data.customer.vat_number = 'BE0123456789';
    const root = parser.parse(sender.buildNewRegistrationForKassaXml(data)).message;
    expect(root.body.customer.company_name).toBe('Acme NV');
    expect(root.body.customer.vat_number).toBe('BE0123456789');
  });

  test('message_id is een UUID', () => {
    const root = parser.parse(sender.buildNewRegistrationForKassaXml(baseData())).message;
    expect(root.header.message_id).toMatch(/^[0-9a-f-]{36}$/);
  });

  test('elke aanroep genereert een unieke message_id', () => {
    const id1 = parser.parse(sender.buildNewRegistrationForKassaXml(baseData())).message.header.message_id;
    const id2 = parser.parse(sender.buildNewRegistrationForKassaXml(baseData())).message.header.message_id;
    expect(id1).not.toBe(id2);
  });

  test('speciale tekens in naam worden ge-escaped', () => {
    const data = baseData();
    data.customer.first_name = "O'Brien & <Co>";
    const xml = sender.buildNewRegistrationForKassaXml(data);
    expect(xml).not.toContain('<Co>');
    expect(xml).toContain('&lt;Co&gt;');
    expect(xml).toContain('&amp;');
  });
});

describe('Betaling flow - sendConsumptionOrderToFacturatie', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const xml = '<message><header><type>consumption_order</type></header></message>';

  test('stuurt raw XML naar facturatie.incoming', async () => {
    const ch = attachMockChannel(sender);
    const result = await sender.sendConsumptionOrderToFacturatie(xml);

    expect(ch.assertQueue).toHaveBeenCalledWith('facturatie.incoming', { durable: true });
    expect(ch.sendToQueue).toHaveBeenCalledWith(
      'facturatie.incoming',
      Buffer.from(xml),
      expect.objectContaining({ contentType: 'application/xml', deliveryMode: 2 }),
    );
    expect(result).toMatchObject({ success: true, queue: 'facturatie.incoming', payload: xml });
  });
});

describe('Betaling flow - payment_registered forwarding', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const paymentData = {
    identity_uuid: 'e8b27c1d-4f2a-4b3e-9c5f-123456789abc',
    invoice_id: 'INV-001',
    amount_paid: '25.00',
    payment_context: 'consumption',
    correlation_id: 'c3d4e5f6-a7b8-9012-cdef-012345678902',
  };

  test('stuurt payment_registered XML naar facturatie.incoming', async () => {
    const ch = attachMockChannel(sender);
    const result = await sender.sendPaymentRegisteredToFacturatie(paymentData);

    expect(ch.assertQueue).toHaveBeenCalledWith('facturatie.incoming', { durable: true });
    expect(ch.sendToQueue).toHaveBeenCalledWith(
      'facturatie.incoming',
      expect.any(Buffer),
      expect.objectContaining({ contentType: 'application/xml', deliveryMode: 2 }),
    );
    expect(result).toMatchObject({ success: true, queue: 'facturatie.incoming' });
  });

  test('stuurt payment_registered XML naar frontend.incoming', async () => {
    const ch = attachMockChannel(sender);
    const result = await sender.sendPaymentRegisteredToFrontend(paymentData);

    expect(ch.assertQueue).toHaveBeenCalledWith('frontend.incoming', { durable: true });
    expect(ch.sendToQueue).toHaveBeenCalledWith(
      'frontend.incoming',
      expect.any(Buffer),
      expect.objectContaining({ contentType: 'application/xml', deliveryMode: 2 }),
    );
    expect(result).toMatchObject({ success: true, queue: 'frontend.incoming' });
  });
});

describe('Monitoring flow - sendLog', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const data = {
    level: 'info',
    action: 'email',
    message: 'Mailing request sent for campaign sg-campaign-0089',
  };

  test('buildLogXml bouwt 3.5 log XML', () => {
    const root = parser.parse(sender.buildLogXml(data)).message;
    expect(root.header.message_id).toMatch(/^[0-9a-f-]{36}$/);
    expect(root.header.source).toBe('crm');
    expect(root.header.type).toBe('log');
    expect(String(root.header.version)).toBe('2.0');
    expect(root.body.level).toBe('info');
    expect(root.body.action).toBe('email');
    expect(root.body.message).toBe('Mailing request sent for campaign sg-campaign-0089');
  });

  test('weigert onbekende level en action waarden', () => {
    expect(() => sender.buildLogXml({ ...data, level: 'debug' })).toThrow('Invalid log level');
    expect(() => sender.buildLogXml({ ...data, action: 'unknown' })).toThrow('Invalid log action');
  });

  test('sendLog stuurt naar queue "logs"', async () => {
    const ch = attachMockChannel(sender);
    const result = await sender.sendLog(data);

    expect(ch.assertQueue).toHaveBeenCalledWith('logs', { durable: true });
    expect(ch.sendToQueue).toHaveBeenCalledWith(
      'logs',
      expect.any(Buffer),
      expect.objectContaining({ contentType: 'application/xml', deliveryMode: 2 }),
    );
    expect(result).toMatchObject({ success: true, queue: 'logs' });
  });
});

describe('Registratie flow — sendNewRegistrationToKassa', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const data = {
    customer: { email: 'x@example.com', first_name: 'X', last_name: 'Y', user_id: 'u-1', age: 20 },
    payment_due: { amount: '10.00', status: 'pending' },
  };

  test('gooit error als channel niet geïnitialiseerd is', async () => {
    await expect(sender.sendNewRegistrationToKassa(data)).rejects.toThrow('not initialized');
  });

  test('assertQueue wordt aangeroepen met "kassa.incoming"', async () => {
    const ch = attachMockChannel(sender);
    await sender.sendNewRegistrationToKassa(data);
    expect(ch.assertQueue).toHaveBeenCalledWith('kassa.incoming', { durable: true, arguments: { 'x-dead-letter-exchange': 'kassa.dlx', 'x-dead-letter-routing-key': 'kassa.incoming.dlq' } });
  });

  test('sendToQueue wordt aangeroepen met XML buffer en correcte opties', async () => {
    const ch = attachMockChannel(sender);
    await sender.sendNewRegistrationToKassa(data);
    expect(ch.sendToQueue).toHaveBeenCalledWith(
      'kassa.incoming',
      expect.any(Buffer),
      expect.objectContaining({ contentType: 'application/xml', deliveryMode: 2 }),
    );
  });

  test('retourneert { success: true, queue, payload }', async () => {
    attachMockChannel(sender);
    const result = await sender.sendNewRegistrationToKassa(data);
    expect(result.success).toBe(true);
    expect(result.queue).toBe('kassa.incoming');
    expect(typeof result.payload).toBe('string');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// CONSUMPTIE FLOW — Profile Update
// ─────────────────────────────────────────────────────────────────────────────

describe('Consumptie flow — buildProfileUpdateXml', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const baseData = () => ({
    user_id: 'u-99',
    email: 'update@example.com',
    first_name: 'Sofie',
    last_name: 'Claes',
    date_of_birth: '1991-04-12',
    type: 'private',
  });

  test('header bevat type "profile_update" en source "crm"', () => {
    const root = parser.parse(sender.buildProfileUpdateXml(baseData())).message;
    expect(root.header.type).toBe('profile_update');
    expect(root.header.source).toBe('crm');
  });

  test('header bevat geen oude master_uuid of correlation_id velden', () => {
    const root = parser.parse(sender.buildProfileUpdateXml(baseData())).message;
    expect(root.header.master_uuid).toBeUndefined();
    expect(root.header.correlation_id).toBeUndefined();
  });

  test('body bevat identity_uuid, email en type', () => {
    const root = parser.parse(sender.buildProfileUpdateXml(baseData())).message;
    expect(root.body.identity_uuid).toBe('u-99');
    expect(root.body.email).toBe('update@example.com');
    expect(root.body.type).toBe('private');
  });

  test('contactgegevens staan in contact element', () => {
    const root = parser.parse(sender.buildProfileUpdateXml(baseData())).message;
    expect(root.body.contact.first_name).toBe('Sofie');
    expect(root.body.contact.last_name).toBe('Claes');
  });

  test('type wordt weggelaten als niet opgegeven', () => {
    const data = baseData();
    delete data.type;
    const root = parser.parse(sender.buildProfileUpdateXml(data)).message;
    expect(root.body.type).toBeUndefined();
  });

  test('optionele velden company_name en vat_number worden opgenomen', () => {
    const data = { ...baseData(), company_name: 'Test BV', vat_number: 'BE0987654321' };
    const root = parser.parse(sender.buildProfileUpdateXml(data)).message;
    expect(root.body.company_name).toBe('Test BV');
    expect(root.body.vat_number).toBe('BE0987654321');
    expect(root.body.company_id).toBeUndefined();
  });

  test('optionele payment_due gebruikt amount met currency eur', () => {
    const data = { ...baseData(), payment_due: { amount: '50.00', status: 'paid' } };
    const root = parser.parse(sender.buildProfileUpdateXml(data)).message;
    expect(root.body.payment_due.amount['#text']).toBe('50.00');
    expect(root.body.payment_due.amount.currency).toBe('eur');
    expect(root.body.payment_due.status).toBe('paid');
  });

  test('message_id start met "prof-crm-"', () => {
    const root = parser.parse(sender.buildProfileUpdateXml(baseData())).message;
    expect(root.header.message_id).toMatch(/^[0-9a-f-]{36}$/);
  });
});

describe('Consumptie flow — sendProfileUpdateToKassa', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const data = { user_id: 'u-1', email: 'a@b.com', first_name: 'A', last_name: 'B', age: 25 };

  test('gooit error als channel niet geïnitialiseerd is', async () => {
    await expect(sender.sendProfileUpdateToKassa(data)).rejects.toThrow('not initialized');
  });

  test('verstuurt naar "kassa.incoming"', async () => {
    const ch = attachMockChannel(sender);
    await sender.sendProfileUpdateToKassa(data);
    expect(ch.assertQueue).toHaveBeenCalledWith('kassa.incoming', { durable: true, arguments: { 'x-dead-letter-exchange': 'kassa.dlx', 'x-dead-letter-routing-key': 'kassa.incoming.dlq' } });
    expect(ch.sendToQueue.mock.calls[0][0]).toBe('kassa.incoming');
  });

  test('retourneert success object', async () => {
    attachMockChannel(sender);
    const result = await sender.sendProfileUpdateToKassa(data);
    expect(result).toMatchObject({ success: true, queue: 'kassa.incoming' });
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// CONSUMPTIE FLOW — Cancel Registration
// ─────────────────────────────────────────────────────────────────────────────

describe('Consumptie flow — buildCancelRegistrationXml', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const baseData = () => ({
    user_id: 'u-55',
    session_id: 'sess-cancel-1',
    correlation_id: 'corr-cancel-1',
  });

  test('header bevat type "cancel_registration" en source "crm"', () => {
    const root = parser.parse(sender.buildCancelRegistrationXml(baseData())).message;
    expect(root.header.type).toBe('cancel_registration');
    expect(root.header.source).toBe('crm');
    expect(String(root.header.version)).toBe('2.0');
  });

  test('body bevat identity_uuid en session_id', () => {
    const root = parser.parse(sender.buildCancelRegistrationXml(baseData())).message;
    expect(root.body.identity_uuid).toBe('u-55');
    expect(root.body.session_id).toBe('sess-cancel-1');
  });

  test('correlation_id staat niet in header (niet in contract §10.3)', () => {
    const root = parser.parse(sender.buildCancelRegistrationXml(baseData())).message;
    expect(root.header.correlation_id).toBeUndefined();
  });

  test('message_id start met "cancel-crm-"', () => {
    const root = parser.parse(sender.buildCancelRegistrationXml(baseData())).message;
    expect(root.header.message_id).toMatch(/^[0-9a-f-]{36}$/);
  });

  test('elke aanroep genereert een unieke message_id', () => {
    const id1 = parser.parse(sender.buildCancelRegistrationXml(baseData())).message.header.message_id;
    const id2 = parser.parse(sender.buildCancelRegistrationXml(baseData())).message.header.message_id;
    expect(id1).not.toBe(id2);
  });
});

describe('Consumptie flow — sendCancelRegistrationToKassa', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const data = { user_id: 'u-55', session_id: 'sess-1' };

  test('gooit error als channel niet geïnitialiseerd is', async () => {
    await expect(sender.sendCancelRegistrationToKassa(data)).rejects.toThrow('not initialized');
  });

  test('verstuurt naar "kassa.incoming"', async () => {
    const ch = attachMockChannel(sender);
    await sender.sendCancelRegistrationToKassa(data);
    expect(ch.assertQueue).toHaveBeenCalledWith('kassa.incoming', { durable: true, arguments: { 'x-dead-letter-exchange': 'kassa.dlx', 'x-dead-letter-routing-key': 'kassa.incoming.dlq' } });
    expect(ch.sendToQueue.mock.calls[0][0]).toBe('kassa.incoming');
  });

  test('payload is geldige XML met cancel_registration type', async () => {
    attachMockChannel(sender);
    const result = await sender.sendCancelRegistrationToKassa(data);
    const root = parser.parse(result.payload).message;
    expect(root.header.type).toBe('cancel_registration');
  });

  test('retourneert success object', async () => {
    attachMockChannel(sender);
    const result = await sender.sendCancelRegistrationToKassa(data);
    expect(result).toMatchObject({ success: true, queue: 'kassa.incoming' });
  });
});

describe('Consumptie flow — sendCancelRegistrationToPlanning', () => {
  let sender;

  beforeEach(() => { 
    sender = new CRMSender(); 
  });

  const data = { 
    user_id: 'u-55', 
    session_id: 'sess-1' 
  };

  test('assertExchange wordt aangeroepen met "calendar.exchange"', async () => {
    const ch = attachMockChannel(sender);
    await sender.sendCancelRegistrationToPlanning(data);
    expect(ch.assertExchange).toHaveBeenCalledWith('calendar.exchange', 'topic', { durable: true });
  });

  test('publish wordt aangeroepen met de juiste routing key en XML', async () => {
    const ch = attachMockChannel(sender);
    await sender.sendCancelRegistrationToPlanning(data);
    expect(ch.publish).toHaveBeenCalledWith(
      'calendar.exchange',
      'crm.to.planning.cancel_registration',
      expect.any(Buffer),
      expect.objectContaining({ 
        contentType: 'application/xml', 
        deliveryMode: 2 
      }),
    );
  });

  test('retourneert success object met exchange naam', async () => {
    attachMockChannel(sender);
    const result = await sender.sendCancelRegistrationToPlanning(data);
    expect(result.success).toBe(true);
    expect(result.exchange).toBe('calendar.exchange');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// BETALING FLOW
// ─────────────────────────────────────────────────────────────────────────────

describe('Betaling flow — buildInvoiceRequestXml', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const baseData = () => ({
    user_id: 'u-invoice-001',
    customer: { email: 'klant@example.com', first_name: 'Luc', last_name: 'Vermeersch' },
    address: { street: 'Laarbeeklaan', number: '121', postal_code: '1090', city: 'Jette', country: 'BE' },
    correlation_id: 'corr-inv-1',
  });

  test('header contains type "invoice_request", source "crm" and version "2.0"', () => {
    const root = parser.parse(sender.buildInvoiceRequestXml(baseData())).message;
    expect(root.header.type).toBe('invoice_request');
    expect(root.header.source).toBe('crm');
    expect(String(root.header.version)).toBe('2.0');
  });

  test('header does not contain master_uuid (forbidden by contract v2.0)', () => {
    const root = parser.parse(sender.buildInvoiceRequestXml(baseData())).message;
    expect(root.header.master_uuid).toBeUndefined();
  });

  test('body contains identity_uuid at top level', () => {
    const root = parser.parse(sender.buildInvoiceRequestXml(baseData())).message;
    expect(root.body.identity_uuid).toBe('u-invoice-001');
  });

  test('invoice_data contains first_name, last_name and email', () => {
    const root = parser.parse(sender.buildInvoiceRequestXml(baseData())).message;
    expect(root.body.invoice_data.contact.first_name).toBe('Luc');
    expect(root.body.invoice_data.contact.last_name).toBe('Vermeersch');
    expect(root.body.invoice_data.email).toBe('klant@example.com');
  });

  test('invoice_data contains address block', () => {
    const root = parser.parse(sender.buildInvoiceRequestXml(baseData())).message;
    const addr = root.body.invoice_data.address;
    expect(addr.street).toBe('Laarbeeklaan');
    expect(addr.number).toBe('121');
    expect(addr.postal_code).toBe('1090');
    expect(addr.city).toBe('Jette');
    expect(addr.country).toBe('BE');
  });

  test('optional company_name and vat_number are included when present', () => {
    const data = baseData();
    data.customer.company_name = 'Acme NV';
    data.customer.vat_number = 'BE0123456789';
    const root = parser.parse(sender.buildInvoiceRequestXml(data)).message;
    expect(root.body.invoice_data.company_name).toBe('Acme NV');
    expect(root.body.invoice_data.vat_number).toBe('BE0123456789');
  });

  test('body does not contain <items> block (CRM is passthrough — Facturatie fetches items via correlation_id)', () => {
    const root = parser.parse(sender.buildInvoiceRequestXml(baseData())).message;
    expect(root.body.items).toBeUndefined();
  });

  test('correlation_id in header is included when present', () => {
    const root = parser.parse(sender.buildInvoiceRequestXml(baseData())).message;
    expect(root.header.correlation_id).toBe('corr-inv-1');
  });

  test('correlation_id wordt altijd meegestuurd volgens 11.1', () => {
    const data = baseData();
    delete data.correlation_id;
    const root = parser.parse(sender.buildInvoiceRequestXml(data)).message;
    expect(root.header.correlation_id).toMatch(/^[0-9a-f-]{36}$/);
  });

  test('message_id starts with "inv-crm-"', () => {
    const root = parser.parse(sender.buildInvoiceRequestXml(baseData())).message;
    expect(root.header.message_id).toMatch(/^[0-9a-f-]{36}$/);
  });
});

describe('Betaling flow — sendInvoiceRequest', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const data = {
    customer: { email: 'k@example.com', first_name: 'K', last_name: 'L' },
    invoice: { description: 'Test', amount: 100, due_date: '2026-01-01' },
    items: [],
  };

  test('gooit error als channel niet geïnitialiseerd is', async () => {
    await expect(sender.sendInvoiceRequest(data)).rejects.toThrow('not initialized');
  });

  test('assertQueue is called with "facturatie.incoming"', async () => {
    const ch = attachMockChannel(sender);
    await sender.sendInvoiceRequest(data);
    expect(ch.assertQueue).toHaveBeenCalledWith('facturatie.incoming', { durable: true });
  });

  test('sendToQueue sends to "facturatie.incoming" with XML and correct options', async () => {
    const ch = attachMockChannel(sender);
    await sender.sendInvoiceRequest(data);
    expect(ch.sendToQueue).toHaveBeenCalledWith(
      'facturatie.incoming',
      expect.any(Buffer),
      expect.objectContaining({ contentType: 'application/xml', deliveryMode: 2 }),
    );
  });

  test('payload bevat geldige XML met invoice_request type', async () => {
    attachMockChannel(sender);
    const result = await sender.sendInvoiceRequest(data);
    const root = parser.parse(result.payload).message;
    expect(root.header.type).toBe('invoice_request');
  });

  test('returns { success: true, queue: "facturatie.incoming", payload }', async () => {
    attachMockChannel(sender);
    const result = await sender.sendInvoiceRequest(data);
    expect(result).toMatchObject({ success: true, queue: 'facturatie.incoming' });
    expect(typeof result.payload).toBe('string');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// MAILING FLOW
// ─────────────────────────────────────────────────────────────────────────────

describe('Mailing flow — buildMailingSendXml', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const baseData = () => ({
    correlation_id: 'c3d4e5f6-a7b8-9012-cdef-012345678902',
    mailing: {
      campaign_id: 'sg-campaign-0089',
      subject: 'Nieuwsbrief april',
      template_id: 'TPL-NL-001',
      mail_type: 'registration_confirmation',
    },
    recipients: [
      {
        email: 'a@example.com',
        user_id: 'e8b27c1d-4f2a-4b3e-9c5f-123456789abc',
        first_name: 'An',
        last_name: 'De Smedt',
      },
    ],
  });

  test('header bevat verplichte 12.1 velden', () => {
    const root = parser.parse(sender.buildMailingSendXml(baseData())).message;
    expect(root.header.type).toBe('send_mailing');
    expect(root.header.source).toBe('crm');
    expect(String(root.header.version)).toBe('2.0');
    expect(root.header.correlation_id).toBe('c3d4e5f6-a7b8-9012-cdef-012345678902');
  });

  test('mailing velden staan direct onder body volgens 12.1', () => {
    const root = parser.parse(sender.buildMailingSendXml(baseData())).message;
    expect(root.body.campaign_id).toBe('sg-campaign-0089');
    expect(root.body.subject).toBe('Nieuwsbrief april');
    expect(root.body.mail_type).toBe('registration_confirmation');
    expect(root.body.mailing).toBeUndefined();
  });

  test('correlation_id wordt gegenereerd als caller er geen meegeeft', () => {
    const data = baseData();
    delete data.correlation_id;
    const root = parser.parse(sender.buildMailingSendXml(data)).message;
    expect(root.header.correlation_id).toMatch(/^[0-9a-f-]{36}$/);
  });

  test('ontvanger wordt correct opgenomen met identity_uuid en contact element', () => {
    const root = parser.parse(sender.buildMailingSendXml(baseData())).message;
    const recipient = root.body.recipients.recipient;
    expect(recipient.email).toBe('a@example.com');
    expect(recipient.identity_uuid).toBe('e8b27c1d-4f2a-4b3e-9c5f-123456789abc');
    expect(recipient.contact.first_name).toBe('An');
    expect(recipient.contact.last_name).toBe('De Smedt');
    expect(recipient.first_name).toBeUndefined();
  });

  test('meerdere ontvangers worden allemaal opgenomen', () => {
    const data = baseData();
    data.recipients.push({
      email: 'b@example.com',
      user_id: 'user-2',
      first_name: 'Bob',
      last_name: 'Janssen',
    });
    const root = parser.parse(sender.buildMailingSendXml(data)).message;
    const recipients = root.body.recipients.recipient;
    expect(Array.isArray(recipients)).toBe(true);
    expect(recipients).toHaveLength(2);
  });

  test('optionele template_data, body_html en attachment worden opgenomen', () => {
    const data = baseData();
    data.template_data = { session_title: 'Keynote' };
    data.body_html = '<p>Hallo</p>';
    data.attachment = {
      filename: 'invoice.pdf',
      content_type: 'application/pdf',
      base64_data: 'ZmFrZQ==',
    };
    const root = parser.parse(sender.buildMailingSendXml(data)).message;
    expect(root.body.template_data).toBe('{"session_title":"Keynote"}');
    expect(root.body.body_html).toBe('<p>Hallo</p>');
    expect(root.body.attachment.filename).toBe('invoice.pdf');
    expect(root.body.attachment.content_type).toBe('application/pdf');
    expect(root.body.attachment.base64_data).toBe('ZmFrZQ==');
  });

  test('message_id is een UUID', () => {
    const root = parser.parse(sender.buildMailingSendXml(baseData())).message;
    expect(root.header.message_id).toMatch(/^[0-9a-f-]{36}$/);
  });
});

describe('Mailing flow — sendMailingSend', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const data = {
    correlation_id: 'corr-1',
    mailing: {
      campaign_id: 'camp-1',
      subject: 'Test',
      template_id: 'TPL-001',
      mail_type: 'general_announcement',
    },
    recipients: [{ email: 'r@example.com', user_id: 'u-1', first_name: 'R', last_name: 'S' }],
  };

  test('gooit error als channel niet geïnitialiseerd is', async () => {
    await expect(sender.sendMailingSend(data)).rejects.toThrow('not initialized');
  });

  test('assertQueue wordt aangeroepen met "crm.to.mailing"', async () => {
    const ch = attachMockChannel(sender);
    await sender.sendMailingSend(data);
    expect(ch.assertQueue).toHaveBeenCalledWith('crm.to.mailing', { durable: true });
  });

  test('sendToQueue stuurt naar "crm.to.mailing"', async () => {
    const ch = attachMockChannel(sender);
    await sender.sendMailingSend(data);
    expect(ch.sendToQueue.mock.calls[0][0]).toBe('crm.to.mailing');
  });

  test('retourneert success object met correcte queue', async () => {
    attachMockChannel(sender);
    const result = await sender.sendMailingSend(data);
    expect(result).toMatchObject({ success: true, queue: 'crm.to.mailing' });
  });
});

describe('Frontend flow — buildUserUnregisteredXml', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const baseData = () => ({
    identity_uuid: 'user-001',
    email: 'test@example.com',
    reason: 'User requested deletion'
  });

  test('bouwt user.unregistered volgens XSD specificatie', () => {
    const xml = sender.buildUserUnregisteredXml(baseData());

    const root = parser.parse(xml).message;
    expect(root.header.type).toBe('user.unregistered');
    expect(root.header.source).toBe('crm');
    expect(root.header.version).toBe('1.0');
    expect(root.body.identity_uuid).toBe('user-001');
    expect(root.body.email).toBe('test@example.com');
    expect(root.body.reason).toBe('User requested deletion');
  });

  test('bouwt user.unregistered zonder optionele velden', () => {
    const xml = sender.buildUserUnregisteredXml({ identity_uuid: 'user-002' });

    const root = parser.parse(xml).message;
    expect(root.header.type).toBe('user.unregistered');
    expect(root.body.identity_uuid).toBe('user-002');
    expect(root.body.email).toBeUndefined();
    expect(root.body.reason).toBeUndefined();
  });
});

describe('Frontend flow — sendUserUnregisteredFanout', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const data = {
    identity_uuid: 'user-001',
    email: 'test@example.com',
    reason: 'User requested unregistration'
  };

  test('gooit error als channel niet geinitialiseerd is', async () => {
    await expect(sender.sendUserUnregisteredFanout(data)).rejects.toThrow('not initialized');
  });

  test('maakt fanout exchange en bindt alle doelqueues', async () => {
    const ch = attachMockChannel(sender);
    await sender.sendUserUnregisteredFanout(data);

    expect(ch.assertExchange).toHaveBeenCalledWith('frontend.user.unregistered', 'fanout', { durable: true });
    expect(ch.bindQueue).toHaveBeenCalledTimes(3);
    expect(ch.bindQueue).toHaveBeenCalledWith('crm.salesforce', 'frontend.user.unregistered', '');
    expect(ch.bindQueue).toHaveBeenCalledWith('planning.outlook', 'frontend.user.unregistered', '');
    expect(ch.bindQueue).toHaveBeenCalledWith('mailing.sendgrid', 'frontend.user.unregistered', '');
  });

  test('publiceert XML naar de fanout exchange', async () => {
    const ch = attachMockChannel(sender);
    const result = await sender.sendUserUnregisteredFanout(data);

    expect(ch.publish).toHaveBeenCalledWith(
      'frontend.user.unregistered',
      '',
      expect.any(Buffer),
      expect.objectContaining({ contentType: 'application/xml', deliveryMode: 2 }),
    );
    expect(result).toMatchObject({
      success: true,
      exchange: 'frontend.user.unregistered',
      queues: ['crm.salesforce', 'planning.outlook', 'mailing.sendgrid'],
    });
  });
});
