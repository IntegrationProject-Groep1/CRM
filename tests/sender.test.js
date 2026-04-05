'use strict';

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
    sendToQueue: jest.fn().mockReturnValue(sendOk),
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
      age: 28,
    },
    payment_due: { amount: '25.00', status: 'pending' },
    correlation_id: 'corr-abc',
    session_id: 'sess-xyz',
  });

  test('header bevat correcte type en source', () => {
    const root = parser.parse(sender.buildNewRegistrationForKassaXml(baseData())).message;
    expect(root.header.type).toBe('new_registration');
    expect(root.header.source).toBe('crm');
    expect(String(root.header.version)).toBe('2.0');
  });

  test('correlation_id en session_id worden NIET meegestuurd (kassa XSD)', () => {
    const root = parser.parse(sender.buildNewRegistrationForKassaXml(baseData())).message;
    expect(root.header.correlation_id).toBeUndefined();
    expect(root.body.session_id).toBeUndefined();
  });

  test('klantgegevens staan correct in body', () => {
    const root = parser.parse(sender.buildNewRegistrationForKassaXml(baseData())).message;
    const c = root.body.customer;
    expect(c.email).toBe('jan@example.com');
    expect(c.contact.first_name).toBe('Jan');
    expect(c.contact.last_name).toBe('Peeters');
    expect(c.user_id).toBe('u-42');
    expect(String(c.age)).toBe('28');
  });

  test('payment_due status "pending" wordt genormaliseerd naar "unpaid"', () => {
    const root = parser.parse(sender.buildNewRegistrationForKassaXml(baseData())).message;
    expect(root.body.payment_due.status).toBe('unpaid');
  });

  test('payment_due status "paid" blijft "paid"', () => {
    const data = baseData();
    data.payment_due.status = 'paid';
    const root = parser.parse(sender.buildNewRegistrationForKassaXml(data)).message;
    expect(root.body.payment_due.status).toBe('paid');
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

  test('message_id start met "reg-crm-"', () => {
    const root = parser.parse(sender.buildNewRegistrationForKassaXml(baseData())).message;
    expect(root.header.message_id).toMatch(/^reg-crm-/);
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
    expect(ch.assertQueue).toHaveBeenCalledWith('kassa.incoming', { durable: true });
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
    age: 35,
    type: 'private',
    correlation_id: 'corr-upd-1',
  });

  test('header bevat type "profile_update" en source "crm"', () => {
    const root = parser.parse(sender.buildProfileUpdateXml(baseData())).message;
    expect(root.header.type).toBe('profile_update');
    expect(root.header.source).toBe('crm');
  });

  test('correlation_id wordt in header opgenomen als aanwezig', () => {
    const root = parser.parse(sender.buildProfileUpdateXml(baseData())).message;
    expect(root.header.correlation_id).toBe('corr-upd-1');
  });

  test('correlation_id wordt weggelaten als niet opgegeven', () => {
    const data = baseData();
    delete data.correlation_id;
    const root = parser.parse(sender.buildProfileUpdateXml(data)).message;
    expect(root.header.correlation_id).toBeUndefined();
  });

  test('body bevat user_id, email, age en type', () => {
    const root = parser.parse(sender.buildProfileUpdateXml(baseData())).message;
    expect(root.body.user_id).toBe('u-99');
    expect(root.body.email).toBe('update@example.com');
    expect(String(root.body.age)).toBe('35');
    expect(root.body.type).toBe('private');
  });

  test('contactgegevens staan in contact element', () => {
    const root = parser.parse(sender.buildProfileUpdateXml(baseData())).message;
    expect(root.body.contact.first_name).toBe('Sofie');
    expect(root.body.contact.last_name).toBe('Claes');
  });

  test('type valt terug op "private" als niet opgegeven', () => {
    const data = baseData();
    delete data.type;
    const root = parser.parse(sender.buildProfileUpdateXml(data)).message;
    expect(root.body.type).toBe('private');
  });

  test('optionele velden company_name en vat_number worden opgenomen', () => {
    const data = { ...baseData(), company_name: 'Test BV', vat_number: 'BE0987654321' };
    const root = parser.parse(sender.buildProfileUpdateXml(data)).message;
    expect(root.body.company_name).toBe('Test BV');
    expect(root.body.vat_number).toBe('BE0987654321');
  });

  test('message_id start met "prof-crm-"', () => {
    const root = parser.parse(sender.buildProfileUpdateXml(baseData())).message;
    expect(root.header.message_id).toMatch(/^prof-crm-/);
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
    expect(ch.assertQueue).toHaveBeenCalledWith('kassa.incoming', { durable: true });
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

  test('body bevat user_id en session_id', () => {
    const root = parser.parse(sender.buildCancelRegistrationXml(baseData())).message;
    expect(root.body.user_id).toBe('u-55');
    expect(root.body.session_id).toBe('sess-cancel-1');
  });

  test('correlation_id wordt in header opgenomen als aanwezig', () => {
    const root = parser.parse(sender.buildCancelRegistrationXml(baseData())).message;
    expect(root.header.correlation_id).toBe('corr-cancel-1');
  });

  test('correlation_id wordt weggelaten als niet opgegeven', () => {
    const data = baseData();
    delete data.correlation_id;
    const root = parser.parse(sender.buildCancelRegistrationXml(data)).message;
    expect(root.header.correlation_id).toBeUndefined();
  });

  test('message_id start met "cancel-crm-"', () => {
    const root = parser.parse(sender.buildCancelRegistrationXml(baseData())).message;
    expect(root.header.message_id).toMatch(/^cancel-crm-/);
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
    expect(ch.assertQueue).toHaveBeenCalledWith('kassa.incoming', { durable: true });
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

// ─────────────────────────────────────────────────────────────────────────────
// BETALING FLOW
// ─────────────────────────────────────────────────────────────────────────────

describe('Betaling flow — buildInvoiceRequestXml', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const baseData = () => ({
    customer: { email: 'klant@example.com', first_name: 'Luc', last_name: 'Vermeersch' },
    invoice: { description: 'Jaarabonnement', amount: 250.0, due_date: '2026-12-31' },
    items: [{ description: 'Abonnement', quantity: 1, unit_price: 250.0 }],
  });

  test('header bevat type "invoice_request", source "crm" en version "2.0"', () => {
    const root = parser.parse(sender.buildInvoiceRequestXml(baseData())).message;
    expect(root.header.type).toBe('invoice_request');
    expect(root.header.source).toBe('crm');
    expect(String(root.header.version)).toBe('2.0');
  });

  test('klantgegevens worden correct opgenomen in body', () => {
    const root = parser.parse(sender.buildInvoiceRequestXml(baseData())).message;
    expect(root.body.customer.email).toBe('klant@example.com');
    expect(root.body.customer.first_name).toBe('Luc');
    expect(root.body.customer.last_name).toBe('Vermeersch');
  });

  test('factuurgegevens worden correct opgenomen', () => {
    const root = parser.parse(sender.buildInvoiceRequestXml(baseData())).message;
    expect(root.body.invoice.description).toBe('Jaarabonnement');
    expect(root.body.invoice.due_date).toBe('2026-12-31');
  });

  test('amount heeft standaard currency "eur"', () => {
    const root = parser.parse(sender.buildInvoiceRequestXml(baseData())).message;
    expect(root.body.invoice.amount.currency).toBe('eur');
  });

  test('alternatieve currency wordt overgenomen', () => {
    const data = baseData();
    data.invoice.currency = 'usd';
    const root = parser.parse(sender.buildInvoiceRequestXml(data)).message;
    expect(root.body.invoice.amount.currency).toBe('usd');
  });

  test('optioneel invoice_number wordt opgenomen als aanwezig', () => {
    const data = baseData();
    data.invoice.invoice_number = 'INV-2026-001';
    const root = parser.parse(sender.buildInvoiceRequestXml(data)).message;
    expect(root.body.invoice.invoice_number).toBe('INV-2026-001');
  });

  test('optioneel klant-telefoonnummer wordt opgenomen', () => {
    const data = baseData();
    data.customer.phone = '+32 470 00 00 00';
    const root = parser.parse(sender.buildInvoiceRequestXml(data)).message;
    expect(root.body.customer.phone).toBe('+32 470 00 00 00');
  });

  test('meerdere items worden allemaal opgenomen', () => {
    const data = baseData();
    data.items = [
      { description: 'Item A', quantity: 2, unit_price: 50.0 },
      { description: 'Item B', quantity: 1, unit_price: 150.0 },
    ];
    const root = parser.parse(sender.buildInvoiceRequestXml(data)).message;
    const items = root.body.items.item;
    expect(Array.isArray(items)).toBe(true);
    expect(items).toHaveLength(2);
    expect(items[0].description).toBe('Item A');
    expect(items[1].description).toBe('Item B');
  });

  test('item vat_rate valt terug op 21 als niet opgegeven', () => {
    const root = parser.parse(sender.buildInvoiceRequestXml(baseData())).message;
    expect(String(root.body.items.item.vat_rate)).toBe('21');
  });

  test('optionele sku wordt opgenomen per item', () => {
    const data = baseData();
    data.items[0].sku = 'SKU-001';
    const root = parser.parse(sender.buildInvoiceRequestXml(data)).message;
    expect(root.body.items.item.sku).toBe('SKU-001');
  });

  test('lege items lijst produceert geen item elementen', () => {
    const data = baseData();
    data.items = [];
    const root = parser.parse(sender.buildInvoiceRequestXml(data)).message;
    expect(root.body.items.item).toBeUndefined();
  });

  test('correlation_id in header wordt opgenomen als aanwezig', () => {
    const data = { ...baseData(), correlation_id: 'corr-inv-1' };
    const root = parser.parse(sender.buildInvoiceRequestXml(data)).message;
    expect(root.header.correlation_id).toBe('corr-inv-1');
  });

  test('message_id start met "inv-crm-"', () => {
    const root = parser.parse(sender.buildInvoiceRequestXml(baseData())).message;
    expect(root.header.message_id).toMatch(/^inv-crm-/);
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

  test('assertQueue wordt aangeroepen met "crm.to.facturatie"', async () => {
    const ch = attachMockChannel(sender);
    await sender.sendInvoiceRequest(data);
    expect(ch.assertQueue).toHaveBeenCalledWith('crm.to.facturatie', { durable: true });
  });

  test('sendToQueue stuurt naar "crm.to.facturatie" met XML en correcte opties', async () => {
    const ch = attachMockChannel(sender);
    await sender.sendInvoiceRequest(data);
    expect(ch.sendToQueue).toHaveBeenCalledWith(
      'crm.to.facturatie',
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

  test('retourneert { success: true, queue: "crm.to.facturatie", payload }', async () => {
    attachMockChannel(sender);
    const result = await sender.sendInvoiceRequest(data);
    expect(result).toMatchObject({ success: true, queue: 'crm.to.facturatie' });
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
    mailing: { subject: 'Nieuwsbrief april', template_id: 'TPL-NL-001' },
    recipients: [
      { email: 'a@example.com', first_name: 'An', last_name: 'De Smedt' },
    ],
  });

  test('header bevat type "mailing_status" en source "crm"', () => {
    const root = parser.parse(sender.buildMailingSendXml(baseData())).message;
    expect(root.header.type).toBe('mailing_status');
    expect(root.header.source).toBe('crm');
  });

  test('mailing subject en template_id worden opgenomen', () => {
    const root = parser.parse(sender.buildMailingSendXml(baseData())).message;
    expect(root.body.mailing.subject).toBe('Nieuwsbrief april');
    expect(root.body.mailing.template_id).toBe('TPL-NL-001');
  });

  test('optionele from_address en reply_to worden opgenomen', () => {
    const data = baseData();
    data.mailing.from_address = 'no-reply@example.com';
    data.mailing.reply_to = 'support@example.com';
    const root = parser.parse(sender.buildMailingSendXml(data)).message;
    expect(root.body.mailing.from_address).toBe('no-reply@example.com');
    expect(root.body.mailing.reply_to).toBe('support@example.com');
  });

  test('ontvanger wordt correct opgenomen', () => {
    const root = parser.parse(sender.buildMailingSendXml(baseData())).message;
    const recipient = root.body.recipients.recipient;
    expect(recipient.email).toBe('a@example.com');
    expect(recipient.first_name).toBe('An');
    expect(recipient.last_name).toBe('De Smedt');
  });

  test('meerdere ontvangers worden allemaal opgenomen', () => {
    const data = baseData();
    data.recipients.push({ email: 'b@example.com', first_name: 'Bob', last_name: 'Janssen' });
    const root = parser.parse(sender.buildMailingSendXml(data)).message;
    const recipients = root.body.recipients.recipient;
    expect(Array.isArray(recipients)).toBe(true);
    expect(recipients).toHaveLength(2);
  });

  test('optionele language per ontvanger wordt opgenomen', () => {
    const data = baseData();
    data.recipients[0].language = 'nl';
    const root = parser.parse(sender.buildMailingSendXml(data)).message;
    expect(root.body.recipients.recipient.language).toBe('nl');
  });

  test('message_id start met "mail-crm-"', () => {
    const root = parser.parse(sender.buildMailingSendXml(baseData())).message;
    expect(root.header.message_id).toMatch(/^mail-crm-/);
  });
});

describe('Mailing flow — sendMailingSend', () => {
  let sender;

  beforeEach(() => { sender = new CRMSender(); });

  const data = {
    mailing: { subject: 'Test', template_id: 'TPL-001' },
    recipients: [{ email: 'r@example.com', first_name: 'R', last_name: 'S' }],
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