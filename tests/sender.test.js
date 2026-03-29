'use strict';

/**
 * Tests for CRM sender XML building (no RabbitMQ required).
 */

const { XMLParser } = require('fast-xml-parser');
const CRMSender = require('../src/sender');

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '',
  textNodeName: '#text',
  parseTagValue: false,
  parseAttributeValue: false,
});

describe('CRMSender XML building', () => {
  let sender;

  beforeEach(() => {
    sender = new CRMSender();
  });

  test('invoice xml structure', () => {
    const data = {
      customer: { email: 'test@example.com', first_name: 'Jan', last_name: 'Janssen' },
      invoice: { description: 'Test invoice', amount: 100.0, due_date: '2026-04-01' },
      items: [{ description: 'Item 1', quantity: 1, unit_price: 100.0 }],
    };

    const xml = sender.buildInvoiceRequestXml(data);
    const parsed = parser.parse(xml);
    const root = parsed.message;

    expect(root).toBeDefined();
    expect(root.header.type).toBe('invoice_request');
    expect(root.header.source).toBe('crm');
    expect(String(root.header.version)).toBe('2.0');
    expect(root.body.customer.email).toBe('test@example.com');
    expect(root.body.invoice.description).toBe('Test invoice');
  });

  test('mailing xml structure', () => {
    const data = {
      mailing: { subject: 'Hello', template_id: 'TPL-001' },
      recipients: [{ email: 'r@example.com', first_name: 'Jan', last_name: 'Doe' }],
    };

    const xml = sender.buildMailingSendXml(data);
    const parsed = parser.parse(xml);
    const root = parsed.message;

    expect(root).toBeDefined();
    expect(root.header.type).toBe('mailing_send');
    expect(root.body.mailing.subject).toBe('Hello');
    expect(root.body.recipients.recipient.email).toBe('r@example.com');
  });

  test('xml escaping of special characters', () => {
    const data = {
      customer: { email: 'test@example.com', first_name: "O'Brien", last_name: 'Test & Co' },
      invoice: { description: '<script>', amount: 50.0, due_date: '2026-04-01' },
      items: [],
    };

    const xml = sender.buildInvoiceRequestXml(data);
    expect(xml).not.toContain('<script>');
    expect(xml).toContain('&lt;script&gt;');
  });
});
