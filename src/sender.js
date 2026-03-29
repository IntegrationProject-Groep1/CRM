'use strict';

require('dotenv').config();
const amqp = require('amqplib');
const { create } = require('xmlbuilder2');
const { v4: uuidv4 } = require('uuid');

class CRMSender {
  constructor() {
    this.connection = null;
    this.channel = null;
    this.rabbitmqUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost/';
  }

  async init() {
    try {
      this.connection = await amqp.connect(this.rabbitmqUrl);
      this.channel = await this.connection.createChannel();
      console.log('CRM Sender initialized');
    } catch (error) {
      console.log(`Failed to initialize CRM Sender: ${error}`);
      throw error;
    }
  }

  buildInvoiceRequestXml(data) {
    const messageId = `inv-crm-${uuidv4()}`;
    const timestamp = new Date().toISOString();

    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(messageId);
    header.ele('version').txt('2.0');
    header.ele('type').txt('invoice_request');
    header.ele('timestamp').txt(timestamp);
    header.ele('source').txt('crm');
    if (data.correlation_id) {
      header.ele('correlation_id').txt(data.correlation_id);
    }

    const body = root.ele('body');

    const customer = body.ele('customer');
    customer.ele('email').txt(data.customer.email);
    customer.ele('first_name').txt(data.customer.first_name);
    customer.ele('last_name').txt(data.customer.last_name);
    if (data.customer.phone) {
      customer.ele('phone').txt(data.customer.phone);
    }

    const invoice = body.ele('invoice');
    invoice.ele('description').txt(data.invoice.description);
    invoice.ele('amount').att('currency', data.invoice.currency || 'eur').txt(String(data.invoice.amount));
    invoice.ele('due_date').txt(data.invoice.due_date);
    if (data.invoice.invoice_number) {
      invoice.ele('invoice_number').txt(data.invoice.invoice_number);
    }

    const items = body.ele('items');
    for (const item of (data.items || [])) {
      const itemElem = items.ele('item');
      itemElem.ele('description').txt(item.description);
      itemElem.ele('quantity').txt(String(item.quantity));
      itemElem.ele('unit_price').att('currency', item.currency || 'eur').txt(String(item.unit_price));
      itemElem.ele('vat_rate').txt(String(item.vat_rate ?? 21));
      if (item.sku) {
        itemElem.ele('sku').txt(item.sku);
      }
    }

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  buildMailingSendXml(data) {
    const messageId = `mail-crm-${uuidv4()}`;
    const timestamp = new Date().toISOString();

    const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');

    const header = root.ele('header');
    header.ele('message_id').txt(messageId);
    header.ele('version').txt('2.0');
    header.ele('type').txt('mailing_send');
    header.ele('timestamp').txt(timestamp);
    header.ele('source').txt('crm');
    if (data.correlation_id) {
      header.ele('correlation_id').txt(data.correlation_id);
    }

    const body = root.ele('body');

    const mailing = body.ele('mailing');
    mailing.ele('subject').txt(data.mailing.subject);
    mailing.ele('template_id').txt(data.mailing.template_id);
    if (data.mailing.from_address) {
      mailing.ele('from_address').txt(data.mailing.from_address);
    }
    if (data.mailing.reply_to) {
      mailing.ele('reply_to').txt(data.mailing.reply_to);
    }

    const recipients = body.ele('recipients');
    for (const recipient of data.recipients) {
      const recipientElem = recipients.ele('recipient');
      recipientElem.ele('email').txt(recipient.email);
      recipientElem.ele('first_name').txt(recipient.first_name);
      recipientElem.ele('last_name').txt(recipient.last_name);
      if (recipient.language) {
        recipientElem.ele('language').txt(recipient.language);
      }
    }

    return root.doc().end({ prettyPrint: true, indent: '  ' });
  }

  async sendInvoiceRequest(data) {
    if (!this.channel) {
      throw new Error('CRM Sender not initialized. Call init() first.');
    }
    try {
      const xmlPayload = this.buildInvoiceRequestXml(data);
      const queue = 'crm.to.facturatie';
      await this.channel.assertQueue(queue, { durable: true });
      this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      console.log(`Invoice request sent to queue "${queue}"`);
      return { success: true, queue, payload: xmlPayload };
    } catch (error) {
      console.log(`Failed to send invoice request: ${error}`);
      throw error;
    }
  }

  async sendMailingSend(data) {
    if (!this.channel) {
      throw new Error('CRM Sender not initialized. Call init() first.');
    }
    try {
      const xmlPayload = this.buildMailingSendXml(data);
      const queue = 'crm.to.mailing';
      await this.channel.assertQueue(queue, { durable: true });
      this.channel.sendToQueue(queue, Buffer.from(xmlPayload), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
      console.log(`Mailing send request sent to queue "${queue}"`);
      return { success: true, queue, payload: xmlPayload };
    } catch (error) {
      console.log(`Failed to send mailing send request: ${error}`);
      throw error;
    }
  }

  async close() {
    try {
      if (this.connection) await this.connection.close();
      console.log('CRM Sender connection closed');
    } catch (error) {
      console.log(`Error closing connection: ${error}`);
    }
  }
}

module.exports = CRMSender;
