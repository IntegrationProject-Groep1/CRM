'use strict';

const winston = require('winston');
const Transport = require('winston-transport');
const { create } = require('xmlbuilder2');
const { v4: uuidv4 } = require('uuid');

class RabbitMQTransport extends Transport {
  constructor(opts = {}) {
    super(opts);
    this.name = 'rabbitmq';
    this.level = opts.level || 'info';
    this.channel = null;
    this.queue = [];
    this.isLoggingSelf = false;
  }

  async setChannel(channel) {
    this.channel = channel;
    try {
      // Assert the logs queue to ensure it exists
      await this.channel.assertQueue('logs', { durable: true });
    } catch (err) {
      console.error('[logger] Failed to assert logs queue:', err.message);
    }
    // Flush queued logs
    while (this.queue.length > 0) {
      const { xml, callback } = this.queue.shift();
      this._publish(xml, callback);
    }
  }

  _publish(xml, callback) {
    if (!this.channel) {
      return callback();
    }
    try {
      this.channel.sendToQueue('logs', Buffer.from(xml), {
        contentType: 'application/xml',
        deliveryMode: 2,
      });
    } catch (err) {
      console.error('[logger] Failed to send log to RabbitMQ:', err.message);
    }
    callback();
  }

  log(info, callback) {
    setImmediate(() => {
      this.emit('logged', info);
    });

    // Recursion guard: do not log if we are already in logging execution
    if (this.isLoggingSelf) {
      return callback();
    }

    // Ignore low-level rabbitmq noise to prevent recursion
    const isLowLevel = info.message && (
      info.message.includes('RabbitMQ') ||
      info.message.includes('amqp') ||
      info.message.includes('[sender]') ||
      info.message.includes('[receiver]') ||
      info.message.includes('assertQueue') ||
      info.message.includes('sendToQueue') ||
      info.message.includes('connection')
    );
    if (isLowLevel) {
      return callback();
    }

    this.isLoggingSelf = true;
    try {
      const level = info.level === 'error' ? 'error' : (info.level === 'warn' || info.level === 'warning' ? 'warning' : 'info');
      let action = info.action || 'system_error';
      const validActions = new Set([
        'registration', 'user', 'payment', 'invoice', 'session', 'calendar',
        'email', 'wallet', 'refund', 'identity', 'xml_validation', 'system_error', 'badge'
      ]);
      if (!validActions.has(action)) {
        action = 'system_error';
      }

      const root = create({ version: '1.0', encoding: 'UTF-8' }).ele('message');
      const header = root.ele('header');
      header.ele('message_id').txt(uuidv4());
      header.ele('timestamp').txt(new Date().toISOString());
      header.ele('source').txt('crm');
      header.ele('type').txt('log');
      header.ele('version').txt('2.0');

      const body = root.ele('body');
      body.ele('level').txt(level);
      body.ele('action').txt(action);
      body.ele('message').txt(info.message);

      const xml = root.doc().end({ prettyPrint: true, indent: '  ' });

      if (this.channel) {
        this._publish(xml, callback);
      } else {
        this.queue.push({ xml, callback });
      }
    } catch (err) {
      console.error('[logger] Formatting log XML failed:', err.message);
      callback();
    } finally {
      this.isLoggingSelf = false;
    }
  }
}

const rabbitmqTransport = new RabbitMQTransport();

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => {
      return `[${timestamp}] [${level.toUpperCase()}]: ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.printf(({ timestamp, level, message }) => {
          return `[${timestamp}] [${level}]: ${message}`;
        })
      )
    }),
    rabbitmqTransport
  ]
});

module.exports = {
  logger,
  setLoggingChannel: (channel) => rabbitmqTransport.setChannel(channel)
};
