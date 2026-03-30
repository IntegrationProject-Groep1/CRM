'use strict';

require('dotenv').config();
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');
const SFConnection = require('./sfConnection');

class Heartbeat {
  constructor() {
    this.connection = null;
    this.channel = null;
    this.queue = 'heartbeat';
    this.interval = 1; // seconds
    this.healthCheckCounter = 0;
    this.sfHealthy = true;
    this.startTime = Date.now();
    this.shuttingDown = false;
    this.intervalId = null;
    this.sf = new SFConnection();
  }

  async connect() {
    try {
      this.connection = await amqp.connect(
        process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost/'
      );
      this.channel = await this.connection.createChannel();
      await this.channel.assertQueue(this.queue, { durable: true });
      console.log(`[Heartbeat] Connected to RabbitMQ, queue: ${this.queue}`);
      return true;
    } catch (err) {
      console.log(`[Heartbeat] Failed to connect to RabbitMQ: ${err}`);
      return false;
    }
  }

  async checkSalesforceHealth() {
    try {
      const isHealthy = await this.sf.healthCheck();
      this.sfHealthy = isHealthy;
      if (isHealthy) {
        console.log('[Heartbeat] Salesforce health check: OK');
      } else {
        console.log('[Heartbeat] Salesforce health check: FAILED');
      }
      return isHealthy;
    } catch (err) {
      console.log(`[Heartbeat] Salesforce health check error: ${err}`);
      this.sfHealthy = false;
      return false;
    }
  }

  getStatus() {
    if (this.shuttingDown) return 'offline';
    return this.sfHealthy ? 'online' : 'degraded';
  }

  generateHeartbeat() {
    const now = new Date().toISOString();
    const messageId = `hb-crm-${uuidv4()}`;
    const status = this.getStatus();
    const uptime = Math.floor((Date.now() - this.startTime) / 1000);

    return `<?xml version="1.0" encoding="UTF-8"?>
<message>
  <header>
    <message_id>${messageId}</message_id>
    <version>2.0</version>
    <type>heartbeat</type>
    <timestamp>${now}</timestamp>
    <source>crm</source>
  </header>
  <body>
    <status>${status}</status>
    <service>crm</service>
    <uptime>${uptime}</uptime>
  </body>
</message>`;
  }

  async sendHeartbeat() {
    try {
      this.healthCheckCounter++;

      if (this.healthCheckCounter % 10 === 0) {
        await this.checkSalesforceHealth();
      }

      const heartbeat = this.generateHeartbeat();

      if (this.channel) {
        const ok = this.channel.sendToQueue(this.queue, Buffer.from(heartbeat), { deliveryMode: 2 });
        if (!ok) console.log('[Heartbeat] Warning: write buffer full, heartbeat may be dropped');
        console.log(`[Heartbeat] Sent (${this.getStatus()}) - Counter: ${this.healthCheckCounter}`);
      }
    } catch (err) {
      console.log(`[Heartbeat] Failed to send heartbeat: ${err}`);
      await this.reconnect();
    }
  }

  async reconnect() {
    console.log('[Heartbeat] Attempting to reconnect to RabbitMQ...');
    try {
      if (this.connection) await this.connection.close();
    } catch { /* ignore */ }

    while (!this.shuttingDown && !(await this.connect())) {
      await new Promise(r => setTimeout(r, 5000));
    }
  }

  async start() {
    console.log('[Heartbeat] Starting heartbeat service...');
    await this.sf.init();
    if (!(await this.connect())) {
      await this.reconnect();
    }
    console.log('[Heartbeat] Heartbeat interval set to 1 second');
    this.intervalId = setInterval(() => this.sendHeartbeat(), this.interval * 1000);
  }

  async shutdown() {
    console.log('[Heartbeat] Initiating graceful shutdown...');
    this.shuttingDown = true;
    if (this.intervalId) clearInterval(this.intervalId);

    await this.sendHeartbeat();

    try {
      if (this.channel) await this.channel.close();
    } catch { /* ignore */ }
    try {
      if (this.connection) await this.connection.close();
    } catch { /* ignore */ }

    console.log('[Heartbeat] Shutdown complete');
    process.exit(0);
  }
}

const heartbeat = new Heartbeat();

process.on('SIGINT', () => heartbeat.shutdown());
process.on('SIGTERM', () => heartbeat.shutdown());

heartbeat.start().catch(err => {
  console.log(`[Heartbeat] Fatal error: ${err}`);
  process.exit(1);
});
