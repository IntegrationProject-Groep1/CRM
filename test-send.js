'use strict';

require('dotenv').config();
const amqp = require('amqplib');
const { create } = require('xmlbuilder2');
const { v4: uuidv4 } = require('uuid');
const { getAmqpOptions } = require('./src/amqpUrl');

async function sendTestMessage() {
  const conn = await amqp.connect(getAmqpOptions());
  const channel = await conn.createChannel();

  // Test 1: new_registration on crm.incoming
  const regXml = create({ version: '1.0', encoding: 'UTF-8' })
    .ele('message')
      .ele('header')
        .ele('message_id').txt(`test-${uuidv4()}`).up()
        .ele('version').txt('2.0').up()
        .ele('type').txt('new_registration').up()
        .ele('timestamp').txt(new Date().toISOString()).up()
        .ele('source').txt('registratie').up()
      .up()
      .ele('body')
        .ele('customer')
          .ele('email').txt('test@example.com').up()
          .ele('contact')
            .ele('first_name').txt('Jan').up()
            .ele('last_name').txt('Peeters').up()
          .up()
          .ele('type').txt('private').up()
          .ele('user_id').txt('user-001').up()
          .ele('age').txt('30').up()
        .up()
        .ele('payment_due')
          .ele('amount').txt('150.00').up()
          .ele('status').txt('unpaid').up()
        .up()
      .up()
    .doc().end({ prettyPrint: true });

  await channel.assertQueue('crm.incoming', { durable: true });
  channel.sendToQueue('crm.incoming', Buffer.from(regXml), { contentType: 'application/xml', deliveryMode: 2 });
  console.log('✓ Sent new_registration to crm.incoming');

  // Test 2: badge_scanned on kassa.payments
  const badgeXml = create({ version: '1.0', encoding: 'UTF-8' })
    .ele('message')
      .ele('header')
        .ele('message_id').txt(`test-${uuidv4()}`).up()
        .ele('version').txt('2.0').up()
        .ele('type').txt('badge_scanned').up()
        .ele('timestamp').txt(new Date().toISOString()).up()
        .ele('source').txt('kassa').up()
      .up()
      .ele('body')
        .ele('badge_id').txt('BADGE-XYZ-001').up()
        .ele('user_id').txt('user-001').up()
        .ele('session_id').txt('session-42').up()
        .ele('location').txt('Entrance A').up()
      .up()
    .doc().end({ prettyPrint: true });

  await channel.assertQueue('kassa.payments', { durable: true });
  channel.sendToQueue('kassa.payments', Buffer.from(badgeXml), { contentType: 'application/xml', deliveryMode: 2 });
  console.log('✓ Sent badge_scanned to kassa.payments');

  await channel.close();
  await conn.close();
  console.log('\nDone. Check receiver logs to see them being processed.');
}

sendTestMessage().catch(console.error);
