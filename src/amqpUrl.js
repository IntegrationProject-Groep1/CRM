'use strict';

function getAmqpOptions() {
  const user = process.env.RABBITMQ_USER;
  const pass = process.env.RABBITMQ_PASS;

  if (!user || !pass) {
    throw new Error('RABBITMQ_USER and RABBITMQ_PASS environment variables are required');
  }

  const protocol = process.env.RABBITMQ_PROTOCOL || 'amqp';
  if (protocol !== 'amqps') {
    console.warn('[amqp] WARNING: Not using TLS (amqps). Set RABBITMQ_PROTOCOL=amqps in production.');
  }

  return {
    protocol,
    hostname: process.env.RABBITMQ_HOST || 'localhost',
    port: parseInt(process.env.RABBITMQ_PORT || '5672', 10),
    username: user,
    password: pass.replace(/\$\$/g, '$'),
    vhost: process.env.RABBITMQ_VHOST || '/',
  };
}

module.exports = { getAmqpOptions };
