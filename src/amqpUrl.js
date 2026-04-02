'use strict';

/**
 * Builds an AMQP connection URL from environment variables.
 *
 * Supported variables:
 *   RABBITMQ_HOST  – hostname (default: localhost)
 *   RABBITMQ_PORT  – port     (default: 5672)
 *   RABBITMQ_USER  – username (default: guest)
 *   RABBITMQ_PASS  – password (default: guest); special characters are handled automatically
 *   RABBITMQ_VHOST – virtual host (default: /)
 */
function getAmqpUrl() {
  const host  = process.env.RABBITMQ_HOST  || 'localhost';
  const port  = process.env.RABBITMQ_PORT  || '5672';
  const user  = encodeURIComponent(process.env.RABBITMQ_USER  || 'guest');
  const pass  = encodeURIComponent(process.env.RABBITMQ_PASS  || 'guest');
  const vhost = encodeURIComponent(process.env.RABBITMQ_VHOST || '/');

  return `amqp://${user}:${pass}@${host}:${port}/${vhost}`;
}

module.exports = { getAmqpUrl };
