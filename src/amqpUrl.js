'use strict';

/**
 * Builds an AMQP connection URL from environment variables.
 *
 * Supports two configuration styles:
 *
 * Option 1 – Full URL (RABBITMQ_URL must be URL-encoded if the password
 *             contains special characters such as #, ^, $):
 *   RABBITMQ_URL=amqp://user:p%40ssw0rd@host:port/
 *
 * Option 2 – Separate variables (recommended; special characters in the
 *             password are handled automatically via encodeURIComponent):
 *   RABBITMQ_HOST=host
 *   RABBITMQ_PORT=5672
 *   RABBITMQ_USER=user
 *   RABBITMQ_PASS=p@ssw0rd
 *
 * If RABBITMQ_URL is set it takes precedence over the separate variables.
 */
function getAmqpUrl() {
  if (process.env.RABBITMQ_URL) {
    return process.env.RABBITMQ_URL;
  }

  const host = process.env.RABBITMQ_HOST || 'localhost';
  const port = process.env.RABBITMQ_PORT || '5672';
  const user = encodeURIComponent(process.env.RABBITMQ_USER || 'guest');
  const pass = encodeURIComponent(process.env.RABBITMQ_PASS || 'guest');

  return `amqp://${user}:${pass}@${host}:${port}/`;
}

module.exports = { getAmqpUrl };
