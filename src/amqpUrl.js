'use strict';

/**
 * Builds an amqplib connection-options object from environment variables.
 *
 * Using a plain options object (instead of an amqp:// URL string) means
 * special characters in the password are passed as-is — no URL-encoding
 * required.  This mirrors the approach used by the other teams in this
 * project (e.g. pika.ConnectionParameters in the Python services).
 *
 * Supported variables:
 *   RABBITMQ_HOST  – hostname      (default: localhost)
 *   RABBITMQ_PORT  – port number   (default: 5672)
 *   RABBITMQ_USER  – username      (default: guest)
 *   RABBITMQ_PASS  – password      (default: guest)
 *   RABBITMQ_VHOST – virtual host  (default: /)
 *
 * If the password is stored in a Compose-friendly form, every "$$" sequence
 * is normalized back to a single "$" before connecting.
 */
function getAmqpOptions() {
  const rawPassword = process.env.RABBITMQ_PASS || 'guest';

  return {
    protocol: 'amqp',
    hostname: process.env.RABBITMQ_HOST  || 'localhost',
    port:     parseInt(process.env.RABBITMQ_PORT  || '5672', 10),
    username: process.env.RABBITMQ_USER  || 'guest',
    password: rawPassword.replace(/\$\$/g, '$'),
    vhost:    process.env.RABBITMQ_VHOST || '/',
  };
}

module.exports = { getAmqpOptions };
