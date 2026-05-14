# CRM Integration Service

## What does this project do?

This is a Node.js integration microservice that receives messages via RabbitMQ, processes them, and stores data in Salesforce. Salesforce is the only external data source for this service. The service also integrates with an Identity Service for Master UUID management and sends outgoing messages to the POS (Kassa), Invoicing (Facturatie), Mailing, and Planning systems.

## Architecture overview

```text
Other systems (Frontend, POS, IoT, etc.)
         |
         v
   RabbitMQ Queues (incoming)
   +----------------------+
   |  crm.incoming        |  <- general CRM messages
   |  kassa.payments      |  <- payment and consumption messages from POS
   |  user.created        |  <- new users from Frontend/Drupal
   |  user.registered     |  <- session registrations from Frontend/Drupal
   +----------------------+
         |
         v
    receiver.js
         |-- Identity Service RPC (identity.user.create.request) for Master UUID management
         |-- Salesforce (Member__c, Task, Consumption__c)
         |
         v  (via sender.js)
   +------------------------------------------+
   |  facturatie.incoming                     |  -> Invoice requests & new registrations
   |  crm.to.mailing                          |  -> Email campaigns
   |  kassa.incoming                          |  -> Customer registrations, profile updates, cancellations
   |  frontend.user.unregistered (fanout)     |  -> crm.salesforce, planning.outlook, mailing.sendgrid
   |  calendar.exchange (topic)               |  -> registration.cancelled (to Planning)
   +------------------------------------------+

   Invalid messages -> crm.dead-letter
```

## File structure

```text
CRM/
|-- src/
|   |-- receiver.js       <- Main file: receives and processes messages
|   |-- sender.js         <- Sends XML messages to other queues
|   |-- sfConnection.js   <- Salesforce connection and authentication
|   |-- heartbeat.js      <- Sends a status signal every second
|   `-- amqpUrl.js        <- Builds the RabbitMQ connection options
|-- tests/
|   |-- receiver.test.js  <- Tests for the receiver flow
|   `-- sender.test.js    <- Tests for XML construction
|-- .env.example          <- Example of required environment variables
|-- Dockerfile            <- Containerisation (Node 20)
|-- docker-compose.yml    <- Starts the service with RabbitMQ
`-- package.json          <- NPM project configuration and scripts
```

## Key components

### `src/receiver.js`

The main file that starts automatically via `npm start`. It:

- starts a health check HTTP server on port `3000`
- connects to RabbitMQ
- listens on `crm.incoming`, `kassa.payments`, `user.created`, and `user.registered`
- parses XML messages
- validates headers and types
- makes RPC calls to the Identity Service to retrieve/create Master UUIDs
- routes each message to the appropriate handler

Supported message types:

| Type | Queue | Action |
|---|---|---|
| `user.created` | `user.created` | Create or update `Member__c` in Salesforce via Master UUID |
| `user.registered` | `user.registered` | Update `Member__c` + store session registration as a `Task` in Salesforce |
| `new_registration` | `crm.incoming` | Upsert customer in Salesforce, forward to POS and Invoicing |
| `user.unregistered` | `crm.incoming` | Publish fanout to `frontend.user.unregistered` exchange |
| `user.updated` | `crm.incoming` | Update `Member__c` in Salesforce |
| `delete_user` | `crm.incoming` | Mark `Member__c` as deleted in Salesforce |
| `user_deleted` | `crm.incoming` | Same as `delete_user` (for frontend-initiated deletions) |
| `payment_registered` | `crm.incoming` | Create `Task` in Salesforce |
| `badge_scanned` | `crm.incoming` | Create `Task` in Salesforce |
| `session_updated` | `crm.incoming` | Create `Task` in Salesforce |
| `invoice_status` | `crm.incoming` | Create `Task` in Salesforce |
| `send_invoice` | `crm.incoming` | Update latest invoice fields on `Member__c` in Salesforce |
| `mailing_status` | `crm.incoming` | Create `Task` in Salesforce |
| `consumption_order` | `kassa.payments` | Create `Consumption__c` records in Salesforce |
| `badge_assigned` | `kassa.payments` | Update badge ID on `Member__c` in Salesforce |
| `refund_processed` | `kassa.payments` | Create `Task` in Salesforce |
| `invoice_request` | `kassa.payments` | Create `Task` in Salesforce and forward to `facturatie.incoming` |
| `invoice_cancelled` | `kassa.payments` | Process cancelled invoice in Salesforce |

Invalid or unparseable messages are sent to `crm.dead-letter`.

### `src/sender.js`

Builds XML messages and sends them to the appropriate RabbitMQ queue or exchange:

| Method | Target / Queue |
|---|---|
| `sendNewRegistrationToKassa` | `kassa.incoming` |
| `sendNewRegistrationToFacturatie` | `facturatie.incoming` |
| `sendProfileUpdateToKassa` | `kassa.incoming` |
| `sendCancelRegistrationToKassa` | `kassa.incoming` |
| `sendCancelRegistrationToPlanning` | `calendar.exchange` (topic, routing key `registration.cancelled`) |
| `sendInvoiceRequest` | `facturatie.incoming` |
| `sendInvoiceCancelledToFacturatie` | `facturatie.incoming` |
| `sendMailingSend` | `crm.to.mailing` |
| `sendUserUnregisteredFanout` | `frontend.user.unregistered` (fanout exchange) |

### `src/sfConnection.js`

Manages authentication and API calls to Salesforce. Supports OAuth2 with refresh token and direct access token fallback. If no valid credentials are present, the service runs in DRY RUN mode (all Salesforce operations are simulated and logged, but not executed).

### `src/heartbeat.js`

Sends a heartbeat message (XML) to the `heartbeat` queue every second. A Salesforce health check is also performed every 10 seconds. The reported status is `online`, `degraded`, or `offline`.

### `src/amqpUrl.js`

Builds the RabbitMQ connection options from environment variables. Logs a warning if TLS (`amqps`) is not enabled.

### Identity Service integration

When processing `new_registration`, `user.created`, and `user.registered` messages, the service makes an RPC call to the Identity Service via the `identity.user.create.request` queue. This ensures that every member in Salesforce receives the same Master UUID as the rest of the infrastructure. The call uses a temporary exclusive reply queue and a 15-second timeout.

## Environment variables

Copy `.env.example` to `.env` and fill in the values:

```env
RABBITMQ_HOST=integrationproject-2526s2-dag01.westeurope.cloudapp.azure.com
RABBITMQ_PORT=30000
RABBITMQ_PROTOCOL=amqps
RABBITMQ_USER=your_rabbitmq_user
RABBITMQ_PASS=your_rabbitmq_password
RABBITMQ_VHOST=/

SF_INSTANCE_URL=https://yourorg.my.salesforce.com
SF_CLIENT_ID=your_client_id
SF_CLIENT_SECRET=your_client_secret
SF_REFRESH_TOKEN=your_refresh_token
SF_ACCESS_TOKEN=your_access_token
SF_API_VERSION=v60.0
SF_CALLBACK_URL=https://oauth.pstmn.io/v1/callback

CRM_RETRY_DELAY_MS=300000
CRM_MAX_RETRY_ATTEMPTS=288

HEALTH_PORT=3000
```

## Getting started

With Docker:

```bash
cp .env.example .env
docker compose up
```

This starts RabbitMQ and the CRM service.

Locally:

```bash
npm install
cp .env.example .env
npm start
npm run heartbeat
```

## Tests

Run tests:

```bash
npm test
```

Linting:

```bash
npm run lint
```

## Dependencies

| Library | Purpose |
|---|---|
| `amqplib` | Receive and send RabbitMQ messages |
| `fast-xml-parser` | Parse incoming XML messages |
| `xml2js` | Parse XML messages for Identity Service RPC responses |
| `xmlbuilder2` | Build outgoing XML messages |
| `jsforce` | Salesforce API client |
| `dotenv` | Load environment variables from `.env` |
| `uuid` | Generate unique message IDs |
