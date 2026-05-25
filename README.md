# CRM Integration Service

This service is the **CRM integration layer** between RabbitMQ, Salesforce, and other platform services (POS/Kassa, Invoicing, Mailing, Planning, Identity).

It consumes XML messages from queues/exchanges, validates them, updates Salesforce, and forwards new XML messages to downstream systems.

## 1) What this service does

- Listens to RabbitMQ messages from multiple sources
- Validates incoming XML (basic structure + XSD where configured)
- Resolves/creates a **Master UUID** through the Identity service when needed
- Creates/updates Salesforce records (`Member__c`, `Task`, `Consumption__c`)
- Forwards events to other services (Kassa, Facturatie, Mailing, Planning, Frontend)
- Retries temporary processing failures via retry queues
- Sends invalid/non-retryable messages to dead-letter

## 2) High-level flow

```text
Incoming queues/exchanges
  - crm.incoming
  - kassa.payments
  - facturatie.to.crm
  - user.created
  - user.registered
  - planning.exchange -> planning.session.events
  - user.events -> crm.identity.user.events

        |
        v
    src/receiver.js
      - parse + validate XML
      - route by message type
      - sync to Salesforce
      - publish follow-up events via src/sender.js

        |
        v
Outgoing destinations
  - kassa.incoming
  - facturatie.incoming
  - crm.to.mailing
  - frontend.incoming
  - frontend.user.unregistered (fanout)
  - calendar.exchange (topic)
  - logs

Error handling
  - retry queues: <queue>.retry
  - dead-letter exchange: crm.dlx
  - dead-letter queue: crm.dead-letter
```

## 3) Main components

### `src/receiver.js`
Main worker process (`npm start`).

- Starts health endpoint (`GET /` on `HEALTH_PORT`, default `3000`)
- Connects to RabbitMQ and consumes all configured queues
- Performs message validation and routing
- Handles retry/dead-letter behavior
- Uses Identity RPC (`identity.user.create.request`) with 15s timeout

> **Identity service pattern:** the CRM never generates UUIDs internally. Whenever a `master_uuid` is needed and not present in the incoming message, the receiver performs an RPC call to the Identity service: it publishes an `identity_request` XML to `identity.user.create.request` with a `correlationId` and `replyTo` queue, then waits for an `identity_response` containing the `master_uuid`. This UUID is then used for all Salesforce operations and outgoing messages.

### `src/sender.js`
Builds and publishes XML messages to queues/exchanges.

Examples of outgoing methods:
- Registration/profile/cancel updates to `kassa.incoming`
- Invoice and payment messages to `facturatie.incoming`
- Mailing messages to `crm.to.mailing`
- Fanout unregister event to `frontend.user.unregistered`
- Planning event publication to `calendar.exchange`

### `src/sfConnection.js`
Salesforce connection and API wrapper.

- OAuth refresh-token authentication (preferred)
- Direct access-token fallback
- Automatic token refresh on expired sessions
- If credentials are missing/invalid: **DRY RUN mode** — all message processing continues normally, but no records are written to Salesforce. Useful for local development without Salesforce access.

### `src/heartbeat.js`
Optional heartbeat process (`npm run heartbeat`).

- Sends a heartbeat XML to `heartbeat` queue every second
- Runs Salesforce health check every 10 seconds
- Reports `online`, `degraded`, or `offline`

### `src/mcp_server.js`
Optional MCP server container/process for CRM Salesforce tooling.

- Runs on port `8008` by default
- Uses same Salesforce credentials from environment

## 4) Message types handled by the receiver

Current routed message types include:

- User lifecycle: `user_created`, `user.created`, `user_registered`, `user_unregistered`, `user_updated`, `delete_user`, `user_deleted`, `user_checkin`
- Registration/session: `new_registration`, `cancel_registration`, `session_created`, `session_updated`, `session_deleted`, `event_ended`
- Payment/invoice: `payment_registered`, `invoice_status`, `send_invoice`, `invoice_request`, `invoice_cancelled`, `consumption_order`, `refund_processed`
- Badge/wallet: `badge_scanned`, `badge_assigned`, `wallet_lease_request`, `wallet_lease_return`, `wallet_topup_request`
- Company: `company_registration`, `company_update`, `company_delete`, `company_member_removed`, `company_invite`
- Mailing: `mailing_status`

Unknown message types are logged but not processed.

> **Note on outgoing messages:** when a `user_created` message is processed, the CRM also sends a `send_mailing` message to `crm.to.mailing` with `campaign_id: registration_confirmation`, triggering a registration confirmation email to the new user.

## 5) Prerequisites

- Node.js `>=22` (see `package.json` engines)
- RabbitMQ access credentials
- Salesforce credentials (unless running intentionally in DRY RUN mode)

## 6) Environment setup

1. Copy environment template:

```bash
cp .env.example .env
```

2. Fill required values in `.env`.

Minimum RabbitMQ settings:

- `RABBITMQ_HOST`
- `RABBITMQ_PORT`
- `RABBITMQ_PROTOCOL` (`amqps` recommended)
- `RABBITMQ_USER`
- `RABBITMQ_PASS`
- `RABBITMQ_VHOST`

Salesforce settings:

- `SF_INSTANCE_URL`
- `SF_CLIENT_ID`
- `SF_CLIENT_SECRET`
- `SF_REFRESH_TOKEN`
- `SF_ACCESS_TOKEN` (fallback)
- `SF_API_VERSION`

Retry settings:

- `CRM_RETRY_DELAY_MS` (default `300000` = 5 min)
- `CRM_MAX_RETRY_ATTEMPTS` (default `288`)

Health endpoint:

- `HEALTH_PORT` (default `3000`)

## 7) Run locally

### Option A: Docker Compose

```bash
cp .env.example .env
docker compose up --build
```

This starts:
- `crm-receiver`
- `rabbitmq_broker`
- `crm_mcp`

### Option B: Node.js directly

```bash
npm install
cp .env.example .env
npm start
```

Optional in a second terminal:

```bash
npm run heartbeat
```

## 8) Development checks

Lint:

```bash
npm run lint
```

Tests:

```bash
npm test -- --runInBand
```

## 9) Repository structure

```text
CRM/
|-- src/
|   |-- receiver.js       # main worker: consumes queues, routes messages, syncs Salesforce
|   |-- sender.js         # builds and publishes outgoing XML messages
|   |-- sfConnection.js   # Salesforce OAuth wrapper + DRY RUN fallback
|   |-- heartbeat.js      # optional heartbeat process
|   |-- amqpUrl.js        # RabbitMQ connection URL builder
|   |-- validator.js      # XSD validation helper (libxmljs)
|   `-- mcp_server.js     # optional MCP server for Salesforce tooling
|-- tests/
|   |-- receiver.test.js
|   `-- sender.test.js
|-- xsd/                  # XML Schema Definition files — one per message type
|                         # both incoming (validated on receive) and outgoing (validated before publish)
|-- .env.example
|-- docker-compose.yml
|-- Dockerfile
`-- package.json
```

## 10) Troubleshooting

- **`RABBITMQ_USER and RABBITMQ_PASS environment variables are required`**
  - Set both values in `.env`.

- **Salesforce not connected / DRY RUN mode**
  - Check Salesforce credentials and network access.

- **Messages end in `crm.dead-letter`**
  - Check XML format/XSD compliance and required message fields.

- **Repeated retries**
  - Inspect temporary upstream outages (Salesforce/Identity/RabbitMQ).

## 11) Security measures

Current security-related measures in this service:

- **RabbitMQ credentials are mandatory** (`RABBITMQ_USER`, `RABBITMQ_PASS`) before startup.
- **TLS-ready RabbitMQ transport**: `amqps` is supported and recommended for production traffic.
- **XML validation on message processing** (structure and XSD where configured) for incoming and outgoing payloads.
- **Controlled retry + dead-letter flow** to isolate invalid/unrecoverable messages from normal processing.
- **Salesforce OAuth2 refresh-token flow** as primary authentication method (with automatic token refresh support).
- **Environment-based secrets/configuration** through `.env` (no hardcoded credentials required).

## 12) Connected departments/services

This CRM integration is connected with these departments/platform domains:

- **Kassa / POS** (`kassa.payments`, `kassa.incoming`)
- **Facturatie / Invoicing** (`facturatie.to.crm`, `facturatie.incoming`)
- **Mailing** (`crm.to.mailing`)
- **Planning / Calendar** (`planning.exchange`, `planning.session.events`, `calendar.exchange`)
- **Identity / User lifecycle** (`user.events`, `crm.identity.user.events`, `user.created`, `user.registered`)
- **Frontend** (`frontend.incoming`, `frontend.user.unregistered`)
- **Salesforce CRM** (sync target for members, tasks, and consumptions)

---

If you are new to this project, start with:
1. Section 6 (Environment setup)
2. Section 7 (Run locally)
3. `src/receiver.js` and `src/sender.js` for processing flow
