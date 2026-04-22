# CRM Integratieservice

## Wat doet dit project?

Dit is een Node.js integratie-microservice die berichten ontvangt via RabbitMQ, ze verwerkt, en gegevens opslaat in Salesforce. Salesforce is de enige databron voor deze service.

## Architectuuroverzicht

```text
Andere systemen (Kassa, IoT, enz.)
         |
         v
   RabbitMQ Queues
   +----------------------+
   |  crm.incoming        |  <- berichten voor het CRM
   |  kassa.payments      |  <- betalings- en consumptieberichten van Kassa
   +----------------------+
         |
         v
    receiver.js  ---> Salesforce (Member__c, Task, Consumption__c)
         |
         v  (via sender.js)
   +----------------------+
   |  crm.to.facturatie   |  -> Factuurverzoeken
   |  crm.to.mailing      |  -> E-mailcampagnes
   |  kassa.incoming      |  -> Klantregistraties, profielen, annuleringen
   +----------------------+
```

## Bestandsstructuur

```text
CRM/
|-- src/
|   |-- receiver.js       <- Hoofdbestand: ontvangt en verwerkt berichten
|   |-- sender.js         <- Verstuurt XML-berichten naar andere queues
|   |-- sfConnection.js   <- Verbinding en authenticatie met Salesforce
|   `-- heartbeat.js      <- Stuurt elke seconde een statussignaal
|-- tests/
|   |-- receiver.test.js  <- Tests voor de receiver-flow
|   `-- sender.test.js    <- Tests voor XML-opbouw
|-- .env.example          <- Voorbeeld van vereiste omgevingsvariabelen
|-- Dockerfile            <- Containerisatie (Node 20)
|-- docker-compose.yml    <- Start de service met RabbitMQ
`-- package.json          <- NPM-projectconfiguratie en scripts
```

## Belangrijkste onderdelen

### `src/receiver.js`

Het hoofdbestand dat automatisch opstart via `npm start`. Het:

- start een health check HTTP-server op poort `3000`
- verbindt met RabbitMQ
- luistert op `crm.incoming` en `kassa.payments`
- parseert XML-berichten
- valideert headers en types
- routeert elk bericht naar de juiste handler

Ondersteunde berichttypen:

| Type | Actie |
|---|---|
| `new_registration` | klant aanmaken of bijwerken in Salesforce en doorsturen naar Kassa |
| `payment_registered` | taak aanmaken in Salesforce |
| `badge_scanned` | taak aanmaken in Salesforce |
| `session_update` | taak aanmaken in Salesforce |
| `invoice_status` | taak aanmaken in Salesforce |
| `mailing_status` | taak aanmaken in Salesforce |
| `consumption_order` | consumptierecords aanmaken in Salesforce |
| `badge_assigned` | badge-ID bijwerken op `Member__c` in Salesforce |
| `refund_processed` | taak aanmaken in Salesforce |
| `invoice_request` | taak aanmaken in Salesforce en doorsturen naar `crm.to.facturatie` |

Ongeldige of niet-parseerbare berichten gaan naar `crm.dead-letter`.

### `src/sender.js`

Bouwt XML-berichten op en verstuurt die naar de juiste RabbitMQ-queue.

### `src/sfConnection.js`

Beheert authenticatie en API-calls naar Salesforce. Ondersteunt OAuth2 met refresh token en directe access token fallback. Als geen geldige credentials aanwezig zijn, draait de service in DRY RUN mode.

### `src/heartbeat.js`

Stuurt elke seconde een heartbeat-bericht naar de `heartbeat` queue. Elke 10 seconden gebeurt ook een Salesforce health check.

## Omgevingsvariabelen

Kopieer `.env.example` naar `.env` en vul aan:

```env
RABBITMQ_HOST=integrationproject-2526s2-dag01.westeurope.cloudapp.azure.com
RABBITMQ_PORT=30000
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

HEALTH_PORT=3000
```

## Opstarten

Met Docker:

```bash
cp .env.example .env
docker compose up
```

Dit start RabbitMQ en de CRM-service.

Lokaal:

```bash
npm install
cp .env.example .env
npm start
npm run heartbeat
```

## Tests

Tests uitvoeren:

```bash
npm test
```

Linting:

```bash
npm run lint
```

## Gebruikte bibliotheken

| Bibliotheek | Doel |
|---|---|
| `amqplib` | RabbitMQ berichten ontvangen en versturen |
| `fast-xml-parser` | inkomende XML-berichten parsen |
| `xmlbuilder2` | uitgaande XML-berichten bouwen |
| `jsforce` | Salesforce API client |
| `dotenv` | omgevingsvariabelen laden uit `.env` |
| `uuid` | unieke message IDs genereren |
