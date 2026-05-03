# CRM Integratieservice

## Wat doet dit project?

Dit is een Node.js integratie-microservice die berichten ontvangt via RabbitMQ, ze verwerkt, en gegevens opslaat in Salesforce. Salesforce is de enige externe databron voor deze service. De service integreert ook met een Identity Service voor het beheer van Master UUIDs en verstuurt uitgaande berichten naar Kassa, Facturatie, Mailing en Planning.

## Architectuuroverzicht

```text
Andere systemen (Frontend, Kassa, IoT, enz.)
         |
         v
   RabbitMQ Queues (inkomend)
   +----------------------+
   |  crm.incoming        |  <- algemene CRM-berichten
   |  kassa.payments      |  <- betalings- en consumptieberichten van Kassa
   |  user.created        |  <- nieuwe gebruikers vanuit Frontend/Drupal
   |  user.registered     |  <- sessie-inschrijvingen vanuit Frontend/Drupal
   +----------------------+
         |
         v
    receiver.js
         |-- Identity Service RPC (identity.user.create.request) voor Master UUID-beheer
         |-- Salesforce (Member__c, Task, Consumption__c)
         |
         v  (via sender.js)
   +------------------------------------------+
   |  facturatie.incoming                     |  -> Factuurverzoeken & nieuwe registraties
   |  crm.to.mailing                          |  -> E-mailcampagnes
   |  kassa.incoming                          |  -> Klantregistraties, profielupdates, annuleringen
   |  frontend.user.unregistered (fanout)     |  -> crm.salesforce, planning.outlook, mailing.sendgrid
   |  calendar.exchange (topic)               |  -> registration.cancelled (naar Planning)
   +------------------------------------------+

   Ongeldige berichten -> crm.dead-letter
```

## Bestandsstructuur

```text
CRM/
|-- src/
|   |-- receiver.js       <- Hoofdbestand: ontvangt en verwerkt berichten
|   |-- sender.js         <- Verstuurt XML-berichten naar andere queues
|   |-- sfConnection.js   <- Verbinding en authenticatie met Salesforce
|   |-- heartbeat.js      <- Stuurt elke seconde een statussignaal
|   `-- amqpUrl.js        <- Bouwt de RabbitMQ-verbindingsopties op
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
- luistert op `crm.incoming`, `kassa.payments`, `user.created` en `user.registered`
- parseert XML-berichten
- valideert headers en types
- doet RPC-calls naar de Identity Service voor het ophalen/aanmaken van Master UUIDs
- routeert elk bericht naar de juiste handler

Ondersteunde berichttypen:

| Type | Queue | Actie |
|---|---|---|
| `user.created` | `user.created` | `Member__c` aanmaken of bijwerken in Salesforce via Master UUID |
| `user.registered` | `user.registered` | `Member__c` bijwerken + sessie-inschrijving als `Task` opslaan in Salesforce |
| `new_registration` | `crm.incoming` | klant upserten in Salesforce, doorsturen naar Kassa en Facturatie |
| `user.unregistered` | `crm.incoming` | fanout versturen naar `frontend.user.unregistered` exchange |
| `user.updated` | `crm.incoming` | `Member__c` bijwerken in Salesforce |
| `delete_user` | `crm.incoming` | `Member__c` markeren als verwijderd in Salesforce |
| `user_deleted` | `crm.incoming` | zelfde als `delete_user` (voor frontend-verwijderingen) |
| `payment_registered` | `crm.incoming` | `Task` aanmaken in Salesforce |
| `badge_scanned` | `crm.incoming` | `Task` aanmaken in Salesforce |
| `session_updated` | `crm.incoming` | `Task` aanmaken in Salesforce |
| `invoice_status` | `crm.incoming` | `Task` aanmaken in Salesforce |
| `send_invoice` | `crm.incoming` | laatste factuurvelden bijwerken op `Member__c` in Salesforce |
| `mailing_status` | `crm.incoming` | `Task` aanmaken in Salesforce |
| `consumption_order` | `kassa.payments` | `Consumption__c`-records aanmaken in Salesforce |
| `badge_assigned` | `kassa.payments` | badge-ID bijwerken op `Member__c` in Salesforce |
| `refund_processed` | `kassa.payments` | `Task` aanmaken in Salesforce |
| `invoice_request` | `kassa.payments` | `Task` aanmaken in Salesforce en doorsturen naar `facturatie.incoming` |
| `invoice_cancelled` | `kassa.payments` | geannuleerde factuur verwerken in Salesforce |

Ongeldige of niet-parseerbare berichten gaan naar `crm.dead-letter`.

### `src/sender.js`

Bouwt XML-berichten op en verstuurt die naar de juiste RabbitMQ-queue of exchange:

| Methode | Doel / Queue |
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

Beheert authenticatie en API-calls naar Salesforce. Ondersteunt OAuth2 met refresh token en directe access token fallback. Als geen geldige credentials aanwezig zijn, draait de service in DRY RUN mode (alle Salesforce-operaties worden dan gesimuleerd en gelogd, maar niet uitgevoerd).

### `src/heartbeat.js`

Stuurt elke seconde een heartbeat-bericht (XML) naar de `heartbeat` queue. Elke 10 seconden wordt ook een Salesforce health check uitgevoerd. De status is `online`, `degraded` of `offline`.

### `src/amqpUrl.js`

Bouwt de RabbitMQ-verbindingsopties op vanuit omgevingsvariabelen. Waarschuwt als TLS (`amqps`) niet is ingeschakeld.

### Identity Service integratie

Bij het verwerken van `new_registration`, `user.created` en `user.registered` doet de service een RPC-call naar de Identity Service via de `identity.user.create.request` queue. Dit garandeert dat elk lid in Salesforce hetzelfde Master UUID krijgt als de rest van de infrastructuur. De call gebruikt een tijdelijke exclusieve reply-queue en een 15-seconden timeout.

## Omgevingsvariabelen

Kopieer `.env.example` naar `.env` en vul aan:

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
| `xml2js` | XML-berichten parsen voor Identity Service RPC-antwoorden |
| `xmlbuilder2` | uitgaande XML-berichten bouwen |
| `jsforce` | Salesforce API client |
| `dotenv` | omgevingsvariabelen laden uit `.env` |
| `uuid` | unieke message IDs genereren |
