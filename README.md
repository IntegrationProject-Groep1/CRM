# 📘 Handleiding – CRM Integratieservice

## Wat doet dit project?

Dit is een Node.js integratie-microservice die fungeert als het centrale communicatiepunt van een CRM-systeem (Customer Relationship Management gebaseerd op Salesforce). De service ontvangt berichten van andere systemen (via een berichtenwachtrij), verwerkt die, en stuurt gegevens door naar Salesforce, MySQL en andere diensten.

---

## Architectuuroverzicht

```
Andere systemen (Kassa, IoT, enz.)
         │
         ▼
   RabbitMQ Queues
   ┌─────────────────────┐
   │  crm.incoming       │  ← berichten voor het CRM
   │  kassa.payments     │  ← betalings- en consumptieberichten van Kassa
   └─────────────────────┘
         │
         ▼
    receiver.js  ──► Salesforce (Member__c, Task, Consumption__c)
         │       ──► MySQL (crm_user_sync, payments, consumptions)
         │
         ▼  (via sender.js)
   ┌─────────────────────┐
   │  crm.to.facturatie  │  → Factuurverzoeken
   │  crm.to.mailing     │  → E-mailcampagnes
   │  kassa.incoming     │  → Klantregistraties, profielen, annuleringen
   └─────────────────────┘
```

---

## Bestandsstructuur

```
CRM/
├── src/
│   ├── receiver.js       ← Hoofdbestand – luistert en verwerkt berichten
│   ├── sender.js         ← Verstuurt XML-berichten naar andere queues
│   ├── sfConnection.js   ← Verbinding en authenticatie met Salesforce
│   ├── mysqlClient.js    ← Database-acties op MySQL
│   └── heartbeat.js      ← Stuurt elke seconde een statussignaal
├── db/
│   └── init.sql          ← Initialisatie van MySQL-tabellen
├── tests/
│   └── sender.test.js    ← Geautomatiseerde tests voor XML-opbouw
├── .env.example          ← Voorbeeld van vereiste omgevingsvariabelen
├── Dockerfile            ← Containerisatie (Node 20)
├── docker-compose.yml    ← Start de service + RabbitMQ + MySQL samen op
└── package.json          ← NPM-projectconfiguratie en scripts
```

---

## Uitleg per bestand

### `src/receiver.js` – Het hart van de service

Dit is het hoofdbestand dat automatisch opstart (`npm start`). Het:

- Start een health check HTTP-server op poort 3000 (`GET /` → `200 OK`)
- Verbindt met RabbitMQ (max. 5 pogingen, daarna crash)
- Luistert op twee queues: `crm.incoming` en `kassa.payments`
- Parset elk binnenkomend bericht als XML met `fast-xml-parser`
- Valideert het bericht (aanwezigheid van header-velden, versie 2.0, geldig type)
- Routeert het bericht op basis van `header.type` naar de juiste handler

**Ondersteunde berichttypen:**

| Type | Actie |
|---|---|
| `new_registration` | Klant aanmaken/bijwerken in Salesforce én MySQL, doorsturen naar Kassa |
| `payment_registered` | Taak aanmaken in Salesforce, betaling opslaan in MySQL |
| `badge_scanned` | Taak aanmaken in Salesforce, check-in tijd opslaan in MySQL |
| `session_update` | Taak aanmaken in Salesforce |
| `invoice_status` | Taak aanmaken in Salesforce |
| `mailing_status` | Taak aanmaken in Salesforce |
| `consumption_order` | Consumptierecords aanmaken in Salesforce en opslaan in MySQL |
| `badge_assigned` | Badge-ID bijwerken op het `Member__c` object in Salesforce |
| `refund_processed` | Taak aanmaken in Salesforce |
| `invoice_request` | Taak aanmaken in Salesforce + doorsturen naar `crm.to.facturatie` queue |

> **Foutafhandeling:** Ongeldige of niet-parseerbare berichten worden naar de `crm.dead-letter` queue gestuurd zodat ze niet verloren gaan.

---

### `src/sender.js` – Berichten versturen

De `CRMSender` klasse bouwt XML-berichten en verstuurt die naar specifieke RabbitMQ-queues. Elke methode werkt in twee stappen:

1. `build...Xml(data)` – bouwt het XML-document op met `xmlbuilder2`
2. `send...(data)` – verstuurt het naar de juiste queue

**Queues en methodes:**

| Methode | Queue | Doel |
|---|---|---|
| `sendInvoiceRequest()` | `crm.to.facturatie` | Factuurverzoek doorsturen |
| `sendMailingSend()` | `crm.to.mailing` | E-mailcampagne starten |
| `sendNewRegistrationToKassa()` | `kassa.incoming` | Nieuwe klant naar kassa sturen |
| `sendProfileUpdateToKassa()` | `kassa.incoming` | Profiel update naar kassa |
| `sendCancelRegistrationToKassa()` | `kassa.incoming` | Annulering naar kassa |

**XML-berichtstructuur:**

```xml
<message>
  <header>
    <message_id>...</message_id>  <!-- unieke UUID -->
    <version>2.0</version>
    <type>...</type>
    <timestamp>...</timestamp>
    <source>crm</source>
  </header>
  <body>...</body>
</message>
```

---

### `src/sfConnection.js` – Salesforce verbinding

De `SFConnection` klasse beheert de authenticatie met Salesforce. Er zijn twee methodes:

- **OAuth2 met refresh token** *(voorkeur)*: vraagt een nieuw access token aan via `SF_REFRESH_TOKEN`
- **Direct access token** *(fallback)*: gebruikt `SF_ACCESS_TOKEN` rechtstreeks

Als geen van beide werkt, draait de service in **DRY RUN modus** – berichten worden ontvangen en gelogd, maar er wordt niets naar Salesforce geschreven.

De methode `apiCall(fn)` handelt automatisch verlopen sessies af: als Salesforce een `INVALID_SESSION_ID` teruggeeft, wordt het token vernieuwd en de API-call herhaald.

---

### `src/mysqlClient.js` – MySQL database

De `MySQLService` klasse is een laag bovenop `mysql2/promise`. Ze beheert meerdere tabellen:

| Tabel | Methoden |
|---|---|
| `crm_user_sync` | `upsertPerson()`, `findPersonByExternalId()`, `findPersonByEmail()`, `findPersonByEmailForCheckIn()`, `updatePersonSalesforceId()`, `syncSalesforceStatus()` |
| `companies` | `upsertCompany()`, `updateCompanySalesforceId()` |
| `event_attendees` | `findEventAttendeeByPersonId()`, `updateEventAttendeeCheckIn()` |
| `payments` | `insertPayment()` |
| `consumptions` | `insertConsumption()` |

De service gebruikt de MySQL-variabelen uit `.env` (`MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD`) en maakt daarmee een connection pool aan. Als die configuratie ontbreekt, wordt MySQL uitgeschakeld en wordt er alleen gelogd.

---

### `src/heartbeat.js` – Statusbewaking

Een aparte service (`npm run heartbeat`) die elke 1 seconde een XML-bericht stuurt naar de `heartbeat` queue. Dit stelt andere systemen in staat te controleren of het CRM nog actief is.

Elke 10 seconden wordt ook een Salesforce health check uitgevoerd. De status is:

- `online` – alles werkt
- `degraded` – Salesforce niet bereikbaar
- `offline` – service is aan het afsluiten

---

## Omgevingsvariabelen (`.env`)

Kopieer `.env.example` naar `.env` en vul aan:

```env
RABBITMQ_HOST=integrationproject-2526s2-dag01.westeurope.cloudapp.azure.com
RABBITMQ_PORT=30000
RABBITMQ_USER=guest
RABBITMQ_PASS=guest
RABBITMQ_VHOST=/

SF_INSTANCE_URL=https://yourorg.my.salesforce.com
SF_CLIENT_ID=...
SF_CLIENT_SECRET=...
SF_REFRESH_TOKEN=...        # Beste authenticatiemethode
SF_ACCESS_TOKEN=...         # Fallback
SF_API_VERSION=v60.0
SF_CALLBACK_URL=https://oauth.pstmn.io/v1/callback

MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=crm_demo
MYSQL_USER=crm_user
MYSQL_PASSWORD=crm_pass

HEALTH_PORT=3000            # Optioneel (standaard 3000)
```

---

## Hoe opstarten?

**Met Docker (aanbevolen):**

```bash
cp .env.example .env
# Vul .env aan
docker compose up
```

Dit start RabbitMQ, MySQL en de CRM-service.

**Lokaal:**

```bash
npm install
cp .env.example .env
# Vul .env aan
npm start          # Start de receiver
npm run heartbeat  # Start heartbeat (apart terminal)
```

**Tests uitvoeren:**

```bash
npm test
```

**Linting:**

```bash
npm run lint
```

---

## Tests (`tests/sender.test.js`)

De tests controleren of de `CRMSender` correcte XML bouwt zonder dat RabbitMQ nodig is. Er zijn 4 testcases:

1. **Invoice XML structuur** – controleert header en klantgegevens
2. **Mailing XML structuur** – controleert onderwerp en ontvanger
3. **XML escaping** – verifieert dat speciale tekens (`<`, `>`, `&`, `'`) correct worden geëscaped (veiligheid)
4. **Kassa registratie XSD** – controleert dat het XML-formaat voldoet aan wat Kassa verwacht

---

## Gebruikte bibliotheken

| Bibliotheek | Doel |
|---|---|
| `amqplib` | RabbitMQ berichten ontvangen/versturen |
| `fast-xml-parser` | Inkomende XML berichten parsen |
| `xmlbuilder2` | Uitgaande XML berichten bouwen |
| `jsforce` | Salesforce API client |
| `mysql2` | MySQL database client |
| `dotenv` | Omgevingsvariabelen laden uit `.env` |
| `uuid` | Unieke message ID's genereren |
