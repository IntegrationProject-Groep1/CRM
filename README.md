# CRM
Dit is de repository voor het team van CRM

## Wat is Salesforce CRM?
Salesforce is een cloud-gebaseerd CRM-systeem (Customer Relationship Management). Het wordt gebruikt door bedrijven en organisaties om klantgegevens bij te houden, relaties te beheren en communicatie op te volgen.

Salesforce kan:
Contactpersonen (personen) aanmaken en beheren
Bedrijven (accounts) bijhouden met hun contactgegevens
Inschrijvingen en activiteiten koppelen aan een persoon of bedrijf
Een REST API aanbieden zodat andere systemen automatisch gegevens kunnen aanmaken of opvragen

## Rol binnen het project
Salesforce is het geheugen van het hele platform. Alle gegevens van personen en bedrijven die binnenkomen via andere systemen worden automatisch opgeslagen en bijgehouden in Salesforce. Na het event kunnen deze gegevens gebruikt worden om zakelijke relaties verder op te zetten.
Concreet betekent dit: zodra iemand zich inschrijft, een betaling doet of consumpties bestelt, worden die gegevens automatisch gesynchroniseerd met Salesforce via RabbitMQ.

## Wat is RabbitMQ?
RabbitMQ is een open-source queueingsysteem dat de communicatie tussen alle systemen in dit project mogelijk maakt. In plaats van dat systemen rechtstreeks met elkaar communiceren, stuurt elk systeem berichten via RabbitMQ. Dit zorgt ervoor dat systemen onafhankelijk van elkaar werken. Als een systeem even offline gaat, blijft de rest gewoon functioneren. De berichten blijven in de wachtrij staan totdat het systeem terug online is.

## Met welke teams zijn wij gelinkt?
Ons systeem staat niet op zichzelf. Salesforce ontvangt en verstuurt berichten via RabbitMQ en is daardoor verbonden met verschillende andere teams.
Frontend (Drupal): nieuwe inschrijving wordt automatisch als contact opgeslagen in Salesforce
Kassa (Odoo POS): betalingen worden gelogd op het juiste contact
Facturatie (FOSSBilling): afstemming over klant- en bedrijfsgegevens
Mailing (SendGrid): wij leveren de contactlijsten aan voor mailingcampagnes
Controlroom: wij sturen elke seconde een heartbeat om onze online/offline status te rapporteren
Infra: beheert de servers, Docker containers en RabbitMQ-infrastructuur
Heartbeat (monitoring)
Elke seconde stuurt ons systeem automatisch een klein signaal naar de Controlroom om aan te geven dat het systeem operationeel is. Als dit signaal wegvalt, weet de Controlroom meteen dat ons systeem offline is en worden de admins onmiddellijk verwittigd. De rest van het platform blijft intussen gewoon werken.
