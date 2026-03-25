import pika
import xml.etree.ElementTree as ET
from xml.dom import minidom
import uuid
from datetime import datetime
import os
from dotenv import load_dotenv
import json

load_dotenv()


class CRMSender:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost/')

    def init(self):
        """Initialize AMQP connection"""
        try:
            self.connection = pika.BlockingConnection(
                pika.URLParameters(self.rabbitmq_url)
            )
            self.channel = self.connection.channel()
            print('CRM Sender initialized')
        except Exception as error:
            print(f'Failed to initialize CRM Sender: {error}')
            raise

    @staticmethod
    def escape_xml(text):
        """Escape XML special characters"""
        if not isinstance(text, str):
            text = str(text)
        return (text
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;')
                .replace("'", '&apos;'))

    @staticmethod
    def get_timestamp():
        """Get current timestamp in ISO-8601 format"""
        return datetime.utcnow().isoformat() + 'Z'

    def build_invoice_request_xml(self, data):
        """Build invoice request XML following PM v3 naming standard"""
        message_id = f'inv-crm-{uuid.uuid4()}'
        timestamp = self.get_timestamp()

        root = ET.Element('message')

        # Header
        header = ET.SubElement(root, 'header')
        ET.SubElement(header, 'message_id').text = message_id
        ET.SubElement(header, 'version').text = '2.0'
        ET.SubElement(header, 'type').text = 'invoice_request'
        ET.SubElement(header, 'timestamp').text = timestamp
        ET.SubElement(header, 'source').text = 'crm'
        if 'correlation_id' in data:
            ET.SubElement(header, 'correlation_id').text = data['correlation_id']

        # Body
        body = ET.SubElement(root, 'body')

        # Customer
        customer = ET.SubElement(body, 'customer')
        ET.SubElement(customer, 'email').text = self.escape_xml(data['customer']['email'])
        ET.SubElement(customer, 'first_name').text = self.escape_xml(data['customer']['first_name'])
        ET.SubElement(customer, 'last_name').text = self.escape_xml(data['customer']['last_name'])
        if 'phone' in data['customer']:
            ET.SubElement(customer, 'phone').text = self.escape_xml(data['customer']['phone'])

        # Invoice
        invoice = ET.SubElement(body, 'invoice')
        ET.SubElement(invoice, 'description').text = self.escape_xml(data['invoice']['description'])
        amount_elem = ET.SubElement(invoice, 'amount')
        amount_elem.set('currency', data['invoice'].get('currency', 'eur'))
        amount_elem.text = self.escape_xml(str(data['invoice']['amount']))
        ET.SubElement(invoice, 'due_date').text = data['invoice']['due_date']
        if 'invoice_number' in data['invoice']:
            ET.SubElement(invoice, 'invoice_number').text = self.escape_xml(data['invoice']['invoice_number'])

        # Items
        items = ET.SubElement(body, 'items')
        for item in data['items']:
            item_elem = ET.SubElement(items, 'item')
            ET.SubElement(item_elem, 'description').text = self.escape_xml(item['description'])
            ET.SubElement(item_elem, 'quantity').text = str(item['quantity'])
            unit_price_elem = ET.SubElement(item_elem, 'unit_price')
            unit_price_elem.set('currency', item.get('currency', 'eur'))
            unit_price_elem.text = self.escape_xml(str(item['unit_price']))
            ET.SubElement(item_elem, 'vat_rate').text = str(item.get('vat_rate', 21))
            if 'sku' in item:
                ET.SubElement(item_elem, 'sku').text = self.escape_xml(item['sku'])

        # Pretty print and return
        xml_str = self._pretty_print_xml(root)
        return xml_str

    def build_mailing_send_xml(self, data):
        """Build mailing send XML following PM v3 naming standard"""
        message_id = f'mail-crm-{uuid.uuid4()}'
        timestamp = self.get_timestamp()

        root = ET.Element('message')

        # Header
        header = ET.SubElement(root, 'header')
        ET.SubElement(header, 'message_id').text = message_id
        ET.SubElement(header, 'version').text = '2.0'
        ET.SubElement(header, 'type').text = 'mailing_send'
        ET.SubElement(header, 'timestamp').text = timestamp
        ET.SubElement(header, 'source').text = 'crm'
        if 'correlation_id' in data:
            ET.SubElement(header, 'correlation_id').text = data['correlation_id']

        # Body
        body = ET.SubElement(root, 'body')

        # Mailing
        mailing = ET.SubElement(body, 'mailing')
        ET.SubElement(mailing, 'subject').text = self.escape_xml(data['mailing']['subject'])
        ET.SubElement(mailing, 'template_id').text = self.escape_xml(data['mailing']['template_id'])
        if 'from_address' in data['mailing']:
            ET.SubElement(mailing, 'from_address').text = self.escape_xml(data['mailing']['from_address'])
        if 'reply_to' in data['mailing']:
            ET.SubElement(mailing, 'reply_to').text = self.escape_xml(data['mailing']['reply_to'])

        # Recipients
        recipients = ET.SubElement(body, 'recipients')
        for recipient in data['recipients']:
            recipient_elem = ET.SubElement(recipients, 'recipient')
            ET.SubElement(recipient_elem, 'email').text = self.escape_xml(recipient['email'])
            ET.SubElement(recipient_elem, 'first_name').text = self.escape_xml(recipient['first_name'])
            ET.SubElement(recipient_elem, 'last_name').text = self.escape_xml(recipient['last_name'])
            if 'language' in recipient:
                ET.SubElement(recipient_elem, 'language').text = self.escape_xml(recipient['language'])

        # Pretty print and return
        xml_str = self._pretty_print_xml(root)
        return xml_str

    @staticmethod
    def _pretty_print_xml(elem):
        """Return a pretty-printed XML string"""
        rough_string = ET.tostring(elem, encoding='unicode')
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent='  ')

    def send_invoice_request(self, data):
        """Send invoice request message to facturatie queue"""
        if not self.channel:
            raise RuntimeError('CRM Sender not initialized. Call init() first.')

        try:
            xml_payload = self.build_invoice_request_xml(data)
            queue = 'facturatie'

            self.channel.queue_declare(queue=queue, durable=True)
            self.channel.basic_publish(
                exchange='',
                routing_key=queue,
                body=xml_payload,
                properties=pika.BasicProperties(
                    content_type='application/xml',
                    delivery_mode=2
                )
            )

            print(f'Invoice request sent to queue "{queue}"')
            return {'success': True, 'queue': queue, 'payload': xml_payload}
        except Exception as error:
            print(f'Failed to send invoice request: {error}')
            raise

    def send_mailing_send(self, data):
        """Send mailing send message to mailing queue"""
        if not self.channel:
            raise RuntimeError('CRM Sender not initialized. Call init() first.')

        try:
            xml_payload = self.build_mailing_send_xml(data)
            queue = 'mailing'

            self.channel.queue_declare(queue=queue, durable=True)
            self.channel.basic_publish(
                exchange='',
                routing_key=queue,
                body=xml_payload,
                properties=pika.BasicProperties(
                    content_type='application/xml',
                    delivery_mode=2
                )
            )

            print(f'Mailing send request sent to queue "{queue}"')
            return {'success': True, 'queue': queue, 'payload': xml_payload}
        except Exception as error:
            print(f'Failed to send mailing send request: {error}')
            raise

    def close(self):
        """Close AMQP connection"""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            print('CRM Sender connection closed')
        except Exception as error:
            print(f'Error closing connection: {error}')


# Standalone test mode
if __name__ == '__main__':
    sender = CRMSender()
    sender.init()

    # Test invoice request
    invoice_data = {
        'customer': {
            'email': 'john@example.com',
            'first_name': 'John',
            'last_name': 'Doe',
            'phone': '+31612345678'
        },
        'invoice': {
            'description': 'Event registration fee',
            'amount': 150.00,
            'currency': 'eur',
            'due_date': '2026-04-01',
            'invoice_number': 'INV-2026-001'
        },
        'items': [
            {
                'description': 'Conference ticket',
                'quantity': 1,
                'unit_price': 150.00,
                'currency': 'eur',
                'vat_rate': 21,
                'sku': 'CONF-2026'
            }
        ],
        'correlation_id': 'crm-inv-20260316-001'
    }

    print('\n=== Testing Invoice Request ===')
    invoice_result = sender.send_invoice_request(invoice_data)
    print('Result:', invoice_result['success'])
    print('XML:', invoice_result['payload'])

    # Test mailing send
    mailing_data = {
        'mailing': {
            'subject': 'Event Confirmation',
            'template_id': 'TPL-001',
            'from_address': 'noreply@crm.example.com',
            'reply_to': 'support@crm.example.com'
        },
        'recipients': [
            {
                'email': 'john@example.com',
                'first_name': 'John',
                'last_name': 'Doe',
                'language': 'en'
            },
            {
                'email': 'jane@example.com',
                'first_name': 'Jane',
                'last_name': 'Smith',
                'language': 'nl'
            }
        ],
        'correlation_id': 'crm-mail-20260316-001'
    }

    print('\n=== Testing Mailing Send ===')
    mailing_result = sender.send_mailing_send(mailing_data)
    print('Result:', mailing_result['success'])
    print('XML:', mailing_result['payload'])

    sender.close()
    print('\n=== Tests completed ===')
