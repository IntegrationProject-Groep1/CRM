import pika
import xml.etree.ElementTree as ET
import json
import os
import time
import signal
import sys
from datetime import datetime
from dotenv import load_dotenv
from sf_connection import SFConnection

load_dotenv()

QUEUE_NAME = 'crm'
DEAD_LETTER_QUEUE = 'crm.dead-letter'

MESSAGE_TYPES = {
    'NEW_REGISTRATION': 'new_registration',
    'PAYMENT_REGISTERED': 'payment_registered',
    'BADGE_SCANNED': 'badge_scanned',
    'SESSION_UPDATE': 'session_update',
    'INVOICE_STATUS': 'invoice_status',
    'MAILING_STATUS': 'mailing_status'
}


class ReceiverV2:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.sf = SFConnection()
        self.running = True

    def connect_rabbitmq(self):
        """Connect to RabbitMQ with retry logic"""
        max_retries = 5
        retry_count = 0

        while retry_count < max_retries and self.running:
            try:
                self.connection = pika.BlockingConnection(
                    pika.URLParameters(os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost/'))
                )
                self.channel = self.connection.channel()

                # Declare queues
                self.channel.queue_declare(queue=QUEUE_NAME, durable=True)
                self.channel.queue_declare(queue=DEAD_LETTER_QUEUE, durable=True)

                # Set prefetch
                self.channel.basic_qos(prefetch_count=1)

                # Start consuming
                self.channel.basic_consume(
                    queue=QUEUE_NAME,
                    on_message_callback=self.handle_message,
                    auto_ack=False
                )

                print(f'[receiver_v2] Connected to RabbitMQ, listening on queue: {QUEUE_NAME}')
                self.channel.start_consuming()
            except pika.exceptions.AMQPConnectionError as error:
                retry_count += 1
                print(f'[receiver_v2] RabbitMQ connection error: {str(error)}')
                if retry_count < max_retries:
                    time.sleep(5)
            except Exception as error:
                print(f'[receiver_v2] Unexpected error: {str(error)}')
                break

    def validate_xml_message(self, root):
        """Validate XML structure and required fields"""
        # Check for message root
        if root.tag != 'message':
            return False, 'Missing message root element'

        # Find header
        header = root.find('header')
        if header is None:
            return False, 'Missing header element'

        # Check required fields
        required_fields = ['message_id', 'version', 'type', 'timestamp', 'source']
        missing_fields = []
        for field in required_fields:
            if header.find(field) is None:
                missing_fields.append(field)

        if missing_fields:
            return False, f"Missing required header fields: {', '.join(missing_fields)}"

        # Validate version
        version = header.find('version')
        if version is None or version.text != '2.0':
            return False, f"Invalid version: expected 2.0, got {version.text if version is not None else 'None'}"

        # Validate message type
        msg_type = header.find('type')
        valid_types = list(MESSAGE_TYPES.values())
        if msg_type is None or msg_type.text not in valid_types:
            return False, f"Invalid message type: {msg_type.text if msg_type is not None else 'None'}"

        return True, None

    def handle_message(self, ch, method, properties, body):
        """Handle incoming message"""
        try:
            xml_content = body.decode('utf-8')
            print(f'[receiver_v2] Received message: {method.delivery_tag}')

            # Parse XML
            try:
                root = ET.fromstring(xml_content)
            except ET.ParseError as error:
                print(f'[receiver_v2] XML parse error: {str(error)}')
                self.send_to_dead_letter(ch, body, 'XML_PARSE_ERROR')
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            # Validate XML
            valid, error = self.validate_xml_message(root)
            if not valid:
                print(f'[receiver_v2] Validation error: {error}')
                self.send_to_dead_letter(ch, body, f'VALIDATION_ERROR: {error}')
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            # Extract header and body
            header = root.find('header')
            body_elem = root.find('body')

            message_id = header.find('message_id').text
            message_type = header.find('type').text

            print(f'[receiver_v2] Processing message type: {message_type}, ID: {message_id}')

            # Route message
            self.route_message(header, body_elem)

            # Acknowledge
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f'[receiver_v2] Message processed successfully: {message_id}')

        except Exception as error:
            print(f'[receiver_v2] Unexpected error: {str(error)}')
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def route_message(self, header, body):
        """Route message to appropriate handler"""
        msg_type = header.find('type').text

        if msg_type == MESSAGE_TYPES['NEW_REGISTRATION']:
            self.handle_new_registration(header, body)
        elif msg_type == MESSAGE_TYPES['PAYMENT_REGISTERED']:
            self.handle_payment_registered(header, body)
        elif msg_type == MESSAGE_TYPES['BADGE_SCANNED']:
            self.handle_badge_scanned(header, body)
        elif msg_type == MESSAGE_TYPES['SESSION_UPDATE']:
            self.handle_session_update(header, body)
        elif msg_type == MESSAGE_TYPES['INVOICE_STATUS']:
            self.handle_invoice_status(header, body)
        elif msg_type == MESSAGE_TYPES['MAILING_STATUS']:
            self.handle_mailing_status(header, body)
        else:
            print(f'[receiver_v2] Unknown message type: {msg_type}')

    def handle_new_registration(self, header, body):
        """Handle new_registration message"""
        try:
            customer = body.find('customer')
            if customer is None:
                print('[receiver_v2] Missing customer element in body')
                return

            contact_data = {
                'FirstName': self.get_element_text(customer, 'first_name'),
                'LastName': self.get_element_text(customer, 'last_name'),
                'Email': self.get_element_text(customer, 'email'),
                'Phone': self.get_element_text(customer, 'phone'),
                'Description': f"New registration from message: {header.find('message_id').text}"
            }

            if not self.sf.is_connected:
                print(f'[receiver_v2] DRY RUN: Would create Contact: {contact_data}')
                return

            # Create Contact
            contact_result = self.sf.api_call('POST', '/sobjects/Contact', contact_data)
            print(f"[receiver_v2] Created Contact: {contact_result.get('id')}")

            # Check if company linked
            is_company_linked = self.get_element_text(customer, 'is_company_linked')
            if is_company_linked in ('true', 'True', True):
                address = customer.find('address')
                account_data = {
                    'Name': self.get_element_text(customer, 'company_name'),
                    'BillingStreet': self.get_element_text(address, 'street') if address is not None else None,
                    'BillingCity': self.get_element_text(address, 'city') if address is not None else None,
                    'BillingPostalCode': self.get_element_text(address, 'postal_code') if address is not None else None,
                    'BillingCountry': self.get_element_text(address, 'country') if address is not None else None,
                    'Description': f"Company for customer: {self.get_element_text(customer, 'first_name')} {self.get_element_text(customer, 'last_name')}"
                }

                # Remove None values
                account_data = {k: v for k, v in account_data.items() if v is not None}

                account_result = self.sf.api_call('POST', '/sobjects/Account', account_data)
                print(f"[receiver_v2] Created Account: {account_result.get('id')}")

                # Link Contact to Account
                self.sf.api_call('PATCH', f"/sobjects/Contact/{contact_result.get('id')}", {
                    'AccountId': account_result.get('id')
                })
                print('[receiver_v2] Linked Contact to Account')

        except Exception as error:
            print(f'[receiver_v2] Error in handle_new_registration: {str(error)}')
            raise

    def handle_payment_registered(self, header, body):
        """Handle payment_registered message"""
        try:
            invoice = body.find('invoice')
            transaction = body.find('transaction')

            task_data = {
                'Subject': f"Payment registered for invoice: {self.get_element_text(invoice, 'id')}",
                'Description': f"Payment Method: {self.get_element_text(transaction, 'payment_method')}\nAmount Paid: {self.get_element_text(invoice, 'amount_paid')}\nDue Date: {self.get_element_text(invoice, 'due_date')}\nStatus: {self.get_element_text(invoice, 'status')}",
                'Status': 'Completed',
                'Type': 'Payment',
                'ActivityDate': datetime.now().date().isoformat()
            }

            if not self.sf.is_connected:
                print(f'[receiver_v2] DRY RUN: Would create Task: {task_data}')
                return

            # Try to find Contact by email
            email = self.get_element_text(body, 'email')
            if email:
                contacts = self.sf.api_call('GET', f"/query?q=SELECT%20Id%20FROM%20Contact%20WHERE%20Email='{email}'")
                if contacts.get('records') and len(contacts['records']) > 0:
                    task_data['WhoId'] = contacts['records'][0]['Id']

            task_result = self.sf.api_call('POST', '/sobjects/Task', task_data)
            print(f"[receiver_v2] Created Task for payment: {task_result.get('id')}")

        except Exception as error:
            print(f'[receiver_v2] Error in handle_payment_registered: {str(error)}')
            raise

    def handle_badge_scanned(self, header, body):
        """Handle badge_scanned message"""
        try:
            task_data = {
                'Subject': f"Badge scanned: {self.get_element_text(body, 'badge_id')}",
                'Description': f"Scan Type: {self.get_element_text(body, 'scan_type')}\nLocation: {self.get_element_text(body, 'location')}\nEmail: {self.get_element_text(body, 'email')}",
                'Status': 'Completed',
                'Type': 'Badge Scan',
                'ActivityDate': datetime.now().date().isoformat()
            }

            if not self.sf.is_connected:
                print(f'[receiver_v2] DRY RUN: Would create Task: {task_data}')
                return

            # Try to find Contact by email
            email = self.get_element_text(body, 'email')
            if email:
                contacts = self.sf.api_call('GET', f"/query?q=SELECT%20Id%20FROM%20Contact%20WHERE%20Email='{email}'")
                if contacts.get('records') and len(contacts['records']) > 0:
                    task_data['WhoId'] = contacts['records'][0]['Id']

            task_result = self.sf.api_call('POST', '/sobjects/Task', task_data)
            print(f"[receiver_v2] Created Task for badge scan: {task_result.get('id')}")

        except Exception as error:
            print(f'[receiver_v2] Error in handle_badge_scanned: {str(error)}')
            raise

    def handle_session_update(self, header, body):
        """Handle session_update message"""
        try:
            task_data = {
                'Subject': f"Session update: {self.get_element_text(body, 'session_name')}",
                'Description': f"Speaker: {self.get_element_text(body, 'speaker')}\nStart Time: {self.get_element_text(body, 'start_time')}\nEnd Time: {self.get_element_text(body, 'end_time')}\nStatus: {self.get_element_text(body, 'status')}",
                'Status': 'Completed',
                'Type': 'Session',
                'ActivityDate': datetime.now().date().isoformat()
            }

            if not self.sf.is_connected:
                print(f'[receiver_v2] DRY RUN: Would create Task: {task_data}')
                return

            task_result = self.sf.api_call('POST', '/sobjects/Task', task_data)
            print(f"[receiver_v2] Created Task for session update: {task_result.get('id')}")

        except Exception as error:
            print(f'[receiver_v2] Error in handle_session_update: {str(error)}')
            raise

    def handle_invoice_status(self, header, body):
        """Handle invoice_status message"""
        try:
            invoice = body.find('invoice')

            task_data = {
                'Subject': f"Invoice status update: {self.get_element_text(invoice, 'id')}",
                'Description': f"Status: {self.get_element_text(invoice, 'status')}\nAmount Paid: {self.get_element_text(invoice, 'amount_paid')}",
                'Status': 'Completed',
                'Type': 'Invoice',
                'ActivityDate': datetime.now().date().isoformat()
            }

            if not self.sf.is_connected:
                print(f'[receiver_v2] DRY RUN: Would create Task: {task_data}')
                return

            # Try to find Contact by email
            email = self.get_element_text(body, 'email')
            if email:
                contacts = self.sf.api_call('GET', f"/query?q=SELECT%20Id%20FROM%20Contact%20WHERE%20Email='{email}'")
                if contacts.get('records') and len(contacts['records']) > 0:
                    task_data['WhoId'] = contacts['records'][0]['Id']

            task_result = self.sf.api_call('POST', '/sobjects/Task', task_data)
            print(f"[receiver_v2] Created Task for invoice status: {task_result.get('id')}")

        except Exception as error:
            print(f'[receiver_v2] Error in handle_invoice_status: {str(error)}')
            raise

    def handle_mailing_status(self, header, body):
        """Handle mailing_status message"""
        try:
            task_data = {
                'Subject': f"Mailing status: {self.get_element_text(body, 'mailing_id')}",
                'Description': f"Status: {self.get_element_text(body, 'status')}\nDelivered: {self.get_element_text(body, 'delivered')}\nBounced: {self.get_element_text(body, 'bounced')}",
                'Status': 'Completed',
                'Type': 'Mailing',
                'ActivityDate': datetime.now().date().isoformat()
            }

            if not self.sf.is_connected:
                print(f'[receiver_v2] DRY RUN: Would create Task: {task_data}')
                return

            task_result = self.sf.api_call('POST', '/sobjects/Task', task_data)
            print(f"[receiver_v2] Created Task for mailing status: {task_result.get('id')}")

        except Exception as error:
            print(f'[receiver_v2] Error in handle_mailing_status: {str(error)}')
            raise

    def send_to_dead_letter(self, ch, original_body, reason):
        """Send message to dead letter queue"""
        try:
            dead_letter_message = {
                'original_message': original_body.decode('utf-8'),
                'error_reason': reason,
                'timestamp': datetime.now().isoformat(),
            }

            ch.basic_publish(
                exchange='',
                routing_key=DEAD_LETTER_QUEUE,
                body=json.dumps(dead_letter_message),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f'[receiver_v2] Message sent to dead letter queue: {reason}')
        except Exception as error:
            print(f'[receiver_v2] Error sending to dead letter queue: {str(error)}')

    @staticmethod
    def get_element_text(parent, tag_name):
        """Safely get element text"""
        if parent is None:
            return None
        element = parent.find(tag_name)
        return element.text if element is not None else None

    def shutdown(self, signum, frame):
        """Graceful shutdown"""
        print('[receiver_v2] Signal received, shutting down gracefully...')
        self.running = False
        if self.channel:
            self.channel.stop_consuming()
        if self.connection:
            self.connection.close()
        sys.exit(0)


def main():
    """Main entry point"""
    receiver = ReceiverV2()

    # Setup signal handlers
    signal.signal(signal.SIGINT, receiver.shutdown)
    signal.signal(signal.SIGTERM, receiver.shutdown)

    try:
        receiver.connect_rabbitmq()
    except Exception as error:
        print(f'[receiver_v2] Failed to start receiver: {str(error)}')
        sys.exit(1)


if __name__ == '__main__':
    main()
