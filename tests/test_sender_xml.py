"""Tests for CRM sender XML building (no RabbitMQ required)."""
import sys
import os
import xml.etree.ElementTree as ET

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from sender import CRMSender


def make_sender():
    return CRMSender()


def test_invoice_xml_structure():
    sender = make_sender()
    data = {
        'customer': {'email': 'test@example.com', 'first_name': 'Jan', 'last_name': 'Janssen'},
        'invoice': {'description': 'Test invoice', 'amount': 100.0, 'due_date': '2026-04-01'},
        'items': [{'description': 'Item 1', 'quantity': 1, 'unit_price': 100.0}],
    }
    xml = sender.build_invoice_request_xml(data)
    root = ET.fromstring(xml.split('?>', 1)[-1].strip())

    assert root.tag == 'message'
    assert root.find('header/type').text == 'invoice_request'
    assert root.find('header/source').text == 'crm'
    assert root.find('header/version').text == '2.0'
    assert root.find('body/customer/email').text == 'test@example.com'
    assert root.find('body/invoice/description').text == 'Test invoice'
    print("PASS: test_invoice_xml_structure")


def test_mailing_xml_structure():
    sender = make_sender()
    data = {
        'mailing': {'subject': 'Hello', 'template_id': 'TPL-001'},
        'recipients': [{'email': 'r@example.com', 'first_name': 'Jan', 'last_name': 'Doe'}],
    }
    xml = sender.build_mailing_send_xml(data)
    root = ET.fromstring(xml.split('?>', 1)[-1].strip())

    assert root.tag == 'message'
    assert root.find('header/type').text == 'mailing_send'
    assert root.find('body/mailing/subject').text == 'Hello'
    assert root.find('body/recipients/recipient/email').text == 'r@example.com'
    print("PASS: test_mailing_xml_structure")


def test_xml_escaping():
    sender = make_sender()
    data = {
        'customer': {'email': 'test@example.com', 'first_name': 'O\'Brien', 'last_name': 'Test & Co'},
        'invoice': {'description': '<script>', 'amount': 50.0, 'due_date': '2026-04-01'},
        'items': [],
    }
    xml = sender.build_invoice_request_xml(data)
    assert '<script>' not in xml or '&lt;script&gt;' in xml
    print("PASS: test_xml_escaping")


if __name__ == '__main__':
    test_invoice_xml_structure()
    test_mailing_xml_structure()
    test_xml_escaping()
    print("\nAll tests passed.")
