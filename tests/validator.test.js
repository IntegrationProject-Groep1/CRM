'use strict';

const { validateXml } = require('../src/validator');

describe('validateXml', () => {
  const validLogXml = `<?xml version="1.0" encoding="UTF-8"?>
<message>
  <header>
    <message_id>123e4567-e89b-12d3-a456-426614174000</message_id>
    <timestamp>2026-05-14T12:00:00Z</timestamp>
    <source>crm</source>
    <type>log</type>
    <version>2.0</version>
  </header>
  <body>
    <level>info</level>
    <action>system_error</action>
    <message>Health check passed</message>
  </body>
</message>`;

  test('accepts valid XML that matches its XSD', () => {
    expect(validateXml(validLogXml, 'log.xsd')).toEqual({ valid: true, errors: [] });
  });

  test('accepts v2.3 user_registered with optional price before payment_status', () => {
    const userRegisteredXml = `<?xml version="1.0" encoding="UTF-8"?>
<message>
  <header>
    <message_id>a1b2c3d4-e5f6-7890-abcd-ef1234567890</message_id>
    <timestamp>2026-05-20T10:45:00+02:00</timestamp>
    <source>frontend</source>
    <type>user_registered</type>
    <version>2.0</version>
    <correlation_id>c92df68d-6c4d-4c87-8e77-7c06b4d8476a</correlation_id>
  </header>
  <body>
    <customer>
      <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-123456789abc</identity_uuid>
      <email>sophie.janssens@example.com</email>
      <contact>
        <first_name>Sophie</first_name>
        <last_name>Janssens</last_name>
      </contact>
      <type>company</type>
      <company_name>Erasmushogeschool Brussel</company_name>
      <vat_number>BE0876543210</vat_number>
      <session_id>sess-2026-workshop-04</session_id>
    </customer>
    <session_title>Workshop Productie</session_title>
    <price currency="eur">40.00</price>
    <payment_status>pending</payment_status>
  </body>
</message>`;

    expect(validateXml(userRegisteredXml, 'user_registered.xsd')).toEqual({ valid: true, errors: [] });
  });

  test('does not resolve external entities during validation', () => {
    const xmlWithExternalEntity = validLogXml.replace(
      '<message>',
      '<!DOCTYPE message [<!ENTITY xxe SYSTEM "file:///etc/passwd">]><message>'
    ).replace('Health check passed', '&xxe;');

    const result = validateXml(xmlWithExternalEntity, 'log.xsd');

    expect(result.valid).toBe(false);
    expect(result.errors.join(' ')).not.toContain('root:');
  });
});
