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
