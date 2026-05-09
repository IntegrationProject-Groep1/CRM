
'use strict';

const fs = require('fs');
const path = require('path');
const libxmljs = require('libxmljs2');

const XSD_DIR = path.join(__dirname, '..', 'xsd');
const schemaCache = new Map();

/**
 * Validates an XML string against a given XSD schema.
 * @param {string} xmlString - The XML content to validate.
 * @param {string} schemaName - The filename of the XSD (e.g., 'user_created.xsd').
 * @returns {object} - { valid: boolean, errors: string[] }
 */
function validateXml(xmlString, schemaName) {
  try {
    const xmlDoc = libxmljs.parseXml(xmlString);
    let xsdDoc = schemaCache.get(schemaName);

    if (!xsdDoc) {
      const xsdPath = path.join(XSD_DIR, schemaName);
      if (!fs.existsSync(xsdPath)) {
        return {
          valid: false,
          errors: [`Schema file not found: ${schemaName}`]
        };
      }
      const xsdSource = fs.readFileSync(xsdPath, 'utf8');
      xsdDoc = libxmljs.parseXml(xsdSource);
      schemaCache.set(schemaName, xsdDoc);
    }

    const isValid = xmlDoc.validate(xsdDoc);
    if (isValid) {
      return { valid: true, errors: [] };
    } else {
      return {
        valid: false,
        errors: xmlDoc.validationErrors.map(err => `Line ${err.line}: ${err.message.trim()}`)
      };
    }
  } catch (err) {
    return {
      valid: false,
      errors: [`XML Parse Error: ${err.message}`]
    };
  }
}

module.exports = {
  validateXml
};
