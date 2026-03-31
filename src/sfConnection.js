'use strict';

/**
 * Shared Salesforce Connection Module - JavaScript
 *
 * Centralizes all Salesforce authentication and API call logic.
 *
 * Dependencies:
 *   npm install jsforce dotenv
 */

require('dotenv').config();
const jsforce = require('jsforce');

class SFConnection {
  static TOKEN_URL = 'https://login.salesforce.com/services/oauth2/token';

  constructor() {
    this.instanceUrl = process.env.SF_INSTANCE_URL || '';
    this.clientId = process.env.SF_CLIENT_ID || '';
    this.clientSecret = process.env.SF_CLIENT_SECRET || '';
    this.refreshToken = process.env.SF_REFRESH_TOKEN || '';
    this.accessToken = process.env.SF_ACCESS_TOKEN || '';
    this.apiVersion = process.env.SF_API_VERSION || 'v60.0';
    this.connection = null;
    this.authMethod = 'none';
  }

  async init() {
    if (this.clientId && this.refreshToken) {
      try {
        const tokenData = await this._doTokenRefresh();
        if (tokenData) {
          this.connection = new jsforce.Connection({
            instanceUrl: tokenData.instance_url,
            accessToken: tokenData.access_token,
          });
          await this.connection.query('SELECT COUNT() FROM Member__c');
          this.instanceUrl = tokenData.instance_url;
          this.authMethod = 'oauth2';
          console.log('[SF Connection] Connected (OAuth2 refresh token)');
          console.log(`[SF Connection] Instance: ${this.instanceUrl}`);
          return this.connection;
        }
      } catch (err) {
        console.log(`[SF Connection] OAuth2 error: ${err}`);
      }
    }

    if (this.accessToken && this.instanceUrl) {
      try {
        this.connection = new jsforce.Connection({
          instanceUrl: this.instanceUrl,
          accessToken: this.accessToken,
        });
        await this.connection.query('SELECT COUNT() FROM Member__c');
        this.authMethod = 'direct';
        console.log('[SF Connection] Connected (direct access token)');
        console.log(`[SF Connection] Instance: ${this.instanceUrl}`);
        return this.connection;
      } catch (err) {
        console.log(`[SF Connection] Direct token failed: ${err}`);
      }
    }

    console.log('[SF Connection] No valid credentials. DRY RUN mode.');
    this.connection = null;
    this.authMethod = 'none';
    return null;
  }

  async _doTokenRefresh() {
    const params = new URLSearchParams({
      grant_type: 'refresh_token',
      client_id: this.clientId,
      client_secret: this.clientSecret,
      refresh_token: this.refreshToken,
    });
    const resp = await fetch(SFConnection.TOKEN_URL, {
      method: 'POST',
      body: params,
    });
    if (resp.ok) {
      return resp.json();
    }
    const text = await resp.text();
    console.log(`[SF Connection] Token refresh failed (${resp.status}): ${text}`);
    return null;
  }

  async refresh() {
    if (this.authMethod !== 'oauth2' || !this.connection) return false;
    try {
      const tokenData = await this._doTokenRefresh();
      if (tokenData) {
        this.connection = new jsforce.Connection({
          instanceUrl: tokenData.instance_url,
          accessToken: tokenData.access_token,
        });
        this.instanceUrl = tokenData.instance_url;
        console.log('[SF Connection] Token refreshed.');
        return true;
      }
      return false;
    } catch (err) {
      console.log(`[SF Connection] Refresh failed: ${err}`);
      return false;
    }
  }

  async apiCall(fn) {
    if (!this.connection) return null;
    try {
      return await fn(this.connection);
    } catch (err) {
      if (String(err).includes('INVALID_SESSION_ID')) {
        console.log('[SF Connection] Session expired, refreshing...');
        if (await this.refresh()) {
          return await fn(this.connection);
        }
      }
      throw err;
    }
  }

  async healthCheck() {
    if (!this.connection) return false;
    try {
      await this.apiCall(conn => conn.query('SELECT COUNT() FROM Member__c'));
      return true;
    } catch (err) {
      console.log(`[SF Connection] Health check failed: ${err}`);
      return false;
    }
  }

  get isConnected() {
    return this.connection !== null;
  }

  getInstanceUrl() {
    return this.instanceUrl;
  }
}

module.exports = SFConnection;
