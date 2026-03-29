'use strict';

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

class SupabaseService {
  constructor() {
    this.client = null;
  }

  init() {
    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_ANON_KEY;
    if (!url || !key) {
      console.log('[supabase] SUPABASE_URL or SUPABASE_ANON_KEY not set — Supabase disabled');
      return;
    }
    this.client = createClient(url, key);
    console.log('[supabase] Client initialized');
  }

  get isConnected() {
    return this.client !== null;
  }

  // ── People ────────────────────────────────────────────────────────────────

  async upsertPerson(data) {
    if (!this.isConnected) return null;
    const { data: result, error } = await this.client
      .from('people')
      .upsert(data, { onConflict: 'external_user_id' })
      .select('id')
      .single();
    if (error) {
      console.log(`[supabase] Error upserting person: ${error.message}`);
      return null;
    }
    return result.id;
  }

  async findPersonByExternalId(externalUserId) {
    if (!this.isConnected) return null;
    const { data, error } = await this.client
      .from('people')
      .select('id')
      .eq('external_user_id', externalUserId)
      .maybeSingle();
    if (error) {
      console.log(`[supabase] Error finding person: ${error.message}`);
      return null;
    }
    return data ? data.id : null;
  }

  async updatePersonSalesforceId(externalUserId, salesforceContactId) {
    if (!this.isConnected) return;
    const { error } = await this.client
      .from('people')
      .update({ salesforce_contact_id: salesforceContactId, updated_at: new Date().toISOString() })
      .eq('external_user_id', externalUserId);
    if (error) {
      console.log(`[supabase] Error updating salesforce_contact_id: ${error.message}`);
    }
  }

  // ── Companies ─────────────────────────────────────────────────────────────

  async upsertCompany(data) {
    if (!this.isConnected) return null;
    const { data: result, error } = await this.client
      .from('companies')
      .upsert(data, { onConflict: 'vat_number' })
      .select('id')
      .single();
    if (error) {
      console.log(`[supabase] Error upserting company: ${error.message}`);
      return null;
    }
    return result.id;
  }

  async updateCompanySalesforceId(vatNumber, salesforceAccountId) {
    if (!this.isConnected) return;
    const { error } = await this.client
      .from('companies')
      .update({ salesforce_account_id: salesforceAccountId, updated_at: new Date().toISOString() })
      .eq('vat_number', vatNumber);
    if (error) {
      console.log(`[supabase] Error updating salesforce_account_id: ${error.message}`);
    }
  }

  // ── Event Attendees ───────────────────────────────────────────────────────

  async findEventAttendeeByPersonId(personId) {
    if (!this.isConnected) return null;
    const { data, error } = await this.client
      .from('event_attendees')
      .select('id')
      .eq('person_id', personId)
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    if (error) {
      console.log(`[supabase] Error finding event_attendee: ${error.message}`);
      return null;
    }
    return data ? data.id : null;
  }

  async updateEventAttendeeCheckIn(personId) {
    if (!this.isConnected) return;
    const { error } = await this.client
      .from('event_attendees')
      .update({ check_in_at: new Date().toISOString(), updated_at: new Date().toISOString() })
      .eq('person_id', personId)
      .is('check_in_at', null);
    if (error) {
      console.log(`[supabase] Error updating check_in_at: ${error.message}`);
    }
  }

  async syncSalesforceStatus(personId, status) {
    if (!this.isConnected) return;
    const { error } = await this.client
      .from('event_attendees')
      .update({ salesforce_sync_status: status, updated_at: new Date().toISOString() })
      .eq('person_id', personId);
    if (error) {
      console.log(`[supabase] Error updating salesforce_sync_status: ${error.message}`);
    }
  }

  // ── Payments ──────────────────────────────────────────────────────────────

  async insertPayment(data) {
    if (!this.isConnected) return null;
    const { data: result, error } = await this.client
      .from('payments')
      .insert(data)
      .select('id')
      .single();
    if (error) {
      console.log(`[supabase] Error inserting payment: ${error.message}`);
      return null;
    }
    return result.id;
  }

  // ── Consumptions ──────────────────────────────────────────────────────────

  async insertConsumption(data) {
    if (!this.isConnected) return null;
    const { data: result, error } = await this.client
      .from('consumptions')
      .insert(data)
      .select('id')
      .single();
    if (error) {
      console.log(`[supabase] Error inserting consumption: ${error.message}`);
      return null;
    }
    return result.id;
  }
}

module.exports = SupabaseService;
