'use strict';

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

class SupabaseService {
  constructor() {
    this.client = null;
    this.userTable = process.env.SUPABASE_USER_TABLE || 'crm_user_sync';
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

    const payload = {
      User_ID__c: data.external_user_id,
      First_Name__c: data.first_name,
      Last_Name__c: data.last_name,
      Email__c: data.email,
      Birthdate__c: data.date_of_birth || null,
      Street__c: data.street || null,
      House_Number__c: data.house_number || null,
      Postal_Code__c: data.postal_code || null,
      City__c: data.city || null,
      Country_Code__c: data.country || null,
      User_Type__c: data.person_type || null,
      updated_at: data.updated_at || new Date().toISOString(),
    };

    const cleanPayload = Object.fromEntries(
      Object.entries(payload).filter(([, value]) => value !== undefined)
    );

    const { data: result, error } = await this.client
      .from(this.userTable)
      .upsert(cleanPayload, { onConflict: 'User_ID__c' })
      .select('User_ID__c')
      .single();
    if (error) {
      console.log(`[supabase] Error upserting person: ${error.message}`);
      return null;
    }
    return result.User_ID__c;
  }

  async findPersonByExternalId(externalUserId) {
    if (!this.isConnected) return null;
    const { data, error } = await this.client
      .from(this.userTable)
      .select('User_ID__c')
      .eq('User_ID__c', externalUserId)
      .maybeSingle();
    if (error) {
      console.log(`[supabase] Error finding person: ${error.message}`);
      return null;
    }
    return data ? data.User_ID__c : null;
  }

  async findPersonByEmail(email) {
    if (!this.isConnected) return null;
    const { data, error } = await this.client
      .from(this.userTable)
      .select('User_ID__c')
      .eq('Email__c', email)
      .maybeSingle();
    if (error) {
      console.log(`[supabase] Error finding person by email: ${error.message}`);
      return null;
    }
    return data ? data.User_ID__c : null;
  }

  async findPersonByEmailForCheckIn(email) {
    if (!this.isConnected) {
      return { personId: null, error: null };
    }

    const { data, error } = await this.client
      .from(this.userTable)
      .select('User_ID__c')
      .eq('Email__c', email)
      .maybeSingle();

    if (error) {
      return { personId: null, error };
    }

    return { personId: data ? data.User_ID__c : null, error: null };
  }

  async updatePersonSalesforceId(externalUserId, salesforceContactId) {
    if (!this.isConnected) return;
    const { error } = await this.client
      .from(this.userTable)
      .update({ Member__c: salesforceContactId, updated_at: new Date().toISOString() })
      .eq('User_ID__c', externalUserId);
    if (error) {
      console.log(`[supabase] Error updating Member__c: ${error.message}`);
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
      .from(this.userTable)
      .update({ iot_last_scan: new Date().toISOString(), updated_at: new Date().toISOString() })
      .eq('User_ID__c', personId);
    if (error) {
      console.log(`[supabase] Error updating iot_last_scan: ${error.message}`);
    }
  }

  async syncSalesforceStatus(personId, status) {
    if (!this.isConnected) return;
    const { error } = await this.client
      .from(this.userTable)
      .update({ sync_status: status, updated_at: new Date().toISOString() })
      .eq('User_ID__c', personId);
    if (error) {
      console.log(`[supabase] Error updating sync_status: ${error.message}`);
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
