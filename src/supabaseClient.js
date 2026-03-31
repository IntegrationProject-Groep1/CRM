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
    const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
    const anonKey = process.env.SUPABASE_ANON_KEY;
    const key = serviceRoleKey || anonKey;
    if (!url || !key) {
      console.log('[supabase] Missing SUPABASE_URL and/or Supabase key (SUPABASE_SERVICE_ROLE_KEY or SUPABASE_ANON_KEY) — Supabase disabled');
      return;
    }

    if (serviceRoleKey) {
      console.log('[supabase] Using SUPABASE_SERVICE_ROLE_KEY for server-side writes');
    } else {
      console.log('[supabase] Warning: using SUPABASE_ANON_KEY; RLS may block inserts/updates');
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

    if (!data.external_user_id) {
      console.log('[supabase] Error upserting person: missing external_user_id');
      return null;
    }

    const normalizedPaymentStatus = data.payment_status === 'paid' ? 'paid' : 'pending';
    const normalizedAmount = Number.isFinite(Number(data.amount)) ? Number(data.amount) : 0;

    const payload = {
      User_ID__c: data.external_user_id,
      User_Type__c: data.person_type || 'Particulier',
      Company_Name__c: data.company_name || null,
      BTW_Number__c: data.vat_number || null,
      Language__c: data.language || 'NL',
      Salutation__c: data.salutation || null,
      First_Name__c: data.first_name || '',
      Last_Name__c: data.last_name || '',
      Email__c: data.email || '',
      Birthdate__c: data.date_of_birth || '1970-01-01',
      Amount__c: normalizedAmount,
      Street__c: data.street || '',
      House_Number__c: data.house_number || '',
      Postal_Code__c: data.postal_code || '',
      City__c: data.city || '',
      Country_Code__c: data.country || 'BE',
      Payment_Status__c: normalizedPaymentStatus,
      Badge_ID__c: data.badge_id || null,
      sync_status: data.sync_status || null,
      sync_log: data.sync_log || null,
      auth_user_id: data.auth_user_id || null,
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
      .update({ Member__c: salesforceContactId })
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
      .update({ salesforce_account_id: salesforceAccountId })
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
      .update({ iot_last_scan: new Date().toISOString() })
      .eq('User_ID__c', personId);
    if (error) {
      console.log(`[supabase] Error updating iot_last_scan: ${error.message}`);
    }
  }

  async syncSalesforceStatus(personId, status) {
    if (!this.isConnected) return;
    const { error } = await this.client
      .from(this.userTable)
      .update({ sync_status: status })
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
