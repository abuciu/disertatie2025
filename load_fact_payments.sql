-- DROP PROCEDURE dwh_foundation.load_fact_payments(date);

CREATE OR REPLACE PROCEDURE dwh_foundation.load_fact_payments(IN p_date date)
 LANGUAGE plpgsql
AS $procedure$
-- Create temporary logging variables
DECLARE
    v_record_count INTEGER;
    v_error_message TEXT;
BEGIN
    -- Create daily staging table
    CREATE TABLE dwh_stage.stg_fact_payments_daily AS
    SELECT 
        p.payment_id,
        TO_CHAR(p.transaction_date, 'YYYYMMDD')::INTEGER AS date_id,
        p.customer_id,
        p.account_id,
        p.branch_id,
        p.channel_id,
        pb.country_id,
        p.payment_type_id,
        p.counterparty_bank_id,
        p.counterparty_name,
        p.counterparty_account,
        p.amount,
        p.commission
    FROM coresys.payments p
    LEFT JOIN coresys.partner_banks pb ON p.counterparty_bank_id = pb.bank_id
    WHERE p.transaction_date = p_date
    AND p.status = 'COMPLETED'; -- Only load completed payments
    
    -- Get record count for logging
    SELECT COUNT(*) INTO v_record_count FROM dwh_stage.stg_fact_payments_daily;
    
    -- Insert new payments
    INSERT INTO dwh_foundation.fact_payments (
        Payment_ID, Date_ID, Customer_ID, Account_ID, Branch_ID, 
        Channel_ID, Country_ID, Pmnt_Type_ID, Counterprty_Bnk_ID,
        Counterprty_Name, Counterprty_Accnt, Amount, Commission
    )
    SELECT 
        payment_id, date_id, customer_id, account_id, branch_id,
        channel_id, country_id, payment_type_id, counterparty_bank_id,
        counterparty_name, counterparty_account, amount, commission
    FROM dwh_stage.stg_fact_payments_daily;
    
    -- Log success
    INSERT INTO dwh_meta.etl_log (
        process_name, execution_date, records_processed, status
    ) VALUES (
        'fact_payments_daily_load', 
        CURRENT_TIMESTAMP, 
        v_record_count, 
        'SUCCESS'
    );
    
    -- Clean up
    DROP TABLE dwh_stage.stg_fact_payments_daily;
    
EXCEPTION WHEN OTHERS THEN
    ROLLBACK;
    v_error_message := SQLERRM;
    
    -- Log error
    INSERT INTO dwh_meta.etl_log (
        process_name, execution_date, records_processed, status, error_message
    ) VALUES (
        'fact_payments_daily_load', 
        CURRENT_TIMESTAMP, 
        0, 
        'FAILED', 
        v_error_message
    );
    
    -- Re-raise error for job monitoring
    RAISE EXCEPTION 'Payment fact load failed: %', v_error_message;
end; $procedure$
;