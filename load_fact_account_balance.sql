-- DROP PROCEDURE dwh_foundation.load_fact_account_balance(date);

CREATE OR REPLACE PROCEDURE dwh_foundation.load_fact_account_balance(IN p_date date)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_record_count INTEGER := 0;
    v_error_message TEXT;
    v_process_name TEXT := 'fact_account_balance_daily_load';

BEGIN
    
    CREATE TABLE dwh_stage.stg_account_balances AS
    SELECT 
        to_char(p_date, 'YYYYMMDD')::integer AS date_id,
        a.account_id,
        a.currency_id,
        a.customer_id,
        a.branch_id,
        a.current_balance,
        CASE
            WHEN a.account_type_id IN (SELECT account_type_id FROM coresys.account_types WHERE type_category IN ('DEPOSIT','LOAN')) THEN
                COALESCE(d.interest_accrued, l.Interest_Past_Due, 0)
            ELSE 0
        END AS interest_accrued,
        CASE 
            WHEN a.closing_date IS NOT NULL THEN 'CLOSED'
            WHEN a.current_balance < 0 AND a.account_type_id NOT IN (SELECT account_type_id FROM coresys.account_types WHERE type_category = 'LOAN') THEN 'OVERDRAWN'
            ELSE 'ACTIVE'
        END AS status
    FROM coresys.bank_accounts a
    LEFT JOIN coresys.loans l ON a.account_id = l.account_id
    LEFT JOIN coresys.deposits d ON a.account_id = d.account_id
    WHERE a.opening_date <= p_date
    AND (a.closing_date IS NULL OR a.closing_date >= p_date);
    
    -- Get record count for logging
    SELECT COUNT(*) INTO v_record_count FROM dwh_stage.stg_account_balances;
    
    -- Validate we have data before proceeding
    IF v_record_count = 0 THEN
        RAISE EXCEPTION 'No account balance data found for date: %', p_date;
    END IF;
    
    -- Start transaction for data load
    BEGIN
        -- Delete existing balances for this snapshot date
        DELETE FROM dwh_foundation.fact_account_balance
        WHERE date_id = to_char(p_date, 'YYYYMMDD')::integer;
        
        -- Insert new balance records
        INSERT INTO dwh_foundation.fact_account_balance (
            date_id,
            account_id,
            currency_id,
            customer_id,
            branch_id,
            current_balance,
            interest_accrued,
            status
        )
        SELECT 
            date_id,
            account_id,
            currency_id,
            customer_id,
            branch_id,
            current_balance,
            interest_accrued,
            status
        FROM dwh_stage.stg_account_balances;
        
        -- Log success
        INSERT INTO dwh_meta.etl_log (
            process_name, 
            execution_date, 
            records_processed, 
            status,
            details
        ) VALUES (
            v_process_name, 
            current_date, 
            v_record_count, 
            'SUCCESS',
            'Loaded account balances for ' || p_date || 
            ' | Accounts: ' || v_record_count
        );
        
        -- Clean up
        DROP TABLE dwh_stage.stg_account_balances;
        

    EXCEPTION WHEN OTHERS THEN
        ROLLBACK;
        v_error_message := SQLERRM;
        
        -- Log error with detailed context
        INSERT INTO dwh_meta.etl_log (
            process_name, 
            execution_date, 
            records_processed, 
            status, 
            error_message,
            details
        ) VALUES (
            v_process_name, 
            current_date, 
            v_record_count, 
            'FAILED', 
            v_error_message,
            'Failed loading account balances for ' || p_date
        );
        
        -- Re-raise error for job monitoring
        RAISE EXCEPTION 'Account balance load failed: %', v_error_message;
    END;
end; $procedure$
;