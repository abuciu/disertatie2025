-- DROP PROCEDURE dwh_foundation.load_fact_loans(date);

CREATE OR REPLACE PROCEDURE dwh_foundation.load_fact_loans(IN p_date date)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_record_count INTEGER := 0;
    v_error_message TEXT;
    v_process_name TEXT := 'fact_loans_daily_load';
    v_execution_date TIMESTAMP := CURRENT_TIMESTAMP;
BEGIN
    -- Create temporary staging table for daily loan data
    CREATE TABLE dwh_stage.stg_fact_loans_daily AS
    SELECT 
        l.loan_id,
        TO_CHAR(p_date, 'YYYYMMDD')::INTEGER AS date_id,
        TO_CHAR(a.opening_date, 'YYYYMMDD')::INTEGER AS initial_date_id,
        TO_CHAR(l.next_payment_date, 'YYYYMMDD')::INTEGER AS next_payment_date_id,
        l.loan_officer_id,
        l.account_id,
        a.branch_id,
        a.currency_id,
        l.customer_id,
        a.interest_type_id,
        l.Loan_Amount AS initial_loan_amount,
        l.Principal_Paid,
        l.Interest_Paid,
        l.Principal_Past_Due,
        l.Interest_Past_Due
    FROM coresys.loans l
    JOIN coresys.bank_accounts a ON l.account_id = a.account_id; 
    
    -- Get record count for logging
    SELECT COUNT(*) INTO v_record_count FROM dwh_stage.stg_fact_loans_daily;
    
    -- Start transaction for data load
    BEGIN
        -- Insert new loan records
        INSERT INTO dwh_foundation.fact_loans (
            Date_ID,
            Initial_Date_ID,
            Next_Payment_Date_ID,
            Loan_Officer_ID,
            Account_ID,
            Branch_ID,
            Currency_ID,
            Customer_ID,
            Interest_ID,
            Initial_Loan_Amount,
            Principal_Paid,
            Interest_Paid,
            Principal_Past_Due,
            Interest_Past_Due
        )
        SELECT 
            date_id,
            initial_date_id,
            next_payment_date_id,
            loan_officer_id,
            account_id,
            branch_id,
            currency_id,
            customer_id,
            interest_type_id,
            initial_loan_amount,
            Principal_Paid,
            Interest_Paid,
            Principal_Past_Due,
            Interest_Past_Due
        FROM dwh_stage.stg_fact_loans_daily;
        
        -- Log success
        INSERT INTO dwh_meta.etl_log (
            process_name, 
            execution_date, 
            records_processed, 
            status,
            details
        ) VALUES (
            v_process_name, 
            v_execution_date, 
            v_record_count, 
            'SUCCESS',
            'Loaded loan facts for ' || (p_date - INTERVAL '1 day')::DATE
        );
        
        -- Clean up
        DROP TABLE dwh_stage.stg_fact_loans_daily;
        
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
            v_execution_date, 
            v_record_count, 
            'FAILED', 
            v_error_message,
            'Failed loading loan facts for ' || (p_date - INTERVAL '1 day')::DATE
        );
        
        -- Re-raise error for job monitoring
        RAISE EXCEPTION 'Loan fact load failed: %', v_error_message;
    END;
end; $procedure$
;