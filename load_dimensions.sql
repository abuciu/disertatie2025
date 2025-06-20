-- DROP PROCEDURE dwh_foundation.load_dimensions();

CREATE OR REPLACE PROCEDURE dwh_foundation.load_dimensions()
 LANGUAGE plpgsql
AS $procedure$
begin

	INSERT INTO dwh_meta.etl_log (
            process_name, 
            execution_date, 
            records_processed, 
            status, 
            error_message,
            details,
            created_at
        ) VALUES (
            'ETL LOAD', 
            CURRENT_DATE, 
            0, 
            'START', 
            'START',
            'Started load for ' || '2025-01-01'::DATE,
            current_timestamp
        );

	
truncate table dwh_stage.stg_address;
truncate table dwh_stage.stg_bank;
truncate table dwh_stage.stg_branch;
truncate table dwh_stage.stg_country;
truncate table dwh_stage.stg_customer_type;
truncate table dwh_stage.stg_date;
truncate table dwh_stage.stg_departments;
truncate table dwh_stage.stg_employees;
truncate table dwh_stage.stg_interest_type;
truncate table dwh_stage.stg_pmnt_channel;
truncate table dwh_stage.stg_pmnt_type;
truncate table dwh_stage.stg_risk_category;
truncate table dwh_stage.stg_account;
truncate table dwh_stage.stg_currency;
truncate table dwh_stage.stg_customer;

/*
 * create table dwh_foundation.dim_date
as select * from dwh_stage.stg_date
where 1 = 2;
 * 
 */

-- 1. DIM_DATE

--CREATE TABLE dwh_stage.stg_date AS
insert into dwh_stage.stg_date
SELECT 
    TO_CHAR(datum, 'YYYYMMDD')::INTEGER AS Date_ID,
    datum AS Date,
    EXTRACT(YEAR FROM datum) AS Year,
    EXTRACT(MONTH FROM datum) AS MonNr,
    TO_CHAR(datum, 'Mon') AS MonNmShort,
    TO_CHAR(datum, 'Month') AS MonNmLng,
    EXTRACT(DOW FROM datum) + 1 AS DayOfWkNr,
    TO_CHAR(datum, 'Dy') AS DayOfWkShrt,
    TO_CHAR(datum, 'Day') AS DayOfWkLng,
    EXTRACT(QUARTER FROM datum) AS QuarterNr,
    'Q' || EXTRACT(QUARTER FROM datum) AS QuarterTxt,
    CASE WHEN datum = (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE THEN 1 ELSE 0 END AS IsEOM
FROM (
    SELECT generate_series(
        '2020-01-01'::DATE,
        '2030-12-31'::DATE,
        INTERVAL '1 day'
    )::DATE AS datum
) dates;



-- Load to foundation (no SCD needed for date dimension)
/*INSERT INTO dwh_foundation.dim_date
SELECT * FROM dwh_stage.stg_date;*/

-- 2. DIM_ADDRESS



-- Stage addresses from all sources
--CREATE TABLE dwh_stage.stg_address AS
insert into dwh_stage.stg_address
SELECT 
    source_id,
    source_type,
    Country_ID,
    Region,
    City,
    Street,
    Street_nr,
    COALESCE(Apt, 0) AS Apt,
    '2025-01-01'::DATE AS Valid_From,
    '9999-12-31'::DATE AS Valid_Until
FROM (
    -- Customer addresses
    SELECT 
        customer_id AS source_id,
        'CUST' AS source_type,
        Country_ID,
        Region,
        City,
        Street,
        Street_nr,
        Building,
        Apt,
        Postal_Code,
        last_update
    FROM coresys.crm_customers
    
    UNION ALL
    
    -- Branch addresses
    SELECT 
        branch_id AS source_id,
        'BRANCH' AS source_type,
        Country_ID,
        Region,
        City,
        Street,
        Street_nr,
        Building,
        Apt,
        Postal_Code,
        last_update
    FROM coresys.branches
    
    UNION ALL
    
    -- Employee addresses
    SELECT 
        employee_id AS source_id,
        'EMP' AS source_type,
        Country_ID,
        Region,
        City,
        Street,
        Street_nr,
        Building,
        Apt,
        Postal_Code,
        last_update
    FROM coresys.employees
) combined_addresses;




UPDATE dwh_foundation.dim_address target
SET Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
FROM dwh_stage.stg_address source
WHERE target.source_id = source.source_id
and target.source_type = source.source_type
AND target.Valid_Until = '9999-12-31'::DATE
AND (
    target.Country_ID IS DISTINCT FROM source.Country_ID OR
    target.Region IS DISTINCT FROM source.Region OR
    target.City IS DISTINCT FROM source.City OR
    target.Street IS DISTINCT FROM source.Street OR
    target.Street_nr IS DISTINCT FROM source.Street_nr OR
    target.Apt IS DISTINCT FROM source.Apt
);

-- Then insert new versions of changed records and brand new records
INSERT INTO dwh_foundation.dim_address (
    Source_ID, Source_Type, Country_ID, Region, City, Street, Street_nr, Apt, Valid_From, Valid_Until
)
SELECT 
    source.source_ID,
    source.source_type,
    source.Country_ID,
    source.Region,
    source.City,
    source.Street,
    source.Street_nr,
    source.Apt,
    '2025-01-01'::DATE,
    source.Valid_Until
FROM dwh_stage.stg_address source
LEFT JOIN dwh_foundation.dim_address target 
    ON source.source_ID = target.source_ID
    and source.source_type = target.source_type
    AND target.Valid_Until = '9999-12-31'::DATE
WHERE (target.source_ID IS null and target.source_type is null) OR (
    target.Country_ID IS DISTINCT FROM source.Country_ID OR
    target.Region IS DISTINCT FROM source.Region OR
    target.City IS DISTINCT FROM source.City OR
    target.Street IS DISTINCT FROM source.Street OR
    target.Street_nr IS DISTINCT FROM source.Street_nr OR
    target.Apt IS DISTINCT FROM source.Apt
);

-- Insert historical records for changed addresses
/*INSERT INTO dwh_foundation.dim_address
SELECT 
    s.source_ID,
    s.source_type,
    s.Country_ID,
    s.Region,
    s.City,
    s.Street,
    s.Street_nr,
    s.Apt,
    '2025-01-01'::DATE,
    s.Valid_Until
FROM dwh_stage.stg_address s
WHERE EXISTS (
    SELECT 1 FROM dwh_foundation.dim_address t
    WHERE t.source_ID = s.source_ID
    and t.source_type = s.source_type
    AND t.Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
);*/

-- 3. DIM_COUNTRY


--CREATE TABLE dwh_stage.stg_country AS
insert into dwh_stage.stg_country
SELECT 
    country_id AS Country_ID,
    country_name AS Country_Name,
    '2025-01-01'::DATE AS Valid_From,
    '9999-12-31'::DATE AS Valid_Until
FROM coresys.reference_countries;

-- Merge into foundation
-- Expire changed records
UPDATE dwh_foundation.dim_country target
SET Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
FROM dwh_stage.stg_country source
WHERE target.Country_ID = source.Country_ID 
AND target.Valid_Until = '9999-12-31'::DATE
AND target.Country_Name IS DISTINCT FROM source.Country_Name;

-- Insert new/changed records
INSERT INTO dwh_foundation.dim_country (
    Country_ID, Country_Name, Valid_From, Valid_Until
)
SELECT 
    source.Country_ID,
    source.Country_Name,
    '2025-01-01'::DATE,
    source.Valid_Until
FROM dwh_stage.stg_country source
LEFT JOIN dwh_foundation.dim_country target 
    ON source.Country_ID = target.Country_ID 
    AND target.Valid_Until = '9999-12-31'::DATE
WHERE target.Country_ID IS NULL OR 
      target.Country_Name IS DISTINCT FROM source.Country_Name;

-- Insert historical records for changed countries
/*INSERT INTO dwh_foundation.dim_country
SELECT 
    s.Country_ID,
    s.Country_Name,
    '2025-01-01'::DATE,
    s.Valid_Until
FROM dwh_stage.stg_country s
WHERE EXISTS (
    SELECT 1 FROM dwh_foundation.dim_country t
    WHERE t.Country_ID = s.Country_ID
    AND t.Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
);*/

-- 4. DIM_CUSTOMER_TYPE


--CREATE TABLE dwh_stage.stg_customer_type AS
insert into dwh_stage.stg_customer_type
SELECT 
    ROW_NUMBER() OVER (ORDER BY customer_type) AS Customer_Type_ID,
    INITCAP(customer_type) AS Customer_Type_Name,
    CASE customer_type
        WHEN 'INDIVIDUAL' THEN 'Individual customer'
        WHEN 'SME' THEN 'Small and medium enterprise'
        WHEN 'CORPORATE' THEN 'Large corporate entity'
        ELSE 'Other customer type'
    END AS Customer_Type_Description,
    '2025-01-01'::DATE AS Valid_From,
    '9999-12-31'::DATE AS Valid_Until
FROM (
    SELECT DISTINCT customer_type 
    FROM coresys.crm_customers
) ct;

-- Expire changed records
UPDATE dwh_foundation.dim_customer_type target
SET Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
FROM dwh_stage.stg_customer_type source
WHERE target.Customer_Type_ID = source.Customer_Type_ID 
AND target.Valid_Until = '9999-12-31'::DATE
AND (
    target.Customer_Type_Name IS DISTINCT FROM source.Customer_Type_Name OR
    target.Customer_Type_Description IS DISTINCT FROM source.Customer_Type_Description
);

-- Insert new/changed records
INSERT INTO dwh_foundation.dim_customer_type (
    Customer_Type_ID, Customer_Type_Name, Customer_Type_Description, Valid_From, Valid_Until
)
SELECT 
    source.Customer_Type_ID,
    source.Customer_Type_Name,
    source.Customer_Type_Description,
    '2025-01-01'::DATE,
    source.Valid_Until
FROM dwh_stage.stg_customer_type source
LEFT JOIN dwh_foundation.dim_customer_type target 
    ON source.Customer_Type_ID = target.Customer_Type_ID 
    AND target.Valid_Until = '9999-12-31'::DATE
WHERE target.Customer_Type_ID IS NULL OR (
    target.Customer_Type_Name IS DISTINCT FROM source.Customer_Type_Name OR
    target.Customer_Type_Description IS DISTINCT FROM source.Customer_Type_Description
);

-- 5. DIM_RISK_CATEGORY

--drop table  dwh_stage.stg_risk_category;


--CREATE TABLE dwh_stage.stg_risk_category AS
insert into dwh_stage.stg_risk_category
SELECT 
    ROW_NUMBER() OVER (ORDER BY risk_category) AS Risk_Category_ID,
    INITCAP(risk_category) AS Risk_Category_Name,
    CASE risk_category
        WHEN 'LOW' THEN 'Low credit risk'
        WHEN 'MEDIUM' THEN 'Medium credit risk'
        WHEN 'HIGH' THEN 'High credit risk'
        WHEN 'VERYHIGH' THEN 'Very high credit risk'
        ELSE 'Undefined risk'
    END AS Risk_Category_Description,
    '2025-01-01'::DATE AS Valid_From,
    '9999-12-31'::DATE AS Valid_Until
FROM (
    SELECT DISTINCT risk_category 
    FROM coresys.crm_customers
) rc;

UPDATE dwh_foundation.dim_risk_category target
SET Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
FROM dwh_stage.stg_risk_category source
WHERE target.Risk_Category_ID = source.Risk_Category_ID 
AND target.Valid_Until = '9999-12-31'::DATE
AND (
    target.Risk_Category_Name IS DISTINCT FROM source.Risk_Category_Name OR
    target.Risk_Category_Description IS DISTINCT FROM source.Risk_Category_Description
);

-- Insert new/changed records
INSERT INTO dwh_foundation.dim_risk_category (
    Risk_Category_ID, Risk_Category_Name, Risk_Category_Description, Valid_From, Valid_Until
)
SELECT 
    source.Risk_Category_ID,
    source.Risk_Category_Name,
    source.Risk_Category_Description,
    '2025-01-01'::DATE,
    source.Valid_Until
FROM dwh_stage.stg_risk_category source
LEFT JOIN dwh_foundation.dim_risk_category target 
    ON source.Risk_Category_ID = target.Risk_Category_ID 
    AND target.Valid_Until = '9999-12-31'::DATE
WHERE target.Risk_Category_ID IS NULL OR (
    target.Risk_Category_Name IS DISTINCT FROM source.Risk_Category_Name OR
    target.Risk_Category_Description IS DISTINCT FROM source.Risk_Category_Description
);

-- 6. DIM_EMPLOYEES

--CREATE TABLE dwh_stage.stg_employees AS
insert into dwh_stage.stg_employees
SELECT 
    e.employee_id AS Employee_ID,
    e.first_name AS Employee_Name,
    e.last_name AS Employee_Surname,
    e.phone_number AS Employee_Phone,
    e.email AS Employee_Email,
    e.department_id AS Employee_Dep_ID,
    (SELECT a.source_ID FROM dwh_stage.stg_address a 
     WHERE a.source_id = e.employee_id AND a.source_type = 'EMP') AS Employee_Address_ID,
    '2025-01-01'::DATE AS Valid_From,
    CASE 
        WHEN e.termination_date IS NOT NULL THEN e.termination_date 
        ELSE '9999-12-31'::DATE 
    END AS Valid_Until
FROM coresys.employees e;



UPDATE dwh_foundation.dim_employees target
SET Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
FROM dwh_stage.stg_employees source
WHERE target.Employee_ID = source.Employee_ID 
AND target.Valid_Until = '9999-12-31'::DATE
AND (
    target.Employee_Name IS DISTINCT FROM source.Employee_Name OR
    target.Employee_Surname IS DISTINCT FROM source.Employee_Surname OR
    target.Employee_Phone IS DISTINCT FROM source.Employee_Phone OR
    target.Employee_Email IS DISTINCT FROM source.Employee_Email OR
    target.Employee_Dep_ID IS DISTINCT FROM source.Employee_Dep_ID OR
    target.Employee_Address_ID IS DISTINCT FROM source.Employee_Address_ID
);

INSERT INTO dwh_foundation.dim_employees (
    Employee_ID, Employee_Name, Employee_Surname, Employee_Phone, Employee_Email,
    Employee_Dep_ID, Employee_Address_ID, Valid_From, Valid_Until
)
SELECT 
    source.Employee_ID,
    source.Employee_Name,
    source.Employee_Surname,
    source.Employee_Phone,
    source.Employee_Email,
    source.Employee_Dep_ID,
    source.Employee_Address_ID,
    '2025-01-01'::DATE,
    source.Valid_Until
FROM dwh_stage.stg_employees source
LEFT JOIN dwh_foundation.dim_employees target 
    ON source.Employee_ID = target.Employee_ID 
    AND target.Valid_Until = '9999-12-31'::DATE
WHERE target.Employee_ID IS NULL OR (
    target.Employee_Name IS DISTINCT FROM source.Employee_Name OR
    target.Employee_Surname IS DISTINCT FROM source.Employee_Surname OR
    target.Employee_Phone IS DISTINCT FROM source.Employee_Phone OR
    target.Employee_Email IS DISTINCT FROM source.Employee_Email OR
    target.Employee_Dep_ID IS DISTINCT FROM source.Employee_Dep_ID OR
    target.Employee_Address_ID IS DISTINCT FROM source.Employee_Address_ID
);

-- 7. DIM_BRANCH

--CREATE TABLE dwh_stage.stg_branch AS
insert into dwh_stage.stg_branch
SELECT 
    b.branch_id AS Branch_ID,
    b.branch_name AS Branch_Name,
    (SELECT a.source_ID FROM dwh_stage.stg_address a 
     WHERE a.source_id = b.branch_id AND a.source_type = 'BRANCH') AS Address_ID,
    b.manager_id AS Manager_ID,
    '2025-01-01'::DATE AS Valid_From,
    CASE 
        WHEN b.status = 'INACTIVE' THEN '2025-01-01'::DATE
        ELSE '9999-12-31'::DATE 
    END AS Valid_Until
FROM coresys.branches b;


-- Expire changed records
UPDATE dwh_foundation.dim_branch target
SET Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
FROM dwh_stage.stg_branch source
WHERE target.Branch_ID = source.Branch_ID 
AND target.Valid_Until = '9999-12-31'::DATE
AND (
    target.Branch_Name IS DISTINCT FROM source.Branch_Name OR
    target.Address_ID IS DISTINCT FROM source.Address_ID OR
    target.Manager_ID IS DISTINCT FROM source.Manager_ID
);

-- Insert new/changed records
INSERT INTO dwh_foundation.dim_branch (
    Branch_ID, Branch_Name, Address_ID, Manager_ID, Valid_From, Valid_Until
)
SELECT 
    source.Branch_ID,
    source.Branch_Name,
    source.Address_ID,
    source.Manager_ID,
    '2025-01-01'::DATE,
    source.Valid_Until
FROM dwh_stage.stg_branch source
LEFT JOIN dwh_foundation.dim_branch target 
    ON source.Branch_ID = target.Branch_ID 
    AND target.Valid_Until = '9999-12-31'::DATE
WHERE target.Branch_ID IS NULL OR (
    target.Branch_Name IS DISTINCT FROM source.Branch_Name OR
    target.Address_ID IS DISTINCT FROM source.Address_ID OR
    target.Manager_ID IS DISTINCT FROM source.Manager_ID
);

-- 8. dim_pmnt_channel

--CREATE TABLE dwh_stage.stg_pmnt_channel AS
insert into dwh_stage.stg_pmnt_channel
SELECT 
    channel_id AS Channel_ID,
    channel_name AS Channel_Name,
    channel_description AS Channel_Description,
    '2025-01-01'::DATE AS Valid_From,
    '9999-12-31'::DATE AS Valid_Until
FROM coresys.payment_channels p;


UPDATE dwh_foundation.dim_pmnt_channel target
SET Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
FROM dwh_stage.stg_pmnt_channel source
WHERE target.Channel_ID = source.Channel_ID 
AND target.Valid_Until = '9999-12-31'::DATE
AND (
    target.Channel_Name IS DISTINCT FROM source.Channel_Name OR
    target.Channel_Description IS DISTINCT FROM source.Channel_Description 
);

-- Insert new/changed records
INSERT INTO dwh_foundation.dim_pmnt_channel (
    Channel_ID, Channel_Name, Channel_Description, Valid_From, Valid_Until
)
SELECT 
    source.Channel_ID,
    source.Channel_Name,
    source.Channel_Description,
    '2025-01-01'::DATE,
    source.Valid_Until
FROM dwh_stage.stg_pmnt_channel source
LEFT JOIN dwh_foundation.dim_pmnt_channel target 
    ON source.Channel_ID = target.Channel_ID 
    AND target.Valid_Until = '9999-12-31'::DATE
WHERE target.Channel_ID IS NULL OR (
    target.Channel_Name IS DISTINCT FROM source.Channel_Name OR
    target.Channel_Description IS DISTINCT FROM source.Channel_Description
);

-- 9. DIM_BANK

--CREATE TABLE dwh_stage.stg_bank AS
insert into dwh_stage.stg_bank
SELECT 
    b.bank_id AS Bank_ID,
    b.bank_name as Bank_Name,
    b.swift_code AS SWIFT_Code,
    b.country_id AS Country_ID,
    '2025-01-01'::DATE AS Valid_From,
    '9999-12-31'::DATE AS Valid_Until
FROM coresys.partner_banks b;


-- Expire changed records
UPDATE dwh_foundation.dim_bank target
SET Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
FROM dwh_stage.stg_bank source
WHERE target.Bank_ID = source.Bank_ID 
AND target.Valid_Until = '9999-12-31'::DATE
AND (
    target.SWIFT_Code IS DISTINCT FROM source.SWIFT_Code OR
    target.Country_ID IS DISTINCT FROM source.Country_ID or
    target.bank_name IS DISTINCT FROM source.bank_name
);

-- Insert new/changed records
INSERT INTO dwh_foundation.dim_bank (
    Bank_ID, bank_name, SWIFT_Code, Country_ID, Valid_From, Valid_Until
)
SELECT 
    source.Bank_ID,
    source.bank_name,
    source.SWIFT_Code,
    source.Country_ID,
    '2025-01-01'::DATE,
    source.Valid_Until
FROM dwh_stage.stg_bank source
LEFT JOIN dwh_foundation.dim_bank target 
    ON source.Bank_ID = target.Bank_ID 
    AND target.Valid_Until = '9999-12-31'::DATE
WHERE target.Bank_ID IS NULL OR (
    target.SWIFT_Code IS DISTINCT FROM source.SWIFT_Code OR
    target.Country_ID IS DISTINCT FROM source.Country_ID or
    target.bank_name IS DISTINCT FROM source.bank_name
);

-- 10. dim_pmnt_type

--CREATE TABLE dwh_stage.stg_pmnt_type AS
insert into dwh_stage.stg_pmnt_type
SELECT 
    pt.payment_type_id AS Pmnt_Type_ID,
    pt.type_name AS Pmnt_Type_Name,
    pt.type_description AS Pmnt_Type_Description,
    '2025-01-01'::DATE AS Valid_From,
    '9999-12-31'::DATE AS Valid_Until
FROM coresys.payment_types pt;


UPDATE dwh_foundation.dim_pmnt_type target
SET Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
FROM dwh_stage.stg_pmnt_type source
WHERE target.Pmnt_Type_ID = source.Pmnt_Type_ID 
AND target.Valid_Until = '9999-12-31'::DATE
AND (
    target.Pmnt_Type_Name IS DISTINCT FROM source.Pmnt_Type_Name OR
    target.Pmnt_Type_Description IS DISTINCT FROM source.Pmnt_Type_Description
);

-- Insert new/changed records
INSERT INTO dwh_foundation.dim_pmnt_type (
    Pmnt_Type_ID, Pmnt_Type_Name, Pmnt_Type_Description, Valid_From, Valid_Until
)
SELECT 
    source.Pmnt_Type_ID,
    source.Pmnt_Type_Name,
    source.Pmnt_Type_Description,
    '2025-01-01'::DATE,
    source.Valid_Until
FROM dwh_stage.stg_pmnt_type source
LEFT JOIN dwh_foundation.dim_pmnt_type target 
    ON source.Pmnt_Type_ID = target.Pmnt_Type_ID 
    AND target.Valid_Until = '9999-12-31'::DATE
WHERE target.Pmnt_Type_ID IS NULL OR (
    target.Pmnt_Type_Name IS DISTINCT FROM source.Pmnt_Type_Name OR
    target.Pmnt_Type_Description IS DISTINCT FROM source.Pmnt_Type_Description
);

-- 11. dim_departments

--CREATE TABLE dwh_stage.stg_departments AS
insert into dwh_stage.stg_departments
SELECT 
    d.department_id AS Dep_ID,
    d.department_name AS Dep_Name,
    d.department_desc AS Dep_Desc,
    '2025-01-01'::DATE AS Valid_From,
    CASE WHEN d.active = 'N' THEN '2025-01-01'::DATE ELSE '9999-12-31'::DATE END AS Valid_Until
FROM coresys.bank_departments d;


-- Expire changed records
UPDATE dwh_foundation.dim_departments target
SET Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
FROM dwh_stage.stg_departments source
WHERE target.Dep_ID = source.Dep_ID 
AND target.Valid_Until = '9999-12-31'::DATE
AND (
    target.Dep_Name IS DISTINCT FROM source.Dep_Name OR
    target.Dep_Desc IS DISTINCT FROM source.Dep_Desc
);

-- Insert new/changed records
INSERT INTO dwh_foundation.dim_departments (
    Dep_ID, Dep_Name, Dep_Desc, Valid_From, Valid_Until
)
SELECT 
    source.Dep_ID,
    source.Dep_Name,
    source.Dep_Desc,
    '2025-01-01'::DATE,
    source.Valid_Until
FROM dwh_stage.stg_departments source
LEFT JOIN dwh_foundation.dim_departments target 
    ON source.Dep_ID = target.Dep_ID 
    AND target.Valid_Until = '9999-12-31'::DATE
WHERE target.Dep_ID IS NULL OR (
    target.Dep_Name IS DISTINCT FROM source.Dep_Name OR
    target.Dep_Desc IS DISTINCT FROM source.Dep_Desc
);

-- 12. dim_interest_type

-- Stage interest types
--CREATE TABLE dwh_stage.stg_interest_type AS
insert into dwh_stage.stg_interest_type
SELECT 
    i.interest_id AS Intrst_Type_ID,
    i.interest_code AS Intrst_Type_Code,
    NULL AS Intrst_Percent, -- Assuming this needs to be populated from another source
    i.calculation_method AS Intrst_Period,
    '2025-01-01'::DATE AS Valid_From,
    '9999-12-31'::DATE AS Valid_Until
FROM coresys.reference_interest_types i;


-- Expire changed records
UPDATE dwh_foundation.dim_interest_type target
SET Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
FROM dwh_stage.stg_interest_type source
WHERE target.Intrst_Type_ID = source.Intrst_Type_ID 
AND target.Valid_Until = '9999-12-31'::DATE
AND (
    target.Intrst_Type_Code IS DISTINCT FROM source.Intrst_Type_Code OR
    target.Intrst_Period IS DISTINCT FROM source.Intrst_Period
);

-- Insert new/changed records
INSERT INTO dwh_foundation.dim_interest_type (
    Intrst_Type_ID, Intrst_Type_Code, Intrst_Percent, Intrst_Period, Valid_From, Valid_Until
)
SELECT 
    source.Intrst_Type_ID,
    source.Intrst_Type_Code,
    source.Intrst_Percent,
    source.Intrst_Period,
    '2025-01-01'::DATE,
    source.Valid_Until
FROM dwh_stage.stg_interest_type source
LEFT JOIN dwh_foundation.dim_interest_type target 
    ON source.Intrst_Type_ID = target.Intrst_Type_ID 
    AND target.Valid_Until = '9999-12-31'::DATE
WHERE target.Intrst_Type_ID IS NULL OR (
    target.Intrst_Type_Code IS DISTINCT FROM source.Intrst_Type_Code OR
    target.Intrst_Period IS DISTINCT FROM source.Intrst_Period
);

-- 13. dim_currency

-- Stage currencies
--CREATE TABLE dwh_stage.stg_currency AS
insert into dwh_stage.stg_currency
SELECT 
    c.currency_id AS Currency_ID,
    c.currency_code AS Currency_Code,
    c.currency_name AS Currency_Name,
    '2025-01-01'::DATE AS Valid_From,
    '9999-12-31'::DATE AS Valid_Until
FROM coresys.reference_currencies c;


-- Expire changed records
UPDATE dwh_foundation.dim_currency target
SET Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
FROM dwh_stage.stg_currency source
WHERE target.Currency_ID = source.Currency_ID 
AND target.Valid_Until = '9999-12-31'::DATE
AND (
    target.Currency_Code IS DISTINCT FROM source.Currency_Code OR
    target.Currency_Name IS DISTINCT FROM source.Currency_Name
);

-- Insert new/changed records
INSERT INTO dwh_foundation.dim_currency (
    Currency_ID, Currency_Code, Currency_Name, Valid_From, Valid_Until
)
SELECT 
    source.Currency_ID,
    source.Currency_Code,
    source.Currency_Name,
    '2025-01-01'::DATE,
    source.Valid_Until
FROM dwh_stage.stg_currency source
LEFT JOIN dwh_foundation.dim_currency target 
    ON source.Currency_ID = target.Currency_ID 
    AND target.Valid_Until = '9999-12-31'::DATE
WHERE target.Currency_ID IS NULL OR (
    target.Currency_Code IS DISTINCT FROM source.Currency_Code OR
    target.Currency_Name IS DISTINCT FROM source.Currency_Name
);

-- 14. dim_customer

-- Stage customers
--CREATE TABLE dwh_stage.stg_customer AS
insert into dwh_stage.stg_customer
SELECT 
    c.customer_id AS Customer_ID,
    c.first_name AS Name,
    c.last_name AS Surname,
    c.CNP_CUI,
    CASE c.customer_type 
        WHEN 'INDIVIDUAL' THEN 1
        WHEN 'SME' THEN 2
        WHEN 'CORPORATE' THEN 3
        ELSE 0 
    END AS Customer_Type_ID,
    CASE c.risk_category
        WHEN 'LOW' THEN 1
        WHEN 'MEDIUM' THEN 2
        WHEN 'HIGH' THEN 3
        WHEN 'VERY_HIGH' THEN 4
        ELSE 0
    END AS Risk_Category_ID,
    TO_DATE(c.kyc_date, 'YYYY-MM-DD') AS Last_KYC_Date,
    c.customer_id AS Address_ID,
    c.Country_ID AS Nationality_ID,
    c.relationship_manager_id AS Relationship_Manager_ID,
    '2025-01-01'::DATE AS Valid_From,
    '9999-12-31'::DATE AS Valid_Until
FROM coresys.crm_customers c;

-- Expire changed records
UPDATE dwh_foundation.dim_customer target
SET Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
FROM dwh_stage.stg_customer source
WHERE target.Customer_ID = source.Customer_ID 
AND target.Valid_Until = '9999-12-31'::DATE
AND (
    target.Name IS DISTINCT FROM source.Name OR
    target.Surname IS DISTINCT FROM source.Surname OR
    target.CNP_CUI IS DISTINCT FROM source.CNP_CUI OR
    target.Customer_Type_ID IS DISTINCT FROM source.Customer_Type_ID OR
    target.Risk_Category_ID IS DISTINCT FROM source.Risk_Category_ID OR
    target.Last_KYC_Date IS DISTINCT FROM source.Last_KYC_Date OR
    target.Nationality_ID IS DISTINCT FROM source.Nationality_ID OR
    target.Relationship_Manager_ID IS DISTINCT FROM source.Relationship_Manager_ID
);

-- Insert new/changed records
INSERT INTO dwh_foundation.dim_customer (
    Customer_ID, Name, Surname, CNP_CUI, Customer_Type_ID, Risk_Category_ID, 
    Last_KYC_Date, Address_ID, Nationality_ID, Relationship_Manager_ID, Valid_From, Valid_Until
)
SELECT 
    source.Customer_ID,
    source.Name,
    source.Surname,
    source.CNP_CUI,
    source.Customer_Type_ID,
    source.Risk_Category_ID,
    source.Last_KYC_Date,
    source.Address_ID,
    source.Nationality_ID,
    source.Relationship_Manager_ID,
    '2025-01-01'::DATE,
    source.Valid_Until
FROM dwh_stage.stg_customer source
LEFT JOIN dwh_foundation.dim_customer target 
    ON source.Customer_ID = target.Customer_ID 
    AND target.Valid_Until = '9999-12-31'::DATE
WHERE target.Customer_ID IS NULL OR (
    target.Name IS DISTINCT FROM source.Name OR
    target.Surname IS DISTINCT FROM source.Surname OR
    target.CNP_CUI IS DISTINCT FROM source.CNP_CUI OR
    target.Customer_Type_ID IS DISTINCT FROM source.Customer_Type_ID OR
    target.Risk_Category_ID IS DISTINCT FROM source.Risk_Category_ID OR
    target.Last_KYC_Date IS DISTINCT FROM source.Last_KYC_Date OR
    target.Nationality_ID IS DISTINCT FROM source.Nationality_ID OR
    target.Relationship_Manager_ID IS DISTINCT FROM source.Relationship_Manager_ID
);

-- 15. dim_account

-- Stage accounts
--CREATE TABLE dwh_stage.stg_account AS
insert into dwh_stage.stg_account
SELECT 
    a.account_id AS Account_ID,
    a.iban AS Account_IBAN,
    a.opening_date AS Opening_Date,
    a.closing_date AS Closing_Date,
    a.currency_id AS Currency_ID,
    '2025-01-01'::DATE AS Valid_From,
    CASE WHEN a.closing_date IS NOT NULL THEN a.closing_date ELSE '9999-12-31'::DATE END AS Valid_Until
FROM coresys.bank_accounts a;

-- Expire changed records
UPDATE dwh_foundation.dim_account target
SET Valid_Until = '2025-01-01'::DATE - INTERVAL '1 day'
FROM dwh_stage.stg_account source
WHERE target.Account_ID = source.Account_ID 
AND target.Valid_Until = '9999-12-31'::DATE
AND (
    target.Account_IBAN IS DISTINCT FROM source.Account_IBAN OR
    target.Opening_Date IS DISTINCT FROM source.Opening_Date OR
    target.Closing_Date IS DISTINCT FROM source.Closing_Date OR
    target.Currency_ID IS DISTINCT FROM source.Currency_ID
);


-- Insert new/changed records
INSERT INTO dwh_foundation.dim_account (
    Account_ID, Account_IBAN, Opening_Date, Closing_Date, Currency_ID, Valid_From, Valid_Until
)
SELECT 
    source.Account_ID,
    source.Account_IBAN,
    source.Opening_Date,
    source.Closing_Date,
    source.Currency_ID,
    '2025-01-01'::DATE,
    source.Valid_Until
FROM dwh_stage.stg_account source
LEFT JOIN dwh_foundation.dim_account target 
    ON source.Account_ID = target.Account_ID 
    AND target.Valid_Until = '9999-12-31'::DATE
WHERE target.Account_ID IS NULL OR (
    target.Account_IBAN IS DISTINCT FROM source.Account_IBAN OR
    target.Opening_Date IS DISTINCT FROM source.Opening_Date OR
    target.Closing_Date IS DISTINCT FROM source.Closing_Date OR
    target.Currency_ID IS DISTINCT FROM source.Currency_ID
);

INSERT INTO dwh_meta.etl_log (
            process_name, 
            execution_date, 
            records_processed, 
            status, 
            error_message,
            details,
            created_at
        ) VALUES (
            'ETL LOAD', 
            CURRENT_DATE, 
            0, 
            'END', 
            'END',
            'Ended load for ' || '2025-01-01'::DATE,
            current_timestamp
        );
commit;
end; $procedure$
;