SELECT
    loan_id,
    glynac_organization_id,
    
    -- Loan classification
    trimBoth(COALESCE(loan_type_class, '')) as loan_type_class,
    trimBoth(COALESCE(loan_number, '')) as loan_number,
    trimBoth(COALESCE(loan_type, '')) as loan_type,
    trimBoth(COALESCE(loan_purpose, '')) as loan_purpose,
    trimBoth(COALESCE(loan_status, '')) as loan_status,
    
    -- Relationships
    trimBoth(COALESCE(account_id, '')) as account_id,
    trimBoth(COALESCE(borrower_id, '')) as borrower_id,
    trimBoth(COALESCE(borrower_name, '')) as borrower_name,
    
    -- Financial details
    toDecimal64OrZero(principal_amount, 2) as principal_amount,
    trimBoth(COALESCE(currency, '')) as currency,
    toDecimal64OrZero(outstanding_balance, 2) as outstanding_balance,
    toDecimal64OrZero(interest_rate, 4) as interest_rate,
    trimBoth(COALESCE(interest_type, '')) as interest_type,
    
    -- Loan terms
    parseDateTime64BestEffortOrNull(toString(origination_date)) as origination_date,
    parseDateTime64BestEffortOrNull(toString(maturity_date)) as maturity_date,
    toInt32OrZero(term_months) as term_months,
    
    -- Payment details
    trimBoth(COALESCE(payment_frequency, '')) as payment_frequency,
    toDecimal64OrZero(payment_amount, 2) as payment_amount,
    parseDateTime64BestEffortOrNull(toString(next_payment_date)) as next_payment_date,
    
    -- Fees and penalties
    toDecimal64OrZero(late_fee_amount, 2) as late_fee_amount,
    toDecimal64OrZero(prepayment_penalty, 2) as prepayment_penalty,
    
    -- Security
    if(is_secured = true, 1, 0) as is_secured,
    trimBoth(COALESCE(collateral, '')) as collateral,
    
    -- Additional information
    trimBoth(COALESCE(tags, '')) as tags,
    
    -- Dates
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.finance_loan_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.finance_loan_raw
WHERE loan_id IS NOT NULL
  AND loan_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY loan_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1