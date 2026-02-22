"""
Transformation Library - 75 Complex SQL Transformations
ALL transformations are complex with 2+ nested functions or conditional logic
"""
import random
from typing import Dict, List, Optional, Callable


class TransformationLibrary:
    """Catalog of complex SQL transformations only"""
    
    # All transformations MUST be complex (2+ nested functions OR conditional logic)
    TRANSFORMATIONS = {
        # === STRING COMPLEX TRANSFORMATIONS (12) ===
        "concat_upper_trim": {
            "template": "CONCAT(UPPER(TRIM({col1})), ' - ', LPAD({col2}, 10, '0'))",
            "min_cols": 2,
            "max_cols": 2,
            "category": "string",
            "description": "Concatenates uppercase trimmed first column with zero-padded second column"
        },
        "case_trim_length": {
            "template": "CASE WHEN LENGTH(TRIM({col})) > 0 THEN UPPER(TRIM({col})) ELSE 'UNKNOWN' END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "string",
            "description": "Standardizes string to uppercase with empty/null handling"
        },
        "regex_replace_standardize": {
            "template": "REGEXP_REPLACE(REGEXP_REPLACE({col}, '[^a-zA-Z0-9]', ''), '[0-9]+', '')",
            "min_cols": 1,
            "max_cols": 1,
            "category": "string",
            "description": "Removes special characters and digits for text standardization"
        },
        "concat_substring_hash": {
            "template": "CONCAT(LEFT({col1}, 3), '-', RIGHT({col2}, 4), '-', MD5(CONCAT({col1}, {col2})))",
            "min_cols": 2,
            "max_cols": 2,
            "category": "string",
            "description": "Creates composite key from partial strings with hash"
        },
        "case_string_validation": {
            "template": "CASE WHEN {col} IS NOT NULL AND LENGTH(TRIM({col})) >= 3 THEN UPPER(TRIM({col})) ELSE NULL END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "string",
            "description": "Validates minimum length before standardizing to uppercase"
        },
        "lpad_concat_upper": {
            "template": "CONCAT(LPAD(UPPER(SUBSTRING({col1}, 1, 3)), 5, 'X'), '_', LPAD({col2}, 8, '0'))",
            "min_cols": 2,
            "max_cols": 2,
            "category": "string",
            "description": "Creates formatted code with prefix and zero-padded sequence"
        },
        "regex_extract_pattern": {
            "template": "CASE WHEN REGEXP_LIKE({col}, '^[A-Z]{{2}}[0-9]{{4}}$') THEN {col} ELSE CONCAT('XX', LPAD('9999', 4, '0')) END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "string",
            "description": "Validates pattern match or returns default code"
        },
        "concat_coalesce_nullif": {
            "template": "CONCAT(COALESCE(NULLIF(TRIM({col1}), ''), 'N/A'), ' | ', COALESCE(NULLIF(TRIM({col2}), ''), 'N/A'))",
            "min_cols": 2,
            "max_cols": 2,
            "category": "string",
            "description": "Concatenates two columns with null/empty handling"
        },
        "replace_upper_trim": {
            "template": "UPPER(TRIM(REPLACE(REPLACE({col}, CHR(10), ' '), CHR(9), ' ')))",
            "min_cols": 1,
            "max_cols": 1,
            "category": "string",
            "description": "Cleans whitespace characters and converts to uppercase"
        },
        "substring_concat_case": {
            "template": "CASE WHEN LENGTH({col}) > 10 THEN CONCAT(SUBSTRING({col}, 1, 10), '...') ELSE {col} END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "string",
            "description": "Truncates long strings with ellipsis indicator"
        },
        "regex_mask_sensitive": {
            "template": "CONCAT(REPEAT('X', LENGTH({col}) - 4), RIGHT({col}, 4))",
            "min_cols": 1,
            "max_cols": 1,
            "category": "string",
            "description": "Masks all but last 4 characters for sensitive data"
        },
        "concat_receipt_format": {
            "template": "CONCAT('#', LPAD({col1}, 8, '0'), ' - ', DATE_FORMAT({col2}, '%Y-%m-%d'), ' - $', FORMAT({col3}, 2))",
            "min_cols": 3,
            "max_cols": 3,
            "category": "string",
            "category_cols": ["string", "date", "numeric"],
            "description": "Formats receipt string with order ID, date, and amount"
        },
        
        # === DATE/TIME COMPLEX TRANSFORMATIONS (10) ===
        "date_format_case": {
            "template": "CASE WHEN {col} IS NOT NULL THEN DATE_FORMAT({col}, '%Y-%m-%d') ELSE '1900-01-01' END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "date",
            "description": "Formats date with null default to epoch date"
        },
        "date_diff_validation": {
            "template": "CASE WHEN DATEDIFF(CURRENT_DATE, {col}) BETWEEN 0 AND 365 THEN 'CURRENT' WHEN DATEDIFF(CURRENT_DATE, {col}) > 365 THEN 'OLD' ELSE 'FUTURE' END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "date",
            "description": "Categorizes records by age: current, old, or future"
        },
        "concat_date_parts": {
            "template": "CONCAT(EXTRACT(YEAR FROM {col}), LPAD(CAST(EXTRACT(MONTH FROM {col}) AS STRING), 2, '0'), LPAD(CAST(EXTRACT(DAY FROM {col}) AS STRING), 2, '0'))",
            "min_cols": 1,
            "max_cols": 1,
            "category": "date",
            "description": "Creates YYYYMMDD integer date key from timestamp"
        },
        "date_calculation_lead_time": {
            "template": "DATE_ADD({col1}, INTERVAL CAST(COALESCE({col2}, 0) AS INT) DAY)",
            "min_cols": 2,
            "max_cols": 2,
            "category": "date",
            "category_cols": ["date", "numeric"],
            "description": "Calculates expected date with lead time from numeric column"
        },
        "date_reconciliation": {
            "template": "CASE WHEN {col1} IS NOT NULL THEN {col1} WHEN {col2} IS NOT NULL THEN {col2} ELSE CURRENT_DATE END",
            "min_cols": 2,
            "max_cols": 2,
            "category": "date",
            "category_cols": ["date", "date"],
            "description": "Coalesces multiple date columns with current date default"
        },
        "date_timestamp_convert": {
            "template": "CAST(DATE_FORMAT({col}, 'yyyyMMdd') AS INT)",
            "min_cols": 1,
            "max_cols": 1,
            "category": "date",
            "description": "Converts timestamp to integer date key for dimension lookup"
        },
        "date_fiscal_period": {
            "template": "CONCAT(CAST(EXTRACT(YEAR FROM {col}) AS STRING), 'Q', CAST(CEIL(EXTRACT(MONTH FROM {col}) / 3.0) AS STRING))",
            "min_cols": 1,
            "max_cols": 1,
            "category": "date",
            "description": "Derives fiscal quarter from transaction date"
        },
        "date_pay_period": {
            "template": "CONCAT(EXTRACT(YEAR FROM {col}), '_', LPAD(CAST(CEIL(EXTRACT(DAYOFYEAR FROM {col}) / 14.0) AS STRING), 2, '0'))",
            "min_cols": 1,
            "max_cols": 1,
            "category": "date",
            "description": "Calculates bi-weekly pay period identifier"
        },
        "date_expected_delivery": {
            "template": "DATE_ADD({col}, INTERVAL (CASE WHEN DAYOFWEEK({col}) IN (1, 7) THEN 5 ELSE 3 END) DAY)",
            "min_cols": 1,
            "max_cols": 1,
            "category": "date",
            "description": "Calculates delivery date adjusting for weekends"
        },
        "date_range_validation": {
            "template": "CASE WHEN {col} BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR) AND DATE_ADD(CURRENT_DATE, INTERVAL 1 YEAR) THEN {col} ELSE NULL END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "date",
            "description": "Validates date is within acceptable range"
        },
        
        # === NUMERIC COMPLEX TRANSFORMATIONS (10) ===
        "numeric_calculation_case": {
            "template": "ROUND(({col1} * {col2}) / NULLIF({col3}, 0), 2)",
            "min_cols": 3,
            "max_cols": 3,
            "category": "numeric",
            "category_cols": ["numeric", "numeric", "numeric"],
            "description": "Weighted average calculation with division by zero protection"
        },
        "case_amount_calculation": {
            "template": "CASE WHEN {col1} > 0 THEN ROUND({col1} * (1 - COALESCE({col2}, 0) / 100.0), 2) ELSE 0 END",
            "min_cols": 2,
            "max_cols": 2,
            "category": "numeric",
            "category_cols": ["numeric", "numeric"],
            "description": "Applies percentage discount with zero protection"
        },
        "numeric_calculation_nullif": {
            "template": "CASE WHEN {col} IS NOT NULL AND {col} != 0 THEN FLOOR(ABS({col})) ELSE 0 END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "numeric",
            "description": "Normalizes numeric value to positive integer with null handling"
        },
        "amount_variance_calculation": {
            "template": "ROUND(((CAST({col1} AS DECIMAL(18,2)) - CAST({col2} AS DECIMAL(18,2))) / NULLIF(CAST({col2} AS DECIMAL(18,2)), 0)) * 100, 2)",
            "min_cols": 2,
            "max_cols": 2,
            "category": "numeric",
            "category_cols": ["numeric", "numeric"],
            "description": "Calculates percentage variance between two amounts"
        },
        "amount_settlement_calculation": {
            "template": "ROUND({col1} - ({col1} * COALESCE({col2}, 0) / 100.0) - COALESCE({col3}, 0), 2)",
            "min_cols": 3,
            "max_cols": 3,
            "category": "numeric",
            "category_cols": ["numeric", "numeric", "numeric"],
            "description": "Calculates net settlement after fees and adjustments"
        },
        "case_numeric_range": {
            "template": "CASE WHEN {col} BETWEEN 0 AND 50 THEN 'LOW' WHEN {col} BETWEEN 51 AND 100 THEN 'MEDIUM' WHEN {col} > 100 THEN 'HIGH' ELSE 'INVALID' END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "numeric",
            "description": "Bucketing numeric values into descriptive categories"
        },
        "amount_deduction_calculation": {
            "template": "ROUND(({col1} * {col2}) - ({col1} * COALESCE({col3}, 0)), 2)",
            "min_cols": 3,
            "max_cols": 3,
            "category": "numeric",
            "category_cols": ["numeric", "numeric", "numeric"],
            "description": "Calculates net amount after rate and deduction adjustments"
        },
        "amount_debit_credit": {
            "template": "CASE WHEN UPPER(TRIM({col1})) = 'DEBIT' THEN ABS({col2}) WHEN UPPER(TRIM({col1})) = 'CREDIT' THEN -ABS({col2}) ELSE 0 END",
            "min_cols": 2,
            "max_cols": 2,
            "category": "numeric",
            "category_cols": ["string", "numeric"],
            "description": "Converts amounts to signed values based on entry type"
        },
        "percentage_calculation_case": {
            "template": "ROUND(({col1} / NULLIF({col2}, 0)) * 100, 2)",
            "min_cols": 2,
            "max_cols": 2,
            "category": "numeric",
            "category_cols": ["numeric", "numeric"],
            "description": "Calculates percentage with division by zero protection"
        },
        "percentage_calculation_roas": {
            "template": "CASE WHEN {col1} > 0 THEN ROUND(({col2} / CAST({col1} AS DECIMAL(18,2))) * 100, 2) ELSE 0 END",
            "min_cols": 2,
            "max_cols": 2,
            "category": "numeric",
            "category_cols": ["numeric", "numeric"],
            "description": "Calculates return on ad spend (ROAS) percentage"
        },
        
        # === CONDITIONAL COMPLEX TRANSFORMATIONS (10) ===
        "coalesce_nullif": {
            "template": "COALESCE(NULLIF(TRIM({col1}), ''), NULLIF(TRIM({col2}), ''), 'DEFAULT_VALUE')",
            "min_cols": 2,
            "max_cols": 2,
            "category": "conditional",
            "description": "Coalesces two columns with empty string handling"
        },
        "case_tender_validation": {
            "template": "CASE WHEN UPPER(TRIM({col})) IN ('CREDIT', 'DEBIT', 'CASH', 'CHECK', 'GIFT_CARD') THEN UPPER(TRIM({col})) ELSE 'OTHER' END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "conditional",
            "description": "Validates and standardizes tender types"
        },
        "null_handling_coalesce": {
            "template": "CASE WHEN {col1} IS NULL OR {col2} IS NULL THEN COALESCE({col1}, {col2}, 0) ELSE {col1} + {col2} END",
            "min_cols": 2,
            "max_cols": 2,
            "category": "conditional",
            "description": "Sums columns or coalesces if either is null"
        },
        "case_delivery_status": {
            "template": "CASE WHEN {col1} IS NOT NULL AND {col2} IS NOT NULL THEN 'DELIVERED' WHEN {col1} IS NOT NULL AND {col2} IS NULL THEN 'IN_TRANSIT' ELSE 'PENDING' END",
            "min_cols": 2,
            "max_cols": 2,
            "category": "conditional",
            "category_cols": ["date", "date"],
            "description": "Derives delivery status from ship and delivery dates"
        },
        "case_pay_type": {
            "template": "CASE WHEN UPPER(TRIM({col})) = 'HOURLY' THEN 'HOURLY_WAGE' WHEN UPPER(TRIM({col})) = 'SALARY' THEN 'ANNUAL_SALARY' WHEN UPPER(TRIM({col})) = 'CONTRACT' THEN 'CONTRACT_RATE' ELSE 'OTHER' END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "conditional",
            "description": "Standardizes pay type classifications"
        },
        "case_task_status": {
            "template": "CASE WHEN {col1} = 'COMPLETE' AND {col2} IS NOT NULL THEN 'DONE' WHEN {col1} = 'IN_PROGRESS' THEN 'ACTIVE' WHEN {col1} = 'PENDING' THEN 'WAITING' ELSE 'UNKNOWN' END",
            "min_cols": 2,
            "max_cols": 2,
            "category": "conditional",
            "description": "Derives task status from state and completion timestamp"
        },
        "case_entry_type": {
            "template": "CASE WHEN UPPER(TRIM({col})) IN ('JOURNAL', 'ADJUSTMENT') THEN 'MANUAL' WHEN UPPER(TRIM({col})) IN ('INVOICE', 'PAYMENT') THEN 'AUTOMATED' ELSE 'SYSTEM' END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "conditional",
            "description": "Categorizes GL entry types by source"
        },
        "case_refund_calculation": {
            "template": "CASE WHEN UPPER(TRIM({col1})) = 'FULL' THEN {col2} WHEN UPPER(TRIM({col1})) = 'PARTIAL' THEN ROUND({col2} * 0.5, 2) ELSE 0 END",
            "min_cols": 2,
            "max_cols": 2,
            "category": "conditional",
            "category_cols": ["string", "numeric"],
            "description": "Calculates refund amount based on type"
        },
        "case_payment_validation": {
            "template": "CASE WHEN {col1} IS NOT NULL AND LENGTH(TRIM({col1})) > 0 AND {col2} > 0 THEN 'VALID' ELSE 'INVALID' END",
            "min_cols": 2,
            "max_cols": 2,
            "category": "conditional",
            "category_cols": ["string", "numeric"],
            "description": "Validates payment record completeness"
        },
        "case_tier_qualification": {
            "template": "CASE WHEN {col} >= 10000 THEN 'PLATINUM' WHEN {col} >= 5000 THEN 'GOLD' WHEN {col} >= 1000 THEN 'SILVER' ELSE 'BRONZE' END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "conditional",
            "description": "Determines loyalty tier based on points threshold"
        },
        
        # === PATTERN COMPLEX TRANSFORMATIONS (8) ===
        "regex_validate_coupon": {
            "template": "CASE WHEN REGEXP_LIKE(UPPER(TRIM({col})), '^[A-Z]{2}[0-9]{4,6}$') THEN UPPER(TRIM({col})) ELSE NULL END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "pattern",
            "description": "Validates coupon code format"
        },
        "regex_extract_order_ref": {
            "template": "REGEXP_EXTRACT({col}, 'ORDER[_-]?([0-9]{6,10})', 1)",
            "min_cols": 1,
            "max_cols": 1,
            "category": "pattern",
            "description": "Extracts order number from reference text"
        },
        "regex_extract_invoice_num": {
            "template": "CASE WHEN REGEXP_LIKE({col}, '^INV-[0-9]{6,8}$') THEN {col} ELSE CONCAT('INV-', LPAD('999999', 8, '0')) END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "pattern",
            "description": "Validates or defaults invoice number format"
        },
        "regex_extract_domain": {
            "template": "REGEXP_EXTRACT({col}, 'https?://([^/]+)', 1)",
            "min_cols": 1,
            "max_cols": 1,
            "category": "pattern",
            "description": "Extracts domain from URL"
        },
        "regex_mask_card": {
            "template": "CONCAT(REPEAT('X', LENGTH(REGEXP_REPLACE({col}, '[^0-9]', '')) - 4), RIGHT(REGEXP_REPLACE({col}, '[^0-9]', ''), 4))",
            "min_cols": 1,
            "max_cols": 1,
            "category": "pattern",
            "description": "Masks card number keeping only last 4 digits"
        },
        "concat_po_reference": {
            "template": "CONCAT('PO-', LPAD(REGEXP_EXTRACT({col1}, '[0-9]+', 0), 8, '0'), '-', DATE_FORMAT({col2}, 'yyyyMMdd'))",
            "min_cols": 2,
            "max_cols": 2,
            "category": "pattern",
            "category_cols": ["string", "date"],
            "description": "Formats PO reference with extracted ID and date"
        },
        "regex_extract_email_domain": {
            "template": "CASE WHEN REGEXP_LIKE(LOWER(TRIM({col})), '^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$') THEN REGEXP_EXTRACT(LOWER(TRIM({col})), '@(.+)$', 1) ELSE 'invalid.domain' END",
            "min_cols": 1,
            "max_cols": 1,
            "category": "pattern",
            "description": "Extracts and validates email domain"
        },
        "concat_utm_parameters": {
            "template": "CONCAT('utm_source=', COALESCE({col1}, 'direct'), '&utm_medium=', COALESCE({col2}, 'none'), '&utm_campaign=', COALESCE({col3}, 'organic'))",
            "min_cols": 3,
            "max_cols": 3,
            "category": "pattern",
            "description": "Builds UTM tracking parameter string"
        },
        
        # === AGGREGATION COMPLEX TRANSFORMATIONS (6) ===
        "aggregation_window": {
            "template": "SUM({col}) OVER (PARTITION BY {partition_col} ORDER BY {order_col} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
            "min_cols": 3,
            "max_cols": 3,
            "category": "aggregation",
            "category_cols": ["numeric", "string", "date"],
            "description": "Running total partitioned by group ordered by date"
        },
        "aggregation_window_partition": {
            "template": "ROUND(AVG({col}) OVER (PARTITION BY {partition_col}), 2) - LAG({col}, 1, 0) OVER (PARTITION BY {partition_col} ORDER BY {order_col})",
            "min_cols": 3,
            "max_cols": 3,
            "category": "aggregation",
            "category_cols": ["numeric", "string", "date"],
            "description": "Difference from group average compared to previous value"
        },
        "points_calculation_aggregation": {
            "template": "CASE WHEN RANK() OVER (PARTITION BY {col1} ORDER BY {col2} DESC) <= 10 THEN ROUND({col2} * 1.5, 0) ELSE ROUND({col2}, 0) END",
            "min_cols": 3,
            "max_cols": 3,
            "category": "aggregation",
            "category_cols": ["string", "numeric", "numeric"],
            "description": "Applies bonus multiplier to top 10 ranked records"
        },
        "aggregation_conversion_rate": {
            "template": "ROUND((COUNT(CASE WHEN {col1} = 'CONVERTED' THEN 1 END) OVER (PARTITION BY {col2}) * 100.0) / NULLIF(COUNT(*) OVER (PARTITION BY {col2}), 0), 2)",
            "min_cols": 2,
            "max_cols": 2,
            "category": "aggregation",
            "category_cols": ["string", "string"],
            "description": "Calculates conversion rate within partition"
        },
        "aggregation_window_session": {
            "template": "COUNT(DISTINCT {col1}) OVER (PARTITION BY {col2} ORDER BY {col3} RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW)",
            "min_cols": 3,
            "max_cols": 3,
            "category": "aggregation",
            "category_cols": ["string", "string", "date"],
            "description": "Rolling 30-day distinct count within partition"
        },
        "case_attribution_model": {
            "template": "CASE WHEN ROW_NUMBER() OVER (PARTITION BY {col1} ORDER BY {col2}) = 1 THEN ROUND({col3} * 0.4, 2) WHEN ROW_NUMBER() OVER (PARTITION BY {col1} ORDER BY {col2}) = 2 THEN ROUND({col3} * 0.3, 2) ELSE ROUND({col3} * 0.3 / NULLIF(COUNT(*) OVER (PARTITION BY {col1}) - 2, 0), 2) END",
            "min_cols": 3,
            "max_cols": 3,
            "category": "aggregation",
            "category_cols": ["string", "date", "numeric"],
            "description": "Position-based attribution model with weighted distribution"
        },
        
        # === CALCULATION COMPLEX TRANSFORMATIONS (4) ===
        "coalesce_calculation": {
            "template": "COALESCE({col1}, 0) + COALESCE({col2}, 0) - COALESCE({col3}, 0)",
            "min_cols": 3,
            "max_cols": 3,
            "category": "numeric",
            "category_cols": ["numeric", "numeric", "numeric"],
            "description": "Safe arithmetic with null coalescing"
        },
        "hours_calculation_overtime": {
            "template": "CASE WHEN ({col2} - {col1}) > 8 THEN 8 ELSE ({col2} - {col1}) END + CASE WHEN ({col2} - {col1}) > 8 THEN ({col2} - {col1}) - 8 ELSE 0 END * 1.5",
            "min_cols": 2,
            "max_cols": 2,
            "category": "numeric",
            "category_cols": ["numeric", "numeric"],
            "description": "Calculates regular and overtime hours with weighted pay"
        },
        "duration_calculation": {
            "template": "CASE WHEN {col2} IS NOT NULL THEN ROUND((UNIX_TIMESTAMP({col2}) - UNIX_TIMESTAMP({col1})) / 60, 0) ELSE NULL END",
            "min_cols": 2,
            "max_cols": 2,
            "category": "date",
            "category_cols": ["date", "date"],
            "description": "Calculates duration in minutes between timestamps"
        },
        "duration_calculation_task": {
            "template": "CASE WHEN {col1} = 'COMPLETE' AND {col2} IS NOT NULL THEN ROUND(TIMESTAMPDIFF(MINUTE, {col3}, {col2}), 0) ELSE NULL END",
            "min_cols": 3,
            "max_cols": 3,
            "category": "conditional",
            "category_cols": ["string", "date", "date"],
            "description": "Calculates task duration only for completed tasks"
        },
        
        # === MULTI-TABLE JOIN TRANSFORMATIONS (15) ===
        "join_concat_customer_order": {
            "template": "CONCAT(UPPER(TRIM({t1_col1})), ' - ', COALESCE({t2_col1}, 'N/A'), ' - $', FORMAT(COALESCE({t2_col2}, 0), 2))",
            "min_cols": 3,
            "max_cols": 3,
            "category": "join",
            "is_join": True,
            "join_type": "LEFT",
            "join_tables": 2,
            "description": "Joins customer name with order details creating formatted display string"
        },
        "join_lookup_customer_segment": {
            "template": "COALESCE({t2_col1}, CASE WHEN {t1_col1} > 10000 THEN 'PREMIUM' WHEN {t1_col1} > 5000 THEN 'GOLD' ELSE 'STANDARD' END)",
            "min_cols": 2,
            "max_cols": 2,
            "category": "join",
            "is_join": True,
            "join_type": "LEFT",
            "join_tables": 2,
            "description": "Looks up customer segment from dimension, falls back to calculated tier"
        },
        "join_calculate_order_total": {
            "template": "ROUND(SUM({t1_col1} * COALESCE({t2_col1}, 1)) OVER (PARTITION BY {t1_col2}), 2)",
            "min_cols": 3,
            "max_cols": 3,
            "category": "join",
            "is_join": True,
            "join_type": "INNER",
            "join_tables": 2,
            "description": "Calculates order total by joining line items with price lookup"
        },
        "join_enriched_product_name": {
            "template": "CONCAT(COALESCE({t2_col1}, 'Unknown'), ' - ', {t1_col1}, ' (', COALESCE({t3_col1}, 'N/A'), ')')",
            "min_cols": 3,
            "max_cols": 3,
            "category": "join",
            "is_join": True,
            "join_type": "LEFT",
            "join_tables": 3,
            "description": "Creates enriched product name from product, brand, and category tables"
        },
        "join_calculate_margin": {
            "template": "ROUND((({t1_col1} - COALESCE({t2_col1}, 0)) / NULLIF({t1_col1}, 0)) * 100, 2)",
            "min_cols": 2,
            "max_cols": 2,
            "category": "join",
            "is_join": True,
            "join_type": "INNER",
            "join_tables": 2,
            "description": "Calculates profit margin by joining sales with cost data"
        },
        "join_validate_address": {
            "template": "CASE WHEN {t1_col1} IS NOT NULL AND {t2_col1} IS NOT NULL THEN CONCAT(TRIM({t1_col1}), ', ', TRIM({t2_col1})) ELSE COALESCE({t1_col1}, {t2_col1}, 'ADDRESS MISSING') END",
            "min_cols": 2,
            "max_cols": 2,
            "category": "join",
            "is_join": True,
            "join_type": "LEFT",
            "join_tables": 2,
            "description": "Validates and combines address from customer and address tables"
        },
        "join_calculate_loyalty_points": {
            "template": "CASE WHEN {t2_col1} = 'PREMIUM' THEN ROUND({t1_col1} * 2, 0) WHEN {t2_col1} = 'GOLD' THEN ROUND({t1_col1} * 1.5, 0) ELSE ROUND({t1_col1}, 0) END",
            "min_cols": 2,
            "max_cols": 2,
            "category": "join",
            "is_join": True,
            "join_type": "LEFT",
            "join_tables": 2,
            "description": "Calculates loyalty points based on transaction amount and customer tier"
        },
        "join_stock_availability": {
            "template": "CASE WHEN COALESCE({t1_col1}, 0) - COALESCE({t2_col1}, 0) > 0 THEN 'IN_STOCK' WHEN COALESCE({t1_col1}, 0) - COALESCE({t2_col1}, 0) <= 0 AND COALESCE({t3_col1}, 0) > 0 THEN 'BACKORDER' ELSE 'OUT_OF_STOCK' END",
            "min_cols": 3,
            "max_cols": 3,
            "category": "join",
            "is_join": True,
            "join_type": "LEFT",
            "join_tables": 3,
            "description": "Determines stock availability from inventory, orders, and replenishment tables"
        },
        "join_customer_lifetime_value": {
            "template": "ROUND(SUM(COALESCE({t1_col1}, 0)) OVER (PARTITION BY {t2_col1}) - SUM(COALESCE({t3_col1}, 0)) OVER (PARTITION BY {t2_col1}), 2)",
            "min_cols": 3,
            "max_cols": 3,
            "category": "join",
            "is_join": True,
            "join_type": "LEFT",
            "join_tables": 3,
            "description": "Calculates customer lifetime value from orders, customer, and returns"
        },
        "join_order_fulfillment_status": {
            "template": "CASE WHEN {t1_col1} = 'SHIPPED' AND {t2_col1} IS NOT NULL THEN 'DELIVERED' WHEN {t1_col1} = 'SHIPPED' THEN 'IN_TRANSIT' WHEN {t3_col1} > 0 THEN 'BACKORDERED' ELSE 'PROCESSING' END",
            "min_cols": 3,
            "max_cols": 3,
            "category": "join",
            "is_join": True,
            "join_type": "LEFT",
            "join_tables": 3,
            "description": "Derives fulfillment status from order, shipment, and inventory tables"
        },
        "join_aggregate_sales_by_region": {
            "template": "ROUND(SUM({t1_col1}) OVER (PARTITION BY {t2_col1}) * 1.0 / NULLIF(COUNT(DISTINCT {t1_col2}) OVER (PARTITION BY {t2_col1}), 0), 2)",
            "min_cols": 3,
            "max_cols": 3,
            "category": "join",
            "is_join": True,
            "join_type": "INNER",
            "join_tables": 2,
            "description": "Calculates average sales per customer by region from sales and store tables"
        },
        "join_product_performance_score": {
            "template": "ROUND((COALESCE({t1_col1}, 0) * 0.4) + (COALESCE({t2_col1}, 0) * 0.3) + (CASE WHEN {t3_col1} > 4 THEN 30 ELSE {t3_col1} * 6 END), 2)",
            "min_cols": 3,
            "max_cols": 3,
            "category": "join",
            "is_join": True,
            "join_type": "LEFT",
            "join_tables": 3,
            "description": "Calculates product performance score from sales, inventory, and reviews"
        },
        "join_payment_reconciliation": {
            "template": "CASE WHEN ABS({t1_col1} - COALESCE({t2_col1}, 0)) < 0.01 THEN 'RECONCILED' WHEN {t1_col1} > COALESCE({t2_col1}, 0) THEN 'SHORT' ELSE 'OVER' END",
            "min_cols": 2,
            "max_cols": 2,
            "category": "join",
            "is_join": True,
            "join_type": "FULL OUTER",
            "join_tables": 2,
            "description": "Reconciles order amounts with payment settlements"
        },
        "join_promotion_effectiveness": {
            "template": "ROUND((SUM(CASE WHEN {t2_col1} IS NOT NULL THEN {t1_col1} ELSE 0 END) OVER (PARTITION BY {t3_col1}) - SUM(CASE WHEN {t2_col1} IS NULL THEN {t1_col1} ELSE 0 END) OVER (PARTITION BY {t3_col1})) * 100.0 / NULLIF(SUM(CASE WHEN {t2_col1} IS NULL THEN {t1_col1} ELSE 0 END) OVER (PARTITION BY {t3_col1}), 0), 2)",
            "min_cols": 3,
            "max_cols": 3,
            "category": "join",
            "is_join": True,
            "join_type": "LEFT",
            "join_tables": 3,
            "description": "Measures promotion effectiveness from sales, promotions, and campaigns"
        },
        "join_supplier_score": {
            "template": "ROUND((CASE WHEN {t1_col1} >= 95 THEN 40 WHEN {t1_col1} >= 90 THEN 30 ELSE 20 END) + (CASE WHEN {t2_col1} <= 7 THEN 30 WHEN {t2_col1} <= 14 THEN 20 ELSE 10 END) + (CASE WHEN {t3_col1} = 'CERTIFIED' THEN 30 ELSE 15 END), 2)",
            "min_cols": 3,
            "max_cols": 3,
            "category": "join",
            "is_join": True,
            "join_type": "LEFT",
            "join_tables": 3,
            "description": "Calculates supplier score from delivery performance, lead time, and certification"
        },
    }
    
    @classmethod
    def get_transformation(cls, category: Optional[str] = None) -> Dict:
        """Get random complex transformation"""
        if category:
            # Filter by category
            candidates = [
                key for key, value in cls.TRANSFORMATIONS.items()
                if value['category'] == category
            ]
            if candidates:
                transform_key = random.choice(candidates)
            else:
                transform_key = random.choice(list(cls.TRANSFORMATIONS.keys()))
        else:
            transform_key = random.choice(list(cls.TRANSFORMATIONS.keys()))
        
        return cls.TRANSFORMATIONS[transform_key]
    
    @classmethod
    def get_transformation_by_key(cls, key: str) -> Optional[Dict]:
        """Get specific transformation by key"""
        return cls.TRANSFORMATIONS.get(key)
    
    @classmethod
    def list_transformations(cls) -> List[str]:
        """List all available transformation keys"""
        return list(cls.TRANSFORMATIONS.keys())
    
    @classmethod
    def get_transformations_by_category(cls, category: str) -> List[str]:
        """Get all transformation keys for a category"""
        return [
            key for key, value in cls.TRANSFORMATIONS.items()
            if value['category'] == category
        ]
    
    @classmethod
    def get_category_count(cls) -> Dict[str, int]:
        """Get count of transformations per category"""
        counts = {}
        for value in cls.TRANSFORMATIONS.values():
            cat = value['category']
            counts[cat] = counts.get(cat, 0) + 1
        return counts
    
    @classmethod
    def count_total(cls) -> int:
        """Get total number of transformations"""
        return len(cls.TRANSFORMATIONS)
