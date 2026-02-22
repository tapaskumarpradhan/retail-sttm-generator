"""
Retail Scenarios - 15 comprehensive retail business scenarios
"""
from typing import Dict, List, Any


RETAIL_SCENARIOS = {
    "Customer Management": {
        "description": "Customer profiles, demographics, preferences, and segments",
        "entities": ["customer"],
        "source_tables": [
            "raw_customers", "raw_customer_contacts", "raw_customer_addresses",
            "raw_customer_preferences", "raw_customer_segments"
        ],
        "target_tables": [
            "dim_customer", "dim_customer_contact", "dim_customer_address",
            "dim_customer_segment", "fact_customer_activity"
        ],
        "typical_transforms": [
            "concat_upper_trim", "case_trim_length", "date_format_case",
            "coalesce_nullif", "regex_replace_standardize"
        ]
    },
    
    "Product Catalog": {
        "description": "Products, categories, brands, SKUs, and variants",
        "entities": ["product"],
        "source_tables": [
            "raw_products", "raw_product_categories", "raw_product_brands",
            "raw_product_variants", "raw_product_attributes"
        ],
        "target_tables": [
            "dim_product", "dim_product_category", "dim_product_brand",
            "dim_product_variant"
        ],
        "typical_transforms": [
            "concat_upper_trim", "case_string_validation", "numeric_calculation_case",
            "regex_extract_pattern", "null_handling_coalesce"
        ]
    },
    
    "Inventory Management": {
        "description": "Stock levels, warehouse locations, movements, and adjustments",
        "entities": ["inventory"],
        "source_tables": [
            "raw_inventory", "raw_inventory_transactions", "raw_stock_levels",
            "raw_warehouse_locations", "raw_inventory_adjustments"
        ],
        "target_tables": [
            "fact_inventory", "fact_inventory_transaction", "dim_warehouse_location",
            "fact_stock_movement"
        ],
        "typical_transforms": [
            "numeric_calculation_nullif", "aggregation_window", "date_diff_case",
            "case_numeric_range", "coalesce_calculation"
        ]
    },
    
    "Sales Orders": {
        "description": "Orders, line items, status tracking, and fulfillment",
        "entities": ["order"],
        "source_tables": [
            "raw_orders", "raw_order_items", "raw_order_status_history",
            "raw_order_payments", "raw_order_shipments"
        ],
        "target_tables": [
            "fact_orders", "fact_order_items", "fact_order_payments",
            "dim_order_status"
        ],
        "typical_transforms": [
            "case_amount_calculation", "date_format_case", "concat_case",
            "aggregation_window_partition", "regex_extract_validation"
        ]
    },
    
    "Point of Sale": {
        "description": "Transactions, receipts, payments, and register data",
        "entities": ["transaction"],
        "source_tables": [
            "raw_transactions", "raw_transaction_items", "raw_tender_details",
            "raw_pos_transactions"
        ],
        "target_tables": [
            "fact_transactions", "fact_transaction_items", "fact_pos_transactions"
        ],
        "typical_transforms": [
            "case_tender_validation", "amount_calculation_case", "date_timestamp_convert",
            "concat_receipt_format", "regex_mask_sensitive"
        ]
    },
    
    "Returns & Refunds": {
        "description": "Return requests, refunds, exchanges, and authorization",
        "entities": ["return"],
        "source_tables": [
            "raw_returns", "raw_return_items", "raw_refunds", 
            "raw_return_reasons", "raw_return_authorizations"
        ],
        "target_tables": [
            "fact_returns", "fact_return_items", "fact_refunds", "dim_return_reason"
        ],
        "typical_transforms": [
            "case_refund_calculation", "date_diff_validation", "concat_reason_code",
            "amount_negative_case", "regex_extract_order_ref"
        ]
    },
    
    "Supplier Management": {
        "description": "Vendors, purchase orders, deliveries, and invoices",
        "entities": ["supplier"],
        "source_tables": [
            "raw_suppliers", "raw_vendor_contacts", "raw_purchase_orders",
            "raw_purchase_order_items", "raw_supplier_invoices"
        ],
        "target_tables": [
            "dim_supplier", "fact_purchase_orders", "fact_po_receipts", "fact_supplier_invoices"
        ],
        "typical_transforms": [
            "date_calculation_lead_time", "case_delivery_status", "concat_po_reference",
            "amount_variance_calculation", "regex_extract_invoice_num"
        ]
    },
    
    "Promotions & Discounts": {
        "description": "Campaigns, coupons, pricing rules, and discount applications",
        "entities": ["promotion"],
        "source_tables": [
            "raw_promotions", "raw_coupons", "raw_discounts", 
            "raw_campaigns", "raw_promotion_rules"
        ],
        "target_tables": [
            "dim_promotion", "dim_campaign", "dim_coupon", "fact_promotion_usage"
        ],
        "typical_transforms": [
            "case_discount_calculation", "date_range_validation", "percentage_calculation_case",
            "concat_promo_code", "regex_validate_coupon"
        ]
    },
    
    "Loyalty Program": {
        "description": "Points, tiers, rewards, and member activities",
        "entities": ["loyalty"],
        "source_tables": [
            "raw_loyalty_accounts", "raw_loyalty_transactions", "raw_points_balances",
            "raw_reward_redemptions", "raw_tier_history"
        ],
        "target_tables": [
            "dim_loyalty_account", "fact_loyalty_transaction", 
            "fact_points_earned", "fact_points_redeemed"
        ],
        "typical_transforms": [
            "case_tier_qualification", "points_calculation_aggregation", "date_diff_tier",
            "concat_member_id", "coalesce_points_balance"
        ]
    },
    
    "Payment Processing": {
        "description": "Payment methods, transactions, refunds, and settlements",
        "entities": ["payment"],
        "source_tables": [
            "raw_payments", "raw_payment_methods", "raw_payment_transactions",
            "raw_refunds", "raw_settlements"
        ],
        "target_tables": [
            "fact_payments", "dim_payment_method", "fact_refunds", "fact_settlements"
        ],
        "typical_transforms": [
            "case_payment_validation", "amount_settlement_calculation", "date_reconciliation",
            "regex_mask_card", "concat_transaction_ref"
        ]
    },
    
    "E-commerce": {
        "description": "Web sessions, shopping carts, wishlists, and reviews",
        "entities": ["ecommerce"],
        "source_tables": [
            "raw_web_sessions", "raw_shopping_carts", "raw_cart_items",
            "raw_wishlists", "raw_product_reviews"
        ],
        "target_tables": [
            "fact_web_sessions", "fact_cart_events", "fact_product_reviews", "fact_wishlist_items"
        ],
        "typical_transforms": [
            "case_session_status", "duration_calculation", "concat_url_tracking",
            "regex_extract_domain", "aggregation_window_session"
        ]
    },
    
    "Warehouse Operations": {
        "description": "Picking, packing, shipping, receiving, and transfers",
        "entities": ["warehouse"],
        "source_tables": [
            "raw_picking_tasks", "raw_packing_tasks", "raw_shipping_tasks",
            "raw_receiving_tasks", "raw_transfer_orders"
        ],
        "target_tables": [
            "fact_picking", "fact_packing", "fact_shipping", "fact_inventory_transfers"
        ],
        "typical_transforms": [
            "duration_calculation_task", "case_task_status", "concat_tracking_ref",
            "weight_calculation_case", "date_expected_delivery"
        ]
    },
    
    "HR & Payroll": {
        "description": "Employees, schedules, timesheets, and payroll runs",
        "entities": ["employee"],
        "source_tables": [
            "raw_employees", "raw_employee_schedules", "raw_timesheets",
            "raw_payroll_runs", "raw_attendance"
        ],
        "target_tables": [
            "dim_employee", "fact_time_entry", "fact_payroll", "fact_attendance"
        ],
        "typical_transforms": [
            "hours_calculation_overtime", "case_pay_type", "date_pay_period",
            "amount_deduction_calculation", "concat_employee_name"
        ]
    },
    
    "Financial Reporting": {
        "description": "GL entries, AP/AR, revenue recognition, and budgets",
        "entities": ["financial"],
        "source_tables": [
            "raw_gl_entries", "raw_ap_invoices", "raw_ar_invoices",
            "raw_journal_entries", "raw_budgets"
        ],
        "target_tables": [
            "fact_gl_transactions", "fact_ap_transactions", 
            "fact_ar_transactions", "fact_budget"
        ],
        "typical_transforms": [
            "case_entry_type", "amount_debit_credit", "date_fiscal_period",
            "variance_calculation", "aging_categorization"
        ]
    },
    
    "Marketing Analytics": {
        "description": "Campaigns, channels, attribution, and conversions",
        "entities": ["marketing"],
        "source_tables": [
            "raw_marketing_campaigns", "raw_ad_spend", "raw_email_opens",
            "raw_conversion_events", "raw_attribution_data"
        ],
        "target_tables": [
            "dim_campaign", "fact_ad_spend", "fact_email_metrics", "fact_attribution"
        ],
        "typical_transforms": [
            "case_attribution_model", "percentage_calculation_roas", "date_campaign_period",
            "aggregation_conversion_rate", "concat_utm_parameters"
        ]
    }
}


def get_scenario(name: str) -> Dict[str, Any]:
    """Get scenario by name"""
    return RETAIL_SCENARIOS.get(name)


def list_scenarios() -> List[str]:
    """List all available scenario names"""
    return list(RETAIL_SCENARIOS.keys())


def get_scenario_entities(scenario_name: str) -> List[str]:
    """Get entities for a specific scenario"""
    scenario = RETAIL_SCENARIOS.get(scenario_name)
    if scenario:
        return scenario.get("entities", [])
    return []
