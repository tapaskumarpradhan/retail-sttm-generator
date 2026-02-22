"""
Naming Generator - Creates business-semantic table and column names
Ensures no numeric suffixes (_01, _02) or generic placeholders
"""
import random
import re
from typing import List, Dict, Optional


class NamingGenerator:
    """Generates meaningful business names without numeric suffixes"""
    
    # Retail entities with their table and column names
    RETAIL_ENTITIES = {
        'customer': {
            'source_tables': [
                'raw_customers', 'raw_customer_contacts', 'raw_customer_addresses',
                'raw_customer_preferences', 'raw_customer_segments', 'raw_customer_activity',
                'stg_customer_master', 'stg_customer_demographics', 'stg_customer_history'
            ],
            'target_tables': [
                'dim_customer', 'dim_customer_contact', 'dim_customer_address',
                'dim_customer_segment', 'fact_customer_activity', 'fact_customer_lifetime_value'
            ],
            'columns': [
                'customer_id', 'customer_key', 'customer_number', 'customer_code',
                'first_name', 'last_name', 'full_name', 'display_name',
                'email_address', 'phone_number', 'mobile_number', 'fax_number',
                'date_of_birth', 'birth_date', 'age', 'gender',
                'registration_date', 'signup_date', 'first_purchase_date',
                'customer_segment', 'customer_tier', 'vip_status', 'loyalty_tier',
                'lifetime_value', 'total_spend', 'purchase_frequency',
                'preferred_store', 'preferred_channel', 'communication_preference',
                'billing_address_line_1', 'billing_address_line_2', 'billing_city',
                'billing_state', 'billing_country', 'billing_postal_code',
                'shipping_address_line_1', 'shipping_address_line_2', 'shipping_city',
                'shipping_state', 'shipping_country', 'shipping_postal_code',
                'is_active', 'is_verified', 'is_email_subscribed', 'is_sms_subscribed',
                'last_login_date', 'last_purchase_date', 'last_activity_date',
                'referral_source', 'acquisition_channel', 'marketing_consent'
            ]
        },
        'product': {
            'source_tables': [
                'raw_products', 'raw_product_categories', 'raw_product_brands',
                'raw_product_variants', 'raw_product_attributes', 'raw_product_pricing',
                'stg_product_master', 'stg_product_hierarchy', 'stg_product_inventory'
            ],
            'target_tables': [
                'dim_product', 'dim_product_category', 'dim_product_brand',
                'dim_product_variant', 'fact_product_sales', 'fact_inventory_movement'
            ],
            'columns': [
                'product_id', 'product_key', 'product_code', 'sku',
                'product_name', 'product_title', 'product_description', 'short_description',
                'brand_name', 'brand_code', 'category_name', 'category_code',
                'subcategory_name', 'department_name', 'division_name',
                'unit_cost', 'unit_price', 'retail_price', 'wholesale_price',
                'cost_currency', 'price_currency', 'margin_percentage',
                'supplier_id', 'supplier_name', 'manufacturer_name',
                'weight', 'weight_unit', 'dimensions', 'dimension_unit',
                'color', 'size', 'style', 'material', 'pattern',
                'is_active', 'is_featured', 'is_discounted', 'is_perishable',
                'launch_date', 'discontinued_date', 'season', 'collection',
                'tax_code', 'tax_rate', 'minimum_order_quantity', 'reorder_point',
                'shelf_life_days', 'warranty_period_months', 'country_of_origin'
            ]
        },
        'order': {
            'source_tables': [
                'raw_orders', 'raw_order_items', 'raw_order_status_history',
                'raw_order_payments', 'raw_order_shipments', 'raw_order_discounts',
                'stg_sales_orders', 'stg_order_line_items', 'stg_order_fulfillment'
            ],
            'target_tables': [
                'fact_orders', 'fact_order_items', 'fact_order_payments',
                'dim_order_status', 'fact_order_shipments', 'fact_sales_daily'
            ],
            'columns': [
                'order_id', 'order_key', 'order_number', 'order_reference',
                'customer_id', 'customer_key', 'store_id', 'channel_id',
                'order_date', 'order_timestamp', 'order_date_key', 'order_time_key',
                'order_status', 'order_type', 'priority_level', 'sales_rep_id',
                'subtotal_amount', 'discount_amount', 'tax_amount', 'shipping_amount',
                'total_amount', 'grand_total', 'currency_code', 'exchange_rate',
                'payment_method', 'payment_status', 'payment_reference',
                'shipping_method', 'shipping_carrier', 'tracking_number',
                'expected_delivery_date', 'actual_delivery_date', 'delivery_status',
                'item_line_number', 'product_id', 'quantity_ordered', 'quantity_shipped',
                'unit_price', 'extended_price', 'item_discount_amount',
                'source_system', 'created_by', 'created_timestamp', 'updated_timestamp'
            ]
        },
        'inventory': {
            'source_tables': [
                'raw_inventory', 'raw_inventory_transactions', 'raw_stock_levels',
                'raw_warehouse_locations', 'raw_inventory_adjustments', 'raw_stock_movements',
                'stg_inventory_snapshot', 'stg_inventory_daily', 'stg_stock_balance'
            ],
            'target_tables': [
                'fact_inventory', 'fact_inventory_transaction', 'dim_warehouse_location',
                'fact_stock_movement', 'fact_inventory_adjustment'
            ],
            'columns': [
                'inventory_id', 'product_id', 'warehouse_id', 'location_id',
                'quantity_on_hand', 'quantity_reserved', 'quantity_available',
                'reorder_level', 'reorder_quantity', 'safety_stock_level',
                'last_receipt_date', 'last_issue_date', 'last_count_date',
                'transaction_type', 'transaction_quantity', 'transaction_date',
                'source_document', 'adjustment_reason', 'movement_type',
                'zone_code', 'aisle_code', 'shelf_code', 'bin_code',
                'warehouse_name', 'warehouse_type', 'warehouse_status',
                'location_type', 'location_capacity', 'location_utilization'
            ]
        },
        'transaction': {
            'source_tables': [
                'raw_transactions', 'raw_transaction_items', 'raw_transaction_payments',
                'raw_tender_details', 'raw_pos_transactions', 'raw_ecommerce_transactions',
                'stg_transaction_header', 'stg_transaction_line', 'stg_payment_transactions'
            ],
            'target_tables': [
                'fact_transactions', 'fact_transaction_items', 'fact_payments',
                'fact_pos_transactions', 'fact_ecommerce_transactions'
            ],
            'columns': [
                'transaction_id', 'transaction_key', 'transaction_number',
                'transaction_date', 'transaction_timestamp', 'transaction_type',
                'register_id', 'terminal_id', 'cashier_id', 'sales_associate_id',
                'subtotal', 'discount_total', 'tax_total', 'total_amount',
                'tender_type', 'tender_amount', 'change_amount', 'card_last_four',
                'authorization_code', 'transaction_status', 'void_reason',
                'return_flag', 'exchange_flag', 'loyalty_points_earned',
                'coupon_code', 'discount_code', 'promotion_applied'
            ]
        },
        'return': {
            'source_tables': [
                'raw_returns', 'raw_return_items', 'raw_refunds', 'raw_exchanges',
                'raw_return_reasons', 'raw_return_authorizations',
                'stg_return_requests', 'stg_return_processing', 'stg_refund_payments'
            ],
            'target_tables': [
                'fact_returns', 'fact_return_items', 'fact_refunds',
                'dim_return_reason', 'fact_exchanges'
            ],
            'columns': [
                'return_id', 'return_number', 'original_order_id', 'original_transaction_id',
                'return_date', 'return_timestamp', 'return_status',
                'return_reason_code', 'return_reason_description', 'return_category',
                'item_condition', 'restocking_fee', 'refund_amount', 'exchange_product_id',
                'authorization_number', 'authorized_by', 'authorized_date',
                'received_date', 'processed_date', 'refund_method',
                'refund_reference_number', 'store_credit_issued', 'gift_card_issued'
            ]
        },
        'supplier': {
            'source_tables': [
                'raw_suppliers', 'raw_vendor_contacts', 'raw_purchase_orders',
                'raw_purchase_order_items', 'raw_supplier_invoices',
                'stg_supplier_master', 'stg_purchase_orders', 'stg_vendor_payments'
            ],
            'target_tables': [
                'dim_supplier', 'dim_vendor', 'fact_purchase_orders',
                'fact_po_receipts', 'fact_supplier_invoices'
            ],
            'columns': [
                'supplier_id', 'supplier_code', 'supplier_name', 'vendor_number',
                'contact_name', 'contact_email', 'contact_phone',
                'address_line_1', 'address_line_2', 'city', 'state', 'country', 'postal_code',
                'payment_terms', 'lead_time_days', 'minimum_order_amount',
                'preferred_flag', 'certification_status', 'supplier_rating',
                'purchase_order_id', 'po_number', 'po_date', 'po_status',
                'expected_delivery_date', 'actual_delivery_date', 'invoice_number'
            ]
        },
        'promotion': {
            'source_tables': [
                'raw_promotions', 'raw_coupons', 'raw_discounts', 'raw_campaigns',
                'raw_promotion_rules', 'raw_coupon_redemptions',
                'stg_promotion_master', 'stg_campaigns', 'stg_discount_events'
            ],
            'target_tables': [
                'dim_promotion', 'dim_campaign', 'dim_coupon',
                'fact_promotion_usage', 'fact_discount_applied'
            ],
            'columns': [
                'promotion_id', 'promotion_code', 'promotion_name', 'campaign_id',
                'discount_type', 'discount_value', 'discount_percentage',
                'minimum_purchase_amount', 'maximum_discount_amount',
                'start_date', 'end_date', 'effective_date', 'expiration_date',
                'applicable_products', 'excluded_products', 'applicable_categories',
                'usage_limit', 'usage_count', 'redemption_count',
                'coupon_code', 'coupon_type', 'is_stackable', 'is_exclusive'
            ]
        },
        'loyalty': {
            'source_tables': [
                'raw_loyalty_accounts', 'raw_loyalty_transactions', 'raw_points_balances',
                'raw_reward_redemptions', 'raw_tier_history',
                'stg_loyalty_members', 'stg_points_earned', 'stg_points_redeemed'
            ],
            'target_tables': [
                'dim_loyalty_account', 'fact_loyalty_transaction', 'fact_points_earned',
                'fact_points_redeemed', 'fact_tier_changes'
            ],
            'columns': [
                'loyalty_account_id', 'member_number', 'membership_tier',
                'points_balance', 'lifetime_points_earned', 'lifetime_points_redeemed',
                'tier_qualification_points', 'tier_start_date', 'tier_end_date',
                'transaction_type', 'points_amount', 'transaction_date',
                'redemption_id', 'reward_id', 'reward_description', 'points_redeemed',
                'enrollment_date', 'last_activity_date', 'membership_status'
            ]
        },
        'payment': {
            'source_tables': [
                'raw_payments', 'raw_payment_methods', 'raw_payment_transactions',
                'raw_refunds', 'raw_chargebacks', 'raw_settlements',
                'stg_payment_header', 'stg_payment_details', 'stg_payment_reconciliation'
            ],
            'target_tables': [
                'fact_payments', 'dim_payment_method', 'fact_refunds',
                'fact_chargebacks', 'fact_settlements'
            ],
            'columns': [
                'payment_id', 'payment_reference', 'transaction_id', 'order_id',
                'payment_date', 'payment_timestamp', 'payment_amount',
                'payment_method', 'payment_type', 'card_type', 'card_last_four',
                'authorization_code', 'avs_result', 'cvv_result',
                'settlement_date', 'settlement_amount', 'settlement_status',
                'merchant_id', 'gateway_reference', 'batch_number',
                'refund_amount', 'refund_date', 'refund_reason',
                'chargeback_amount', 'chargeback_date', 'chargeback_reason_code'
            ]
        },
        'ecommerce': {
            'source_tables': [
                'raw_web_sessions', 'raw_shopping_carts', 'raw_cart_items',
                'raw_wishlists', 'raw_product_reviews', 'raw_page_views',
                'stg_web_sessions', 'stg_cart_events', 'stg_browse_behavior'
            ],
            'target_tables': [
                'fact_web_sessions', 'fact_cart_events', 'fact_page_views',
                'fact_product_reviews', 'fact_wishlist_items'
            ],
            'columns': [
                'session_id', 'visitor_id', 'customer_id', 'session_start_time',
                'session_end_time', 'session_duration_seconds', 'page_views_count',
                'referrer_url', 'landing_page', 'exit_page', 'device_type',
                'browser_type', 'operating_system', 'ip_address', 'user_agent',
                'cart_id', 'cart_status', 'cart_created_date', 'cart_abandoned_date',
                'wishlist_id', 'wishlist_created_date', 'review_id', 'review_rating',
                'review_text', 'review_date', 'helpful_votes', 'verified_purchase_flag'
            ]
        },
        'warehouse': {
            'source_tables': [
                'raw_picking_tasks', 'raw_packing_tasks', 'raw_shipping_tasks',
                'raw_receiving_tasks', 'raw_putaway_tasks', 'raw_transfer_orders',
                'stg_pick_operations', 'stg_pack_operations', 'stg_ship_operations'
            ],
            'target_tables': [
                'fact_picking', 'fact_packing', 'fact_shipping',
                'fact_receiving', 'fact_inventory_transfers'
            ],
            'columns': [
                'task_id', 'task_type', 'order_id', 'warehouse_id',
                'assigned_to', 'task_status', 'priority_level',
                'start_time', 'completion_time', 'duration_minutes',
                'items_picked', 'items_packed', 'items_shipped',
                'carrier_name', 'service_level', 'tracking_number',
                'ship_date', 'expected_delivery', 'weight', 'package_count',
                'transfer_order_id', 'from_location', 'to_location', 'transfer_quantity'
            ]
        },
        'employee': {
            'source_tables': [
                'raw_employees', 'raw_employee_schedules', 'raw_timesheets',
                'raw_payroll_runs', 'raw_benefits', 'raw_attendance',
                'stg_employee_master', 'stg_time_entries', 'stg_payroll_details'
            ],
            'target_tables': [
                'dim_employee', 'fact_time_entry', 'fact_payroll',
                'fact_attendance', 'fact_schedule'
            ],
            'columns': [
                'employee_id', 'employee_number', 'first_name', 'last_name',
                'email', 'phone', 'hire_date', 'termination_date',
                'job_title', 'department', 'manager_id', 'employment_status',
                'pay_type', 'pay_rate', 'pay_currency', 'standard_hours',
                'schedule_date', 'shift_start', 'shift_end', 'break_duration',
                'timesheet_date', 'hours_worked', 'overtime_hours', 'pay_period',
                'gross_pay', 'tax_deductions', 'net_pay', 'pay_date'
            ]
        },
        'financial': {
            'source_tables': [
                'raw_gl_entries', 'raw_ap_invoices', 'raw_ar_invoices',
                'raw_journal_entries', 'raw_budgets', 'raw_revenue_recognition',
                'stg_general_ledger', 'stg_accounts_payable', 'stg_accounts_receivable'
            ],
            'target_tables': [
                'fact_gl_transactions', 'fact_ap_transactions', 'fact_ar_transactions',
                'fact_budget', 'fact_revenue_recognition'
            ],
            'columns': [
                'gl_entry_id', 'account_number', 'account_name', 'account_type',
                'debit_amount', 'credit_amount', 'transaction_date', 'fiscal_period',
                'cost_center', 'department', 'project_code', 'currency_code',
                'invoice_id', 'vendor_id', 'invoice_date', 'due_date', 'payment_date',
                'invoice_amount', 'amount_paid', 'balance_due', 'aging_category',
                'journal_entry_id', 'entry_type', 'posting_date', 'approved_by',
                'budget_amount', 'actual_amount', 'variance_amount', 'variance_percentage'
            ]
        },
        'marketing': {
            'source_tables': [
                'raw_marketing_campaigns', 'raw_ad_spend', 'raw_email_opens',
                'raw_click_events', 'raw_conversion_events', 'raw_attribution_data',
                'stg_campaign_performance', 'stg_channel_attribution', 'stg_customer_journey'
            ],
            'target_tables': [
                'dim_campaign', 'fact_ad_spend', 'fact_email_metrics',
                'fact_conversions', 'fact_attribution'
            ],
            'columns': [
                'campaign_id', 'campaign_name', 'campaign_type', 'channel',
                'start_date', 'end_date', 'budget_amount', 'spend_amount',
                'impressions', 'clicks', 'conversions', 'revenue_attributed',
                'cost_per_click', 'cost_per_acquisition', 'return_on_ad_spend',
                'email_id', 'sent_count', 'open_count', 'click_count', 'bounce_count',
                'conversion_id', 'touchpoint_sequence', 'attribution_model',
                'attributed_revenue', 'attributed_conversions'
            ]
        }
    }
    
    @classmethod
    def generate_table_name(cls, entity: Optional[str] = None, 
                           layer: str = 'raw') -> str:
        """Generate business-meaningful table name"""
        if entity is None:
            entity = random.choice(list(cls.RETAIL_ENTITIES.keys()))
        
        entity_data = cls.RETAIL_ENTITIES.get(entity, cls.RETAIL_ENTITIES['customer'])
        
        if layer in ['raw', 'staging', 'stg']:
            tables = entity_data['source_tables']
        else:  # target layer
            tables = entity_data['target_tables']
        
        return random.choice(tables)
    
    @classmethod
    def generate_column_name(cls, entity: Optional[str] = None,
                            attribute_category: Optional[str] = None) -> str:
        """Generate business-meaningful column name"""
        if entity is None:
            entity = random.choice(list(cls.RETAIL_ENTITIES.keys()))
        
        entity_data = cls.RETAIL_ENTITIES.get(entity, cls.RETAIL_ENTITIES['customer'])
        columns = entity_data['columns']
        
        if attribute_category:
            # Filter by category keywords
            category_keywords = {
                'id': ['id', 'key', 'code', 'number'],
                'name': ['name', 'title', 'description'],
                'date': ['date', 'timestamp', 'time'],
                'amount': ['amount', 'price', 'cost', 'value', 'total'],
                'status': ['status', 'flag', 'active'],
                'contact': ['email', 'phone', 'address', 'contact']
            }
            
            keywords = category_keywords.get(attribute_category, [])
            if keywords:
                filtered = [col for col in columns if any(kw in col.lower() for kw in keywords)]
                if filtered:
                    return random.choice(filtered)
        
        return random.choice(columns)
    
    @classmethod
    def get_entity_for_table(cls, table_name: str) -> str:
        """Get entity type from table name"""
        table_lower = table_name.lower()
        
        for entity, data in cls.RETAIL_ENTITIES.items():
            all_tables = data.get('source_tables', []) + data.get('target_tables', [])
            for t in all_tables:
                if entity in t.lower() or any(part in t.lower() for part in table_lower.split('_')):
                    return entity
        
        return 'customer'  # Default
    
    @classmethod
    def get_columns_for_entity(cls, entity: str, count: int = 1) -> List[str]:
        """Get multiple column names for an entity"""
        entity_data = cls.RETAIL_ENTITIES.get(entity, cls.RETAIL_ENTITIES['customer'])
        columns = entity_data['columns']
        
        if count >= len(columns):
            return columns
        
        return random.sample(columns, count)
    
    @classmethod
    def validate_no_numeric_suffixes(cls, name: str) -> bool:
        """Ensure name doesn't contain numeric suffixes like _01, _02"""
        pattern = r'_\d+$|_\d+_'
        return not re.search(pattern, name)
    
    @classmethod
    def list_all_entities(cls) -> List[str]:
        """Return list of all available entities"""
        return list(cls.RETAIL_ENTITIES.keys())
    
    @classmethod
    def get_entity_stats(cls) -> Dict:
        """Get statistics about available names"""
        stats = {}
        for entity, data in cls.RETAIL_ENTITIES.items():
            stats[entity] = {
                'source_tables': len(data.get('source_tables', [])),
                'target_tables': len(data.get('target_tables', [])),
                'columns': len(data.get('columns', []))
            }
        return stats
