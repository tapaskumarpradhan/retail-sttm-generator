# STTM Generator

A Python-based Source-to-Target Mapping (STTM) generator for ETL pipeline testing. Generates production-like data mappings with **complex SQL transformations only** (2+ nested functions or conditional logic). Now supports **multi-table JOIN transformations**.

## Features

- **75+ Complex Transformations** - All transformations use 2+ nested functions or CASE logic
- **15 Multi-Table Join Transformations** - NEW: JOIN-based transformations across multiple source tables
- **15 Retail Business Scenarios** - Complete retail ecosystem coverage
- **No LLM Dependencies** - Pure Python implementation
- **Deterministic & Random Modes** - Reproducible with seeds or random each time
- **Multiple Output Formats** - CSV, Excel, and Markdown
- **CLI & Python API** - Use from command line or import as module

## Installation

No installation required. Just clone and use:

```bash
git clone https://github.com/tapaskumarpradhan/retail-sttm-generator.git
cd retail-sttm-generator
```

### Optional Dependencies

For Excel output support:
```bash
pip install openpyxl
```

## Quick Start

### Command Line

```bash
# Generate 50 random STTM records
python3 -m sttm_generator.cli --rows 50

# Deterministic generation with seed
python3 -m sttm_generator.cli --rows 100 --seed 42 --format excel

# Specific retail scenario
python3 -m sttm_generator.cli --scenario "Customer Management" --rows 20

# List all scenarios
python3 -m sttm_generator.cli --list-scenarios

# Show statistics
python3 -m sttm_generator.cli --show-stats
```

### Python API

```python
from sttm_generator import STTMGenerator

# Generate random mappings (default 30% join transforms)
generator = STTMGenerator()
mappings = generator.generate(rows=50)

# Control join transformation probability
generator = STTMGenerator(join_probability=0.5)  # 50% joins
mappings = generator.generate(rows=100)

# Disable join transformations
generator = STTMGenerator(join_probability=0.0)  # No joins
mappings = generator.generate(rows=100)

# Deterministic generation
generator = STTMGenerator(seed=42)
mappings = generator.generate(rows=100, scenario="Sales Orders")

# Save to different formats
generator.save_csv(mappings, "output.csv")
generator.save_excel(mappings, "output.xlsx")
generator.save_text(mappings, "output.md")

# Get as markdown table
markdown = generator.to_markdown_table(mappings)
print(markdown)
```

## Output Schema

| Column | Description |
|--------|-------------|
| `source_table` | Source table name (business semantic) |
| `source_column` | Source column name(s) - multiple for complex transforms |
| `source_data_type` | Source data type(s) |
| `target_table` | Target table name (business semantic) |
| `target_column` | Target column name |
| `transformation_logic` | **Complex SQL expression** (2+ nested functions or CASE) |
| `target_data_type` | Target data type |
| `is_nullable` | True/False based on business logic |
| `description` | Human-readable transformation description |
| `is_join` | True if transformation involves multiple tables |
| `join_type` | Type of join (LEFT, INNER, FULL OUTER) - for join transforms |
| `join_conditions` | JSON array of join conditions - for join transforms |
| `additional_source_tables` | JSON array of additional source tables - for join transforms |

## Complex Transformation Examples

### String Transformations
```sql
CONCAT(UPPER(TRIM(customer_name)), ' - ', LPAD(customer_id, 10, '0'))

CASE WHEN LENGTH(TRIM(email)) > 0 
     THEN UPPER(TRIM(email)) 
     ELSE 'UNKNOWN' 
END

REGEXP_REPLACE(REGEXP_REPLACE(phone, '[^0-9]', ''), '^0+', '')
```

### Date/Time Transformations
```sql
CASE WHEN order_date IS NOT NULL 
     THEN DATE_FORMAT(order_date, '%Y-%m-%d') 
     ELSE '1900-01-01' 
END

CONCAT(EXTRACT(YEAR FROM timestamp), 
       LPAD(EXTRACT(MONTH FROM timestamp), 2, '0'))

DATEDIFF(CURRENT_DATE, DATE_ADD(ship_date, INTERVAL 7 DAY))
```

### Numeric Transformations
```sql
ROUND((amount * quantity) / NULLIF(discount_rate, 0), 2)

CASE WHEN amount > 0 THEN FLOOR(amount) ELSE CEIL(amount) END

ROUND((SUM(sales) OVER (PARTITION BY region)) * 100.0 / 
      NULLIF(SUM(sales) OVER (), 0), 4)
```

### Conditional Transformations
```sql
CASE WHEN status IN ('ACTIVE', 'LIVE', 'ENABLED') THEN 1 ELSE 0 END

COALESCE(NULLIF(TRIM(notes), ''), 'No notes provided')

CASE WHEN REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
     THEN LOWER(email) 
     ELSE NULL 
END
```

### Aggregation Transformations
```sql
SUM(amount) OVER (PARTITION BY customer_id 
                  ORDER BY order_date 
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

CASE WHEN RANK() OVER (PARTITION BY category ORDER BY sales DESC) <= 10 
     THEN 'TOP_10' 
     ELSE 'OTHER' 
END
```

### Multi-Table JOIN Transformations (NEW)
```sql
-- Join customer and order tables
CONCAT(UPPER(TRIM(t1.customer_name)), ' - ', COALESCE(t2.order_id, 'N/A'), 
       ' - $', FORMAT(COALESCE(t2.total_amount, 0), 2))

-- Lookup customer segment with fallback calculation
COALESCE(t2.segment, CASE WHEN t1.lifetime_value > 10000 THEN 'PREMIUM' 
                          WHEN t1.lifetime_value > 5000 THEN 'GOLD' 
                          ELSE 'STANDARD' END)

-- Calculate profit margin from sales and cost tables
ROUND(((t1.sale_price - COALESCE(t2.unit_cost, 0)) / NULLIF(t1.sale_price, 0)) * 100, 2)

-- Three-table join for stock availability
CASE WHEN COALESCE(t1.quantity_on_hand, 0) - COALESCE(t2.reserved_qty, 0) > 0 
     THEN 'IN_STOCK' 
     WHEN COALESCE(t3.reorder_qty, 0) > 0 THEN 'BACKORDER' 
     ELSE 'OUT_OF_STOCK' END
```

## Retail Scenarios

1. **Customer Management** - Profiles, demographics, preferences, segments
2. **Product Catalog** - Products, categories, brands, SKUs
3. **Inventory Management** - Stock levels, warehouse, movements
4. **Sales Orders** - Orders, line items, fulfillment
5. **Point of Sale** - Transactions, receipts, payments
6. **Returns & Refunds** - Returns, refunds, exchanges
7. **Supplier Management** - Vendors, POs, deliveries
8. **Promotions & Discounts** - Campaigns, coupons, pricing
9. **Loyalty Program** - Points, tiers, rewards
10. **Payment Processing** - Methods, transactions, settlements
11. **E-commerce** - Sessions, carts, wishlists, reviews
12. **Warehouse Operations** - Picking, packing, shipping
13. **HR & Payroll** - Employees, schedules, payroll
14. **Financial Reporting** - GL, AP/AR, revenue
15. **Marketing Analytics** - Campaigns, channels, attribution

## Architecture

```
sttm_generator/
├── sttm_generator.py          # Main orchestrator
├── cli.py                     # Command line interface
├── scenarios/
│   ├── retail_scenarios.py    # 15 retail scenarios
│   └── scenario_manager.py    # Scenario management
├── transformations/
│   ├── library.py             # 60+ complex transformations
│   └── generators.py          # Transformation generation
└── utils/
    ├── datatypes.py           # Data type mapping
    ├── naming.py              # Business naming (200+ columns)
    └── formatters.py          # CSV/Excel/Text output
```

## Transformation Categories

| Category | Count | Examples |
|----------|-------|----------|
| String | 12 | CONCAT, UPPER, TRIM, LPAD, REGEXP_REPLACE |
| Date/Time | 10 | DATE_FORMAT, DATEDIFF, CASE with date logic |
| Numeric | 10 | ROUND, calculations with NULLIF, aggregations |
| Conditional | 10 | CASE WHEN, COALESCE, NULLIF |
| Pattern | 8 | REGEXP_LIKE, REGEXP_EXTRACT, validation |
| Aggregation | 6 | Window functions, RANK, SUM OVER |
| **Join (NEW)** | **15** | Multi-table lookups, JOIN calculations, cross-table aggregations |

**Total: 75+ complex transformations**

## Business Rules

### Naming Conventions
- ✓ **Business semantic names only**: `customer_orders`, `order_total_amount`
- ✗ **No numeric suffixes**: `table_01`, `col_02`, `field_003`
- ✗ **No generic placeholders**: `table_x`, `col_y`, `field_tmp`

### Transformation Requirements
- ✓ **Every transformation is complex**: 2+ nested functions OR CASE logic
- ✓ **Multiple source columns**: Most transformations use 2-3 source columns
- ✓ **Explicit column references**: All SQL expressions reference source columns
- ✓ **Syntactically valid**: All SQL is valid and executable

## Statistics

- **15** Retail business scenarios
- **75** Complex transformation patterns (including 15 JOIN transforms)
- **211** Unique table names (source + target)
- **450** Unique column names
- **15** Business entities (customer, product, order, etc.)
- **3** Table join support (up to 3 tables in a single transformation)

## Use Cases

### ETL Pipeline Testing
```python
# Generate test mappings for ETL validation
generator = STTMGenerator(seed=42)  # Deterministic
mappings = generator.generate(rows=1000)
generator.save_csv(mappings, "etl_test_mappings.csv")
```

### Regression Testing
```python
# Same seed = same output every time
for seed in [1, 2, 3, 4, 5]:
    generator = STTMGenerator(seed=seed)
    mappings = generator.generate(rows=100)
    # Validate ETL produces same results
```

### Performance Testing
```python
# Generate large datasets
generator = STTMGenerator()
mappings = generator.generate(rows=10000)
generator.save_excel(mappings, "performance_test.xlsx")
```

### Documentation
```python
# Generate markdown for documentation
generator = STTMGenerator()
mappings = generator.generate(rows=50)
markdown = generator.to_markdown_table(mappings)
# Include in data dictionary
```

## CLI Reference

```
python3 -m sttm_generator.cli [OPTIONS]
```

## API Reference

### STTMGenerator

```python
class STTMGenerator(seed=None, join_probability=0.3)
```

**Parameters:**
- `seed` (int, optional): Random seed for deterministic output
- `join_probability` (float, default 0.3): Probability of generating multi-table join transformations (0.0-1.0)

**Methods:**
- `generate(rows, scenario=None)` - Generate STTM records
- `save_csv(data, filepath)` - Save as CSV
- `save_excel(data, filepath)` - Save as Excel
- `save_text(data, filepath)` - Save as Markdown
- `to_markdown_table(data)` - Return markdown string

**Class Methods:**
- `list_available_scenarios()` - List all scenarios
- `get_transformation_count()` - Get total transformations
- `get_transformation_categories()` - Get counts by category

### ScenarioManager

```python
ScenarioManager.list_scenarios()           # List scenario names
ScenarioManager.get_scenario(name)         # Get scenario details
ScenarioManager.get_scenario_description(name)  # Get description
```

## Testing

Run the test suite:
```bash
python -m pytest tests/
```

Basic validation:
```bash
python -c "from sttm_generator import STTMGenerator; g = STTMGenerator(); print(g.generate(rows=5))"
```

## License

MIT License - See LICENSE file

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Support

For issues and feature requests, please use the GitHub issue tracker.

---

**Built with ❤️ for ETL testing**  
*No LLMs were harmed in the making of this generator* 😄
