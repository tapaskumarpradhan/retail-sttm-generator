# STTM Generator - Agent Guidelines

This file provides guidance for AI agents working on this codebase.

## Project Overview

STTM Generator is a Python library that generates Source-to-Target Mappings (STTM) for ETL pipeline testing. It produces production-like data mappings with complex SQL transformations (2+ nested functions or CASE logic), including multi-table JOIN transformations.

## Project Structure

```
sttm_generator/
├── __init__.py
├── sttm_generator.py       # Main orchestrator (STTMGenerator class)
├── cli.py                  # Command-line interface
├── scenarios/
│   ├── scenario_manager.py
│   └── retail_scenarios.py
├── transformations/
│   ├── library.py          # 60+ complex transformations
│   └── generators.py
└── utils/
    ├── datatypes.py
    ├── naming.py
    └── formatters.py
```

---

## Build / Lint / Test Commands

### Running the Application

```bash
# Generate 10 STTM records (default)
python3 -m sttm_generator.cli --rows 10

# Generate with specific format
python3 -m sttm_generator.cli --rows 50 --format excel

# Deterministic generation with seed
python3 -m sttm_generator.cli --rows 100 --seed 42

# Custom output path (saved to output/ folder)
python3 -m sttm_generator.cli -o custom_output.csv

# List available scenarios
python3 -m sttm_generator.cli --list-scenarios

# Show transformation statistics
python3 -m sttm_generator.cli --show-stats
```

### Testing

```bash
# Run all tests
python3 -m pytest tests/

# Run a specific test file
python3 -m pytest tests/test_sttm_generator.py

# Run a single test
python3 -m pytest tests/test_sttm_generator.py::test_function_name -v

# Run tests matching a pattern
python3 -m pytest -k "test_name_pattern"

# Run with verbose output
python3 -m pytest -v

# Run with coverage (if pytest-cov installed)
python3 -m pytest --cov=sttm_generator --cov-report=term-missing
```

### Linting / Code Quality

```bash
# Run ruff linter
ruff check sttm_generator/

# Run ruff with auto-fix
ruff check --fix sttm_generator/

# Run ruff formatter
ruff format sttm_generator/

# Run mypy type checker
python3 -m mypy sttm_generator/

# Run all checks
ruff check sttm_generator/ && ruff format --check sttm_generator/ && python3 -m mypy sttm_generator/
```

### Basic Validation

```bash
# Quick import test
python3 -c "from sttm_generator import STTMGenerator; g = STTMGenerator(seed=42); print(len(g.generate(rows=5)))"

# Run usage examples
python3 examples/usage_examples.py
```

---

## Code Style Guidelines

### General Principles

- **No comments** unless explaining complex business logic
- **Single quotes** for strings (e.g., `'example'`, not `"example"`)
- **No emojis** in code
- **Concise code** - prefer direct solutions over verbose ones

### Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Classes | PascalCase | `STTMGenerator`, `TransformationLibrary` |
| Functions/Variables | snake_case | `generate_mappings()`, `source_table` |
| Constants | UPPER_SNAKE_CASE | `MAX_ROWS = 1000` |
| Private methods | snake_case with underscore prefix | `_generate_single_record()` |

### File Structure

- All Python files in `sttm_generator/` package
- Each module should have a docstring at the top
- Group imports: standard library, third-party, local
- Maximum line length: 100 characters (soft limit: 120)

### Type Hints

```python
from typing import List, Dict, Optional, Tuple

def generate(self, rows: int, scenario: Optional[str] = None) -> List[Dict]:
    """Generate STTM records."""
    pass

class STTMGenerator:
    def __init__(self, seed: Optional[int] = None, join_probability: float = 0.3):
        self.seed: Optional[int] = seed
        self.join_probability: float = join_probability
```

### Imports

```python
# Standard library first
import os
import sys
import random
from typing import List, Dict, Optional

# Third-party (alphabetical)
from openpyxl import Workbook

# Local imports
from sttm_generator.scenarios.scenario_manager import ScenarioManager
from sttm_generator.transformations.library import TransformationLibrary
```

### Error Handling

```python
try:
    OutputFormatter.to_csv(data, filepath)
except ValueError as e:
    print(f"Error: {e}")
    sys.exit(1)

try:
    from openpyxl import Workbook
except ImportError:
    raise ImportError("openpyxl required for Excel output. Install with: pip install openpyxl")
```

### Class Structure

```python
class STTMGenerator:
    """Main STTM Generator class."""
    
    COLUMNS = ['col1', 'col2', 'col3']
    
    def __init__(self, seed: Optional[int] = None):
        """Initialize generator."""
        self.seed = seed
    
    def generate(self, rows: int) -> List[Dict]:
        """Generate STTM records."""
        results = []
        for _ in range(rows):
            results.append(self._generate_single())
        return results
    
    @classmethod
    def list_available_scenarios(cls) -> List[str]:
        """List all available scenarios."""
        pass
```

### Transformations

All transformations MUST meet these criteria:
- Minimum 2 nested functions OR CASE/WHEN logic
- Multiple source columns (typically 2-3)
- Syntactically valid SQL
- Business semantic naming (no `table_01`, `col_02`)

---

## Common Development Tasks

### Adding a New Transformation

1. Add to `TransformationLibrary.TRANSFORMATIONS` dict
2. Include: template, min_cols, max_cols, category, description
3. Test with: `python3 -c "from sttm_generator import STTMGenerator; g = STTMGenerator(seed=42); print(g.generate(rows=5))"`

### Adding a New Scenario

1. Add scenario data to `scenarios/retail_scenarios.py`
2. Register in `ScenarioManager` if needed

### Output Paths

- Default output directory: `output/`
- CLI creates the directory if it doesn't exist
- Absolute paths bypass the output/ prefix

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `sttm_generator.py` | Main STTMGenerator class |
| `cli.py` | Command-line interface |
| `transformations/library.py` | 60+ SQL transformation templates |
| `transformations/generators.py` | Transformation generation logic |
| `scenarios/retail_scenarios.py` | 15 retail business scenarios |
| `utils/naming.py` | Business semantic naming (450+ columns) |
| `utils/formatters.py` | CSV/Excel/Markdown output |
| `utils/datatypes.py` | Source/target data type mapping |

---

## Available Scenarios

1. **Customer Management** - Customer profiles, demographics, preferences, and segments
2. **Product Catalog** - Products, categories, brands, SKUs, and variants
3. **Inventory Management** - Stock levels, warehouse locations, movements, and adjustments
4. **Sales Orders** - Orders, line items, status tracking, and fulfillment
5. **Point of Sale** - Transactions, receipts, payments, and register data
6. **Returns & Refunds** - Return requests, refunds, exchanges, and authorization
7. **Supplier Management** - Vendors, purchase orders, deliveries, and invoices
8. **Promotions & Discounts** - Campaigns, coupons, pricing rules, and discount applications
9. **Loyalty Program** - Points, tiers, rewards, and member activities
10. **Payment Processing** - Payment methods, transactions, refunds, and settlements
11. **E-commerce** - Web sessions, shopping carts, wishlists, and reviews
12. **Warehouse Operations** - Picking, packing, shipping, receiving, and transfers
13. **HR & Payroll** - Employees, schedules, timesheets, and payroll runs
14. **Financial Reporting** - GL entries, AP/AR, revenue recognition, and budgets
15. **Marketing Analytics** - Campaigns, channels, attribution, and conversions

---

## Notes

- No LLM dependencies - pure Python implementation
- Deterministic output with seeds for regression testing
- Default join probability is 0.3 (30% of transforms include JOINs)
- Excel output requires: `pip install openpyxl`
- Output filenames include scenario name and timestamp (e.g., `Customer_Management_sttm_20260222_120000.csv`)
