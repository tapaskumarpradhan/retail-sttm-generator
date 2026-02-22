"""
Transformation Generator - Applies complex transformations to columns
"""
import random
import re
from typing import List, Dict, Tuple, Optional
from .library import TransformationLibrary
from ..scenarios.scenario_manager import ScenarioManager
from ..utils.naming import NamingGenerator
from ..utils.datatypes import DataTypeMapper


class TransformationGenerator:
    """Generates appropriate complex transformations for given columns"""
    
    JOIN_CONDITION_TEMPLATES = {
        'customer_order': "{t1}.customer_id = {t2}.customer_id",
        'customer_address': "{t1}.customer_id = {t2}.customer_id",
        'customer_segment': "{t1}.customer_id = {t2}.customer_id",
        'product_brand': "{t1}.brand_id = {t2}.brand_id",
        'product_category': "{t1}.category_id = {t2}.category_id",
        'product_inventory': "{t1}.product_id = {t2}.product_id",
        'order_item': "{t1}.order_id = {t2}.order_id",
        'order_customer': "{t1}.customer_id = {t2}.customer_id",
        'order_shipment': "{t1}.order_id = {t2}.order_id",
        'order_payment': "{t1}.order_id = {t2}.order_id",
        'transaction_item': "{t1}.transaction_id = {t2}.transaction_id",
        'supplier_product': "{t1}.supplier_id = {t2}.supplier_id",
        'inventory_warehouse': "{t1}.warehouse_id = {t2}.warehouse_id",
        'loyalty_customer': "{t1}.customer_id = {t2}.customer_id",
        'promotion_order': "{t1}.promotion_id = {t2}.promotion_id",
        'generic_id': "{t1}.{join_key} = {t2}.{join_key}",
    }
    
    def __init__(self, library: TransformationLibrary, scenario_manager: ScenarioManager):
        self.library = library
        self.scenario_manager = scenario_manager
    
    def generate_for_columns(self, source_columns: List[str], source_types: List[str],
                            target_column: str, target_type: str,
                            scenario_name: Optional[str] = None,
                            is_join: bool = False,
                            table_aliases: Optional[Dict[str, str]] = None) -> Tuple[str, str, Optional[Dict]]:
        """
        Generate complex transformation for given columns
        
        Returns:
            Tuple of (transformation_logic, description, join_info)
        """
        join_info = None
        
        if is_join and table_aliases:
            transform = self._select_join_transformation(source_types, len(table_aliases), scenario_name)
            sql, join_info = self._build_join_sql(transform, source_columns, source_types, table_aliases)
        else:
            transform = self._select_transformation(source_types, scenario_name)
            sql = self._build_sql(transform, source_columns, source_types)
        
        description = self._build_description(sql, source_columns, target_column)
        
        return sql, description, join_info
    
    def _select_transformation(self, source_types: List[str], 
                              scenario_name: Optional[str] = None) -> Dict:
        """Select appropriate complex transformation based on data types"""
        primary_category = self._get_primary_category(source_types)
        
        candidates = self.library.get_transformations_by_category(primary_category)
        
        if not candidates:
            candidates = self.library.list_transformations()
        
        valid_candidates = []
        for key in candidates:
            transform = self.library.get_transformation_by_key(key)
            if transform:
                min_cols = transform.get('min_cols', 1)
                max_cols = transform.get('max_cols', 1)
                if min_cols <= len(source_types) <= max_cols:
                    valid_candidates.append(key)
        
        if not valid_candidates:
            valid_candidates = candidates
        
        if scenario_name:
            preferred = self.scenario_manager.get_typical_transforms_for_scenario(scenario_name)
            if preferred:
                preferred_valid = [p for p in preferred if p in valid_candidates]
                if preferred_valid:
                    valid_candidates = preferred_valid
        
        selected_key = random.choice(valid_candidates)
        result = self.library.get_transformation_by_key(selected_key)
        return result if result else self.library.get_transformation()
    
    def _get_primary_category(self, source_types: List[str]) -> str:
        """Determine primary category from source types"""
        categories = []
        for st in source_types:
            cat = DataTypeMapper.get_category_from_type(st)
            categories.append(cat)
        
        if categories:
            return max(set(categories), key=categories.count)
        return 'string'
    
    def _select_join_transformation(self, source_types: List[str], num_tables: int,
                                    scenario_name: Optional[str] = None) -> Dict:
        """Select appropriate join transformation based on data types and table count"""
        candidates = self.library.get_transformations_by_category('join')
        
        if not candidates:
            candidates = self.library.list_transformations()
        
        valid_candidates = []
        for key in candidates:
            transform = self.library.get_transformation_by_key(key)
            if transform:
                min_cols = transform.get('min_cols', 1)
                max_cols = transform.get('max_cols', 1)
                join_tables = transform.get('join_tables', 2)
                if min_cols <= len(source_types) <= max_cols and join_tables <= num_tables:
                    valid_candidates.append(key)
        
        if not valid_candidates:
            valid_candidates = candidates
        
        selected_key = random.choice(valid_candidates)
        result = self.library.get_transformation_by_key(selected_key)
        return result if result else self.library.get_transformation()
    
    def _build_join_sql(self, transform: Dict, source_columns: List[str],
                        source_types: List[str], table_aliases: Dict[str, str]) -> Tuple[str, Dict]:
        """Build SQL expression for join transformation"""
        template = transform['template']
        join_type = transform.get('join_type', 'LEFT')
        
        sql = template
        
        all_placeholders = re.findall(r'\{t\d+_col\d+\}', template)
        placeholder_counts = {}
        for ph in all_placeholders:
            match_t = re.search(r't(\d+)_col', ph)
            match_c = re.search(r'col(\d+)\}', ph)
            if match_t and match_c:
                table_num = int(match_t.group(1))
                col_num = int(match_c.group(1))
                key = (table_num, col_num)
                placeholder_counts[key] = placeholder_counts.get(key, 0) + 1
        
        col_assignments = {}
        col_idx = 0
        for (table_num, col_num), count in sorted(placeholder_counts.items()):
            if col_idx < len(source_columns):
                alias = f"t{table_num}"
                col = source_columns[col_idx]
                col_assignments[(table_num, col_num)] = f"{alias}.{col}"
                col_idx += 1
        
        for (table_num, col_num), qualified_col in col_assignments.items():
            placeholder = f"{{t{table_num}_col{col_num}}}"
            sql = sql.replace(placeholder, qualified_col)
        
        remaining_placeholders = re.findall(r'\{t\d+_col\d+\}', sql)
        default_cols = ['id', 'amount', 'status']
        for i, ph in enumerate(remaining_placeholders):
            match_t = re.search(r't(\d+)_col', ph)
            if match_t:
                table_num = int(match_t.group(1))
                alias = f"t{table_num}"
                default_col = default_cols[i % len(default_cols)]
                sql = sql.replace(ph, f"{alias}.{default_col}", 1)
        
        join_info = self._generate_join_info(table_aliases, join_type, source_columns)
        
        return sql, join_info
    
    def _generate_join_info(self, table_aliases: Dict[str, str], join_type: str,
                           source_columns: List[str]) -> Dict:
        """Generate join condition information"""
        join_conditions = []
        tables = list(table_aliases.values())
        aliases = list(table_aliases.keys())
        
        common_keys = ['id', 'customer_id', 'product_id', 'order_id', 'transaction_id',
                      'supplier_id', 'warehouse_id', 'promotion_id', 'brand_id', 'category_id']
        
        for i in range(len(tables) - 1):
            t1_alias = f"t{i + 1}"
            t2_alias = f"t{i + 2}"
            
            join_key = None
            for key in common_keys:
                if any(key in col.lower() for col in source_columns):
                    join_key = key
                    break
            
            if not join_key:
                join_key = random.choice(common_keys[:5])
            
            condition = f"{t1_alias}.{join_key} = {t2_alias}.{join_key}"
            join_conditions.append({
                'left_table': t1_alias,
                'right_table': t2_alias,
                'condition': condition,
                'join_key': join_key
            })
        
        return {
            'join_type': join_type,
            'join_conditions': join_conditions,
            'num_tables': len(tables)
        }
    
    def _build_sql(self, transform: Dict, source_columns: List[str], 
                   source_types: List[str]) -> str:
        """Build SQL expression from transformation template"""
        template = transform['template']
        min_cols = transform.get('min_cols', 1)
        max_cols = transform.get('max_cols', 1)
        category_cols = transform.get('category_cols', None)
        
        # Ensure we have the right number of columns
        num_cols_needed = min(len(source_columns), max_cols)
        num_cols_needed = max(num_cols_needed, min_cols)
        
        # Select columns to use
        selected_cols = source_columns[:num_cols_needed]
        
        # Replace placeholders in template
        sql = template
        
        # Replace {col1}, {col2}, etc.
        for i, col in enumerate(selected_cols, 1):
            sql = sql.replace(f'{{col{i}}}', col)
        
        # Handle special column references in template
        # Check for partition_col, order_col, etc.
        special_cols = ['partition_col', 'order_col']
        for special in special_cols:
            placeholder = f'{{{special}}}'
            if placeholder in sql:
                # Pick a column for this purpose
                if selected_cols:
                    sql = sql.replace(placeholder, random.choice(selected_cols))
        
        # Handle remaining {col} references
        if '{col}' in sql and selected_cols:
            sql = sql.replace('{col}', selected_cols[0])
        
        return sql
    
    def _build_description(self, transform_sql: str, source_cols: List[str], 
                          target_col: str) -> str:
        """Generate human-readable description of the transformation"""
        # Count the complexity
        functions_used = self._extract_functions(transform_sql)
        has_case = 'CASE' in transform_sql.upper()
        has_regex = 'REGEXP' in transform_sql.upper()
        has_window = 'OVER' in transform_sql.upper()
        
        # Build description parts
        parts = []
        
        if has_case:
            parts.append("Conditional logic")
        
        if has_regex:
            parts.append("Pattern matching")
        
        if has_window:
            parts.append("Window function")
        
        if len(functions_used) >= 2:
            parts.append(f"{len(functions_used)} nested functions")
        
        if len(source_cols) >= 2:
            parts.append(f"Multi-column ({len(source_cols)} sources)")
        
        if not parts:
            parts.append("Complex transformation")
        
        # Add target context
        description = f"Transforms {', '.join(source_cols)} into {target_col} using {'; '.join(parts)}"
        
        return description
    
    def _extract_functions(self, sql: str) -> List[str]:
        """Extract SQL function names from expression"""
        # Pattern to match function calls
        pattern = r'\b([A-Z_]+)\s*\('
        matches = re.findall(pattern, sql.upper())
        return list(set(matches))  # Remove duplicates
    
    def generate_multiple_source_columns(self, entity: str, min_cols: int = 2, 
                                        max_cols: int = 3) -> Tuple[List[str], List[str]]:
        """
        Generate multiple source columns with appropriate types
        
        Returns:
            Tuple of (column_names, data_types)
        """
        # Get columns for entity
        all_columns = NamingGenerator.get_columns_for_entity(entity, count=max_cols + 5)
        
        # Select random columns
        num_cols = random.randint(min_cols, min(max_cols, len(all_columns)))
        selected = random.sample(all_columns, num_cols)
        
        # Assign realistic types based on column names
        types = []
        for col in selected:
            col_lower = col.lower()
            if any(x in col_lower for x in ['date', 'time', 'timestamp']):
                types.append(random.choice(['DATE', 'DATETIME', 'TIMESTAMP']))
            elif any(x in col_lower for x in ['amount', 'price', 'cost', 'value', 'total', 'rate']):
                types.append(random.choice(['DECIMAL(18,2)', 'NUMERIC(18,2)']))
            elif any(x in col_lower for x in ['id', 'key', 'code', 'number', 'count', 'qty']):
                types.append(random.choice(['INT', 'BIGINT']))
            elif any(x in col_lower for x in ['flag', 'is_', 'status']):
                types.append(random.choice(['BOOLEAN', 'INT']))
            else:
                types.append(DataTypeMapper.get_random_source_type('string'))
        
        return selected, types
    
    def generate_multi_table_sources(self, entities: List[str], 
                                     tables_per_entity: int = 1) -> Tuple[Dict[str, str], List[str], List[str]]:
        """
        Generate source tables and columns for multi-table join transformations
        
        Args:
            entities: List of entity types to include (e.g., ['customer', 'order'])
            tables_per_entity: Number of tables per entity (default 1)
            
        Returns:
            Tuple of (table_aliases_dict, column_names, data_types)
        """
        table_aliases = {}
        all_columns = []
        all_types = []
        
        for i, entity in enumerate(entities):
            alias = f"t{i + 1}"
            
            source_table = NamingGenerator.generate_table_name(entity, layer='raw')
            table_aliases[alias] = source_table
            
            columns = NamingGenerator.get_columns_for_entity(entity, count=3)
            
            for col in columns[:3]:
                all_columns.append(col)
                
                col_lower = col.lower()
                if any(x in col_lower for x in ['date', 'time', 'timestamp']):
                    all_types.append(random.choice(['DATE', 'DATETIME', 'TIMESTAMP']))
                elif any(x in col_lower for x in ['amount', 'price', 'cost', 'value', 'total', 'rate']):
                    all_types.append(random.choice(['DECIMAL(18,2)', 'NUMERIC(18,2)']))
                elif any(x in col_lower for x in ['id', 'key', 'code', 'number', 'count', 'qty']):
                    all_types.append(random.choice(['INT', 'BIGINT']))
                elif any(x in col_lower for x in ['flag', 'is_', 'status']):
                    all_types.append(random.choice(['BOOLEAN', 'INT']))
                else:
                    all_types.append(DataTypeMapper.get_random_source_type('string'))
        
        return table_aliases, all_columns, all_types
    
    def get_related_entities(self, primary_entity: str, num_related: int = 1) -> List[str]:
        """
        Get related entities for join transformations
        
        Args:
            primary_entity: The main entity type
            num_related: Number of related entities to return
            
        Returns:
            List of related entity names
        """
        entity_relationships = {
            'customer': ['order', 'loyalty', 'transaction', 'return'],
            'order': ['customer', 'product', 'payment', 'shipment', 'return'],
            'product': ['inventory', 'supplier', 'promotion', 'category'],
            'inventory': ['product', 'warehouse', 'supplier'],
            'transaction': ['customer', 'product', 'payment', 'promotion'],
            'payment': ['order', 'transaction', 'customer'],
            'supplier': ['product', 'inventory', 'order'],
            'promotion': ['order', 'product', 'customer'],
            'loyalty': ['customer', 'transaction', 'order'],
            'warehouse': ['inventory', 'order', 'employee'],
            'employee': ['warehouse', 'order', 'transaction'],
            'financial': ['order', 'payment', 'supplier'],
            'marketing': ['customer', 'order', 'promotion'],
            'return': ['order', 'customer', 'product'],
            'ecommerce': ['customer', 'product', 'order']
        }
        
        related = entity_relationships.get(primary_entity, ['customer', 'order'])
        
        if len(related) <= num_related:
            return related
        
        return random.sample(related, num_related)
