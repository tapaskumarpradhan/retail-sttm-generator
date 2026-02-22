"""
STTM Generator - Main orchestrator class
Generates production-like source-to-target mappings with complex transformations
"""
import random
from typing import List, Dict, Optional, Tuple

from .scenarios.scenario_manager import ScenarioManager
from .scenarios.retail_scenarios import get_scenario_entities
from .transformations.library import TransformationLibrary
from .transformations.generators import TransformationGenerator
from .utils.datatypes import DataTypeMapper
from .utils.naming import NamingGenerator
from .utils.formatters import OutputFormatter


class STTMGenerator:
    """
    Main STTM Generator class.
    
    Generates production-like source-to-target mappings for ETL testing.
    All transformations are complex with 2+ nested functions or conditional logic.
    Supports deterministic (seeded) and random generation modes.
    Supports both single-table and multi-table join transformations.
    """
    
    COLUMNS = [
        'source_table', 'source_column', 'source_data_type',
        'target_table', 'target_column', 'transformation_logic',
        'target_data_type', 'is_nullable', 'description',
        'is_join', 'join_type', 'join_conditions', 'additional_source_tables'
    ]
    
    def __init__(self, seed: Optional[int] = None, join_probability: float = 0.3):
        """
        Initialize generator.
        
        Args:
            seed: Optional random seed for deterministic output
            join_probability: Probability of generating join transformations (0.0-1.0)
        """
        self.seed = seed
        self.join_probability = join_probability
        if seed is not None:
            random.seed(seed)
        
        self.scenario_manager = ScenarioManager()
        self.transformation_library = TransformationLibrary()
        self.transformation_generator = TransformationGenerator(
            self.transformation_library,
            self.scenario_manager
        )
    
    def generate(self, rows: int, scenario: Optional[str] = None) -> List[Dict]:
        """
        Generate STTM records with complex transformations.
        
        Args:
            rows: Number of records to generate
            scenario: Specific scenario name or None for random
            
        Returns:
            List of dictionaries representing STTM rows
        """
        results = []
        
        for _ in range(rows):
            record = self._generate_single_record(scenario)
            results.append(record)
        
        return results
    
    def _generate_single_record(self, scenario_name: Optional[str] = None) -> Dict:
        """Generate one STTM record with complex transformation logic"""
        
        if scenario_name is None:
            scenario_name = self.scenario_manager.get_random_scenario()
        
        entities = get_scenario_entities(scenario_name)
        if not entities:
            entities = ['customer']
        
        entity = random.choice(entities)
        
        is_join = random.random() < self.join_probability
        
        if is_join:
            return self._generate_join_record(entity, scenario_name)
        else:
            return self._generate_single_table_record(entity, scenario_name)
    
    def _generate_single_table_record(self, entity: str, scenario_name: str) -> Dict:
        """Generate a single-table transformation record"""
        source_table = NamingGenerator.generate_table_name(entity, layer='raw')
        target_table = NamingGenerator.generate_table_name(entity, layer='target')
        
        num_source_cols = random.randint(2, 3)
        source_columns, source_types = self.transformation_generator.generate_multiple_source_columns(
            entity, min_cols=num_source_cols, max_cols=num_source_cols
        )
        
        if len(source_columns) < 2:
            additional_cols = NamingGenerator.get_columns_for_entity(entity, count=2)
            for col in additional_cols:
                if col not in source_columns:
                    source_columns.append(col)
                    source_types.append(DataTypeMapper.get_random_source_type())
                    if len(source_columns) >= 2:
                        break
        
        target_column = NamingGenerator.generate_column_name(entity)
        primary_source_type = source_types[0]
        target_type = DataTypeMapper.map_to_target_type(primary_source_type)
        
        transform_sql, description, _ = self.transformation_generator.generate_for_columns(
            source_columns, source_types, target_column, target_type, scenario_name
        )
        
        if len(source_columns) == 1:
            source_column = source_columns[0]
            source_data_type = source_types[0]
        else:
            source_column = ', '.join(source_columns)
            source_data_type = ', '.join(source_types)
        
        is_nullable = self._determine_nullable(target_column, transform_sql)
        
        return {
            'source_table': source_table,
            'source_column': source_column,
            'source_data_type': source_data_type,
            'target_table': target_table,
            'target_column': target_column,
            'transformation_logic': transform_sql,
            'target_data_type': target_type,
            'is_nullable': is_nullable,
            'description': description,
            'is_join': False,
            'join_type': None,
            'join_conditions': None,
            'additional_source_tables': None
        }
    
    def _generate_join_record(self, entity: str, scenario_name: str) -> Dict:
        """Generate a multi-table join transformation record"""
        num_related = random.randint(1, 2)
        related_entities = self.transformation_generator.get_related_entities(entity, num_related)
        all_entities = [entity] + related_entities
        
        table_aliases, source_columns, source_types = self.transformation_generator.generate_multi_table_sources(
            all_entities, tables_per_entity=1
        )
        
        primary_table = table_aliases.get('t1', NamingGenerator.generate_table_name(entity, layer='raw'))
        additional_tables = [table_aliases[k] for k in sorted(table_aliases.keys()) if k != 't1']
        
        target_table = NamingGenerator.generate_table_name(entity, layer='target')
        target_column = NamingGenerator.generate_column_name(entity)
        primary_source_type = source_types[0] if source_types else 'VARCHAR'
        target_type = DataTypeMapper.map_to_target_type(primary_source_type)
        
        transform_sql, description, join_info = self.transformation_generator.generate_for_columns(
            source_columns, source_types, target_column, target_type, scenario_name,
            is_join=True, table_aliases=table_aliases
        )
        
        if len(source_columns) == 1:
            source_column = source_columns[0]
            source_data_type = source_types[0]
        else:
            qualified_columns = []
            for i, col in enumerate(source_columns):
                table_idx = min(i // 3 + 1, len(all_entities))
                alias = f"t{table_idx}"
                qualified_columns.append(f"{alias}.{col}")
            source_column = ', '.join(qualified_columns[:3])
            source_data_type = ', '.join(source_types[:3])
        
        is_nullable = self._determine_nullable(target_column, transform_sql)
        
        join_type = join_info.get('join_type', 'LEFT') if join_info else 'LEFT'
        join_conditions = join_info.get('join_conditions', []) if join_info else []
        
        return {
            'source_table': primary_table,
            'source_column': source_column,
            'source_data_type': source_data_type,
            'target_table': target_table,
            'target_column': target_column,
            'transformation_logic': transform_sql,
            'target_data_type': target_type,
            'is_nullable': is_nullable,
            'description': description,
            'is_join': True,
            'join_type': join_type,
            'join_conditions': join_conditions,
            'additional_source_tables': additional_tables if additional_tables else None
        }
    
    def _determine_nullable(self, column_name: str, transform_sql: str) -> bool:
        """Determine if target column should be nullable"""
        col_lower = column_name.lower()
        
        # ID/Key columns usually not nullable
        if any(x in col_lower for x in ['_id', '_key', '_code', '_number']):
            return False
        
        # If transformation has CASE with NULL handling, it might not be nullable
        if 'COALESCE' in transform_sql.upper() or 'ELSE' in transform_sql.upper():
            # Has null handling, likely not nullable
            if 'NULL' not in transform_sql.upper().split('ELSE')[-1].upper():
                return False
        
        # Default to nullable for safety
        return True
    
    def save_csv(self, data: List[Dict], filepath: str):
        """Save mappings to CSV file"""
        OutputFormatter.to_csv(data, filepath)
    
    def save_excel(self, data: List[Dict], filepath: str):
        """Save mappings to Excel file"""
        OutputFormatter.to_excel(data, filepath)
    
    def save_text(self, data: List[Dict], filepath: str):
        """Save mappings as formatted text/markdown"""
        OutputFormatter.to_text(data, filepath, format_type='markdown')
    
    def to_markdown_table(self, data: List[Dict]) -> str:
        """Convert mappings to markdown table format"""
        return OutputFormatter.to_markdown_string(data)
    
    @classmethod
    def list_available_scenarios(cls) -> List[str]:
        """List all available scenario names"""
        return ScenarioManager.list_scenarios()
    
    @classmethod
    def get_transformation_count(cls) -> int:
        """Get total number of available complex transformations"""
        return TransformationLibrary.count_total()
    
    @classmethod
    def get_transformation_categories(cls) -> Dict[str, int]:
        """Get count of transformations per category"""
        return TransformationLibrary.get_category_count()
