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
    Output format matches STTM_Sample.csv.
    """
    
    COLUMNS = [
        'Source_schema', 'Source_Table', 'Source_Column', 'Source_DataType',
        'Target_schema', 'Target_Table', 'Target_Column', 'Target_DataType',
        'Transformation_Logic', 'Primary_key', 'Nullable'
    ]
    
    DEFAULT_SOURCE_SCHEMA = 'ecom_demo'
    DEFAULT_TARGET_SCHEMA = 'ecom_tgt'
    
    def __init__(self, seed: Optional[int] = None, join_probability: float = 0.3,
                 select_probability: float = 0.7, source_schema: str = 'ecom_demo',
                 target_schema: str = 'ecom_tgt'):
        """
        Initialize generator.
        
        Args:
            seed: Optional random seed for deterministic output
            join_probability: Probability of generating join transformations (0.0-1.0)
            select_probability: Probability of generating SELECT statements vs simple transforms (0.0-1.0)
            source_schema: Source schema name
            target_schema: Target schema name
        """
        self.seed = seed
        self.join_probability = join_probability
        self.select_probability = select_probability
        self.source_schema = source_schema
        self.target_schema = target_schema
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
        
        use_select = random.random() < self.select_probability
        
        if use_select:
            return self._generate_select_record(entity, scenario_name)
        else:
            return self._generate_simple_transform_record(entity, scenario_name)
    
    def _generate_simple_transform_record(self, entity: str, scenario_name: str) -> Dict:
        """Generate a simple (non-SELECT) transformation record"""
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
        
        transform_sql, description = self.transformation_generator.generate_simple_transformation(
            source_columns, source_types, target_column, target_type, scenario_name
        )
        
        if len(source_columns) == 1:
            source_column = source_columns[0]
            source_data_type = source_types[0]
        else:
            source_column = ', '.join(source_columns)
            source_data_type = ', '.join(source_types)
        
        is_primary_key = self._is_primary_key_column(target_column)
        nullable = 'No' if is_primary_key else random.choice(['Yes', 'No'])
        
        return {
            'Source_schema': self.source_schema,
            'Source_Table': source_table,
            'Source_Column': source_column,
            'Source_DataType': source_data_type,
            'Target_schema': self.target_schema,
            'Target_Table': target_table,
            'Target_Column': target_column,
            'Target_DataType': target_type,
            'Transformation_Logic': transform_sql,
            'Primary_key': 'Yes' if is_primary_key else '',
            'Nullable': nullable
        }
    
    def _generate_select_record(self, entity: str, scenario_name: str) -> Dict:
        """Generate a SELECT transformation record with complex joins"""
        use_join = random.random() < self.join_probability
        
        if use_join:
            return self._generate_complex_join_record(entity, scenario_name)
        else:
            return self._generate_simple_select_record(entity, scenario_name)
    
    def _generate_simple_select_record(self, entity: str, scenario_name: str) -> Dict:
        """Generate a simple SELECT transformation (single table or simple join)"""
        num_related = random.randint(0, 1)
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
        
        transform_sql = self.transformation_generator.generate_select_sql(
            source_columns, source_types, target_column, target_type, scenario_name,
            is_join=num_related > 0, table_aliases=table_aliases, primary_table=primary_table
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
        
        is_primary_key = self._is_primary_key_column(target_column)
        nullable = 'No' if is_primary_key else random.choice(['Yes', 'No'])
        
        return {
            'Source_schema': self.source_schema,
            'Source_Table': primary_table,
            'Source_Column': source_column,
            'Source_DataType': source_data_type,
            'Target_schema': self.target_schema,
            'Target_Table': target_table,
            'Target_Column': target_column,
            'Target_DataType': target_type,
            'Transformation_Logic': transform_sql,
            'Primary_key': 'Yes' if is_primary_key else '',
            'Nullable': nullable
        }
    
    def _generate_complex_join_record(self, entity: str, scenario_name: str) -> Dict:
        """Generate a complex multi-table join transformation record (3+ tables)"""
        num_related = random.randint(2, 4)
        related_entities = self.transformation_generator.get_related_entities(entity, num_related)
        all_entities = [entity] + related_entities[:3]
        
        table_aliases, source_columns, source_types = self.transformation_generator.generate_multi_table_sources(
            all_entities, tables_per_entity=1
        )
        
        primary_table = table_aliases.get('t1', NamingGenerator.generate_table_name(entity, layer='raw'))
        additional_tables = [table_aliases[k] for k in sorted(table_aliases.keys()) if k != 't1']
        
        target_table = NamingGenerator.generate_table_name(entity, layer='target')
        target_column = NamingGenerator.generate_column_name(entity)
        primary_source_type = source_types[0] if source_types else 'VARCHAR'
        target_type = DataTypeMapper.map_to_target_type(primary_source_type)
        
        transform_sql = self.transformation_generator.generate_complex_select_sql(
            source_columns, source_types, target_column, target_type, scenario_name,
            table_aliases=table_aliases
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
            source_column = ', '.join(qualified_columns[:4])
            source_data_type = ', '.join(source_types[:4])
        
        is_primary_key = self._is_primary_key_column(target_column)
        nullable = 'No' if is_primary_key else random.choice(['Yes', 'No'])
        
        return {
            'Source_schema': self.source_schema,
            'Source_Table': primary_table,
            'Source_Column': source_column,
            'Source_DataType': source_data_type,
            'Target_schema': self.target_schema,
            'Target_Table': target_table,
            'Target_Column': target_column,
            'Target_DataType': target_type,
            'Transformation_Logic': transform_sql,
            'Primary_key': 'Yes' if is_primary_key else '',
            'Nullable': nullable
        }
    
    def _is_primary_key_column(self, column_name: str) -> bool:
        """Determine if column is a primary key based on naming conventions"""
        col_lower = column_name.lower()
        return any(x in col_lower for x in ['_id', '_key', '_code', '_number'])
    
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
