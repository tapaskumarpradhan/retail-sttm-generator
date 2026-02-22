"""
Data Type Mapper - Generates realistic SQL data types
"""
import random
from typing import Dict, List, Optional


class DataTypeMapper:
    """Maps and generates realistic SQL data types"""
    
    SOURCE_TYPES = {
        'VARCHAR': {'generator': lambda: f"VARCHAR({random.randint(10, 500)})", 'category': 'string'},
        'CHAR': {'generator': lambda: f"CHAR({random.randint(1, 20)})", 'category': 'string'},
        'TEXT': {'generator': lambda: 'TEXT', 'category': 'string'},
        'INT': {'generator': lambda: 'INT', 'category': 'numeric'},
        'BIGINT': {'generator': lambda: 'BIGINT', 'category': 'numeric'},
        'SMALLINT': {'generator': lambda: 'SMALLINT', 'category': 'numeric'},
        'DECIMAL': {'generator': lambda: f"DECIMAL({random.randint(10, 18)},{random.randint(2, 4)})", 'category': 'numeric'},
        'NUMERIC': {'generator': lambda: f"NUMERIC({random.randint(10, 18)},{random.randint(2, 4)})", 'category': 'numeric'},
        'DATE': {'generator': lambda: 'DATE', 'category': 'date'},
        'DATETIME': {'generator': lambda: 'DATETIME', 'category': 'date'},
        'TIMESTAMP': {'generator': lambda: 'TIMESTAMP', 'category': 'date'},
        'BOOLEAN': {'generator': lambda: 'BOOLEAN', 'category': 'boolean'},
        'FLOAT': {'generator': lambda: 'FLOAT', 'category': 'numeric'},
        'DOUBLE': {'generator': lambda: 'DOUBLE', 'category': 'numeric'},
    }
    
    # Mapping from source to compatible target types
    TYPE_COMPATIBILITY = {
        'string': ['VARCHAR', 'CHAR', 'TEXT', 'STRING'],
        'numeric': ['INT', 'BIGINT', 'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE'],
        'date': ['DATE', 'DATETIME', 'TIMESTAMP', 'STRING'],
        'boolean': ['BOOLEAN', 'INT', 'STRING'],
    }
    
    @classmethod
    def get_random_source_type(cls, preferred_category: Optional[str] = None) -> str:
        """Get random source data type"""
        if preferred_category:
            # Filter by category
            candidates = [
                key for key, value in cls.SOURCE_TYPES.items()
                if value['category'] == preferred_category
            ]
            if candidates:
                type_key = random.choice(candidates)
            else:
                type_key = random.choice(list(cls.SOURCE_TYPES.keys()))
        else:
            type_key = random.choice(list(cls.SOURCE_TYPES.keys()))
        
        return cls.SOURCE_TYPES[type_key]['generator']()
    
    @classmethod
    def get_category_from_type(cls, data_type: str) -> str:
        """Determine category from data type string"""
        data_type_upper = data_type.upper()
        
        for type_key, config in cls.SOURCE_TYPES.items():
            if type_key in data_type_upper:
                return config['category']
        
        # Default mappings
        if any(x in data_type_upper for x in ['CHAR', 'TEXT', 'STRING']):
            return 'string'
        elif any(x in data_type_upper for x in ['INT', 'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE']):
            return 'numeric'
        elif any(x in data_type_upper for x in ['DATE', 'TIME']):
            return 'date'
        elif 'BOOL' in data_type_upper:
            return 'boolean'
        
        return 'string'  # Default
    
    @classmethod
    def map_to_target_type(cls, source_type: str, target_category: Optional[str] = None) -> str:
        """Map source type to appropriate target type"""
        source_category = cls.get_category_from_type(source_type)
        
        if target_category and target_category in cls.TYPE_COMPATIBILITY:
            # Use requested category
            candidates = cls.TYPE_COMPATIBILITY[target_category]
        else:
            # Use source category or compatible categories
            candidates = cls.TYPE_COMPATIBILITY.get(source_category, ['VARCHAR'])
        
        # Select appropriate target type
        if source_category == 'string':
            if 'TEXT' in source_type.upper():
                return 'STRING'  # For data lakes
            else:
                # Generate VARCHAR with similar or different size
                return f"VARCHAR({random.randint(10, 500)})"
        elif source_category == 'numeric':
            if 'DECIMAL' in source_type.upper() or 'NUMERIC' in source_type.upper():
                # Keep precision for decimals
                return source_type
            elif 'BIGINT' in source_type.upper():
                return random.choice(['BIGINT', 'INT'])
            else:
                return random.choice(['INT', 'BIGINT', 'DECIMAL(18,2)'])
        elif source_category == 'date':
            if 'TIMESTAMP' in source_type.upper():
                return random.choice(['TIMESTAMP', 'DATETIME', 'STRING'])
            else:
                return random.choice(['DATE', 'STRING', 'INT'])  # INT for date keys
        elif source_category == 'boolean':
            return random.choice(['BOOLEAN', 'INT', 'STRING'])
        
        return 'VARCHAR(255)'  # Default
    
    @classmethod
    def get_compatible_types(cls, category: str) -> List[str]:
        """Get list of compatible types for a category"""
        return cls.TYPE_COMPATIBILITY.get(category, ['VARCHAR'])
