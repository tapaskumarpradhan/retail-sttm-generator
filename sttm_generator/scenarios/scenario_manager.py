"""
Scenario Manager - Manages retail business scenarios
"""
import random
from typing import List, Dict, Optional, Any
from .retail_scenarios import RETAIL_SCENARIOS, get_scenario, list_scenarios, get_scenario_entities


class ScenarioManager:
    """Manages and provides access to retail business scenarios"""
    
    def __init__(self):
        self.scenarios = RETAIL_SCENARIOS
    
    @classmethod
    def list_scenarios(cls) -> List[str]:
        """List all available scenario names"""
        return list_scenarios()
    
    @classmethod
    def get_scenario(cls, name: str) -> Optional[Dict[str, Any]]:
        """Get scenario configuration by name"""
        return get_scenario(name)
    
    @classmethod
    def get_random_scenario(cls) -> str:
        """Get a random scenario name"""
        return random.choice(list_scenarios())
    
    @classmethod
    def get_scenario_entities(cls, scenario_name: str) -> List[str]:
        """Get entities for a specific scenario"""
        return get_scenario_entities(scenario_name)
    
    @classmethod
    def get_scenario_description(cls, scenario_name: str) -> str:
        """Get scenario description"""
        scenario = get_scenario(scenario_name)
        if scenario:
            return scenario.get("description", "")
        return ""
    
    @classmethod
    def get_source_tables_for_scenario(cls, scenario_name: str) -> List[str]:
        """Get source tables for scenario"""
        scenario = get_scenario(scenario_name)
        if scenario:
            return scenario.get("source_tables", [])
        return []
    
    @classmethod
    def get_target_tables_for_scenario(cls, scenario_name: str) -> List[str]:
        """Get target tables for scenario"""
        scenario = get_scenario(scenario_name)
        if scenario:
            return scenario.get("target_tables", [])
        return []
    
    @classmethod
    def get_typical_transforms_for_scenario(cls, scenario_name: str) -> List[str]:
        """Get typical transformation types for scenario"""
        scenario = get_scenario(scenario_name)
        if scenario:
            return scenario.get("typical_transforms", [])
        return []
    
    @classmethod
    def validate_scenario(cls, scenario_name: str) -> bool:
        """Check if scenario exists"""
        return scenario_name in list_scenarios()
    
    @classmethod
    def get_all_scenario_info(cls) -> Dict[str, Dict]:
        """Get information about all scenarios"""
        info = {}
        for name, scenario in RETAIL_SCENARIOS.items():
            info[name] = {
                "description": scenario.get("description", ""),
                "entities": scenario.get("entities", []),
                "source_table_count": len(scenario.get("source_tables", [])),
                "target_table_count": len(scenario.get("target_tables", [])),
            }
        return info
