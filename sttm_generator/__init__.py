"""
STTM Generator - Source-to-Target Mapping Generator for ETL Testing

A Python-based tool that generates production-like source-to-target mappings
with complex SQL transformations for testing ETL pipelines.

Features:
- 15 retail business scenarios
- 60+ complex SQL transformations only
- CLI and Python API
- CSV, Excel, and Text output formats
- Deterministic (seeded) and random generation modes
"""

from .sttm_generator import STTMGenerator
from .scenarios.scenario_manager import ScenarioManager

__version__ = "1.0.0"
__all__ = ["STTMGenerator", "ScenarioManager"]
