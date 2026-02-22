"""
Example Usage Script for STTM Generator

This script demonstrates various ways to use the STTM Generator
for ETL pipeline testing.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sttm_generator.sttm_generator import STTMGenerator
from sttm_generator.scenarios.scenario_manager import ScenarioManager


def example_1_basic_generation():
    """Example 1: Basic generation of STTM records"""
    print("=" * 60)
    print("Example 1: Basic Generation")
    print("=" * 60)
    
    generator = STTMGenerator()
    mappings = generator.generate(rows=5)
    
    print(f"Generated {len(mappings)} mappings\n")
    
    # Display first mapping
    print("First mapping:")
    for key, value in mappings[0].items():
        print(f"  {key}: {value}")
    print()


def example_2_deterministic_generation():
    """Example 2: Deterministic generation with seed"""
    print("=" * 60)
    print("Example 2: Deterministic Generation (Same Seed)")
    print("=" * 60)
    
    # First run with seed 42
    generator1 = STTMGenerator(seed=42)
    mappings1 = generator1.generate(rows=3)
    
    # Second run with same seed 42
    generator2 = STTMGenerator(seed=42)
    mappings2 = generator2.generate(rows=3)
    
    # Verify they're identical
    identical = mappings1 == mappings2
    print(f"Same seed produces identical output: {identical}\n")
    
    print("Sample from first run:")
    print(f"  Source: {mappings1[0]['source_table']}")
    print(f"  Transform: {mappings1[0]['transformation_logic'][:60]}...")
    print()


def example_3_specific_scenario():
    """Example 3: Generate for specific retail scenario"""
    print("=" * 60)
    print("Example 3: Specific Scenario - Customer Management")
    print("=" * 60)
    
    generator = STTMGenerator(seed=123)
    mappings = generator.generate(rows=5, scenario="Customer Management")
    
    print(f"Generated {len(mappings)} customer-related mappings\n")
    
    # Show variety
    for i, mapping in enumerate(mappings[:3], 1):
        print(f"Record {i}:")
        print(f"  {mapping['source_table']} -> {mapping['target_table']}")
        print(f"  Transform: {mapping['transformation_logic'][:50]}...")
        print()


def example_4_save_formats():
    """Example 4: Save to different formats"""
    print("=" * 60)
    print("Example 4: Save to Different Formats")
    print("=" * 60)
    
    generator = STTMGenerator(seed=456)
    mappings = generator.generate(rows=10)
    
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")
    os.makedirs(output_dir, exist_ok=True)
    
    generator.save_csv(mappings, os.path.join(output_dir, "example_output.csv"))
    print("[OK] Saved to output/example_output.csv")
    
    generator.save_text(mappings, os.path.join(output_dir, "example_output.md"))
    print("[OK] Saved to output/example_output.md")
    
    try:
        generator.save_excel(mappings, os.path.join(output_dir, "example_output.xlsx"))
        print("[OK] Saved to output/example_output.xlsx")
    except ImportError:
        print("[SKIP] Excel output requires: pip install openpyxl")
    
    print()


def example_5_markdown_table():
    """Example 5: Generate markdown table for documentation"""
    print("=" * 60)
    print("Example 5: Markdown Table Output")
    print("=" * 60)
    
    generator = STTMGenerator(seed=789)
    mappings = generator.generate(rows=3)
    
    markdown = generator.to_markdown_table(mappings)
    print(markdown)
    print()


def example_6_list_scenarios():
    """Example 6: List available scenarios"""
    print("=" * 60)
    print("Example 6: Available Scenarios")
    print("=" * 60)
    
    scenarios = ScenarioManager.list_scenarios()
    print(f"Total scenarios: {len(scenarios)}\n")
    
    for i, scenario in enumerate(scenarios[:5], 1):
        description = ScenarioManager.get_scenario_description(scenario)
        print(f"{i}. {scenario}")
        print(f"   {description}")
    print(f"   ... and {len(scenarios) - 5} more\n")


def example_7_transformation_stats():
    """Example 7: Show transformation statistics"""
    print("=" * 60)
    print("Example 7: Transformation Statistics")
    print("=" * 60)
    
    total = STTMGenerator.get_transformation_count()
    categories = STTMGenerator.get_transformation_categories()
    
    print(f"Total complex transformations: {total}\n")
    print("By category:")
    for cat, count in sorted(categories.items()):
        print(f"  - {cat.capitalize()}: {count}")
    print()


def example_8_batch_generation():
    """Example 8: Batch generation for testing"""
    print("=" * 60)
    print("Example 8: Batch Generation for ETL Testing")
    print("=" * 60)
    
    # Generate multiple batches with different seeds
    for seed in [1, 2, 3]:
        generator = STTMGenerator(seed=seed)
        mappings = generator.generate(rows=5)
        print(f"Batch {seed}: Generated {len(mappings)} mappings")
        print(f"  Sample: {mappings[0]['source_table']} -> {mappings[0]['target_table']}")
    print()


def run_all_examples():
    """Run all examples"""
    examples = [
        example_1_basic_generation,
        example_2_deterministic_generation,
        example_3_specific_scenario,
        example_4_save_formats,
        example_5_markdown_table,
        example_6_list_scenarios,
        example_7_transformation_stats,
        example_8_batch_generation,
    ]
    
    for example in examples:
        try:
            example()
        except Exception as e:
            print(f"Error in {example.__name__}: {e}")
            print()
    
    print("=" * 60)
    print("All examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    run_all_examples()
