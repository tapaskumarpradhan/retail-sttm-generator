#!/usr/bin/env python3
"""
STTM Generator CLI

Generate production-like Source-to-Target Mappings for ETL testing.
All transformations are complex with 2+ nested functions or conditional logic.
"""

import argparse
import sys
import os
from datetime import datetime

# Add parent directory to path for imports when running as script
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sttm_generator.sttm_generator import STTMGenerator
from sttm_generator.scenarios.scenario_manager import ScenarioManager


def create_parser():
    parser = argparse.ArgumentParser(
        description='Generate Source-to-Target Mapping records for ETL testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 50 random STTM records
  python cli.py --rows 50
  
  # Deterministic generation with seed
  python cli.py --rows 100 --seed 42 --format excel
  
  # Specific retail scenario
  python cli.py --scenario "Customer Management" --rows 20
  
  # List all scenarios
  python cli.py --list-scenarios
  
  # Show transformation statistics
  python cli.py --show-stats
        """
    )
    
    parser.add_argument('-r', '--rows', type=int, default=10,
                       help='Number of STTM records to generate (default: 10)')
    parser.add_argument('-s', '--seed', type=int, default=None,
                       help='Random seed for deterministic output')
    parser.add_argument('--scenario', type=str, default=None,
                       help='Specific scenario to use (default: random)')
    parser.add_argument('-f', '--format', choices=['csv', 'excel', 'text'], 
                       default='csv', help='Output format (default: csv)')
    parser.add_argument('-o', '--output', type=str, default=None,
                       help='Output file path (default: sttm_output.{format})')
    parser.add_argument('--list-scenarios', action='store_true',
                       help='List all available scenarios and exit')
    parser.add_argument('--show-stats', action='store_true',
                       help='Show transformation statistics and exit')
    
    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()
    
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Handle list scenarios
    if args.list_scenarios:
        scenarios = ScenarioManager.list_scenarios()
        print("\nAvailable Retail Scenarios:")
        print("=" * 60)
        for i, scenario in enumerate(scenarios, 1):
            description = ScenarioManager.get_scenario_description(scenario)
            print(f"{i:2d}. {scenario}")
            if description:
                print(f"    {description}")
        print(f"\nTotal: {len(scenarios)} scenarios")
        sys.exit(0)
    
    # Handle show stats
    if args.show_stats:
        print("\nSTTM Generator Statistics")
        print("=" * 60)
        print(f"\nTotal Complex Transformations: {STTMGenerator.get_transformation_count()}")
        print("\nBy Category:")
        categories = STTMGenerator.get_transformation_categories()
        for cat, count in sorted(categories.items()):
            print(f"  - {cat.capitalize()}: {count}")
        
        print(f"\nTotal Scenarios: {len(ScenarioManager.list_scenarios())}")
        print("\nNaming Stats:")
        from sttm_generator.utils.naming import NamingGenerator
        stats = NamingGenerator.get_entity_stats()
        total_tables = sum(s['source_tables'] + s['target_tables'] for s in stats.values())
        total_columns = sum(s['columns'] for s in stats.values())
        print(f"  - Total Entities: {len(stats)}")
        print(f"  - Total Tables: {total_tables}")
        print(f"  - Total Columns: {total_columns}")
        sys.exit(0)
    
    # Validate scenario if specified
    if args.scenario:
        available = ScenarioManager.list_scenarios()
        if args.scenario not in available:
            print(f"Error: Unknown scenario '{args.scenario}'")
            print(f"Available scenarios: {', '.join(available)}")
            sys.exit(1)
    
    # Generate output filename if not provided
    if args.output is None:
        scenario_name = args.scenario.replace(' ', '_') if args.scenario else 'random'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_name = f"{scenario_name}_sttm_{timestamp}"
        if args.format == 'excel':
            args.output = os.path.join(output_dir, f"{base_name}.xlsx")
        elif args.format == 'text':
            args.output = os.path.join(output_dir, f"{base_name}.md")
        else:
            args.output = os.path.join(output_dir, f"{base_name}.csv")
    elif not os.path.isabs(args.output):
        args.output = os.path.join(output_dir, args.output)
    
    # Initialize generator
    print(f"\nGenerating {args.rows} STTM records...")
    if args.seed:
        print(f"Using seed: {args.seed}")
    if args.scenario:
        print(f"Scenario: {args.scenario}")
    print("Complex transformations only (2+ nested functions or CASE logic)")
    
    generator = STTMGenerator(seed=args.seed)
    mappings = generator.generate(rows=args.rows, scenario=args.scenario)
    
    # Save to file
    try:
        if args.format == 'csv':
            generator.save_csv(mappings, args.output)
        elif args.format == 'excel':
            generator.save_excel(mappings, args.output)
        else:  # text
            generator.save_text(mappings, args.output)
        
        print(f"[OK] Generated {len(mappings)} records")
        print(f"[OK] Saved to: {args.output}")
        
        # Show sample
        if mappings:
            print("\nSample Output (first record):")
            print("-" * 60)
            for key, value in mappings[0].items():
                print(f"{key}: {value}")
        
    except Exception as e:
        print(f"Error saving output: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
