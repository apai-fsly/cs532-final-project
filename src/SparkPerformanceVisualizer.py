import os
import csv
from ChartPlot import create_performance_chart, ensure_output_dir
from CommonHelper import resolve_path

def load_average_results(filepath):
    """Load pre-calculated average results from CSV file"""
    results = []
    
    try:
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Convert execution_time which is always present
                row['execution_time'] = float(row['execution_time'])
                
                # Check if throughput exists before converting
                if 'throughput' in row:
                    row['throughput'] = float(row['throughput'])
                
                results.append(row)
        print(f"Loaded {len(results)} results from {filepath}")
        return results
    except Exception as e:
        print(f"Error loading results: {e}")
        return []

def create_charts(results, output_dir, chart_type="pipeline"):
    """Create performance charts for all parameters"""
    parameters = ['cores', 'memory', 'parallelism']
    
    # Ensure output directory exists
    ensure_output_dir(output_dir)
    
    # Set title prefix based on chart type
    title_prefix = "Data Cleaning" if chart_type == "cleaning" else "Database Pipeline"
    
    # Create charts for each parameter
    for param in parameters:
        # Filter results for this parameter
        param_results = [r for r in results if r['parameter'] == param]
        if not param_results:
            print(f"No results found for parameter: {param}")
            continue

        # Sort results by parameter value before plotting
        if param == "memory":
            # Extract numeric part from memory values (e.g., "4g" becomes 4)
            param_results.sort(key=lambda x: float(str(x['value']).replace('g', '').replace('m', '')))
        else:
            # For cores and parallelism, convert to numeric and sort
            try:
                param_results.sort(key=lambda x: float(str(x['value'])))
            except (ValueError, TypeError):
                # Fall back to string sort if numeric conversion fails
                param_results.sort(key=lambda x: str(x['value']))
      
        print(f"BEFORE CHART - {param} values: {[r['value'] for r in param_results]}")

        # Create execution time chart
        create_performance_chart(
            param_results,
            param,
            metrics=['execution_time'],
            labels=['Execution Time'],
            title=f"{title_prefix} Time by {param.capitalize()}",
            y_label='Execution Time (seconds)',
            output_dir=output_dir,
            chart_type=f"{chart_type}_time"
        )
        
        # Only create throughput chart if throughput data exists
        if 'throughput' in param_results[0]:
            create_performance_chart(
                param_results,
                param,
                metrics=['throughput'],
                labels=['Throughput'],
                title=f"{title_prefix} Throughput by {param.capitalize()}",
                y_label='Throughput (rows/second)', 
                output_dir=output_dir,
                chart_type=f"{chart_type}_throughput"
            )
        
        print(f"Created charts for {param}")

def main():
    # File paths
    pipeline_results_file = resolve_path("./assets/results/PerformanceAverageResults.csv")
    cleaning_results_file = resolve_path("./assets/results/CleaningAverageResults.csv")
    output_dir = resolve_path("./assets/charts")
    
    print("Spark Performance Visualizer")
    
    # Process pipeline results
    if os.path.exists(pipeline_results_file):
        print("\nProcessing Pipeline Performance Results:")
        pipeline_results = load_average_results(pipeline_results_file)
        if pipeline_results:
            create_charts(pipeline_results, output_dir, chart_type="pipeline")
            print(f"Pipeline charts have been saved to {output_dir}")
    
    # Process data cleaning results
    if os.path.exists(cleaning_results_file):
        print("\nProcessing Data Cleaning Performance Results:")
        cleaning_results = load_average_results(cleaning_results_file)
        if cleaning_results:
            create_charts(cleaning_results, output_dir, chart_type="cleaning")
            print(f"Data cleaning charts have been saved to {output_dir}")
if __name__ == "__main__":
    main()