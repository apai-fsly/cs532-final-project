import os
import matplotlib.pyplot as plt
import numpy as np
from CommonHelper import resolve_path

# Create directory for charts
def ensure_output_dir(output_dir=None):
    if output_dir is None:
        output_dir = resolve_path("assets/charts")
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

#Create a Performance Chart for parameters cores, memory, threads
def create_performance_chart(results, parameter, metrics=None, labels=None, 
                            title=None, y_label='Time (seconds)', output_dir=None, chart_type=None):
    """
    Create a generic performance comparison chart for any metrics
    
    Args:
        results: List of benchmark result dictionaries
        parameter: Parameter name to filter and group by (e.g., 'cores', 'memory', 'threads')
        metrics: List of metric keys to plot from results (e.g., ['execution_time', 'row_count'])
        labels: Labels for the legend corresponding to metrics
        title: Custom title for the chart (if None, will use parameter name)
        y_label: Label for the y-axis
        output_dir: Directory to save chart
        
    Returns:
        Path to the saved chart file
    """
    # Setup default metrics if not provided
    if metrics is None:
        metrics = ['total_time']
    
    # Setup default labels if not provided
    if labels is None:
        labels = [m.replace('_', ' ').title() for m in metrics]
    
    # Setup chart title
    if title is None:
        title = f'Performance by {parameter.capitalize()}'
    
    # Setup output path
    output_dir = ensure_output_dir(output_dir)
    filename = f"{parameter}_{chart_type}_chart.png"
    output_path = os.path.join(output_dir, filename)
    
    # Filter results for the selected parameter
    filtered_results = [r for r in results if r['parameter'] == parameter]
    
    # Sort results by parameter value
    filtered_results.sort(key=lambda x: sort_key(x['value']))
    
    # Extract data for plotting
    x_values = [str(r['value']) for r in filtered_results]
    
    # Create figure
    plt.figure(figsize=(10, 6))
    
    # Define x-axis positions
    x = np.arange(len(x_values))
    
    # Define marker styles and line styles for different metrics
    markers = ['o', 's', '^', 'd', '*', 'x']
    line_styles = ['-', '--', '-.', ':']
    
    # Plot each metric as a line
    for i, (metric, label) in enumerate(zip(metrics, labels)):
        # Extract values for this metric
        values = [r.get(metric, 0) for r in filtered_results]
        
        # Get marker and line style (cycling through available options)
        marker = markers[i % len(markers)]
        line_style = line_styles[i % len(line_styles)]
        
        # Create line with markers
        plt.plot(x, values, marker=marker, linestyle=line_style, linewidth=2, 
                 label=label, markersize=8)
        
        # Add value labels on the data points
        for j, value in enumerate(values):
            plt.text(x[j], value*1.02, f"{value:.2f}", ha='center', va='bottom', 
                     fontsize=9)
    
    # Add labels and title
    plt.xlabel(parameter.capitalize())
    plt.ylabel(y_label)
    plt.title(title)
    plt.xticks(x, x_values)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
   
    # Save chart
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    
    print(f"Chart saved to {output_path}")
    return output_path

# Helper function to sort parameter values correctly
def sort_key(value):
    """Helper function to sort parameter values correctly"""
    if isinstance(value, str) and value.endswith('g'):
        return float(value.rstrip('g'))
    return value