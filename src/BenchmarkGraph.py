import matplotlib.pyplot as plt
import numpy as np

# Static data
queries = [
    "COUNT(*)",
    "Filter (rating > 8)",
    "JOIN with filter",
    "Range with sort",
    "GROUP BY with limit"
]

# These values are pulled from storage_performance.csv
ssd_times = [0.15, 0.45, 1.20, 0.80, 1.50]
hdd_times = [1.20, 3.80, 12.50, 6.50, 9.80]
speedups = [8.0, 8.4, 10.4, 8.1, 6.5]
returned_rows = [8000000, 120000, 45000, 150000, 10]

# Create figure with better spacing
fig, (ax, ax_table) = plt.subplots(
    nrows=2, 
    gridspec_kw={'height_ratios': [3, 1]},
    figsize=(10, 8)
)
fig.subplots_adjust(hspace=0.5)  # Space between plot and table

# Bar plot
bar_width = 0.35
x = np.arange(len(queries))
bars1 = ax.bar(x - bar_width/2, ssd_times, bar_width, label='SSD', color='orange')
bars2 = ax.bar(x + bar_width/2, hdd_times, bar_width, label='HDD', color='blue')

# Add speedup text
for i, (ssd, hdd) in enumerate(zip(ssd_times, hdd_times)):
    y_pos = max(ssd, hdd) + 0.5
    ax.text(x[i], y_pos, f'{speedups[i]:.1f}x', 
            ha='center', fontsize=10, fontweight='bold')

# Format plot
ax.set_title('HDD vs SSD Query Performance Comparison', pad=20)
ax.set_ylabel('Execution Time (seconds)')
ax.set_xticks(x)
ax.set_xticklabels(queries, rotation=45, ha='right')
ax.legend()
ax.grid(True, linestyle='--', alpha=0.6)
ax.set_ylim(0, max(hdd_times)*1.2)

# Create table
row_labels = queries
table_data = [[f"{row:,}" for row in returned_rows]]
col_labels = ["Returned rows"]

ax_table.axis('off')
table = ax_table.table(
    cellText=table_data,
    rowLabels=col_labels,
    colLabels=queries,
    cellLoc='center',
    loc='center'
)

# Style table
table.auto_set_font_size(False)
table.set_fontsize(10)
table.scale(1, 2)

# Save and show
plt.savefig('hdd_vs_ssd.png', dpi=300, bbox_inches='tight')
plt.show()