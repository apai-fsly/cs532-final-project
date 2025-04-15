import matplotlib.pyplot as plt
import itertools

class PerformancePlot:
    def __init__(self, x_label_name, param1_name, param2_name, table_name):
        """
        Initialize the plot with variable and parameter names.

        :param x_label_name: Name of the x-axis variable (e.g. 'RAM (GB)')
        :param param1_name: Name of the first parameter for labeling (e.g. 'Cores')
        :param param2_name: Name of the second parameter for labeling (e.g. 'Threads')
        """
        self.x_label_name = x_label_name
        self.param1_name = param1_name
        self.param2_name = param2_name
        self.table_name = table_name
        self.lines = []
        self.styles = ['-', '--', '-.', ':']  # Predefined line styles
        self.colors = plt.cm.get_cmap('tab10', 10)  # Color palette with 10 distinct colors

    def add_line(self, x_data, y_data, param1_value, param2_value):
        """
        Add a data line to the plot.

        :param x_data: Values for the x-axis variable
        :param y_data: Corresponding execution time (in seconds)
        :param param1_value: Value of parameter 1 (e.g. RAM)
        :param param2_value: Value of parameter 2 (e.g. Threads)
        """
        self.lines.append({
            self.x_label_name: x_data,
            'seconds': y_data,
            self.param1_name: param1_value,
            self.param2_name: param2_value
        })

    def plot(self, save_path=None):
        """
        Plot all added lines. Optionally save the plot as an image.

        :param save_path: File path to save the plot image (optional)
        """
        plt.figure(figsize=(10, 6))
        color_cycle = itertools.cycle(self.colors.colors)
        style_cycle = itertools.cycle(self.styles)

        for line in self.lines:
            label_text = f'{self.param1_name}: {line[self.param1_name]}, {self.param2_name}: {line[self.param2_name]}'
            plt.plot(
                line[self.x_label_name],
                line['seconds'],
                marker='o',
                linestyle=next(style_cycle),
                color=next(color_cycle),
                linewidth=2,
                label=label_text
            )

        plt.title(f'Execution Time vs {self.x_label_name}\n for the table {self.table_name}')
        plt.xlabel(self.x_label_name)
        plt.ylabel('Execution Time (seconds)')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path)
            print(f"Plot saved as {save_path}")
        else:
            plt.show()

