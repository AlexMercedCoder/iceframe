# Visualization

IceFrame integrates with **Altair** to provide declarative statistical visualization directly from your Iceberg tables.

## Installation

```bash
pip install "iceframe[viz]"
```

## Usage

Access the visualizer via the `viz` property.

```python
# Plot distribution (Histogram)
chart = ice.viz.plot_distribution("my_table", "age")
chart.save("age_dist.html")

# Scatter Plot
chart = ice.viz.plot_scatter("my_table", x="age", y="salary", color="department")
chart.save("scatter.html")

# Bar Chart
chart = ice.viz.plot_bar("my_table", x="department", y="salary")
chart.save("bar.html")

# Line Chart
chart = ice.viz.plot_line("my_table", x="date", y="sales")
chart.save("sales.html")
```

## Performance Note

The visualizer automatically limits the number of rows fetched (default 10,000) to prevent crashing your browser or notebook. You can adjust this limit via the `limit` parameter, but be cautious with large datasets. For larger data, consider aggregating using `ice.query()` first and then plotting the result.
