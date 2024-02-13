#!/usr/bin/env python
# coding: utf-8

from sklearn.datasets import make_blobs
import pandas as pd
import numpy as np
import os
from pandas.plotting import scatter_matrix
from matplotlib import pyplot
from pandas import DataFrame

# Number of dimensions, centroids, and samples
dimensions = 3  # can be modified to create multiple dataset dimensions
centroids = 7
samples = 100000

# Set the directory to save files
directory = os.getcwd()
plot_directory = os.path.join(directory, "plots")
os.makedirs(plot_directory, exist_ok=True)

# Ensure the 'txt' directory exists; create it if not
txt_directory = os.path.join(directory, "txt")
os.makedirs(txt_directory, exist_ok=True)

# File path for the dataset within the specified directory, including the .txt extension
filename = os.path.join(txt_directory, f"data_{samples}_{dimensions}_{centroids}.txt")

# Open dataset file for writing
with open(filename, "w") as dataset:
    # Points creation
    x, y = make_blobs(n_samples=samples, centers=centroids, n_features=dimensions)  # generate the dataset

    # Points file writing
    for point in x:
        for dim in range(dimensions):
            if dim == dimensions - 1:
                dataset.write(str(round(point[dim], 4)))
            else:
                dataset.write(str(round(point[dim], 4)) + ",")
        dataset.write("\n")

# Dataset plot
data = np.array(x)

# Scatterplot
df = pd.DataFrame(data, columns=[f'x_{i}' for i in range(dimensions)])
scatter_matrix(df, alpha=0.2, figsize=(10, 10))
scatter_matrix_plot_path = os.path.join(plot_directory, f"scatter_matrix_{samples}_{dimensions}_{centroids}.png")
pyplot.savefig(scatter_matrix_plot_path)

# Scatter plot with color-coded centroids
df = pd.DataFrame({f'x_{i}': x[:, i] for i in range(dimensions)})
df['label'] = y
colors = {label: np.random.rand(3,) for label in np.unique(y)}
fig, ax = pyplot.subplots()
for key, group in df.groupby('label'):
    group.plot(ax=ax, kind='scatter', x=f'x_0', y=f'x_1', label=key, color=colors[key])
scatter_plot_path = os.path.join(plot_directory, f"scatter_plot_{samples}_{dimensions}_{centroids}.png")
pyplot.savefig(scatter_plot_path)

# Display the paths to the saved files
print("Dataset saved to:", filename)
print("Scatter Matrix Plot saved to:", scatter_matrix_plot_path)
print("Scatter Plot saved to:", scatter_plot_path)
