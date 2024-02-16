import matplotlib.pyplot as plt

# Dati
dataset_2d = [118618, 118558, 133608]  # Tempo in millisecondi
dataset_3d = [115604, 121274, 124299]
dataset_5d = [118483, 118362, 123445]

# Etichette
datasets = ['10K', '100K', '1M']

# Plot
fig, ax = plt.subplots(figsize=(10, 6))

bar_width = 0.25
bar_positions_2d = range(len(datasets))
bar_positions_3d = [pos + bar_width for pos in bar_positions_2d]
bar_positions_5d = [pos + 2 * bar_width for pos in bar_positions_2d]

ax.bar(bar_positions_2d, dataset_2d, width=bar_width, label='2D', color='royalblue')
ax.bar(bar_positions_3d, dataset_3d, width=bar_width, label='3D', color='limegreen')
ax.bar(bar_positions_5d, dataset_5d, width=bar_width, label='5D', color='tomato')

# Aggiungi etichette e legenda
ax.set_xticks([pos + bar_width for pos in bar_positions_2d])
ax.set_xticklabels(datasets)
ax.set_xlabel('Dimension of datasets')
ax.set_ylabel('Execution time (ms)')
ax.set_title('Comparison between execution time')

plt.legend()
plt.show()
