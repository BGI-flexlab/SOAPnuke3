"""Regenerate the benchmark figures from the adjacent frozen TSV tables."""
import csv
import os
from pathlib import Path

HERE = Path(__file__).resolve().parent
BENCH = HERE.parent
CACHE = BENCH / '.scratch' / 'plot'
CACHE.mkdir(parents=True, exist_ok=True)
os.environ['MPLCONFIGDIR'] = str(CACHE / 'matplotlib')
os.environ['TMPDIR'] = str(CACHE)

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import FixedLocator, FuncFormatter

with (BENCH / 'data/preprocessing_54_observations.tsv').open() as f:
    observations = list(csv.DictReader(f, delimiter='\t'))
with (BENCH / 'data/preprocessing_medians.tsv').open() as f:
    medians = list(csv.DictReader(f, delimiter='\t'))
with (BENCH / 'data/variant_calling_accuracy.tsv').open() as f:
    accuracy = list(csv.DictReader(f, delimiter='\t'))

ORDER = ['SOAPnuke 3.0.0', 'SOAPnuke 2.1.10', 'SOAPnuke 1.6.5',
         'fastp 1.4.0', 'Trimmomatic 0.41', 'Cutadapt 5.2']
COLORS = ['#007C78', '#527CB8', '#828894', '#D58A2A', '#AF6494', '#7764B0']
MARKERS = ['o', 's', '^', 'D', 'P', 'v']
plt.rcParams.update({
    'font.family': 'DejaVu Sans', 'font.size': 10.5,
    'axes.labelcolor': '#263746', 'text.color': '#263746',
    'axes.edgecolor': '#CCD3DB', 'xtick.color': '#526171', 'ytick.color': '#526171',
    'svg.fonttype': 'none', 'pdf.fonttype': 42,
    'savefig.facecolor': 'white', 'axes.facecolor': 'white',
})

fig, axes = plt.subplots(1, 3, figsize=(15.5, 6.7))
panels = [
    ('median_input_MB_s', 'input_MB_s', 'A   Input throughput', 'Compressed-input MB/s', 1),
    ('median_max_rss_GiB', 'max_rss_kib', 'B   Peak process memory', 'Peak RSS (GiB; log scale)', 1 / 1048576),
    ('median_cpu_percent', 'cpu_percent', 'C   CPU consumption', 'Effective CPU cores', .01),
]
for ax, (median_key, observation_key, title, ylabel, factor) in zip(axes, panels):
    for tool, color, marker in zip(ORDER, COLORS, MARKERS):
        cells = sorted([r for r in medians if r['tool'] == tool], key=lambda r: int(r['threads']))
        yfactor = .01 if median_key == 'median_cpu_percent' else 1
        ax.plot([int(r['threads']) for r in cells], [float(r[median_key]) * yfactor for r in cells],
                color=color, marker=marker, markersize=6, linewidth=2.8 if tool == ORDER[0] else 1.8,
                zorder=5 if tool == ORDER[0] else 3)
        for t in [8, 16, 32]:
            rows = sorted([r for r in observations if r['tool'] == tool and int(r['threads']) == t],
                          key=lambda r: int(r['repetition']))
            ax.scatter([t - .45, t, t + .45], [float(r[observation_key]) * factor for r in rows],
                       color=color, s=16, alpha=.35, edgecolors='none', zorder=2)
    ax.set_title(title, loc='left', fontweight='bold', fontsize=12, pad=15)
    ax.set_xlabel('Allocated physical cores', labelpad=9)
    ax.set_ylabel(ylabel, labelpad=8)
    ax.set_xticks([8, 16, 32])
    ax.set_xlim(5.5, 34.5)
    ax.grid(axis='y', color='#E5E9EE', linewidth=.8)
    ax.spines[['top', 'right']].set_visible(False)
    ax.set_axisbelow(True)
axes[0].set_ylim(0, 180)
axes[1].set_yscale('log')
axes[1].set_ylim(.035, 16)
axes[1].yaxis.set_major_locator(FixedLocator([.05, .1, .2, .5, 1, 2, 5, 10]))
axes[1].yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:g}'))
axes[2].plot([8, 16, 32], [8, 16, 32], color='#A7AFBA', linestyle=(0, (4, 4)), linewidth=1.1, zorder=1)
axes[2].set_ylim(0, 35)
axes[2].text(.03, .95, 'Dashed: allocated cores', transform=axes[2].transAxes, fontsize=9,
             color='#647182', va='top')
fig.suptitle('HG002 WGS preprocessing: throughput, memory and CPU use',
             x=.065, y=.98, ha='left', fontsize=19, fontweight='bold')
fig.text(.065, .915, '430,952,721 pairs  ·  58.144 GB compressed input  ·  3 runs per tool/core setting',
         fontsize=11, color='#59697B')
handles = [Line2D([], [], color=c, marker=m, linewidth=2, label=t)
           for t, c, m in zip(ORDER, COLORS, MARKERS)]
fig.legend(handles=handles, loc='lower center', bbox_to_anchor=(.52, .07),
           ncol=3, frameon=False, columnspacing=2.4, handlelength=2.2, fontsize=10.5)
fig.text(.065, .02, 'Lines: medians. Faint points: individual runs (horizontal jitter). Shared Xeon host / Lustre. Profiles differ; see methods.',
         fontsize=9, color='#627181')
fig.subplots_adjust(left=.065, right=.985, bottom=.30, top=.79, wspace=.31)
for extension in ['svg', 'png', 'pdf']:
    fig.savefig(HERE / f'preprocessing_scaling.{extension}', dpi=240)
plt.close(fig)

by_score = {(r['tool'], r['type']): r for r in accuracy}
accuracy_order = sorted(ORDER, key=lambda tool: sum(float(by_score[tool, kind]['f1']) for kind in ['SNP', 'INDEL']), reverse=True)
fig, ax = plt.subplots(figsize=(14.6, 5.8))
ax.set_xlim(0, 1)
ax.set_ylim(0, 1)
ax.axis('off')
ax.text(.025, .97, 'HG002 downstream variant-calling accuracy', fontsize=19, fontweight='bold', va='top')
ax.text(.025, .875, 'Common BWA-MEM2 / GATK workflow  ·  GIAB v4.2.1 confident regions  ·  all values are percentages',
        fontsize=10.5, color='#617080')
ax.text(.49, .77, 'SNP', ha='center', fontweight='bold', fontsize=12, color='#007C78')
ax.text(.815, .77, 'INDEL', ha='center', fontweight='bold', fontsize=12, color='#007C78')
ax.text(.025, .685, 'Preprocessing tool', fontsize=10, fontweight='bold')
columns = [.39, .49, .59, .715, .815, .915]
for x, heading in zip(columns, ['Precision', 'Recall', 'F1'] * 2):
    ax.text(x, .685, heading, ha='center', fontsize=10, fontweight='bold')
ax.hlines(.65, .025, .975, colors='#BAC7CF', linewidth=1)
for i, tool in enumerate(accuracy_order):
    y = .592 - i * .078
    label = tool
    ax.text(.025, y, label, va='center', fontsize=10.5, fontweight='normal')
    values = [100 * float(by_score[tool, kind][metric])
              for kind in ['SNP', 'INDEL'] for metric in ['precision', 'recall', 'f1']]
    for x, value in zip(columns, values):
        ax.text(x, y, f'{value:.4f}', ha='center', va='center', fontsize=10.5,
                fontweight='normal')
ax.vlines(.651, .16, .81, colors='#D5DFE4', linewidth=1)
ax.hlines(.15, .025, .975, colors='#BAC7CF', linewidth=1)
ax.text(.025, .09, 'One HG002 WGS sample. Formal v3 / fastp results reused by exact FASTQ identity.',
        fontsize=9.2, color='#617080')
fig.subplots_adjust(left=.025, right=.985, top=.96, bottom=.025)
for extension in ['svg', 'png', 'pdf']:
    fig.savefig(HERE / f'variant_accuracy_table.{extension}', dpi=240)
plt.close(fig)
print('Generated two figures in SVG, PNG and PDF from 54 observations and 12 accuracy rows.')
