import sys
import random
import argparse

from io import StringIO

import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt

from matplotlib.dates import DateFormatter, HourLocator, DayLocator
from matplotlib.ticker import MaxNLocator

palettes = [a for a in mpl.colormaps.keys()]

parser = argparse.ArgumentParser()
parser.add_argument('-', dest='is_stdin', help='read CSV from stdin',
                    action='store_true', default=False)
parser.add_argument('-f', dest='file', help='A CSV input file')
parser.add_argument('-s', dest='sample', default='1h',
                    help='resample rate. Ex: 1min, 30min, 1h, 1d, ...')
parser.add_argument('-p', dest='palette', default='cool', help='Color palette: %s' % (palettes + ['random']))
parser.add_argument('-o', dest='outfile', default=None, help='Save figure to outfile')
args = parser.parse_args()

if args.is_stdin == True:
    df = pd.read_csv(StringIO(sys.stdin.read()))
else:
    df = pd.read_csv(args.file)

sample = args.sample

# Dark
bgcolor = '#161618'
fgcolor = '#fffff5'
grcolor = '#999'

# # Light
# bgcolor = '#fff'
# fgcolor = '#000'
# grcolor = '#444'

if args.palette == 'random':
    palette = random.choice(palettes)
    print("Used palette: %s" % palette)
else:
    palette = args.palette

df['timestamp'] = pd.to_datetime(df['timestamp'])
df.set_index('timestamp', inplace=True)

tag = df['tag'][0]

if df['nuid'].unique().size == 1:
    name = df['name'][0]
    # Resample data at sample intervals
    # ffill for 1 dimension
    resampled_df = df.resample(sample).ffill().reset_index()
else:
    name = 'All stations'
    # Plot all stations on a network
    # station-n dimensions
    # Group by network-id to resample and fill values
    resampled_df = df.groupby('nuid').resample(sample).ffill()
    # Group by timestamp and aggregate columns
    resampled_df.reset_index(level='nuid', drop=True, inplace=True)
    resampled_df = resampled_df.groupby('timestamp').agg(
        {'bikes': 'sum', 'ebikes': 'sum', 'normal': 'sum', 'free': 'sum'}).reset_index()

x = resampled_df['timestamp']
bikes = resampled_df['bikes']
free = resampled_df['free']
ebikes = resampled_df['ebikes']
normal = resampled_df['normal']

# Does not really interpolate values. This is useful for noisy sources
# from scipy.ndimage import gaussian_filter1d
# sigma = 2
# bikes = gaussian_filter1d(bikes, sigma=sigma)
# free = gaussian_filter1d(free, sigma=sigma)
# ebikes = gaussian_filter1d(ebikes, sigma=sigma)
# normal = gaussian_filter1d(normal, sigma=sigma)

cmap = mpl.colormaps[palette]
color1 = cmap(0.3)
color2 = cmap(0.6)
color3 = cmap(0.9)
mpl.rc('font', family='Roboto Mono')

# Plot the resampled data
fig = plt.figure(figsize=(20, 2), facecolor=bgcolor)     # inches ...
ax = plt.subplot(111)

ax.plot(x, bikes, label='Bikes', color=color1)
# ax.fill_between(x, ebikes, normal + ebikes, color=color1)
# ax.plot(x, ebikes, label='Electric bikes', color=color2)
# ax.fill_between(x, 0, ebikes, color=color2)

ax.plot(resampled_df['timestamp'], free + bikes, label='Bikes + Slots',
        linewidth=1, color=color3)
plt.ylim(0)

# Customize x-axis to show ticks for each hour and label days
# ax = plt.gca()
ax.set_facecolor(bgcolor)
ax.xaxis.set_major_locator(DayLocator())
ax.xaxis.set_major_formatter(DateFormatter('%a %d'))

ax.yaxis.set_major_locator(MaxNLocator(integer=True))
# ax.xaxis.set_minor_formatter(DateFormatter('%H:00'))

ax.spines['bottom'].set_color(fgcolor)
ax.spines['left'].set_color(fgcolor)
ax.spines['top'].set_color(fgcolor)
ax.spines['right'].set_color(fgcolor)

ax.tick_params(axis='x', which='major', labelsize=8, colors=fgcolor)
ax.tick_params(axis='y', which='major', labelsize=8, colors=fgcolor)

plt.title(f'{tag} - {name} - Bike and Slot Availability Over Time (Resampled {sample})',
          fontsize=12, color=fgcolor)

plt.grid(True, which='major', linestyle='-', linewidth=0.2, color=grcolor)
plt.grid(True, which='minor', linestyle='-', linewidth=0.2, color=grcolor)

plt.xticks(rotation=0)


# No fucking idea, this fucking sucks
ax.legend(
    fancybox=True, shadow=True, ncol=1,
    framealpha=0.7,
    fontsize=8, facecolor=bgcolor, labelcolor=fgcolor,
    loc='lower left',
)

plt.tight_layout()

if args.outfile:
    plt.savefig(args.outfile)
else:
    plt.show()

