import pandas as pd
import matplotlib.pyplot as plt
import os
import argparse
import numpy as np

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", dest="dir", required=True, help="Path to the directory")
    args = parser.parse_args()

    granularity_pct = 10  # 10% bins

    # Bins for percentage of different authors
    pct_bins = np.arange(0, 101, granularity_pct)
    pct_labels = [f"{i}-{i+granularity_pct}%" for i in pct_bins[:-1]]
    pct_counts = dict.fromkeys(pct_labels, 0)

    # Bins for Fix Time (days), with last bin open-ended
    INITIAL_WIDTH = 1
    WIDTH_INCREASE = 7

    bin_starts = [0]
    width = INITIAL_WIDTH
    while bin_starts[-1] < 1000:
        bin_starts.append(bin_starts[-1] + width)
        width += WIDTH_INCREASE  # increase bin width linearly

    # Create labels and counts
    day_labels = [f"{bin_starts[i]}-{bin_starts[i+1]}d" for i in range(len(bin_starts) - 1)]
    day_labels.append(f"{bin_starts[-1]}d+")
    day_counts = dict.fromkeys(day_labels, 0)

    for file in os.listdir(args.dir):
        df = pd.read_csv(os.path.join(args.dir, file))
        df.dropna(inplace=True)
        df = df.loc[df['Keyword Label'] == 1]

        # Convert date columns
        df["Commit Date"] = pd.to_datetime(df["Commit Date"])
        df["Previous Commit Date"] = pd.to_datetime(df["Previous Commit Date"])
        df["Fix Time (days)"] = (df["Commit Date"] - df["Previous Commit Date"]).dt.total_seconds() / 86400


        diff_authors = df[df["Author"] != df["Previous Author"]]
        if len(df) > 0:
            percent = len(diff_authors) / len(df) * 100
        else:
            percent = 0


        for i in range(len(pct_bins) - 1):
            if pct_bins[i] <= percent < pct_bins[i + 1]:
                pct_counts[pct_labels[i]] += 1
                break

        
        for days in df["Fix Time (days)"]:
            if days < 0 or pd.isna(days):
                continue
            binned = False
            for i in range(len(bin_starts) - 1):
                if bin_starts[i] <= days < bin_starts[i + 1]:
                    day_counts[day_labels[i]] += 1
                    binned = True
                    break
            if not binned:
                day_counts[day_labels[-1]] += 1

    # Plots
    plt.figure(figsize=(12, 5))

    plt.subplot(1, 2, 1)
    plt.bar(pct_counts.keys(), pct_counts.values(), color="#66b3ff")
    plt.xticks(rotation=45)
    plt.title("Change of Author")
    plt.xlabel("% of Hunks with different previous & current authors")
    plt.ylabel("Number of Repositories")

    plt.subplot(1, 2, 2)
    plt.bar(day_counts.keys(), day_counts.values(), color="#ffa07a")
    plt.xticks(rotation=45)
    plt.title("Time taken to fix (days)")
    plt.xlabel("Time taken to fix")
    plt.ylabel("Number of Hunks")

    plt.tight_layout()
    plt.show()
