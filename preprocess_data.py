import pandas as pd
import numpy as np
from multiprocessing import Pool
import tqdm
import multiprocessing as mp

"""Script for data preprocessing"""


DATA_PATH = "https://raw.githubusercontent.com/mkmkl93/ml-ca/master/data/uniform_200k/dataset1_200.csv"
OUTPUT_DATA_PATH = "data/data.csv"


def preprocess_row(idx):
    row = data.iloc[idx]
    new_df = pd.DataFrame()
    new_df["time"] = [v for i, v in enumerate(row[1:-2]) if (i % 2 == 0)] + [0]
    n = new_df.shape[0] + 1
    new_df["dose"] = [v for i, v in enumerate(row[1:-1]) if (i % 2 != 0)] + [0]
    new_df["series"] = idx
    new_df["time_idx"] = range(n - 1)
    new_df["is_target"] = [0 for _ in range(n - 2)] + [1]
    new_df["target"] = [0 for _ in range(n - 2)] + [row[-1]]
    return new_df


def main():
    print(f"Availiable CPU cores number is {mp.cpu_count()}")
    k = data.shape[0]

    with Pool(mp.cpu_count() - 1) as p:
        results = list(tqdm.tqdm(p.imap_unordered(preprocess_row, range(k)), total=k))

    new_data = pd.concat(results)
    new_data.to_csv(OUTPUT_DATA_PATH)


if __name__ == "__main__":
    data = pd.read_csv(DATA_PATH)
    main()