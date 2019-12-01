#!/usr/bin/env python


import pandas as pd
import numpy as np
import os
import sys


MODEL_DATA_PATH = os.path.join("data", "model_data.csv")


def calculate_partisan_score(partisan_lean):
    # 0:                0
    # (+/-) 1-4:        (+/-) 1
    # (+/-) 5-9:        (+/-) 2 
    # (+/-) 10-14:      (+/-) 3 
    # (+/-) 15-19:      (+/-) 4
    # (>=/<=) (+/-) 20: (+/-) 5

    sign = -1 if partisan_lean < 0 else 1

    if partisan_lean == 0:
        return 0

    if abs(partisan_lean) < 20:
        return (abs(partisan_lean) // 5 + 1) * sign
    else:
        return 5 * sign


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: {} <demographic data path> <cleaned election data path>".format(sys.argv[0]))
        sys.exit(1)

    demographic_data = pd.read_csv(sys.argv[1])
    election_data = pd.read_csv(sys.argv[2])

    demographic_data["partisan_score"] = election_data.apply(lambda row: calculate_partisan_score(row["partisan_lean"]), axis=1)

    demographic_data.to_csv(MODEL_DATA_PATH, index=False)

    print()
    print("[*] Wrote labeled data to {}".format(MODEL_DATA_PATH))
    print()

