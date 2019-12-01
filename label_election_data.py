#!/usr/bin/env python


import pandas as pd
import numpy as np
import sys


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: {} <cleaned election data path>".format(sys.argv[0]))
        sys.exit(1)

    data = pd.read_csv(sys.argv[1])

    r_counties = data[data["PVI"].str.startswith('R')]
    r_leans = pd.to_numeric(r_counties["PVI"].str[1:])
    r_lean_avg = round(np.mean(r_leans))
    r_lean_stddev = round(np.std(r_leans))

    d_counties = data[data["PVI"].str.startswith('D')]  
    d_leans = pd.to_numeric(d_counties["PVI"].str[1:])
    d_lean_avg = round(np.mean(d_leans))
    d_lean_stddev = round(np.std(d_leans))

    print("Average republican lean: {}".format(r_lean_avg))
    print("Republican lean stddev: {}".format(r_lean_stddev))
    print("Average democrat lean: {}".format(d_lean_avg))
    print("Democrat lean stddev: {}".format(d_lean_stddev))

    print()
    print("Average partisan lean: {}".format(np.mean(data["partisan_lean"])))

