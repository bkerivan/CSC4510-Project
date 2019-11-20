#!/usr/bin/env python

# Writes cleaned election data for a particular year to a separate CSV. Takes a
# minute to run.

import pandas as pd
import numpy as np

import os
import sys


popular_vote_percentages = {
    2000: {'R': 47.9, 'D': 48.4},
    2004: {'R': 50.7, 'D': 48.3},
    2008: {'R': 45.7, 'D': 52.9},
    2012: {'R': 47.2, 'D': 51.1},
    2016: {'R': 46.1, 'D': 48.2}
}


DEFAULT_DATA_PATH = os.path.join("data", "presidential_elections.csv")


# Calculate Cook Partisan Voting index for a county (row) in the table
def calculate_pvi(year, row):
    # This isn't actually the sum for some rows. Could just drop those rows...
    row = row.drop("totalvotes")

    party_vote_cols = [col for col in row.index if col.endswith("votes")]
    total_votes = sum(row[party_vote_cols])

    winning_party = 'R' if row["republicanvotes"] > row["democratvotes"] else 'D'
    percentage = (max(row["republicanvotes"], row["democratvotes"]) / total_votes) * 100
    pvi = int(round(percentage - popular_vote_percentages[year][winning_party])) 
    return "{}{}{}".format(winning_party, "+" if pvi >= 0 else "", pvi)


def clean_election_data(path=DEFAULT_DATA_PATH, year=2016):
    data = pd.read_csv(path)
    cleaned = data[data["year"] == year].drop(columns = ["FIPS", "office", "candidate", "version"])

    # "NA" party cleaned to NaN by Pandas, but it means "other" for us
    cleaned["party"] = cleaned["party"].fillna(value="other")

    # Some rows have state_po as NaN which for our purposes means those records
    # are irrelevant. If no votes were cast, candidate_votes is also NaN.
    cleaned.dropna(inplace=True)

    # No further use for state_po.
    cleaned.drop("state_po", axis=1, inplace=True)

    # Want one record for each county, with vote counts for each party
    parties = cleaned["party"].unique()
    cleaned[[party + "votes" for party in parties]] = pd.DataFrame([[np.nan] * len(parties)], index=cleaned.index)

    # Need this for getting unique values
    cleaned["full_county"] = cleaned["county"] + ", " + cleaned["state"]

    # Only want one record per county
    for county in cleaned["full_county"].unique():
        records = cleaned[cleaned["full_county"] == county]

        for party in parties:
            party_votes = records.loc[records["party"] == party, "candidatevotes"]
            party_votes = np.nan if party_votes.size == 0 else party_votes.values[0]
            cleaned.loc[cleaned["full_county"] == county, party + "votes"] = party_votes

    # Don't need these anymore
    cleaned.drop(columns=["party", "candidatevotes", "full_county"], inplace=True)

    cleaned.drop_duplicates(inplace=True)

    cleaned["PVI"] = cleaned.apply(lambda row: calculate_pvi(year, row), axis=1)

    return cleaned


if __name__ == "__main__":
    if len(sys.argv) > 2:
        print("Usage: {} <optional election year>".format(sys.argv[0]))
        sys.exit(1)
    elif len(sys.argv) == 2:
        year = int(sys.argv[1])
    else:
        year = 2016
    
    cleaned = clean_election_data(year=year)

    output_path = os.path.join("data", "election_{}_cleaned.csv".format(year))
    cleaned.to_csv(output_path, index=False)

    print()
    print("[*] Wrote cleaned data to {}".format(output_path))
    print()

