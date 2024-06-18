import argparse

import yaml
from yaml import CLoader as Loader
import pandas.errors
import pandas as pd
import Pseudonymization as pseudPy


def main(config_file):

    with open(config_file, 'r') as config_file:
        config = yaml.load(config_file, Loader=Loader)

    input_file = config["input_file"]
    k = config["k"]

    try:
        input_df = pd.read_csv(input_file)

        df_header = input_df.columns.to_list()
        depths = {}
        if k > 0:
            for col in df_header:
                depths[col] = k - 1
        is_k_anonym = pseudPy.KAnonymity(
            df=input_df,
            k=k,
            depths=depths
        )
        print(is_k_anonym.is_k_anonymized())
    except pandas.errors.ParserError:
        print("Error: The data is not structured. Aggregation or k-anonymization is only available for structured data.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', type=str)
    args = parser.parse_args()

    main(args.config_file)
