import argparse
import re

import yaml
from yaml import CLoader as Loader
import Pseudonymization as pseudPy
import pandas.errors
import pandas as pd


def is_valid_date(date):
    date_regex = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    return bool(date_regex.match(date))


def main(config_file):
    is_structured = True

    with open(config_file, 'r') as config_file:
        config = yaml.load(config_file, Loader=Loader)

    agg_columns = config["agg_columns"].split(",")
    agg_columns = [i.strip() for i in agg_columns]
    input_file = config["input_file"]
    output = config["output"]
    k = config["k"]
    agg_range = config["aggregation_range"]

    try:
        input_df = pd.read_csv(input_file)
        print("The data is structured.")
    except pandas.errors.ParserError:
        is_structured = False
        print("Error: The data is not structured. Aggregation or k-anonymization is only available for structured data.")

    if agg_columns is not None:
        for col in agg_columns:
            if agg_range is not None:
                if pd.api.types.is_float_dtype(input_df[col]) or pd.api.types.is_integer_dtype(input_df[col]):
                    agg = pseudPy.Aggregation(
                        column=col,
                        method=['number', agg_range],
                        df=input_df
                    )
                    input_df = agg.group()
                elif is_valid_date(input_df[col].iloc[0]):
                    agg = pseudPy.Aggregation(
                        column=col,
                        method=['dates-to-years', agg_range],
                        df=input_df
                    )
                    input_df = agg.group_dates_to_years()
    df_header = input_df.columns.to_list()
    if k > 0:
        depths = {}
        if k > 0:
            for col in df_header:
                depths[col] = k - 1
        print(depths)
        k_anonymity = pseudPy.KAnonymity(
            df=input_df,
            depths=depths,
            k=k,
            output=output
        )
        print(k_anonymity.k_anonymity())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', type=str)
    args = parser.parse_args()

    main(args.config_file)
