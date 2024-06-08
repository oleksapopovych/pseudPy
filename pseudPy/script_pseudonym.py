import argparse

import yaml
from yaml import CLoader as Loader
import Pseudonymization
import polars as pl
import polars.exceptions


def main(config_file):
    is_structured = True

    with open(config_file, 'r') as config_file:
        config = yaml.load(config_file, Loader=Loader)

    map_columns = config["map_columns"]
    map_method = config["map_method"]
    input_file = config["input_file"]
    pos_type = config["pos_type"]
    patterns = config["patterns"]
    output = config["output"]
    mapping = config["mapping"]
    encrypt_map = config["encrypt_map"]
    all_ne = config["all_ne"]
    seed = config["seed"]

    try:
        pl.read_csv(input_file)
        print("The data is structured.")
    except polars.exceptions.ComputeError:
        is_structured = False
        print("The data is not structured.")

    pseudo = Pseudonymization.Pseudonymization(
        map_columns=map_columns,
        map_method=map_method,
        input_file=input_file,
        pos_type=pos_type,
        patterns=patterns,
        output=output,
        mapping=mapping,
        encrypt_map=encrypt_map,
        all_ne=all_ne,
        seed=seed
    )

    if not is_structured:
        pseudo.nlp_pseudonym()
    else:
        pseudo.pseudonym()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', type=str)
    args = parser.parse_args()

    main(args.config_file)
