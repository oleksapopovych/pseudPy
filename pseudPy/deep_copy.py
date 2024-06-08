import base64
import hashlib
import math
import sys
import uuid
import polars as pl
import os
import re
import pandas as pd
import polars.exceptions
import spacy
from cryptography.hazmat.primitives.padding import PKCS7

from pseudPy import merkle_trees
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from faker import Faker

home_dir = os.path.expanduser('~')
test_files_folder = f'{home_dir}/PycharmProjects/pseudPy/tests/new_test_files/'
destination_folder = f'{home_dir}'


class Pseudonymization:

    def __init__(self, map_method, map_columns=None, input_file=None, output=None, df=None, mapping=True,
                 encrypt_map=False, text=None, output_to_file=False, only_ne=True, seed=None):
        self._map_columns = map_columns
        self._map_method = map_method
        self._input_file = input_file
        self._output = output
        self._df = df
        self._mapping = mapping
        self._encrypt_map = encrypt_map
        self._text = text
        self._output_to_file = output_to_file
        self._only_ne = only_ne
        self._seed = seed

    def pseudonym(self):
        """Main function to initiate pseudonymization of csv"""
        if self._input_file is not None:
            self._df = pl.DataFrame()
            self._df = pl.read_csv(self._input_file)
        self._df = self._df.filter(~pl.all_horizontal(pl.all().is_null()))
        self._df = self._df[[s.name for s in self._df if not (s.null_count() == self._df.height)]]
        if self._df.select(pl.len()).item() == 0:
            print("Error: the number of rows must be at least 1.")
            sys.exit()
        if isinstance(self._map_columns, str):
            self._map_columns = [self._map_columns]
        if self._map_method in map_method_handlers:
            if self._output is not None:
                handle_map_tiers(self._df, self._output, self._map_columns, self._map_method,
                                 self._mapping, self._encrypt_map, self._seed, output_files=True)
            else:
                return handle_map_tiers(self._df, self._output, self._map_columns,
                                        self._map_method,
                                        self._mapping, self._encrypt_map, self._seed, output_files=False)

    def revert_pseudonym(self, revert_df):
        self._df.insert_column(self._df.get_column_index(f'Index_{self._map_columns}'), revert_df[self._map_columns])
        self._df = self._df.drop(f'Index_{self._map_columns}')
        return self._df

    def nlp_pseudonym(self):
        """Main function for pseudonymization of free text"""
        # definitions
        counter = 0
        list_with_all_df = []

        nlp = spacy.load("en_core_web_sm")
        if self._input_file is not None:
            file = open(self._input_file, "r")
            self._text = file.read()
            file.close()

        map_dict = entity_mapping(self._text, nlp, self._only_ne)
        # create pseudonyms and replace entities with pseudonyms in text
        for key in map_dict:
            df_pos = pl.DataFrame()
            if map_dict[key]:
                df_pos = pseudo_nlp_mapper(map_dict[key], self._map_method, df_pos, counter, key)
                if self._map_method == 'counter':
                    counter = int((df_pos.select(pl.last(f'Index_{key}')).to_series())[0]) + 1
                for subst in df_pos.to_dicts():
                    self._text = self._text.replace(str(subst[key]), str(subst[f'Index_{key}']))
                # encrypt mapping data if requested
                if self._encrypt_map:
                    df_pos = df_pos.with_columns(df_pos[key].map_elements(lambda x: Mapping.encrypt_data(x),
                                                                          return_dtype=pl.Utf8).alias(key))
            list_with_all_df.append(df_pos)
        # output options
        if self._output_to_file:
            for index in range(len(list_with_all_df)):
                if not list_with_all_df[index].is_empty():
                    list_with_all_df[index].write_csv(f'{test_files_folder}output_{index}')
            with open(f"{test_files_folder}text.txt", "w") as text_file:
                print(self._text, file=text_file)
        else:
            list_with_all_df.append(self._text)
            return list_with_all_df

    def revert_nlp_pseudonym(self, revert_df):
        for subst in revert_df.to_dicts():
            self._text = self._text.replace(str(subst[f'Index_{self._map_columns}']), str(subst[self._map_columns]))
        return self._text


class Mapping:

    def __init__(self, df, first_tier=None, count_start=0, seed=None):
        self.df = df
        self.first_tier = first_tier
        self.count_start = count_start
        self.seed = seed

    def counter_tier(self):
        df_height = len(self.df)
        return pl.Series(f'Index_{self.first_tier}',
                         [*range(self.count_start, self.count_start + df_height)])

    @staticmethod
    def random_uuid_1(fake):
        """Generate a UUID from a host ID, sequence number, and the current time"""
        if fake is not None:
            return str(fake.uuid1())
        else:
            return str(uuid.uuid1())

    @staticmethod
    def random_uuid_4(fake):
        """Generate a random UUID"""
        if fake is not None:
            return str(fake.uuid4())
        else:
            return str(uuid.uuid4())

    def random1_tier(self):
        if self.seed is not None:
            Faker.seed(self.seed)
            fake = Faker()
        else:
            fake = None
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: Mapping.random_uuid_1(fake), return_dtype=pl.Utf8))

    def random4_tier(self):
        if self.seed is not None:
            Faker.seed(self.seed)
            fake = Faker()
        else:
            fake = None
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: Mapping.random_uuid_4(fake), return_dtype=pl.Utf8))

    def hash_tier(self):
        """Hashing function"""
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: hashlib.sha256(x.encode()).hexdigest(), return_dtype=pl.Utf8))

    def hash_salt_tier(self):
        """Hashing function with salt"""
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: hashlib.sha256(uuid.uuid4().hex.encode() + x.encode()).hexdigest(), return_dtype=pl.Utf8))

    def merkle_tree_tier(self):
        """Merkle Trees root as pseudonym"""
        list_of_rows = self.df.rows()
        output = []
        for user in list_of_rows:
            output.append(merkle_trees.mixmerkletree(list(filter(lambda item: item is not None, user))))
        return pl.Series(f'Index_{self.first_tier}', output)

    @staticmethod
    def generate_keys():
        key = os.urandom(32)
        hex_key = key.hex()

        with open('secure_key.txt', 'w') as file:
            file.write(hex_key)

    @staticmethod
    # Source: https://www.askpython.com/python/examples/implementing-aes-with-padding
    def encrypt_data(data):
        data = data.encode('utf-8')
        with open('secure_key.txt', 'r') as file:
            hex_key = file.read()
        key = bytes.fromhex(hex_key)

        cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=default_backend())
        encryptor = cipher.encryptor()
        padder = PKCS7(algorithms.AES.block_size).padder()
        padded_data = padder.update(data) + padder.finalize()
        ciphertext = encryptor.update(padded_data) + encryptor.finalize()
        encodedciphertext = base64.b64encode(ciphertext)
        return encodedciphertext.decode('utf-8')

    def encrypt_tier(self):
        """Encrypt all data in the Dataframe"""
        try:
            return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
                lambda x: Mapping.encrypt_data(x), return_dtype=pl.Utf8))
        except polars.exceptions.ColumnNotFoundError:
            print("Error: check whether all elements in the selected column are not empty and not None.")

    @staticmethod
    def decrypt_data(data):
        data = data.encode('utf-8')
        with open('secure_key.txt', 'r') as file:
            hex_key = file.read()
        key = bytes.fromhex(hex_key)

        cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=default_backend())
        decryptor = cipher.decryptor()
        decodedciphertext = base64.b64decode(data)
        padded_data = decryptor.update(decodedciphertext) + decryptor.finalize()
        unpadder = PKCS7(algorithms.AES.block_size).unpadder()
        plaintext = unpadder.update(padded_data) + unpadder.finalize()
        return plaintext.decode('utf-8')

    def decrypt_tier(self):
        """Encrypt all data in the Dataframe"""
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: Mapping.decrypt_data(x), return_dtype=pl.Utf8))

    @staticmethod
    def generate_fake_names():
        fake = Faker()
        return fake.name()

    def faker_names_tier(self):
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: Mapping.generate_fake_names(), return_dtype=pl.Utf8))

    @staticmethod
    def generate_fake_phone_number() -> str:
        fake = Faker()
        return f'+49 {fake.msisdn()[3:]}'

    def faker_phone_number_tier(self):
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: Mapping.generate_fake_phone_number(), return_dtype=pl.Utf8))

    @staticmethod
    def generate_fake_location():
        fake = Faker()
        return fake.city()

    def faker_location_tier(self):
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: Mapping.generate_fake_location(), return_dtype=pl.Utf8))

    @staticmethod
    def generate_fake_email():
        fake = Faker()
        return fake.email()

    def faker_email_tier(self):
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: Mapping.generate_fake_email(), return_dtype=pl.Utf8))

    @staticmethod
    def generate_fake_org_name():
        fake = Faker()
        return fake.company()

    def faker_org_tier(self):
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: Mapping.generate_fake_org_name(), return_dtype=pl.Utf8))


class Aggregation:

    def __init__(self, column, method, df=None, input_file=None, output=None):
        self.column = column
        self.method = method
        self.df = df
        self.input_file = input_file
        self.output = output

    def group(self):
        """Main function to initiate aggregation of csv"""
        if self.input_file is not None:
            self.df = pd.DataFrame()
            self.df = pd.read_csv(self.input_file)
        if self.method[0] in group_handlers:
            group_handlers[self.method[0]](self)
        if self.output is not None:
            self.df.to_csv(f'{self.output}/output.csv', index=False)
        else:
            return self.df

    def group_num(self):
        """Aggregate only numerical data in the given columns of a Dataframe"""
        bins, labels = [0], []
        bin_update = 0
        max_number = self.df[self.column].max()
        for i in range(math.ceil(max_number / self.method[1])):
            bin_update = bin_update + self.method[1]
            if bin_update < max_number:
                bins.append(bin_update)
                if len(labels) == 0:
                    labels.append(f'0-{bin_update}')
                    labels.append(f'{bin_update + 1}-{bin_update + self.method[1]}')
                else:
                    labels.append(f'{bin_update + 1}-{bin_update + self.method[1]}')
        if len(labels) == len(bins):
            bins.append(max_number)
        try:
            self.df[self.column] = pd.cut(self.df[self.column], bins=bins, labels=labels)
        except ValueError:
            print("ValueError: the number of bins must be by one greater than of labels.")
        return self.df[self.column]

    def group_dates_to_years(self):
        """Aggregate only date values to years"""
        self.df[self.column] = pd.to_datetime(self.df[self.column]).dt.year
        self.group_num()
        return self.df


def handle_map_tiers(df, output, map_columns, map_method, mapping, encrypt_map, seed, output_files):
    """General function for handling most pseudonymization methods"""
    count_start = 0
    return_map_output = []
    df = int_to_str(df)
    df_map_all = df.clone()

    if encrypt_map:
        Mapping.generate_keys()

    for i in range(0, len(map_columns)):
        df_copy = df.clone()
        columns_to_keep = []

        mapping_instance = Mapping(df, map_columns[i], count_start, seed)
        # call the pseudonymization methods
        df_copy.insert_column(0, map_method_handlers[map_method](mapping_instance))

        if map_method == 'counter':
            last_index = (df_copy.select(pl.last(f'Index_{map_columns[i]}')).to_series())[0]
            count_start = last_index + 1
        # replace columns with pseudonyms
        try:
            df_map_all.insert_column(df.get_column_index(map_columns[i]), df_copy[f'Index_{map_columns[i]}'])
        except TypeError:
            print('TypeError: Check whether the column names match the input column names.')
        df_map_all = df_map_all.drop(map_columns[i])

        columns_to_keep.append(map_columns[i])
        columns_to_keep.append(f'Index_{map_columns[i]}')
        df_copy = df_copy.drop([col for col in df.columns if col not in columns_to_keep])
        # encrypt mappings if requested
        if encrypt_map:
            df_copy = df_copy.with_columns(
                df_copy[map_columns[i]].map_elements(lambda x: Mapping.encrypt_data(x),
                                                     return_dtype=pl.Utf8).alias(map_columns[i])
            )
        # outputs
        if output_files and (map_method != 'encrypt') and (map_method != 'decrypt'):
            if mapping:
                # mapping file contains only the pseudonyms and corresponding original row
                df_copy.write_csv(f'{output}mapping_output_{i}.csv')
        return_map_output.append(df_copy)
    if output_files:
        # output file contains pseudonyms and other rows that were not modified
        df_map_all.write_csv(f'{output}output.csv')
    if mapping:
        return [df_map_all, return_map_output]
    else:
        return df_map_all


def int_to_str(df):
    for col in df.columns:
        df = df.with_columns(pl.col(col).cast(pl.Utf8))
    return df


def pseudo_nlp_mapper(list_, map_method, df, counter, field):
    """Pseudonym mapper for nlp"""
    df = df.with_columns(pl.Series(field, list_))
    mapping_instance = Mapping(df, field, count_start=counter)
    if map_method == 'faker':
        if field in faker_pos_handlers:
            df.insert_column(0, faker_pos_handlers[field](mapping_instance))
    else:
        if map_method in map_method_handlers:
            # call the pseudonymization methods
            df.insert_column(0, map_method_handlers[map_method](mapping_instance))
    df = int_to_str(df)
    return df


def entity_mapping(text, nlp, only_ne):
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    phone_number_pattern = "\\+?[1-9][0-9]{7,14}"
    doc = nlp(text)
    if not only_ne:
        map_dict = {'Names': [], 'Locations': [], 'Organizations': [],
                    'Numbers': re.findall(phone_number_pattern, text, flags=re.IGNORECASE),
                    'Emails': re.findall(email_pattern, text, flags=re.IGNORECASE)}
    else:
        map_dict = {'Names': [], 'Locations': [], 'Organizations': []}

    # find and append entities
    for ent in doc.ents:
        if ent.label_ == 'PERSON':
            map_dict['Names'].append(ent.text)
        elif ent.label_ == 'GPE':
            map_dict['Locations'].append(ent.text)
        elif ent.label_ == 'ORG':
            map_dict['Organizations'].append(ent.text)
    return map_dict


map_method_handlers = {
    'counter': Mapping.counter_tier,
    'encrypt': Mapping.encrypt_tier,
    'decrypt': Mapping.decrypt_tier,
    'random1': Mapping.random1_tier,
    'random4': Mapping.random4_tier,
    'hash': Mapping.hash_tier,
    'hash-salt': Mapping.hash_salt_tier,
    'merkle-tree': Mapping.merkle_tree_tier,
    'faker-name': Mapping.faker_names_tier,
    'faker-loc': Mapping.faker_location_tier,
    'faker-email': Mapping.faker_email_tier,
    'faker-phone': Mapping.faker_phone_number_tier
}

faker_pos_handlers = {
    'Names': Mapping.faker_names_tier,
    'Locations': Mapping.faker_location_tier,
    'Emails': Mapping.faker_email_tier,
    'Numbers': Mapping.faker_phone_number_tier,
    'Organizations': Mapping.faker_org_tier
}

group_handlers = {
    'number': Aggregation.group_num,
    'dates-to-years': Aggregation.group_dates_to_years
}


def run():
    pass
    #pseudo = Pseudonymization('faker',
    #                          input_file='/Users/oleksandrapopovych/PycharmProjects/pseudPy/tests/free_text.txt',
    #                          output_to_file=True)
    #pseudo.nlp_pseudonym()

    file = open('/Users/oleksandrapopovych/PycharmProjects/pseudPy/pseudPy/text.txt', "r")
    text = file.read()
    file.close()
    list_columns = ['Names', 'Locations', 'Organizations']

    for elem in range(len(list_columns)):
        pseudo = Pseudonymization(map_columns=list_columns[elem], text=text, map_method='faker')
        df_revert = pl.read_csv(f'/Users/oleksandrapopovych/PycharmProjects/pseudPy/pseudPy/output_{elem}')
        text = pseudo.revert_nlp_pseudonym(df_revert)
    print(text)

    #df = pl.read_csv('output_0')
    #mapping = Mapping(df, first_tier='Numbers')
    #print(Mapping.decrypt_tier(mapping))

    #agg = Aggregation(column='Age', method=['number', 2],
    #                  input_file='/Users/oleksandrapopovych/PycharmProjects/pseudPy/tests/test_files/test.csv',
    #                  output=test_files_folder)

    #agg.group()


if __name__ == '__main__':
    run()
