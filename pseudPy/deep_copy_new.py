import base64
import hashlib
from typing import List
import math
import random
import sys
import uuid
import polars as pl
import os
import re
import pandas as pd
import polars.exceptions
import spacy
from cryptography.hazmat.primitives.padding import PKCS7
from presidio_evaluator.data_generator import PresidioDataGenerator
from spacy.matcher import Matcher
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from faker import Faker


class Pseudonymization:
    """Main pseudonymization class"""
    def __init__(self, map_method='counter', map_columns=None, input_file=None, output=None, df=None, mapping=True,
                 encrypt_map=False, text=None, all_ne=False, seed=None, pos_type=None, patterns=None):
        self.map_columns = map_columns
        self.map_method = map_method
        self.input_file = input_file
        self.output = output
        self.df = df
        self.mapping = mapping
        self.encrypt_map = encrypt_map
        self.text = text
        self.all_ne = all_ne
        self.seed = seed
        # TODO select multiple pos_types in GUI
        self.pos_type = pos_type
        self.patterns = patterns

    def pseudonym(self):
        """Main function to initiate pseudonymization of csv.
            Returns a pseudonymized Dataframe or a String. Writes pseudonymized and mapping files."""
        if self.input_file is not None:
            self.df = pl.DataFrame()
            self.df = pl.read_csv(self.input_file)
        # remove columns and rows with all null values
        self.df = self.df.filter(~pl.all_horizontal(pl.all().is_null()))
        self.df = self.df[[s.name for s in self.df if not (s.null_count() == self.df.height)]]
        if self.df.select(pl.len()).item() == 0:
            print("Error: the number of rows must be at least 1.")
            sys.exit()
        if isinstance(self.map_columns, str):
            self.map_columns = [self.map_columns]
        if self.map_method in map_method_handlers:
            if self.output is not None:
                handle_map_tiers(self.df, self.output, self.map_columns, self.map_method,
                                 self.mapping, self.encrypt_map, self.seed, output_files=True)
            else:
                return handle_map_tiers(self.df, self.output, self.map_columns,
                                        self.map_method,
                                        self.mapping, self.encrypt_map, self.seed, output_files=False)

    def revert_pseudonym(self, revert_df=None, pseudonyms=None):
        """Revert structured data to original in form of Dataframe."""
        if pseudonyms is not None:
            try:
                revert_df = revert_df.filter(pl.col(f"Index_{self.map_columns}").is_in(pseudonyms))
            except polars.exceptions.InvalidOperationError:
                pseudonyms = [int(i) for i in pseudonyms]
                revert_df = revert_df.filter(pl.col(f"Index_{self.map_columns}").is_in(pseudonyms))
            self.df = self.df.filter(pl.col(f"Index_{self.map_columns}").is_in(pseudonyms))
        self.df.insert_column(self.df.get_column_index(f'Index_{self.map_columns}'), revert_df[self.map_columns])
        self.df = self.df.drop(f'Index_{self.map_columns}')
        if self.output is None:
            return self.df
        else:
            self.df.write_csv(f'{self.output}/reverted_output.csv')
            return self.df

    def nlp_pseudonym(self):
        """Main function for pseudonymization of free text.
            Returns a pseudonymized String or writes the pseudonymized String and mappings to files."""
        # definitions
        counter = 0
        list_with_all_df = []

        nlp = spacy.load("en_core_web_sm")

        if self.input_file is not None:
            file = open(self.input_file, "r")
            self.text = file.read()
            file.close()

        if isinstance(self.pos_type, str) and self.patterns is None:
            self.pos_type = [self.pos_type]

        map_dict = entity_mapping(self.text, nlp, self.all_ne, self.pos_type, self.patterns)
        # create pseudonyms and replace entities with pseudonyms in text
        for key in map_dict:
            df_pos = pl.DataFrame()
            if self.map_method == 'encrypt':
                mapping = Mapping(df_pos, output=self.output, first_tier=key)
                mapping.generate_keys()
            if map_dict[key]:
                df_pos = pseudo_nlp_mapper(map_dict[key], self.map_method, df_pos, counter, key, output=self.output)
                if self.map_method == 'counter':
                    counter = int((df_pos.select(pl.last(f'Index_{key}')).to_series())[0]) + 1
                for subst in df_pos.to_dicts():
                    self.text = self.text.replace(str(subst[key]), str(subst[f'Index_{key}']))
                # encrypt mapping data if requested
                if self.encrypt_map:
                    mapping = Mapping(df_pos, output=self.output, first_tier=key)
                    mapping.generate_keys()
                    df_pos = df_pos.with_columns(df_pos[key].map_elements(lambda x: Mapping.encrypt_data(mapping, x),
                                                                          return_dtype=pl.Utf8).alias(key))
                if self.map_method == 'encrypt':
                    df_pos = df_pos.drop(key)
            list_with_all_df.append(df_pos)
        # output options
        if self.output:
            for index in range(len(list_with_all_df)):
                if not list_with_all_df[index].is_empty():
                    list_with_all_df[index].write_csv(f'{self.output}/mapping_output_{list_with_all_df[index].columns[0].
                                                      split('_', 1)[-1]}.csv')

                with open(f"{self.output}/text.txt", "w") as text_file:
                    print(self.text, file=text_file)
        else:
            list_with_all_df.append(self.text)
            return list_with_all_df

    def revert_nlp_pseudonym(self, revert_df, pseudonyms=None):
        """Revert free text to original and return it as a String."""
        if pseudonyms is not None:
            try:
                revert_df = revert_df.filter(pl.col(f"Index_{self.map_columns}").is_in(pseudonyms))
            except polars.exceptions.InvalidOperationError:
                pseudonyms = [int(i) for i in pseudonyms]
                revert_df = revert_df.filter(pl.col(f"Index_{self.map_columns}").is_in(pseudonyms))
        for subst in revert_df.to_dicts():
            self.text = self.text.replace(str(subst[f'Index_{self.map_columns}']), str(subst[self.map_columns]))
        if self.output is None:
            return self.text
        else:
            with open(f"{self.output}/reverted_text.txt", "w") as text_file:
                print(self.text, file=text_file)
            return self.text


class Mapping:
    """Helper class for pseudonym creation, defines all pseudonymization methods and additional processing functions."""
    def __init__(self, df, first_tier=None, count_start=0, seed=None, output=None):
        self.df = df
        self.first_tier = first_tier
        self.count_start = count_start
        self.seed = seed
        self.output = output

    def counter_tier(self):
        """Counter method: return Series of ascending numbers as pseudonyms"""
        df_height = len(self.df)
        return pl.Series(f'Index_{self.first_tier}',
                         [*range(self.count_start, self.count_start + df_height)])

    @staticmethod
    def random_uuid_1():
        """Generate a UUID from a host ID, sequence number, and the current time"""
        return str(uuid.uuid1())

    @staticmethod
    def random_uuid_4():
        """Generate a random UUID"""
        return str(uuid.uuid4())

    def random1_tier(self):
        """Random1 method: return a Series of pseudonyms as a UUID from a host ID,
        sequence number, and the current time. Seed is possible."""
        if self.seed is not None:
            random.seed(self.seed)
            return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
                lambda x: str(uuid.UUID(int=random.getrandbits(128), version=1)), return_dtype=pl.Utf8))
        else:
            return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
                lambda x: Mapping.random_uuid_1(), return_dtype=pl.Utf8))

    def random4_tier(self):
        """Random4 method: return a Series of pseudonyms as a random UUID. Seed is possible."""
        if self.seed is not None:
            random.seed(self.seed)
            return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
                lambda x: str(uuid.UUID(int=random.getrandbits(128), version=4)), return_dtype=pl.Utf8))
        else:
            return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
                lambda x: Mapping.random_uuid_4(), return_dtype=pl.Utf8))

    def hash_tier(self):
        """Hashing method: use sha256 to generate a Series of pseudonyms."""
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: hashlib.sha256(x.encode()).hexdigest(), return_dtype=pl.Utf8))

    def hash_salt_tier(self):
        """Hashing method with salt: use sha256 and additional random generated salt. Return a Series of pseudonyms."""
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: hashlib.sha256(uuid.uuid4().hex.encode() + x.encode()).hexdigest(), return_dtype=pl.Utf8))

    def merkle_tree_tier(self):
        """Merkle Trees root as pseudonym. Return a Series of pseudonyms."""
        def mixmerkletree(elems):
            mtree = MerkleTree(elems)
            return mtree.getRootHash()

        list_of_rows = self.df.rows()
        output = []
        for user in list_of_rows:
            output.append(mixmerkletree(list(filter(lambda item: item is not None, user))))
        return pl.Series(f'Index_{self.first_tier}', output)

    def generate_keys(self):
        """Generate secret keys for data encryption/decryption."""
        key = os.urandom(32)
        hex_key = key.hex()
        if self.output is not None:
            with open(f'{self.output}/secure_key_Index_{self.first_tier}.txt', 'w') as file:
                file.write(hex_key)
        else:
            with open(f'secure_key_Index_{self.first_tier}.txt', 'w') as file:
                file.write(hex_key)

    # Source: https://www.askpython.com/python/examples/implementing-aes-with-padding
    def encrypt_data(self, data):
        """Return encrypted data string."""
        data = data.encode('utf-8')
        if self.output is not None:
            with open(f'{self.output}/secure_key_Index_{self.first_tier}.txt', 'r') as file:
                hex_key = file.read()
        else:
            with open(f'secure_key_Index_{self.first_tier}.txt', 'r') as file:
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
        """Encrypt the data in Dataframe. Return Series of encrypted data."""
        try:
            return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
                lambda x: Mapping.encrypt_data(self, x), return_dtype=pl.Utf8))
        except polars.exceptions.ColumnNotFoundError:
            print("Error: check whether all elements in the selected column are not empty and not None.")

    # Source: https://www.askpython.com/python/examples/implementing-aes-with-padding
    def decrypt_data(self, data):
        """Return decrypted data string."""
        data = data.encode('utf-8')
        if self.output is not None:
            with open(f'{self.output}/secure_key_{self.first_tier}.txt', 'r') as file:
                hex_key = file.read()
        else:
            with open(f'secure_key_{self.first_tier}.txt', 'r') as file:
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
        """Decrypt the data in Dataframe. Return Series of decrypted data."""
        return pl.Series(f'Decrypted_{self.first_tier}', self.df[f'{self.first_tier}'].map_elements(
            lambda x: Mapping.decrypt_data(self, x), return_dtype=pl.Utf8))

    def decrypt_nlp_tier(self, text):
        for subst in self.df.to_dicts():
            index_value = str(subst[f'{self.first_tier}'])
            decrypted_value = str(Mapping.decrypt_data(self, index_value))
            text = text.replace(index_value, decrypted_value)
        return text

    @staticmethod
    def generate_fake_names():
        """Generate fake name using Faker."""
        fake = Faker()
        return fake.name()

    def faker_names_tier(self):
        """Apply fake names as pseudonyms. Return Series of pseudonyms."""
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: Mapping.generate_fake_names(), return_dtype=pl.Utf8))

    @staticmethod
    def generate_fake_phone_number() -> str:
        """Generate fake phone number using Faker."""
        fake = Faker()
        return f'+49 {fake.msisdn()[3:]}'

    def faker_phone_number_tier(self):
        """Apply fake phone numbers as pseudonyms. Return Series of pseudonyms."""
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: Mapping.generate_fake_phone_number(), return_dtype=pl.Utf8))

    @staticmethod
    def generate_fake_location():
        """Generate fake location using Faker."""
        fake = Faker()
        return fake.city()

    def faker_location_tier(self):
        """Apply fake locations as pseudonyms. Return Series of pseudonyms."""
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: Mapping.generate_fake_location(), return_dtype=pl.Utf8))

    @staticmethod
    def generate_fake_email():
        """Generate fake email using Faker."""
        fake = Faker()
        return fake.email()

    def faker_email_tier(self):
        """Apply fake email as pseudonyms. Return Series of pseudonyms."""
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: Mapping.generate_fake_email(), return_dtype=pl.Utf8))

    @staticmethod
    def generate_fake_org_name():
        """Generate fake company name using Faker."""
        fake = Faker()
        return fake.company()

    def faker_org_tier(self):
        """Apply fake company as pseudonyms. Return Series of pseudonyms."""
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
            lambda x: Mapping.generate_fake_org_name(), return_dtype=pl.Utf8))



# Merkle Tree adapted from: https://github.com/onuratakan/mix_merkletree
class Node:
    def __init__(self, left, right, value: str, content, is_copied=False) -> None:
        self.left: Node = left
        self.right: Node = right
        self.value = value
        self.content = content
        self.is_copied = is_copied

    @staticmethod
    def hash(val: str) -> str:
        return hashlib.sha256(val.encode('utf-8')).hexdigest()

    def __str__(self):
        return str(self.value)

    def copy(self):
        return Node(self.left, self.right, self.value, self.content, True)


class MerkleTree:
    def __init__(self, values: List[str]) -> None:
        self.__buildTree(values)

    def __buildTree(self, values: List[str]) -> None:

        leaves: List[Node] = [Node(None, None, Node.hash(e), e) for e in values]
        if len(leaves) % 2 == 1:
            leaves.append(leaves[-1].copy())  # duplicate last elem if odd number of elements
        self.root: Node = self.__buildTreeRec(leaves)

    def __buildTreeRec(self, nodes: List[Node]) -> Node:
        if len(nodes) % 2 == 1:
            nodes.append(nodes[-1].copy())  # duplicate last elem if odd number of elements
        half: int = len(nodes) // 2

        if len(nodes) == 2:
            return Node(nodes[0], nodes[1], Node.hash(nodes[0].value + nodes[1].value),
                        nodes[0].content + "+" + nodes[1].content)

        left: Node = self.__buildTreeRec(nodes[:half])
        right: Node = self.__buildTreeRec(nodes[half:])
        value: str = Node.hash(left.value + right.value)
        content: str = f'{left.content}+{right.content}'
        return Node(left, right, value, content)

    def getRootHash(self) -> str:
        return self.root.value


class Aggregation:
    """Class for data aggregation."""
    def __init__(self, column, method, df=None, input_file=None, output=None):
        self.column = column
        self.method = method
        self.df = df
        self.input_file = input_file
        self.output = output

    def group(self):
        """Main function to initiate aggregation of csv. Return a Dataframe or write output file."""
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
        """Aggregate only numerical data in the given columns of a Dataframe.
        Return Dataframe column with aggregated values."""
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
        """Aggregate only date values to years. Return aggregated Dataframe."""
        self.df[self.column] = pd.to_datetime(self.df[self.column]).dt.year
        if self.method[1] == 1:
            return self.df
        self.df[self.column] = self.group_num()
        return self.df


# TODO create helpers class
def handle_map_tiers(df, output, map_columns, map_method, mapping, encrypt_map, seed, output_files):
    """General function for organizing pseudonymized data. Return dataframes with pseudonymized data and mappings,
    write both to files."""
    count_start = 0
    return_map_output = []
    df = int_to_str(df)
    df_map_all = df.clone()

    for i in range(0, len(map_columns)):
        df_copy = df.clone()
        columns_to_keep = []

        mapping_instance = Mapping(df, map_columns[i], count_start, seed, output)
        if encrypt_map or (map_method == 'encrypt'):
            Mapping.generate_keys(mapping_instance)
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
                df_copy[map_columns[i]].map_elements(lambda x: Mapping.encrypt_data(mapping_instance, x),
                                                     return_dtype=pl.Utf8).alias(map_columns[i])
            )
        # outputs
        if output_files and (map_method != 'encrypt') and (map_method != 'decrypt'):
            if mapping:
                # mapping file contains only the pseudonyms and corresponding original row
                df_copy.write_csv(f'{output}/mapping_output_{map_columns[i]}.csv')
        return_map_output.append(df_copy)
    if output_files:
        # output file contains pseudonyms and other rows that were not modified
        df_map_all.write_csv(f'{output}/output.csv')
    if mapping:
        return [df_map_all, return_map_output]
    else:
        return df_map_all


def int_to_str(df):
    """Convert all values to String."""
    for col in df.columns:
        df = df.with_columns(pl.col(col).cast(pl.Utf8))
    return df


def pseudo_nlp_mapper(list_, map_method, df, counter, field, output=None):
    """Pseudonym mapper for free text. Return df with pseudonyms."""
    df = df.with_columns(pl.Series(field, list_))
    mapping_instance = Mapping(df, field, count_start=counter, output=output)
    if map_method == 'faker':
        if field in faker_pos_handlers:
            df.insert_column(0, faker_pos_handlers[field](mapping_instance))
    else:
        if map_method in map_method_handlers:
            # call the pseudonymization methods
            df.insert_column(0, map_method_handlers[map_method](mapping_instance))
    df = int_to_str(df)
    return df


def entity_mapping(text, nlp, all_ne, pos_type, patterns):
    """Use spaCy and regex for entity categorization. Return organized data as dictionary."""
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    phone_number_pattern = "\\+?[1-9][0-9]{7,14}"
    doc = nlp(text)
    map_dict = dict()

    if patterns is not None:
        map_dict[pos_type] = []
        matcher = Matcher(nlp.vocab)
        matcher.add(f"pattern_{uuid.uuid4()}", patterns)
        matches = matcher(doc)

        for match_id, start, end in matches:
            span = doc[start:end]
            if span.text not in map_dict[pos_type]:
                map_dict[pos_type].append(span.text)
    else:
        if pos_type is not None:
            for pos in pos_type:
                map_dict[f'{pos}'] = []
        if all_ne:
            map_dict = {'Names': [], 'Locations': [], 'Organizations': []}
        else:
            if pos_type is not None:
                if 'Phone-Numbers' in pos_type:
                    map_dict['Phone-Numbers'].extend(re.findall(phone_number_pattern, text, flags=re.IGNORECASE))
                if 'Emails' in pos_type:
                    map_dict['Emails'].extend(re.findall(email_pattern, text, flags=re.IGNORECASE))
    # find and append entities
    for ent in doc.ents:
        if all_ne and (pos_type is None) and (patterns is None):
            if ent.label_ == 'PERSON' and ent.text not in map_dict['Names']:
                map_dict['Names'].append(ent.text)
            elif ent.label_ == 'GPE' and ent.text not in map_dict['Locations']:
                map_dict['Locations'].append(ent.text)
            elif ent.label_ == 'ORG' and ent.text not in map_dict['Organizations']:
                map_dict['Organizations'].append(ent.text)
        if (pos_type is not None) and (patterns is None):
            if 'Names' in pos_type and ent.label_ == 'PERSON' and ent.text not in map_dict['Names']:
                map_dict['Names'].append(ent.text)
            elif 'Locations' in pos_type and ent.label_ == 'GPE' and ent.text not in map_dict['Locations']:
                map_dict['Locations'].append(ent.text)
            elif 'Organizations' in pos_type and ent.label_ == 'ORG' and ent.text not in map_dict['Organizations']:
                map_dict['Organizations'].append(ent.text)
    """
    count_ent = 0
    for i in map_dict:
        count_ent = count_ent + len(map_dict[i])
    print(f"Count results: {count_ent}")
    token_count = len(doc)
    print(f"The number of tokens in the text is: {token_count}")
    """
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
    'Phone-Numbers': Mapping.faker_phone_number_tier,
    'Organizations': Mapping.faker_org_tier
}

faker_pattern_handlers = {
    'Names': Mapping.generate_fake_names,
    'Locations': Mapping.generate_fake_location,
    'Emails': Mapping.generate_fake_email,
    'Phone-Numbers': Mapping.generate_fake_phone_number,
    'Organizations': Mapping.generate_fake_org_name
}

group_handlers = {
    'number': Aggregation.group_num,
    'dates-to-years': Aggregation.group_dates_to_years
}


# Adapted from: https://programming-dp.com/ch2.html
# TODO : output as file
def k_anonymity(df, depths, k, mask_others=False):
    def generalize(col):
        if col in depths:
            depth = depths[col]
            return df[col].apply(lambda y: int(int(y / (10 ** depth)) * (10 ** depth))if pd.notnull(y) else y)
        return df[col]

    for key in depths:
        if pd.api.types.is_integer_dtype(df[key]):
            df[key] = generalize(key)
        elif pd.api.types.is_float_dtype(df[key]):
            df[key].apply(lambda x: round(x) if pd.notnull(x) else x)
            df[key] = generalize(key)
        else:
            if mask_others:
                df[key] = '*'

    # filter data with at least k records
    grouped = df.groupby(list(depths.keys())).filter(lambda x: len(x) >= k)

    return grouped


def is_k_anonymized(df, k):
    for index, row in df.iterrows():
        query = ' & '.join([f'`{col}` == {repr(row[col])}' for col in df.columns])
        rows = df.query(query)
        if rows.shape[0] < k:
            return False
    return True


def remove_other_datatypes(df, columns):
    for key in columns:
        df[key] = '*'
    return df


def run():
    pass
    # TODO : Presidio - fake data generation

    sentence_templates = [
        "My name is {{name}}",
        "Please send it to {{address}}",
        "I just moved to {{city}} from {{country}}",
    ]

    data_generator = PresidioDataGenerator()
    fake_records = data_generator.generate_fake_data(
        templates=sentence_templates, n_samples=10
    )

    fake_records = list(fake_records)

    # Print the spans of the first sample
    #for i in range(len(fake_records)):
    #    print(fake_records[i].fake)
    #print(fake_records[0].spans)

    for i in range(len(fake_records)):
        print(fake_records[i].fake)
        pseudo = Pseudonymization(
            map_method='merkle-tree',
            text=fake_records[i].fake,
            pos_type=['Names', 'Locations']
        )
        print(pseudo.nlp_pseudonym()[2])


if __name__ == '__main__':
    run()

