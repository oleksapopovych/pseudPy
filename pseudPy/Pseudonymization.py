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
    """Main class for data pseudonymization.

    Parameters
    ----------
    map_method : str
        Pseudonymization method. Select one of the following: *'counter', 'random1', 'random4', 'hash', 'hash-salt',
        'merkle-tree', 'encrypt', 'decrypt', 'faker'*.

        Or specify the faker method: *'faker-name', 'faker-loc','faker-email', 'faker-phone', 'faker-org'*.
    map_columns : str or list
        Column(s) to be pseudonymized for structured data.
    input_file : str
        Path to input file. Use if the parameter df is not specified.
    output : str
        Path to output folder. Use if the output to file is required.
    df : Polars DataFrame
        Input CSV, read as Polars DataFrame. Use if data is structured and the input_file is not specified.
    mapping : bool
        Enable or disable mapping output. Required for reversibility of pseudonyms.
    encrypt_map : bool
        Enable or disable encryption of the mapping table. Output includes additionally secret key file for decryption.
    text : str
        Input text. Use if data is unstructured and the input_file is not specified.
    all_ne : bool
        Enable or disable pseudonymization of all named entities such as names, locations,
        and organizations. Use if data is unstructured and no other entities have to be pseudonymized.
    seed : int
        Seed random pseudonymization methods like random1, random4, or faker. Return always expected result.
    pos_type : str or list
        Type(s) of entities in data to be pseudonymized such as names, locations,
        organizations, emails, and phone numbers. Use if data is unstructured.
    patterns : str or spaCy Matcher
        If data is structured, use *"column,operation,value"*,

        else *patterns = [[{"LOWER": "abc"}, {"LOWER": "corporation"}]...]*.
    """

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
        self.pos_type = pos_type
        self.patterns = patterns

    def pseudonym(self):
        # TOD
        """
        Main function for pseudonymization of csv data.

        Returns
        -------
        Pseudonymized Dataframe or a String. If output parameter is passed, writes pseudonymized and mapping files.

        If the encryption is involved, the secret keys are written to the .txt files by default.

        Example
        -------
        Pseudonymization of structured data using 'faker-name' method and filtering.
        ::
            >>> import pseudPy.Pseudonymization as pseudPy
            >>> pseudo = pseudPy.Pseudonymization(
            >>>        map_method = 'faker-name',
            >>>        map_columns = 'names',
            >>>        input_file= '/path/to/data.csv',
            >>>        output='/output/dir',
            >>>        patterns=['salary', '>', 100000])
            >>>
            >>> pseudo.pseudonym()
        """
        # read data as Polars DataFrame
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
        # initialize helper functions
        helpers = Helpers(df=self.df, output=self.output, map_columns=self.map_columns, map_method=self.map_method,
                          mapping=self.mapping, encrypt_map=self.encrypt_map, seed=self.seed, patterns=self.patterns)
        if self.map_method in map_method_handlers and self.map_method != 'decrypt':
            if self.output is not None:
                helpers.handle_map_tiers(output_files=True)
            else:
                return helpers.handle_map_tiers(output_files=False)
        elif self.map_method == 'decrypt':
            for i in range(len(self.map_columns)):
                mapping_instance = Mapping(df=self.df, first_tier=self.map_columns[i], output=self.output)
                decrypt = map_method_handlers[self.map_method](mapping_instance)
                decrypt = decrypt.rename(f"{self.map_columns[i]}")
                self.df = self.df.drop(self.map_columns[i])
                self.df = self.df.insert_column(1, decrypt)
                self.df.write_csv(f"{self.output}/decrypted_output_{self.map_columns[i]}.csv")

    def revert_pseudonym(self, revert_df=None, pseudonyms=None):
        """Revert structured data to original in form of Dataframe.

        Parameters
        ----------
        revert_df : Polars DataFrame
            The mapping table in form of Polars Dataframe.
        pseudonyms : list
            Filter for exact pseudonyms to revert. Optional.

        Returns
        -------
        A reverted Dataframe and if output parameter is passed, a file with reverted pseudonyms.

        Example
        -------
        Revert pseudonymized values to the original. For structured data.
        ::
            >>> import pseudPy.Pseudonymization as pseudPy
            >>> df = pl.read_csv('/path/to/data.csv')
            >>> output='/output/dir'
            >>> pseudo = pseudPy.Pseudonymization(
            >>>        map_columns = 'column1',
            >>>        df=df,
            >>>        output=output)
            >>> df_revert = pl.read_csv(f'{output}/mapping_output_column1.csv')
            >>>
            >>> pseudo.revert_pseudonym(df_revert)
        """
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

        Returns
        -------
        A pseudonymized String or, if output parameter is passed, writes the pseudonymized String and mappings to files.

        If the encryption is involved, the secret keys are written to the .txt files by default.

        Example
        -------
        Pseudonymization of unstructured data using 'merkle-tree' method and filtering.
        ::
            >>> import pseudPy.Pseudonymization as pseudPy
            >>> pseudo = pseudPy.Pseudonymization(
            >>>        map_method = 'merkle-tree',
            >>>        input_file = '/path/to/input.txt',
            >>>        output='/output/dir',
            >>>        patterns = [[{"LOWER": "Emily"}, {"LOWER": "White"}],
            >>>        [{"LOWER": "Bob"}]])
            >>>
            >>> pseudo.pseudonym()
        """
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
        helpers = Helpers(text=self.text, nlp=nlp, all_ne=self.all_ne, pos_type=self.pos_type, patterns=self.patterns)
        map_dict = helpers.entity_mapping()

        # create pseudonyms and replace entities with pseudonyms in text
        for key in map_dict:
            df_pos = pl.DataFrame()
            if self.map_method == 'encrypt':
                mapping = Mapping(df_pos, output=self.output, first_tier=key)
                mapping.generate_keys()
            if self.map_method == 'decrypt':
                for pos in self.pos_type:
                    map_df = pl.read_csv(f'{self.output}/mapping_output_{pos}.csv')
                    mapping = Mapping(map_df, first_tier=pos, output=self.output)
                    self.text = mapping.decrypt_nlp_tier(self.text)
                with open(f'{self.output}/decrypted_text.txt', 'w') as file:
                    print(self.text, file=file)
            else:
                helpers = Helpers(list_=map_dict[key], map_method=self.map_method, df=df_pos, counter=counter,
                                  field=key, output=self.output)
                df_pos = helpers.pseudo_nlp_mapper()
                if not df_pos.is_empty():
                    if self.map_method == 'counter':
                        try:
                            counter = int((df_pos.select(pl.last(f'Index_{key}')).to_series())[0]) + 1
                        except TypeError:
                            counter = (df_pos.select(pl.last(f'Index_{key}')).to_series())[0] + 1
                    for subst in df_pos.to_dicts():
                        self.text = self.text.replace(str(subst[key]), str(subst[f'Index_{key}']))
                    # encrypt mapping data if requested
                    if self.encrypt_map and self.map_method != 'encrypt':
                        mapping = Mapping(df_pos, output=self.output, first_tier=key)
                        mapping.generate_keys()
                        df_pos = df_pos.with_columns(df_pos[key].map_elements(lambda x: Mapping.encrypt_data(mapping, x),
                                                                              return_dtype=pl.Utf8).alias(key))
                if self.map_method == 'encrypt':
                    df_pos = df_pos.drop(key)
                    df_pos = df_pos.rename({f"Index_{key}": f"{key}"})

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
        """Revert free text to original.

        Parameters
        ----------
        revert_df : Polars DataFrame
            The mapping table.
        pseudonyms : list
            Filter for exact pseudonyms to revert. Optional.

        Returns
        -------
        A String or, if output parameter is passed, output as a file.

        Example
        -------
        Revert pseudonymized values to the original. For unstructured data.
        ::
            >>> import pseudPy.Pseudonymization as pseudPy
            >>> with open('/path/to/input.txt', "r") as file:
            >>>      text = file.read()
            >>> output = '/output/dir'
            >>> pseudo = pseudPy.Pseudonymization(
            >>>        map_columns = 'Names',   # select from: 'Names', 'Locations', 'Organizations', 'Emails' and 'Phone-Numbers'
            >>>        text=text,
            >>>        output=output)
            >>> df_revert = pl.read_csv(f'{output}/mapping_output_Names.csv')
            >>>
            >>> pseudo.revert_nlp_pseudonym(df_revert)
        """
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
        def merkletree(elem):
            mtree = MerkleTree(elem)
            return mtree.getRootHash()

        list_of_rows = self.df.rows()
        output = []
        for user in list_of_rows:
            output.append(merkletree(list(filter(lambda item: item is not None, user))))
        return pl.Series(f'Index_{self.first_tier}', output)

    def generate_keys(self):
        """Generate secret keys for data encryption/decryption."""
        key = os.urandom(32)
        hex_key = key.hex()
        if self.output is not None:
            with open(f'{self.output}/secure_key_{self.first_tier}.txt', 'w') as file:
                file.write(hex_key)
        else:
            with open(f'secure_key_{self.first_tier}.txt', 'w') as file:
                file.write(hex_key)

    # Source: https://www.askpython.com/python/examples/implementing-aes-with-padding
    def encrypt_data(self, data):
        """Return encrypted data string."""
        data = data.encode('utf-8')
        if self.output is not None:
            with open(f'{self.output}/secure_key_{self.first_tier}.txt', 'r') as file:
                hex_key = file.read()
        else:
            with open(f'secure_key_{self.first_tier}.txt', 'r') as file:
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
        if self.first_tier.startswith("Index_"):
            self.first_tier = self.first_tier.replace("Index_", "")
        if self.output is not None:
            try:
                with open(f'{self.output}/secure_key_{self.first_tier}.txt', 'r') as file:
                    hex_key = file.read()
            except FileNotFoundError:
                with open(f'secure_key_{self.first_tier}.txt', 'r') as file:
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


class Helpers:
    """Class with all utility functions responsible for the main data manipulations and format of output"""

    def __init__(self, df=None, map_columns=None, map_method=None, mapping=None, encrypt_map=None, seed=None,
                 list_=None, counter=None, field=None, text=None, nlp=None, all_ne=None,
                 pos_type=None, patterns=None, output=None):
        self.df = df
        self.map_columns = map_columns
        self.map_method = map_method
        self.mapping = mapping
        self.encrypt_map = encrypt_map
        self.seed = seed
        self.list_ = list_
        self.counter = counter
        self.field = field
        self.text = text
        self.nlp = nlp
        self.all_ne = all_ne
        self.pos_type = pos_type
        self.patterns = patterns
        self.output = output

    def handle_map_tiers(self, output_files):
        """General function for organizing pseudonymized data. Return dataframes with pseudonymized data and mappings,
        write both to files."""
        count_start = 0
        return_map_output = []
        # filter the data
        if self.patterns is not None:
            column, op, value = (self.patterns[0], self.patterns[1].strip(), self.patterns[2])
            if op == '>':
                condition = pl.col(column) > value
                filtered_df = self.df.filter(~condition)
            elif op == '<':
                condition = pl.col(column) < value
                filtered_df = self.df.filter(~condition)
            elif op == '==':
                condition = pl.col(column) == value
                filtered_df = self.df.filter(~condition)
            elif op == '!=':
                condition = pl.col(column) != value
                filtered_df = self.df.filter(~condition)
            else:
                raise ValueError("Invalid operation")

            self.df = self.df.filter(condition)

        self.df = Helpers.int_to_str(self.df)
        df_map_all = self.df.clone()
        for i in range(0, len(self.map_columns)):
            df_copy = self.df.clone()
            columns_to_keep = []

            mapping_instance = Mapping(self.df, self.map_columns[i], count_start, self.seed, self.output)
            # generate secret keys for encryption
            if self.encrypt_map or (self.map_method == 'encrypt'):
                Mapping.generate_keys(mapping_instance)
            # call the pseudonymization methods
            df_copy.insert_column(0, map_method_handlers[self.map_method](mapping_instance))
            # update the counter with the correct start number
            if self.map_method == 'counter':
                last_index = (df_copy.select(pl.last(f'Index_{self.map_columns[i]}')).to_series())[0]
                count_start = last_index + 1
            # replace columns with pseudonyms
            try:
                df_map_all.insert_column(self.df.get_column_index(self.map_columns[i]),
                                         df_copy[f'Index_{self.map_columns[i]}'])
            except TypeError:
                print('TypeError: Check whether the column names match the input column names.')
            df_map_all = df_map_all.drop(self.map_columns[i])

            columns_to_keep.append(self.map_columns[i])
            columns_to_keep.append(f'Index_{self.map_columns[i]}')
            df_copy = df_copy.drop([col for col in self.df.columns if col not in columns_to_keep])
            # encrypt mappings if requested
            if self.encrypt_map and (self.map_method != 'encrypt'):
                df_copy = df_copy.with_columns(
                    df_copy[self.map_columns[i]].map_elements(lambda x: Mapping.encrypt_data(mapping_instance, x),
                                                              return_dtype=pl.Utf8).alias(self.map_columns[i])
                )
            # outputs
            if output_files and (self.map_method != 'encrypt') and (self.map_method != 'decrypt'):
                if self.mapping:
                    # mapping file contains only the pseudonyms and corresponding original row
                    df_copy.write_csv(f'{self.output}/mapping_output_{self.map_columns[i]}.csv')
            return_map_output.append(df_copy)
            if self.patterns is not None:
                filtered_df = filtered_df.rename({f"{self.map_columns[i]}": f"Index_{self.map_columns[i]}"})
        # if the data is filtered, the filtered data is pseudonymized and the remaining data is output in its
        # original state. Both are concatenated into one file.
        if self.patterns is not None:
            filtered_df = Helpers.int_to_str(filtered_df)
            df_map_all = Helpers.int_to_str(df_map_all)
            df_map_all = pl.concat([df_map_all, filtered_df])
        if output_files:
            # output file contains pseudonyms and other rows that were not modified
            df_map_all.write_csv(f'{self.output}/output.csv')
        if self.mapping:
            return [df_map_all, return_map_output]
        else:
            return df_map_all

    @staticmethod
    def int_to_str(df):
        """Convert all values to String."""
        for col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))
        return df

    def pseudo_nlp_mapper(self):
        """Pseudonym mapper for free text. Return df with pseudonyms."""
        self.df = self.df.with_columns(pl.Series(self.field, self.list_))
        mapping_instance = Mapping(self.df, self.field, count_start=self.counter, output=self.output)
        if self.map_method == 'faker':
            if self.field in faker_pos_handlers:
                self.df.insert_column(0, faker_pos_handlers[self.field](mapping_instance))
        else:
            if self.map_method in map_method_handlers:
                # call the pseudonymization methods
                self.df.insert_column(0, map_method_handlers[self.map_method](mapping_instance))
        self.df = Helpers.int_to_str(self.df)
        return self.df

    def entity_mapping(self):
        """Use spaCy and regex for entity categorization. Return organized data as dictionary."""
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        phone_number_pattern = "\\+?[1-9][0-9]{7,14}"
        doc = self.nlp(self.text)
        map_dict = dict()

        if self.patterns is not None:
            for pos in self.pos_type:
                if pos == "Others":
                    map_dict[pos] = []
                    matcher = Matcher(self.nlp.vocab)
                    matcher.add(f"pattern_{uuid.uuid4()}", self.patterns)
                    matches = matcher(doc)

                    # filter for exact entities in the text and append to the dictionary under "Others"
                    for match_id, start, end in matches:
                        span = doc[start:end]
                        if span.text not in map_dict[pos]:
                            map_dict[pos].append(span.text)
                else:
                    map_dict[f'{pos}'] = []
        else:
            # append other entities specified in pos_type
            if self.pos_type is not None:
                for pos in self.pos_type:
                    map_dict[f'{pos}'] = []
            # append only named entities if all_ne is True
            if self.all_ne:
                map_dict = {'Names': [], 'Locations': [], 'Organizations': []}
            else:
                # find all phone numbers and e-mails on request
                if self.pos_type is not None:
                    if 'Phone-Numbers' in self.pos_type:
                        map_dict['Phone-Numbers'].extend(re.findall(phone_number_pattern, self.text, flags=re.IGNORECASE))
                    if 'Emails' in self.pos_type:
                        map_dict['Emails'].extend(re.findall(email_pattern, self.text, flags=re.IGNORECASE))
        # find and append named entities
        for ent in doc.ents:
            # if only named entities are requested
            if self.all_ne and (self.pos_type is None) and (self.patterns is None):
                if ent.label_ == 'PERSON' and ent.text not in map_dict['Names']:
                    map_dict['Names'].append(ent.text)
                elif ent.label_ == 'GPE' and ent.text not in map_dict['Locations']:
                    map_dict['Locations'].append(ent.text)
                elif ent.label_ == 'ORG' and ent.text not in map_dict['Organizations']:
                    map_dict['Organizations'].append(ent.text)
            # if certain of the named entities are requested
            if self.pos_type is not None:
                if 'Names' in self.pos_type and ent.label_ == 'PERSON' and ent.text not in map_dict['Names']:
                    map_dict['Names'].append(ent.text)
                elif 'Locations' in self.pos_type and ent.label_ == 'GPE' and ent.text not in map_dict['Locations']:
                    map_dict['Locations'].append(ent.text)
                elif 'Organizations' in self.pos_type and ent.label_ == 'ORG' and ent.text not in map_dict['Organizations']:
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


class Aggregation:
    """Class for data aggregation.

    Parameters
    ----------
    column : str
        Column to aggregate.
    method : list
        Aggregation method and the range - ['number', int] for numerical data or ['dates-to-years', int] for dates.
    df : Pandas Dataframe
        Input Dataframe. Required if input path is not provided.
    input_file : str
        Path to input file. Required if dataframe is not provided.
    output : str
        Path to output folder.

    """
    def __init__(self, column, method, df=None, input_file=None, output=None):
        self.column = column
        self.method = method
        self.df = df
        self.input_file = input_file
        self.output = output

    def group(self):
        """Main function to initiate aggregation of csv.

        Returns
        -------
        Output to file, if the output path is passed, or to a Pandas Dataframe.

        Example
        -------
        Aggregate salary data to the groups of range 10000.
        ::
            >>> import pseudPy.Pseudonymization as pseudPy
            >>> agg = pseudPy.Aggregation(
            >>>     column='salary',
            >>>     method=['number', 10000],
            >>>     input_file='some_user_data.csv',
            >>>     output='/output/dir')
            >>>
            >>> agg.group()
        """
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

        Returns
        -------
        Dataframe column with aggregated numerical values."""
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
        """Aggregate only date values to years.

        Returns
        -------
        Aggregated Dataframe with years or year periods."""
        self.df[self.column] = pd.to_datetime(self.df[self.column]).dt.year
        if self.method[1] == 1:
            return self.df
        self.df[self.column] = self.group_num()
        return self.df


class KAnonymity:
    # Adapted from: https://programming-dp.com/ch2.html

    """
    Class for data k-anonymization.

    Parameters
    ----------
    df : Pandas Dataframe
        Input data.
    k : int
        The level of anonymity. Each row in the data must be matched to at least k other rows.
    depths : dict
        The depths of rounding the values in form {column_name: depth}. Ex. value=55222, depths = {value: 1}, result=55220.
    mask_others : bool
        Enable or disable tokenizing all string values with token ' * '.  To achieve k-anonymity,
        string data must be masked in most cases, unless it has also been k-anonymized externally beforehand.
    output : str
        Path to output folder.
    """

    def __init__(self, df, k, depths=None, mask_others=True, output=None):
        self.df = df
        self.depths = depths
        self.k = k
        self.mask_others = mask_others
        self.output = output

    def k_anonymity(self):
        """k-anonymize the data by providing the dataframe, k, depths of anonymization.

        Returns
        -------
        k-anonymized Dataframe or output to file, if the output path is passed.

        Example
        -------
        k-anonymize salary data with k of 2.
        ::
            >>> import pseudPy.Pseudonymization as pseudPy
            >>> df = pl.read_csv('/path/to/input.csv')
            >>> k_anonymity = pseudPy.KAnonymity(
            >>>             df=df,
            >>>             depths={'salary': 1},
            >>>             k=2,
            >>>             output='/output/dir')
            >>>
            >>> grouped = k_anonymity.k_anonymity())
        """
        def generalize(col):
            if col in self.depths:
                depth = self.depths[col]
                # floor the data to the nearest multiple of 10**depth
                return self.df[col].apply(lambda y: int(int(y / (10 ** depth)) * (10 ** depth))if pd.notnull(y) else y)
            return self.df[col]

        if self.mask_others:
            df_header = set(self.df.columns)
            for key in df_header - set(self.depths.keys()):
                self.df[key] = '*'

        for key in self.depths:
            if pd.api.types.is_integer_dtype(self.df[key]):
                self.df[key] = generalize(key)
            elif pd.api.types.is_float_dtype(self.df[key]):
                self.df[key].apply(lambda x: round(x) if pd.notnull(x) else x)
                self.df[key] = generalize(key)
            else:
                self.df[key] = '*'

        # filter data with at least k records
        grouped = self.df.groupby(list(self.depths.keys())).filter(lambda x: len(x) >= self.k)
        if self.output is None:
            return grouped
        else:
            grouped.to_csv(f'{self.output}/k_anonym_output.csv', index=False)
            return grouped

    def is_k_anonymized(self):
        """Check if the data is k-anonymous.

        Returns
        -------
        True or False.

        Example
        -------
        Check if the data is k-anonymous.
        ::
            >>> import pseudPy.Pseudonymization as pseudPy
            >>> grouped = pl.read_csv('/path/to/input.csv')
            >>> is_k_anonym = pseudPy.KAnonymity(
            >>>             df=grouped,
            >>>             depths={'salary': 1},
            >>>             k=2)
            >>>
            >>> print(is_k_anonym.is_k_anonymized())
        """
        for index, row in self.df.iterrows():
            query = ' & '.join([f'`{col}` == {repr(row[col])}' for col in self.df.columns])
            rows = self.df.query(query)
            if rows.shape[0] < self.k:
                return False
        return True


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
    'faker-phone': Mapping.faker_phone_number_tier,
    'faker-org': Mapping.faker_org_tier
}

faker_pos_handlers = {
    'Names': Mapping.faker_names_tier,
    'Locations': Mapping.faker_location_tier,
    'Emails': Mapping.faker_email_tier,
    'Phone-Numbers': Mapping.faker_phone_number_tier,
    'Organizations': Mapping.faker_org_tier
}

group_handlers = {
    'number': Aggregation.group_num,
    'dates-to-years': Aggregation.group_dates_to_years
}


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

