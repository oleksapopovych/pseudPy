import uuid

import pandas as pd
import math
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from base64 import b64encode, b64decode
import hashlib
from Deprecated import merkle_trees
import spacy
import re


home_dir = os.path.expanduser('~')
test_files_folder = f'{home_dir}/PycharmProjects/pseudPy/tests/test_files/'

# RSA encryption adapted from: https://cryptography.io/en/latest/hazmat/primitives/asymmetric/rsa/


def generate_rsa_keys():
    """Generate RSA keys for further encryption/decryption"""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    public_key = private_key.public_key()

    with open("private_key.pem", "wb") as f:    # Save the private key
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()))

    with open("public_key.pem", "wb") as f:     # Save the public key
        f.write(public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo))


def encrypt_data(data):
    """Encrypt single data row"""
    with open("public_key.pem", "rb") as f:     # Load the public key
        public_key = serialization.load_pem_public_key(
            f.read(),
            backend=default_backend())

    encrypted = public_key.encrypt(
        data.encode(),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return b64encode(encrypted).decode('utf-8')


def decrypt_data(data):
    """Decrypt single data row"""
    with open("private_key.pem", "rb") as f:    # Load the private key
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None,
            backend=default_backend())
    try:
        decoded_data = b64decode(data.encode('utf-8'))
        decrypted = private_key.decrypt(
            decoded_data,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return decrypted.decode('utf-8')
    except Exception as e:
        print(f"Decryption failed: {e}")
        return None


def decrypt(column, df=None, file_name=None, destination_name=None):
    """Decrypt all data in the Dataframe"""
    if file_name is not None:
        df = pd.read_csv(file_name)
    for i in range(len(column)):
        df[column[i]] = df[column[i]].apply(lambda x: decrypt_data(x))
    if destination_name is not None:
        df.to_csv(f'{destination_name}decrypt_output.csv', index=False)
    else:
        return df


def encrypt_tier(df, first_tier, count_start=0):
    """Encrypt all data in the Dataframe"""
    df = int_to_str(df)
    df[f'Index_{first_tier}'] = df[first_tier].apply(lambda x: encrypt_data(x))
    df_pop = df.pop(f'Index_{first_tier}')
    df.insert(loc=0, column=f'Index_{first_tier}', value=df_pop)
    return df[f'Index_{first_tier}']


# Numerical Aggregation


def group_num(df, column, method):
    """Aggregate only numerical data in the given columns of a Dataframe"""
    bins, labels = [0], []
    bin_update = 0
    max_number = df[column].max()
    for i in range(math.ceil(max_number / method[1])):
        bin_update = bin_update + method[1]
        if bin_update < max_number:
            bins.append(bin_update)
            if len(labels) == 0:
                labels.append(f'0-{bin_update}')
                labels.append(f'{bin_update + 1}-{bin_update + method[1]}')
            else:
                labels.append(f'{bin_update + 1}-{bin_update + method[1]}')
    if len(labels) == len(bins):
        bins.append(max_number)
    try:
        df[column] = pd.cut(df[column], bins=bins, labels=labels)
    except ValueError:
        print("ValueError: the number of bins must be by one greater than of labels.")
    return df[column]


def group_dates_to_years(df, column, method):
    """Aggregate only date values to years"""
    df[column] = pd.to_datetime(df[column]).dt.year
    group_num(df, column, method)
    return df


def handle_map_tiers(df, output, map_columns, map_method, mapping, encrypt_map, output_files):
    """General function for handling most pseudonymization methods"""
    df = int_to_str(df)
    df_map_all = df.copy()
    count_start = 0
    return_map_output = []
    for i in range(0, len(map_columns)):
        df_copy = df.copy()
        columns_to_keep = []
        # call the pseudonymization methods
        df_copy[f'Index_{map_columns[i]}'] = map_method_handlers[map_method](df_copy, map_columns[i], count_start)

        df_pop = df_copy.pop(f'Index_{map_columns[i]}')
        df_copy.insert(loc=0, column=f'Index_{map_columns[i]}', value=df_pop)

        if map_method == 'counter':
            last_index = df_copy[f'Index_{map_columns[i]}'].iloc[-1]
            count_start = last_index + 1
        # replace columns with pseudonyms
        df_map_all.insert(loc=df.columns.get_loc(map_columns[i]), column=f'Index_{map_columns[i]}', value=df_copy[f'Index_{map_columns[i]}'])
        df_map_all = df_map_all.drop(map_columns[i], axis=1)

        columns_to_keep.append(map_columns[i])
        columns_to_keep.append(f'Index_{map_columns[i]}')
        df_copy = df_copy.drop([col for col in df.columns if col not in columns_to_keep], axis=1)
        # encrypt mappings if requested
        if encrypt_map:
            df_copy[map_columns[i]] = df_copy[map_columns[i]].apply(lambda x: encrypt_data(x))
        # outputs
        if output_files and (map_method != 'encrypt'):
            if mapping:
                # mapping file contains only the pseudonyms and corresponding original row
                df_copy.to_csv(f'{output}mapping_output_{i}.csv', index=False)
        return_map_output.append(df_copy)

    if output_files:
        # output file contains pseudonyms and other rows that were not modified
        df_map_all.to_csv(f'{output}output.csv', index=False)
    if mapping:
        return [df_map_all, return_map_output]
    else:
        return df_map_all


def counter_tier(df, first_tier, count_start=0):
    """Add indexes in form of incrementation"""
    df[f'Index_{first_tier}'] = range(count_start, count_start+len(df))
    return df[f'Index_{first_tier}']


def remove_column(df, column_name):
    for i in range(len(column_name)):
        df = df.drop(column_name[i], axis=1)
    return df


def random_uuid_1():
    """Generate a UUID from a host ID, sequence number, and the current time"""
    return uuid.uuid1()


def random1_tier(df, first_tier, count_start=0):
    df[f'Index_{first_tier}'] = df[first_tier].apply(lambda x: random_uuid_1())
    return df[f'Index_{first_tier}']


def random_uuid_4():
    """Generate a random UUID"""
    return uuid.uuid4()


def random4_tier(df, first_tier, count_start=0):
    df[f'Index_{first_tier}'] = df[first_tier].apply(lambda x: random_uuid_4())
    return df[f'Index_{first_tier}']


def hash_salt_tier(df, first_tier, count_start=0):
    """Hashing function"""
    df[f'Index_{first_tier}'] = df[first_tier].apply(
        lambda x:
        hashlib.sha256(uuid.uuid4().hex.encode()+x.encode()).hexdigest()
    )
    return df[f'Index_{first_tier}']


def hash_tier(df, first_tier, count_start=0):
    """Hashing function"""
    df[f'Index_{first_tier}'] = df[first_tier].apply(
        lambda x:
        hashlib.sha256(x.encode()).hexdigest()
    )
    return df[f'Index_{first_tier}']


def merkle_tree_tier(df, first_tier, count_start=0):
    """Merkle Trees root as pseudonym"""
    list_of_data = df.values.tolist()
    output = []
    for user in list_of_data:
        output.append(merkle_trees.mixmerkletree(user))
    df[f'Index_{first_tier}'] = output
    return df[f'Index_{first_tier}']


def int_to_str(df):
    updated_data = {}
    for col in df.columns:
        updated_data[col] = [str(item) if not isinstance(item, str) else item for item in df[col]]
    df_output = pd.DataFrame(updated_data)
    return df_output


# Method for Data Pseudonymization


def pseudonym(map_columns, map_method, input_file=None, output=None, df=None, mapping=True, encrypt_map=False):
    """Main function to initiate pseudonymization of csv"""
    if input_file is not None:
        df = pd.DataFrame()
        df = pd.read_csv(input_file)
    if map_method in map_method_handlers:
        if output is not None:
            handle_map_tiers(df, output, map_columns, map_method, mapping, encrypt_map, output_files=True)
        else:
            return handle_map_tiers(df, output, map_columns, map_method, mapping, encrypt_map, output_files=False)


# Method for Data Aggregation - no re-identification possibility


def group(column, method, df=None, input_file=None, output=None):
    """Main function to initiate aggregation of csv"""
    if input_file is not None:
        df = pd.DataFrame()
        df = pd.read_csv(input_file)
    for i in range(len(method)):
        if method[i][0] in group_handlers:
            group_handlers[method[i][0]](df, column[i], method[i])
    if output is not None:
        df.to_csv(f'{output}/output.csv', index=False)
    else:
        return df

# Pseudonymization of free text data with NLP


def nlp_pseudonym(map_method, file_name=None, text=None, output_to_file=False, encrypt_map=False, lan='en'):
    """Main function for pseudonymization of free text"""
    # definitions
    if lan == 'de':
        nlp = spacy.load("de_core_news_sm")
    else:
        nlp = spacy.load("en_core_web_sm")
    if file_name is not None:
        file = open(file_name, "r")
        text = file.read()
        file.close()
    doc = nlp(text)
    df_list = []
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    phone_number_pattern = "\\+?[1-9][0-9]{7,14}"
    df_name = pd.DataFrame()
    df_email = pd.DataFrame()
    df_phone = pd.DataFrame()
    counter = 0
    # find and append entities
    phone_numbers = re.findall(phone_number_pattern, text, flags=re.IGNORECASE)
    emails = re.findall(email_pattern, text, flags=re.IGNORECASE)
    for ent in doc.ents:
        if ent.text not in emails:
            if ent.text not in phone_numbers:
                df_list.append(ent.text)
    # create pseudonyms and replace entities with pseudonyms in text
    if df_list:
        df_name = pseudo_nlp_mapper(df_list, map_method, df_name, counter, 'Names')
        if map_method == 'counter':
            counter = int(df_name.loc[len(df_name) - 1, 'Index_Names']) + 1
        for idx, row in df_name.iterrows():
            text = text.replace(str(row['Names']), str(row['Index_Names']))
    if emails:
        df_email = pseudo_nlp_mapper(emails, map_method, df_email, counter, 'Emails')
        if map_method == 'counter':
            counter = int(df_email.loc[len(df_email)-1, 'Index_Emails']) + 1
        for idx, row in df_email.iterrows():
            text = text.replace(str(row['Emails']), str(row['Index_Emails']))
    if phone_numbers:
        df_phone = pseudo_nlp_mapper(phone_numbers, map_method, df_phone, counter, 'Numbers')
        for idx, row in df_phone.iterrows():
            text = text.replace(str(row['Numbers']), str(row['Index_Numbers']))

    # encrypt mapping data if requested
    if encrypt_map:
        df_name['Names'] = df_name['Names'].apply(lambda x: encrypt_data(x))
        if emails:
            df_email['Emails'] = df_email['Emails'].apply(lambda x: encrypt_data(x))
        if phone_numbers:
            df_phone['Numbers'] = df_phone['Numbers'].apply(lambda x: encrypt_data(x))
    # output options
    if output_to_file:
        df_name.to_csv('output_name.csv', index=False)
        if emails:
            df_email.to_csv('output_email.csv', index=False)
        if phone_numbers:
            df_phone.to_csv('output_phone.csv', index=False)
        with open("text.txt", "w") as text_file:
            print(text, file=text_file)
    else:
        if emails:
            if phone_numbers:
                return [text, df_name, df_email, df_phone]
            return [text, df_name, df_email]
        return [text, df_name]


def pseudo_nlp_mapper(list_, map_method, df, counter, field):
    """Pseudonym mapper for nlp"""
    df[field] = list_
    if map_method in map_method_handlers:
        df[f'Index_{field}'] = map_method_handlers[map_method](df, field, count_start=counter)
    df = int_to_str(df)
    return df


# Dictionaries for the categorisation of pseudonymization methods


group_handlers = {
    'number': group_num,
    'dates-to-years': group_dates_to_years
}

map_method_handlers = {
    'counter': counter_tier,
    'encrypt': encrypt_tier,
    'random1': random1_tier,
    'random4': random4_tier,
    'hash': hash_tier,
    'hash-salt': hash_salt_tier,
    'merkle-tree': merkle_tree_tier
}

