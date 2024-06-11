import os
import sys
import unittest
import polars as pl
from polars.testing import assert_frame_equal
import pseudPy.Pseudonymization as pseudPy
import yaml
from yaml import CLoader as Loader
import pandas as pd

home_dir = os.path.expanduser('~')
test_files_folder = f'{home_dir}/PycharmProjects/pseudPy/tests/test_files'


class TestStructuredPseudonymization(unittest.TestCase):

    def test_pseudonym_with_valid_data_and_counter_method(self):
        """Test pseudonymization of a column in structured data using counter method"""

        map_method = 'counter'
        map_columns = 'name'
        input_file = f'{test_files_folder}/plain_user_data.csv'
        output = test_files_folder
        output_path = f'{test_files_folder}/output.csv'

        df = pl.read_csv(input_file)

        pseudo = pseudPy.Pseudonymization(
            map_method,
            map_columns,
            df=df,
            output=output,
            #patterns=['salary', '>', 100000]
        )

        pseudo.pseudonym()

        # check the main output file
        actual_output = pl.read_csv(output_path)
        expected_output = pl.read_csv(f'{test_files_folder}/expected_output_plain_user_data.csv')

        pl.testing.assert_frame_equal(expected_output, actual_output)

        if os.path.exists(output_path):
            os.remove(output_path)
            print('File successfully removed')
        else:
            print('File does not exist')

        # check the mapping tables

        output_path = f'{test_files_folder}/mapping_output_name.csv'

        actual_output = pl.read_csv(output_path)
        expected_output = pl.read_csv(f'{test_files_folder}/expected_mapping_0_plain_user_data.csv')

        pl.testing.assert_frame_equal(expected_output, actual_output)

        if os.path.exists(output_path):
            os.remove(output_path)
            print('File successfully removed')
        else:
            print('File does not exist')

    def test_pseudonym_with_valid_data_and_encrypted_maps(self):
        """Test pseudonymization of two columns in structured data using counter method.
            Additionally encrypt and decrypt data."""

        map_method = 'counter'
        map_columns = ['name', 'country']
        input_file = f'{test_files_folder}/plain_user_data.csv'

        pseudo = pseudPy.Pseudonymization(
            map_method,
            map_columns,
            input_file=input_file,
            output=test_files_folder,
            encrypt_map=True
        )

        pseudo.pseudonym()

        #os.rename(f'{test_files_folder}/secure_key_Index_name.txt', f'{test_files_folder}/secure_key_name.txt')
        #os.rename(f'{test_files_folder}/secure_key_Index_country.txt', f'{test_files_folder}/secure_key_country.txt')

        output_path_map_0 = f'{test_files_folder}/mapping_output_name.csv'
        output_path_map_1 = f'{test_files_folder}/mapping_output_country.csv'

        df_0 = pl.read_csv(output_path_map_0)
        df_1 = pl.read_csv(output_path_map_1)

        """
        df_for_revert = pl.read_csv(f'{test_files_folder}/output.csv')
            for i in map_columns:
            input_file = f'{test_files_folder}/mapping_output_{i}.csv'
            revert_pseudo = pseudPy.Pseudonymization(
                df=df_for_revert,
                output=test_files_folder,
                map_columns=map_columns[map_columns.index(i)]
            )
            revert_pseudo.revert_pseudonym(revert_df=input_file)
        """

        for i in map_columns:
            pseudo_decrypt = pseudPy.Pseudonymization(
                input_file=f'{test_files_folder}/mapping_output_{i}.csv',
                map_method='decrypt',
                map_columns=map_columns[map_columns.index(i)],
                output=test_files_folder
            )
            pseudo_decrypt.pseudonym()

        decrypted_0 = pl.read_csv(f'{test_files_folder}/decrypted_output_name.csv')
        decrypted_1 = pl.read_csv(f'{test_files_folder}/decrypted_output_country.csv')

        expected_output = pl.read_csv(f'{test_files_folder}/expected_mapping_0_plain_user_data.csv')
        pl.testing.assert_frame_equal(expected_output, decrypted_0)

        expected_output = pl.read_csv(f'{test_files_folder}/'
                                      'expected_mapping_1_plain_user_data.csv')

        pl.testing.assert_frame_equal(expected_output, decrypted_1)

        output_path = f'{test_files_folder}/output.csv'
        if os.path.exists(output_path):
            os.remove(output_path)
            os.remove(output_path_map_0)
            os.remove(output_path_map_1)
            os.remove(f'{test_files_folder}/decrypted_output_name.csv')
            os.remove(f'{test_files_folder}/decrypted_output_country.csv')
            os.remove(f'{test_files_folder}/secure_key_country.txt')
            os.remove(f'{test_files_folder}/secure_key_name.txt')
            os.remove(f'{test_files_folder}/secure_key_salary.txt')
            print('Files successfully removed')
        else:
            print('Files does not exist')

    def test_pseudonym_with_valid_data_and_hash_method(self):
        """Test pseudonymization of one column in structured data using hash method."""
        map_method = 'hash'
        map_columns = 'name'
        input_file = f'{test_files_folder}/plain_user_data.csv'
        output = test_files_folder

        pseudo = pseudPy.Pseudonymization(map_method, map_columns,
                                          input_file=input_file,
                                          output=output
                                          )

        pseudo.pseudonym()
        output_path = f'{test_files_folder}/output.csv'

        # check the main output file
        actual_output = pl.read_csv(output_path)
        expected_output = pl.read_csv(f'{test_files_folder}/expected_output_hash_plain_user_data.csv')

        pl.testing.assert_frame_equal(expected_output, actual_output)

        if os.path.exists(output_path):
            os.remove(output_path)
            print('File successfully removed')
        else:
            print('File does not exist')

        # check the mapping tables

        output_path = f'{test_files_folder}/mapping_output_name.csv'

        actual_output = pl.read_csv(output_path)
        expected_output = pl.read_csv(f'{test_files_folder}/expected_mapping_hash_0_plain_user_data.csv')

        pl.testing.assert_frame_equal(expected_output, actual_output)

        if os.path.exists(output_path):
            os.remove(output_path)
            print('File successfully removed')
        else:
            print('File does not exist')

    def test_pseudonym_with_valid_data_and_merkle_tree_method(self):
        """ this test helped to spot a data error: if one of the column elements is empty, thus of type None,
        the Merkle Tree throws an error. It was fixed by filtering the None elements out."""
        map_method = 'merkle-tree'
        map_columns = 'name'
        input_file = f'{test_files_folder}/plain_user_data.csv'
        output = test_files_folder
        output_path = f'{test_files_folder}/output.csv'

        pseudo = pseudPy.Pseudonymization(map_method, map_columns,
                                          input_file=input_file,
                                          output=output)

        pseudo.pseudonym()

        # check the main output file
        actual_output = pl.read_csv(output_path)
        expected_output = pl.read_csv(f'{test_files_folder}/expected_output_merkle_tree_plain_user_data.csv')

        pl.testing.assert_frame_equal(expected_output, actual_output)

        if os.path.exists(output_path):
            os.remove(output_path)
            print('File successfully removed')
        else:
            print('File does not exist')

        # check the mapping table

        output_path = f'{test_files_folder}/mapping_output_name.csv'

        actual_output = pl.read_csv(output_path)
        expected_output = pl.read_csv(f'{test_files_folder}/expected_mapping_merkle_tree_0_plain_user_data.csv')

        pl.testing.assert_frame_equal(expected_output, actual_output)

        if os.path.exists(output_path):
            os.remove(output_path)
            print('File successfully removed')
        else:
            print('File does not exist')

    def test_pseudonym_with_valid_data_and_random_method_and_revert(self):
        """Test pseudonymization of two columns in structured data using random4 method.
            Finally, revert the data to the original."""
        map_method = 'random4'
        map_columns = ['name', 'salary']
        input_file = f'{test_files_folder}/plain_user_data.csv'
        output = test_files_folder
        output_path = f'{test_files_folder}/output.csv'

        pseudo = pseudPy.Pseudonymization(map_method, map_columns,
                                          input_file=input_file,
                                          output=output)

        pseudo.pseudonym()

        df = pl.read_csv(output_path)

        for elem in range(2):
            pseudo = pseudPy.Pseudonymization(map_columns=map_columns[elem], df=df, output=output)
            df_revert = pl.read_csv(f'{test_files_folder}/mapping_output_{map_columns[elem]}.csv')
            df = pseudo.revert_pseudonym(df_revert)

        df_input = pl.read_csv(input_file)

        pl.testing.assert_frame_equal(df, df_input)

        if os.path.exists(output_path):
            os.remove(output_path)
            os.remove(f'{test_files_folder}/mapping_output_name.csv')
            os.remove(f'{test_files_folder}/mapping_output_salary.csv')
            os.remove(f'{test_files_folder}/reverted_output.csv')
            print('Files successfully removed')
        else:
            print('Files do not exist')

    def test_pseudonym_with_valid_data_and_random_method_seed(self):
        """Test pseudonymization of two columns in structured data using random4 method.
            Additionally, use seed to get a repeating output"""
        map_method = 'random4'
        map_columns = 'name'
        input_file = f'{test_files_folder}/plain_user_data.csv'
        output = test_files_folder
        output_path = f'{test_files_folder}/output.csv'
        seed = 4321

        pseudo = pseudPy.Pseudonymization(map_method, map_columns,
                                          input_file=input_file,
                                          output=output, seed=seed)

        pseudo.pseudonym()

        actual_output = pl.read_csv(output_path)
        expected_output = pl.read_csv(f'{test_files_folder}/expected_output_random4_with_seed.csv')

        pl.testing.assert_frame_equal(expected_output, actual_output)

        actual_output = pl.read_csv(f'{test_files_folder}/mapping_output_name.csv')
        expected_output = pl.read_csv(f'{test_files_folder}/expected_mapping_0_random4_with_seed.csv')

        pl.testing.assert_frame_equal(expected_output, actual_output)

        if os.path.exists(output_path):
            os.remove(output_path)
            os.remove(f'{test_files_folder}/mapping_output_name.csv')
            print('Files successfully removed')
        else:
            print('Files do not exist')

    def test_pseudonym_with_valid_data_and_counter_method_10000_rows_speed(self):
        """Test pseudonymization on the higher-performance parameters:
            10000 rows, encrypt the mapping."""
        map_method = 'counter'
        map_columns = ['name', 'country', 'salary']
        input_file = f'{test_files_folder}/user_data_100000_rows.csv'
        output = test_files_folder
        output_path = f'{test_files_folder}/output.csv'

        pseudo = pseudPy.Pseudonymization(
            map_method,
            map_columns,
            input_file=input_file,
            output=output,
            encrypt_map=True
        )

        pseudo.pseudonym()

        if os.path.exists(output_path):
            os.remove(output_path)
            print('File successfully removed')
        else:
            print('File does not exist')

        # check the mapping tables
        for i in range(3):
            output_path = f'{test_files_folder}/mapping_output_{map_columns[i]}.csv'

            if os.path.exists(output_path):
                os.remove(output_path)
                print('File successfully removed')
            else:
                print('File does not exist')


class TestUnstructuredPseudonymization(unittest.TestCase):

    def test_nlp_pseudonym_encrypt_decrypt_method(self):
        """Encrypt sensitive data in text"""
        map_method = 'encrypt'
        input_file = f'{test_files_folder}/free_text.txt'
        output = test_files_folder

        pseudo = pseudPy.Pseudonymization(
            map_method,
            input_file=input_file,
            pos_type=['Names', 'Locations'],
            output=output
        )
        pseudo.nlp_pseudonym()

        """Decrypt the text encrypted in the previous test."""
        map_method = 'decrypt'
        input_file = f'{test_files_folder}/text.txt'
        output = test_files_folder

        with open(input_file, "r") as file:
            text = file.read()

        pseudo = pseudPy.Pseudonymization(
            map_method,
            text=text,
            pos_type=['Names', 'Locations'],
            output=output
        )
        pseudo.nlp_pseudonym()

        if os.path.exists(f'{test_files_folder}/text.txt'):
            os.remove(f'{test_files_folder}/text.txt')
        if os.path.exists(f'{test_files_folder}/mapping_output_Locations.csv'):
            os.remove(f'{test_files_folder}/mapping_output_Locations.csv')
            os.remove(f'{test_files_folder}/mapping_output_Names.csv')
        if os.path.exists(f'{test_files_folder}/secure_key_Locations.txt'):
            os.remove(f'{test_files_folder}/secure_key_Locations.txt')
            os.remove(f'{test_files_folder}/secure_key_Names.txt')

    def test_nlp_pseudonym_work_report_faker_method_only_orgs(self):
        """Pseudonymize free text using faker method. Apply word patterns that must be pseudonymized."""
        map_method = 'faker'
        input_file = f'{test_files_folder}/free_text.txt'
        output_path = f'{test_files_folder}/text.txt'

        patterns = [
            [{"LOWER": "abc"}, {"LOWER": "corporation"}],
            [{"LOWER": "abc"}]
        ]

        # patterns = [
        #    [{"LOWER": "emily"}, {"LOWER": "white"}],
        #    [{"LOWER": "miss"}, {"LOWER": "white"}],
        #    [{"LOWER": "emily"}],
        #    [{"LOWER": "white"}]
        # ]

        pseudo = pseudPy.Pseudonymization(map_method, input_file=input_file, pos_type=['Organizations'],
                                          patterns=patterns, output=test_files_folder)
        pseudo.nlp_pseudonym()

        expected_header = ['Index_Organizations', 'Organizations']

        actual_result = pl.read_csv(f'{test_files_folder}/mapping_output_Organizations.csv')

        for col in expected_header:
            if col not in actual_result:
                print(f'No such column expected: {col}')
                sys.exit(0)
        for col in actual_result.columns:
            if col not in expected_header:
                print(f'No such column expected: {col}')
                sys.exit(0)

        if os.path.exists(output_path):
            os.remove(output_path)
        else:
            print("No output generated.")
            sys.exit(0)
        if os.path.exists(f'{test_files_folder}/mapping_output_Organizations.csv'):
            os.remove(f'{test_files_folder}/mapping_output_Organizations.csv')
        else:
            print("No mapping output generated.")
            sys.exit(0)

    def test_nlp_pseudonym_work_report_faker_method_only_name_yaml(self):
        """Pseudonymize free text by using YAML as config file."""
        """ works but cannot filter for Emily (just a name), this causes repetitions in names"""

        with open(f"{home_dir}/PycharmProjects/pseudPy/tests/config.yaml", "rt") as config_file:
            config = yaml.load(config_file, Loader=Loader)

        output_path = 'text.txt'

        map_method = config["map_method"]
        input_file = config["input_file"]
        patterns = config["patterns"]
        output = config["output"]
        all_ne = config["all_ne"]

        pseudo = pseudPy.Pseudonymization(map_method,
                                          input_file=input_file,
                                          patterns=patterns,
                                          output=output,
                                          all_ne=all_ne,
                                          encrypt_map=True)
        pseudo.nlp_pseudonym()

        df = pl.read_csv('mapping_output_Names.csv')

        #os.rename('secure_key_Index_Names.txt', 'secure_key_Names.txt')

        pseudo_decrypt = pseudPy.Pseudonymization(
            input_file='mapping_output_Names.csv',
            map_method='decrypt',
            map_columns='Names',
            output=test_files_folder
        )
        pseudo_decrypt.pseudonym()

        df_decrypted = pl.read_csv(f'{test_files_folder}/decrypted_output_Names.csv')

        df_expected = pl.read_csv(f"{test_files_folder}/expected_free_text_mapping_decrypted.csv")
        pl.testing.assert_frame_equal(df_expected, df_decrypted)

        if os.path.exists(output_path):
            os.remove(output_path)
        if os.path.exists('mapping_output_Names.csv'):
            os.remove('mapping_output_Names.csv')
            os.remove('mapping_output_Locations.csv')
            os.remove('mapping_output_Organizations.csv')
        if os.path.exists(f'{test_files_folder}/decrypted_output_Names.csv'):
            os.remove(f'{test_files_folder}/decrypted_output_Names.csv')
            os.remove('secure_key_Names.txt')
            os.remove('secure_key_Locations.txt')
            os.remove('secure_key_Organizations.txt')

    def test_nlp_pseudonym_work_report_counter_method(self):
        """Pseudonymize free text by using counter method."""
        map_method = 'counter'
        input_file = f'{test_files_folder}/free_text.txt'
        output_path = f'{test_files_folder}/text.txt'
        pos_types = ['Names']

        pseudo = pseudPy.Pseudonymization(map_method, input_file=input_file,
                                          pos_type=pos_types,
                                          encrypt_map=False,
                                          output=test_files_folder
                                          )
        pseudo.nlp_pseudonym()

        with open(output_path, "r") as file:
            output_text = file.read()

        expected_file = f'{test_files_folder}/expected_free_text_output_counter.txt'

        with open(expected_file, "r") as file:
            expected_text = file.read()

        self.assertEqual(expected_text, output_text)

        if os.path.exists(output_path):
            os.remove(output_path)
            print('File successfully removed')
        else:
            print('File does not exist')

        output_path = f'{test_files_folder}/mapping_output_Names.csv'

        actual_output = pl.read_csv(output_path)
        expected_output = pl.read_csv(f'{test_files_folder}/expected_output_0_free_text_counter.csv')

        pl.testing.assert_frame_equal(expected_output, actual_output)

        if os.path.exists(output_path):
            os.remove(output_path)
            print('File successfully removed')
        else:
            print('File does not exist')

    def test_nlp_pseudonym_work_report_data_with_revert(self):
        """Pseudonymize data with faker and revert data to the original."""
        map_method = 'faker'
        input_file = f'{test_files_folder}/free_text.txt'
        output = test_files_folder
        output_path = f'{output}/text.txt'

        with open(input_file, "r") as file:
            input_text = file.read().rstrip('\n')

        pseudo = pseudPy.Pseudonymization(map_method, input_file=input_file, output=output, all_ne=True)
        pseudo.nlp_pseudonym()

        with open(output_path, "r") as file:
            text = file.read().rstrip('\n')

        list_columns = ['Names', 'Locations', 'Organizations']

        for elem in range(len(list_columns)):
            pseudo = pseudPy.Pseudonymization(map_columns=list_columns[elem], text=text, output=output)
            df_revert = pl.read_csv(f'{output}/mapping_output_{list_columns[elem]}.csv')
            text = pseudo.revert_nlp_pseudonym(df_revert)

        self.assertEqual(input_text, text)

        if os.path.exists(output_path):
            os.remove(output_path)
            os.remove(f'{output}/mapping_output_Names.csv')
            os.remove(f'{output}/mapping_output_Locations.csv')
            os.remove(f'{output}/mapping_output_Organizations.csv')
            os.remove(f'{output}/reverted_text.txt')
            print('Files successfully removed')
        else:
            print('Files do not exist')


class TestInvalidStructuredData(unittest.TestCase):
    """Test invalid data inputs."""
    def test_only_header(self):
        """Test case: input file contains only header."""
        map_method = 'counter'
        map_columns = ['name', 'job_title']
        output = test_files_folder

        data = {
            "name": None,
            "job_title": None,
            "gender": None,
            "city": None,
            "postal_code": None
        }

        df = pl.DataFrame(data)

        pseudo = pseudPy.Pseudonymization(
            map_method,
            map_columns,
            df=df,
            output=output
        )

        with self.assertRaises(SystemExit):
            pseudo.pseudonym()

    def test_all_empty_elements_in_rows(self):
        """Test case: input rows have only None elements."""
        map_method = 'counter'
        map_columns = ['name', 'job_title']
        output = test_files_folder

        data = {
            "name": [None, None],
            "job_title": [None, None],
            "gender": [None, None],
            "city": [None, None],
            "postal_code": [None, None]
        }

        df = pl.DataFrame(data)

        pseudo = pseudPy.Pseudonymization(
            map_method,
            map_columns,
            df=df,
            output=output
        )

        with self.assertRaises(SystemExit):
            pseudo.pseudonym()

    def test_empty_encryption(self):
        """Test case: encrypt None value."""
        map_method = 'encrypt'
        map_columns = 'name'
        input_file = f'{test_files_folder}main_empty_row.csv'
        output = test_files_folder

        data = {
            "name": None,
            "job_title": "Nurse",
            "gender": "female",
            "city": "Warsaw",
            "postal_code": "38574"
        }

        df = pl.DataFrame(data)

        pseudo = pseudPy.Pseudonymization(
            map_method,
            map_columns,
            df=df,
            output=output
        )

        with self.assertRaises(AttributeError):
            pseudo.pseudonym()


class TestStructuredDataAggregation(unittest.TestCase):
    """Test data aggregation and k-anonymity"""

    def test_aggregate_salary_plain_user_data(self):
        """Aggregate salary data, return a generalized value of an element, dependent on an input gap."""
        column = 'salary'
        method = ['number', 10000]
        input_file = f'{test_files_folder}/plain_user_data.csv'

        agg = pseudPy.Aggregation(
           column=column,
           method=method,
           input_file=input_file,
           output=test_files_folder
        )

        agg.group()

        output_path = f'{test_files_folder}/output.csv'

        expected_output = pl.read_csv(f'{test_files_folder}/expected_aggregate_output_salary_plain_data.csv')
        actual_output = pl.read_csv(output_path)

        pl.testing.assert_frame_equal(expected_output, actual_output)

        if os.path.exists(output_path):
            os.remove(output_path)
            print('File successfully removed')
        else:
            print('File does not exist')

    def test_aggregate_date_mock_data(self):
        """Aggregate dates, return years or generalized year periods."""
        column = 'date_of_birth'
        method = ['dates-to-years', 5]
        input_file = f'{test_files_folder}/MOCK_DATA.csv'

        agg = pseudPy.Aggregation(
           column=column,
           method=method,
           input_file=input_file,
           output=test_files_folder
        )

        agg.group()

        output_path = f'{test_files_folder}/output.csv'

        expected_output = pl.read_csv(f'{test_files_folder}/expected_aggregate_output_date_mock_data.csv')
        actual_output = pl.read_csv(output_path)

        pl.testing.assert_frame_equal(expected_output, actual_output)

        if os.path.exists(output_path):
            os.remove(output_path)
            print('File successfully removed')
        else:
            print('File does not exist')

    def test_k_anonymity_and_agg_mock_data(self):
        """Aggregate and k-anonymize data, plot the results"""
        df = pd.read_csv(f"{test_files_folder}/MOCK_DATA_small.csv")

        # aggregate dates to years
        agg = pseudPy.Aggregation(column='date_of_birth', method=['dates-to-years', 1], df=df)

        df = agg.group_dates_to_years()

        # df['date_of_birth'].hist()
        # plt.show()
        # plt.close()

        depths = {
            'salary': 1,
            'date_of_birth': 1
        }
        k = 2
        # apply k-anonymization
        k_anonymity = pseudPy.KAnonymity(
            df=df,
            depths=depths,
            k=k,
            output=test_files_folder
        )
        grouped = k_anonymity.k_anonymity()

        # check if k-anonymized
        k = 2
        is_k_anonym = pseudPy.KAnonymity(
            df=grouped,
            k=k,
            depths=depths
        )

        print(is_k_anonym.is_k_anonymized())

        if os.path.exists(f'{test_files_folder}/k_anonym_output.csv'):
            os.remove(f'{test_files_folder}/k_anonym_output.csv')
            print("File removed.")

        # df['date_of_birth'].hist()
        # plt.show()

    def test_k_anon_more_depth(self):
        """k-anonymize data with k=3, plot the results"""
        df = pd.read_csv(f"{test_files_folder}/plain_user_data.csv")

        # df['salary'].hist()
        # plt.show()
        # plt.close()

        depths = {
            'salary': 2
        }
        k = 3
        # apply k-anonymization

        k_anonymity = pseudPy.KAnonymity(
            df=df,
            depths=depths,
            k=k,
            output=test_files_folder
        )

        grouped = k_anonymity.k_anonymity()

        print(grouped)
        # check if k-anonymized
        k = 4
        is_k_anonym = pseudPy.KAnonymity(
            df=grouped,
            k=k,
            depths=depths
        )
        print(is_k_anonym.is_k_anonymized())

        if os.path.exists(f'{test_files_folder}/k_anonym_output.csv'):
            os.remove(f'{test_files_folder}/k_anonym_output.csv')
            print("File removed.")

        # grouped['salary'].hist()
        # plt.show()
        # plt.close()


if __name__ == '__main__':
    unittest.main()
