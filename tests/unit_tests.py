import unittest
import pandas as pd
from pseudPy import myfunctions as pseudPy
import os

home_dir = os.path.expanduser('~')
test_files_folder = f'{home_dir}/PycharmProjects/pseudPy/tests/test_files/'


class TestAggregateNumbers(unittest.TestCase):
    def test_agg_age(self):

        input_path = f'{test_files_folder}test.csv'
        output_path = f'{test_files_folder}output.csv'
        out_folder = test_files_folder
        columns = ['Age']
        method = [['number', 3]]

        pseudPy.group(
            columns,
            method,
            input_file=input_path,
            output=out_folder
        )

        expected_file = pd.read_csv(f'{test_files_folder}expected_agg_output.csv')
        actual_file = pd.read_csv(output_path)

        pd.testing.assert_frame_equal(expected_file, actual_file, check_dtype=True)

        if os.path.exists(output_path):
            os.remove(output_path)
            print('File successfully removed')
        else:
            print('File does not exist')

    def test_agg_salary(self):

        input_path = f'{test_files_folder}university_employees.csv'
        output_path = f'{test_files_folder}output.csv'
        out_folder = test_files_folder
        columns = ['Salary']
        methods = [['number', 10000]]

        pseudPy.group(
            columns,
            methods,
            input_file=input_path,
            output=out_folder
        )

        expected_file = pd.read_csv(f'{test_files_folder}expected_agg_salary.csv')
        actual_file = pd.read_csv(output_path)

        pd.testing.assert_frame_equal(expected_file, actual_file, check_dtype=True)

        if os.path.exists(output_path):
            os.remove(output_path)
            print('File successfully removed')
        else:
            print('File does not exist')

    def test_agg_more_columns(self):

        input_path = f'{test_files_folder}test.csv'
        output_path = f'{test_files_folder}output.csv'
        out_folder = test_files_folder
        columns = ['Age', 'Average Grade']
        methods = [['number', 5], ['number', 1.7]]

        pseudPy.group(
            columns,
            methods,
            input_file=input_path,
            output=out_folder
        )

        expected_file = pd.read_csv(f'{test_files_folder}expected_more_columns.csv')
        actual_file = pd.read_csv(output_path)

        pd.testing.assert_frame_equal(expected_file, actual_file, check_dtype=True)

        if os.path.exists(output_path):
            os.remove(output_path)
            print('File successfully removed')
        else:
            print('File does not exist')


class TestDecryption(unittest.TestCase):
    def test_decryption(self):

        output_path = f'{test_files_folder}decrypt_output.csv'
        encrypted_path = f'{test_files_folder}output.csv'

        input_file = f'{test_files_folder}plain_user_data.csv'
        output = test_files_folder
        map_columns = ['name']
        map_method = 'hash'

        pseudPy.generate_rsa_keys()

        pseudPy.pseudonym(
            map_columns,
            map_method,
            input_file=input_file,
            output=output,
            encrypt_map=True
        )

        pseudPy.decrypt(column=['name'], file_name=f'{test_files_folder}mapping_output_0.csv', destination_name=test_files_folder)

        #expected_file = pd.read_csv(f'{test_files_folder}expected_decrypt.csv')
        #actual_file = pd.read_csv(f'{test_files_folder}decrypt_output.csv')

        #pd.testing.assert_frame_equal(expected_file, actual_file, check_dtype=True)

        if os.path.exists(output_path):
            os.remove(output_path)
            os.remove('private_key.pem')
            os.remove('public_key.pem')
            print('File successfully removed')
        else:
            print('File does not exist')

        if os.path.exists(encrypted_path):
            os.remove(encrypted_path)
            os.remove(f'{test_files_folder}mapping_output_0.csv')
            print('File successfully removed')
        else:
            print('File does not exist')

    def test_decryption_more_columns(self):

        output_path = f'{test_files_folder}decrypt_output.csv'
        encrypted_path = f'{test_files_folder}output.csv'

        input_path = f'{test_files_folder}university_employees.csv'
        out_folder = test_files_folder
        map_columns = ['Name', 'Role']
        map_method = 'encrypt'

        pseudPy.generate_rsa_keys()

        pseudPy.pseudonym(
            map_columns,
            map_method,
            input_path,
            out_folder,
            encrypt_map=True
        )

        pseudPy.decrypt(column=['Index_Name', 'Index_Role'], file_name=f'{test_files_folder}output.csv', destination_name=test_files_folder )

        expected_file = pd.read_csv(f'{test_files_folder}expected_decrypt_more_columns.csv')
        actual_file = pd.read_csv(f'{test_files_folder}decrypt_output.csv')

        pd.testing.assert_frame_equal(expected_file, actual_file, check_dtype=True)

        if os.path.exists(output_path):
            os.remove(output_path)
            os.remove('private_key.pem')
            os.remove('public_key.pem')
            print('File successfully removed')
        else:
            print('File does not exist')

        if os.path.exists(encrypted_path):
            os.remove(encrypted_path)
        #    os.remove(f'{test_files_folder}mapping_output_0.csv')
        #    os.remove(f'{test_files_folder}mapping_output_1.csv')
            print('File successfully removed')
        else:
            print('File does not exist')

    def test_decryption_more_columns_10000_rows(self):

        output_path = f'{test_files_folder}decrypt_output.csv'
        encrypted_path = f'{test_files_folder}output.csv'

        input_path = f'{test_files_folder}uni_empl_10000_rows.csv'
        out_folder = test_files_folder
        map_columns = ['Name', 'Role']
        map_method = 'encrypt'

        pseudPy.generate_rsa_keys()

        pseudPy.pseudonym(
            map_columns,
            map_method,
            input_path,
            out_folder,
            encrypt_map=True
        )

        pseudPy.decrypt(column=['Index_Name', 'Index_Role'], file_name=f'{test_files_folder}output.csv', destination_name=test_files_folder )

        #expected_file = pd.read_csv(f'{test_files_folder}expected_decrypt_more_columns.csv')
        #actual_file = pd.read_csv(f'{test_files_folder}decrypt_output.csv')

        #pd.testing.assert_frame_equal(expected_file, actual_file, check_dtype=True)

        if os.path.exists(output_path):
            #os.remove(output_path)
            #os.remove('private_key.pem')
            #os.remove('public_key.pem')
            print('File successfully removed')
        else:
            print('File does not exist')

        if os.path.exists(encrypted_path):
            os.remove(encrypted_path)
        #    os.remove(f'{test_files_folder}mapping_output_0.csv')
        #    os.remove(f'{test_files_folder}mapping_output_1.csv')
            print('File successfully removed')
        else:
            print('File does not exist')

class TestMapMethods(unittest.TestCase):

    def test_map_two_tier(self):

        output_path = f'{test_files_folder}output.csv'

        input_file = f'{test_files_folder}user_data_100000_rows.csv'
        output = test_files_folder
        map_columns = ['name']
        map_method = 'counter'

        pseudPy.generate_rsa_keys()

        pseudPy.pseudonym(
            map_columns,
            map_method,
            input_file,
            output,
            encrypt_map=True
        )

        """
        expected_file = pd.read_csv(f'{test_files_folder}expected_output_two_method.csv')
        actual_file = pd.read_csv(f'{test_files_folder}output.csv')

        pd.testing.assert_frame_equal(expected_file, actual_file, check_dtype=True)

        pseudPy.decrypt(column=['Name'], file_name='test_files/mapping_output_0.csv', destination_name=test_files_folder)

        expected_file = pd.read_csv(f'{test_files_folder}expected_output_mapping_0.csv')
        actual_file = pd.read_csv(f'{test_files_folder}decrypt_output.csv')

        pd.testing.assert_frame_equal(expected_file, actual_file, check_dtype=True)

        pseudPy.decrypt(column=['Age'], file_name='test_files/mapping_output_1.csv', destination_name=test_files_folder)

        expected_file = pd.read_csv(f'{test_files_folder}expected_output_mapping_1.csv')
        actual_file = pd.read_csv(f'{test_files_folder}decrypt_output.csv')

        pd.testing.assert_frame_equal(expected_file, actual_file, check_dtype=True)

        if os.path.exists(output_path):
            os.remove(output_path)
            print('File successfully removed')
        else:
            print('File does not exist')

        if os.path.exists(f'{test_files_folder}mapping_output_0.csv'):
            os.remove(f'{test_files_folder}mapping_output_0.csv')
            os.remove('test_files/decrypt_output.csv')
            os.remove('private_key.pem')
            os.remove('public_key.pem')
            print('File successfully removed')
        else:
            print('File does not exist')

        if os.path.exists(f'{test_files_folder}mapping_output_1.csv'):
            os.remove(f'{test_files_folder}mapping_output_1.csv')
            print('File successfully removed')
        else:
            print('File does not exist')
        """




class NLPPseudonymMethods(unittest.TestCase):

    def test_single_instance_german_hash(self):

        text = 'Max Mustermann lebt in Berlin.'

        actual_result = pseudPy.nlp_pseudonym('hash', text=text, lan='de')[0]
        expected_result = 'dddfab9b5b8a360150547065daff114ff218b39c8b0986b761075977aeeca3c3 lebt in dad114b6ed7342bac65c79575f6c7ff761ec26b52c1f5f7a9110532973d05df2.'
        self.assertEqual(expected_result, actual_result, 'The result is wrong')

    def test_multiple_instance_german_counter(self):

        text = 'Max Mustermann lebt in Berlin. Kontaktieren Sie Max unter max@example.de.'

        actual_result = pseudPy.nlp_pseudonym('counter', text=text, lan='de')
        expected_result = '0 lebt in 1. 2 unter 3.'

        self.assertEqual(expected_result, actual_result[0], 'The result is wrong')

    def test_sentences_no_entities(self):
        """This testing revealed the situation where there is nothing to pseudonymize and my code throws an error"""
        text = 'Der Datenschutzbeauftragte arbeitet mit Personendaten'

        actual_result = pseudPy.nlp_pseudonym('counter', text=text, lan='de')
        expected_result = 'Der Datenschutzbeauftragte arbeitet mit Personendaten'

        self.assertEqual(expected_result, actual_result[0], 'The result is wrong')

    def test_contextual_sensitivity(self):
        text = 'Der Präsident der Firma, Hans Müller, und der Präsident von Frankreich besuchten das Event.'

        actual_result = pseudPy.nlp_pseudonym('counter', text=text, lan='de')
        expected_result = 'Der Präsident der Firma, 0, und der Präsident von 1 besuchten das Event.'

        self.assertEqual(expected_result, actual_result[0], 'The result is wrong')

        expected_result_df = {
            'Names': ['Hans Müller', 'Frankreich'],
            'Index_Names': ['0', '1']
        }
        df = pd.DataFrame(expected_result_df)

        pd.testing.assert_frame_equal(df, actual_result[1], check_dtype=True)


if __name__ == '__main__':
    unittest.main()
