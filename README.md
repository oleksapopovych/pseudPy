# pseudPy - Data Pseudonymization and Anonymization Library

---

### Automated PII de-identification and pseudonymization library for CSV and txt formats.

---

- Data Pseudonymizer
- Reversible Pseudonymization
- Structured and Unstructured Data
- Eight Pseudonymization Methods
- k-Anonymization
- Customizable 

## What is pseudPy?

---

***pseudPy***, a library focused primarily on the pseudonymization of PII data, was created to 
emphasize the relevance of data protection for all projects that use, store or process personal data. 
The aim is to simplify the task of pseudonymizing data and make it more accessible for Python users. 
It provides fast *__pseudonymization__* possibilities, relatively *__easy configuration__* and *__multiple pseudonymization 
methods__*. With respect to unstructured text, the library is able to recognize personal data such 
as names, locations, organizations, emails and phone numbers. k-anonymity and aggregation are also 
provided as anonymization and data minimization options. 

### Documentation

---
[pseudPy Documentation](https://github.com/oleksapopovych/pseudPy/blob/main/pseudPy/docs/build/pdf/pseudPy.pdf)

**Check out the [Tutorial](https://youtu.be/H2FiyKSnIx4) for the video instructions!** (primarily for GUI)

The sample files from the video can be found in the folder *pseudPy/test_files*.

### Main Features

---

1. Choose between pseudonymization of structured and unstructured data;
2. Select one of eight available pseudonymization methods; 
3. Allow one-way or reversible pseudonymization;
4. Encrypt the mapping tables after the pseudonymization process; 
5. Pseudonymization script for easier configuration;
6. GUI for users with any background;
7. Filter for the specific data for pseudonymization;
8. Allow data minimization and anonymization using k-anonymity and aggregation.

**Note**: ___pseudPy___ uses the spaCy package to recognize PII in free text. However, there is no guarantee that all PII
will be found correctly, so the user is also responsible for verifying the output results. 

## Install

---

#### Clone the repository:
```bash
git clone https://github.com/oleksapopovych/pseudPy.git
```

#### Navigate to the main directory of the repository (pseudPy) and create the virtual environment:
```bash
python -m venv .venv
```

Linux and MacOS venv activation:
```bash
source .venv/bin/activate
```

Deactivate:
```bash
deactivate
```

Windows venv activation:
```bash
# In cmd.exe
.venv\Scripts\activate.bat
# In PowerShell
.venv\Scripts\Activate.ps1
```

If complications occur, please refer to the [venv documentation](https://docs.python.org/3/library/venv.html). 

#### Install requirements:
```bash
pip install -r requirements.txt
```

### Script Configuration

---

***Note***: please change the paths in the *config_\*.yaml* files before running the scripts and unit tests.

### Unit Tests

---

If a path error occurs, try navigating to the */pseudPy/pseudPy/* folder and then run the unit tests. 
The problem might depend on the IDE.

In the worst case, configure the paths manually in *unit_tests.py*.:

```python
home_dir = os.path.expanduser('~')
path_to_repo = f'{os.getcwd()}'
test_files_folder = f'{path_to_repo}/test_files'
```

## Examples

---
### 1. Use the automated script for data pseudonymization

Run the script  with YAML config file(from the repository directory):
```bash
python /pseudPy/script_pseudonym.py /pseudPy/config_pseudonym.yaml
```
YAML for unstructured data:
```yaml
map_columns: null
map_method: faker
input_file: /path/to/file.txt
patterns: null
pos_type: null
output: /path/to/output/directory
mapping: true
encrypt_map: false
all_ne: true
seed: null
```
Input:
```txt
Project Titan led by Project Manager Emily White in London achieved a 20% cost saving.
```
Output:
```txt
Project Stevenport led by Project Manager Tina Villanueva in Susanfort achieved a 20% cost saving.
```

### 2. Import and apply pseudonymization functions

```python
import pseudPy.Pseudonymization as pseudPy

pseudo = pseudPy.Pseudonymization(
    input_file='/path/to/output/file.csv',
    map_method='counter',
    map_columns='name',
    output='/path/to/output/directory'
)

pseudo.pseudonym()
```
Input:
```csv
name,country,gender,salary,job_title
Maren Colhoun,China,Female,162080,Community Outreach Specialist
Yule Ruppert,Bangladesh,Male,51766,GIS Technical Architect
```
Output:
```csv
Index_name,country,gender,salary,job_title
0,China,Female,162080,Community Outreach Specialist
1,Bangladesh,Male,51766,GIS Technical Architect
```
Mapping Table:
```csv
Index_name,name
0,Maren Colhoun
1,Yule Ruppert
```
### 3. Execute the pseudonymization GUI
```bash
python gui.py
```
### 4. Aggregate data
```python
import pseudPy.Pseudonymization as pseudPy

agg = pseudPy.Aggregation(
    column='salary',
    method=['number', 10000],
    input_file='/path/to/file.csv',
    output='/path/to/output/directory'
)

agg.group()
```
Input:
```csv
name,country,gender,salary,job_title
Maren Colhoun,China,Female,162080,Community Outreach Specialist
Yule Ruppert,Bangladesh,Male,51766,GIS Technical Architect
```
Output:
```csv
name,country,gender,salary,job_title
Maren Colhoun,China,Female,160001-170000,Community Outreach Specialist
Yule Ruppert,Bangladesh,Male,50001-60000,GIS Technical Architect
```

## Customization

---
Add new pseudonym generator and the mapping functionality to the Mapping class:
```python
class Mapping:

    ...
    
    @staticmethod
    def custom_pseudonym_generator():
        return pseudonym

    def custom_tier(self):
        return pl.Series(f'Index_{self.first_tier}', self.df[self.first_tier].map_elements(
                lambda x: Mapping.custom_pseudonym_generator(), return_dtype=pl.Utf8))
```
### Structured/Unstructured Data
Add new method to the dictionary:
```python
map_method_handlers = {
    ...,
    'custom-method': Mapping.custom_pseudonym_generator
}
```
### Unstructured data
If a new Faker method is created for some new entity:
```python
faker_pos_handlers = {
    ...,
    'New Entity': Mapping.faker_custom_tier
}
```
And add the entity recognition to the entity_mapping() function(regex, spaCy, etc.):
```python
class Helpers:
    
    def entity_mapping(self):
        ...
        if 'New Entity' in pos_type:
            new_entity_list = list()    # find data with regex or spaCy
            map_dict['New Entity'].append(new_entity_list)
        ...
```
### Update GUI 
Update GUI with new pseudonymization methods:
```python
method_options = [
    ...,
    'custom-method'
]
```
Update GUI with new entity type:
```python
types_of_data = [
    ...,
    'New Entity'
]
```
More complex structures naturally require a deeper understanding of the library. 

## Included techniques

---

### Pseudonymization

- counter

A monotonic counter that starts at a specific value, e.g., 0, and is incremented by 1 for each
new identity.
- encrypt

The symmetric encryption algorithm AES. Use encrypted values as pseudonyms.
- decrypt

Decrypt the pseudonyms using the secret key.
- random1

A random1 method generates a UUID9, uuid1(), from a host ID, a sequence number, and the
current time.
- random4

The random4 method generates an entirely random UUID, uuid4().
- hash

The hashing method generates a series of pseudonyms by applying the SHA-256 algorithm to
the hexadecimal encoding of each entry value.
- hash-salt

With the hash-salt method, pseudonyms are created by hashing the initial values with the SHA-
256 algorithm and using a randomly generated UUID value, uuid4(), in hexadecimal coding as
a salt.
- merkle-tree 

The method combines all identifiers of each structured entry row in a Merkle tree. The 
pseudonyms are defined as the unique root values of the individual corresponding Merkle trees.
- faker

The faker method can only be used on unstructured data. Depending on the entity type, the
method automatically replaces the input value with the fake name of the corresponding entity.
For structured data, the specific faker type that matches the desired fake name type must be
selected manually. "faker-name" for the full names, "faker-email" for the emails, "faker-phone" 
for the phone numbers and "faker-org" for the organization names.

### Anonymization
- aggregation

Aggregation is an anonymization technique in which the attributes of the dataset are converted
into combined values. For example, grouping the specific age
values into more general age groups.
- k-anonymity

The main requirement to consider data as k-anonymous: the combination of
quasi-identifiers in the dataset must be matched to at least k individuals.
