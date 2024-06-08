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
url

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

#### Navigate to the directory with the repository and create the virtual environment:
```bash
python -m venv <directory>
```

Linux and MacOS venv activation:
```bash
source myvenv/bin/activate
```

Windows venv activation:
```bash
# In cmd.exe
venv\Scripts\activate.bat
# In PowerShell
venv\Scripts\Activate.ps1
```

If complications occur, please refer to the [venv documentation](https://docs.python.org/3/library/venv.html). 

#### Install requirements:
```bash
pip install -r requirements.txt
```

## Examples

---
### 1. Use the automated script for data pseudonymization

Run the script  with YAML config file(from the repository directory):
```bash
python /pseudPy/script_pseudonym.py /tests/config.yaml
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
- encrypt
- decrypt
- random1
- random4
- hash
- hash-salt
- merkle-tree 
- faker

### Anonymization
- aggregation
- k-anonymity