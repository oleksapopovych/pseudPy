# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
# Insert the parent directory into sys.path
sys.path.insert(0, os.path.abspath('../..'))

project = 'pseudPy'
copyright = '2024, Oleksandra Popovych'
author = 'Oleksandra Popovych'
version = '1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx_autodoc_typehints',
    'sphinx.ext.napoleon',
    'rst2pdf.pdfbuilder',

]

pdf_documents = [
    ('1', 'pseudPy', 'Library for Automatic Pseudonymization', 'Oleksandra Popovych'),
]

templates_path = ['_templates']
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']
