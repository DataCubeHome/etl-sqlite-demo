"""
ETL-Query script
"""

from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import query

#Extract
print("Extracting data ..")
extract()

#Transform and Load
print("Transforming data ..")
load()

#Query
print("Querying ..")
query()
