"""
general name,count_products,ingred_FPro,avg_FPro_Products,avg_distance_root,ingred_normalization_term,semantic_tree_na
"""
import sqlite3
import csv
import os

#load the csv file and insert into a new sqlite3 database

def load(dataset="/workspace/etl-sqlite-demo/data/GroceryDB_IgFPro.csv"):
    """"Transforms and loads data into the local SQlite3 database"""

    #prints the full working directory and path
    print(os.getcwd())
    payload = csv.reader(open(dataset, newline=''),delimiter=',')
    conn = sqlite3.connect('GroceryDB.db')
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS GroceryDB")
    c.execute("CREATE TABLE GroceryDB(id,general_name,count_products,ingred_FPro,avg_FPro_products,avg_distance_root,ingred_normalization_term,semantic_tree_name,semantic_tree_name2")
    #insert
    c.executemany("INSERT INTO GroceryDB VALUES(?,?,?,?,?,?,?,?,?)",payload)
    conn.commit()
    conn.close()
    return "GroceryDB.db"

