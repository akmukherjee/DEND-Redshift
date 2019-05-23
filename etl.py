""" 
    This file contains the functions to run the ETL job to populate the songplays, users, songs, artists and time tables.       First the staging tables are loaded then the analysis tables are populated from the staging tables
"""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """  
  
    This function processes iterates over the queries listed in the copy_table_queries list.
    The goal of this function is to copy the staging tables from the designated S3 bucket.
    
    Parameters: 
    curr: Cursor variable with the currently connected DW
    conn: The connection variable passed into the function
    
    
    Returns: 
    None
    """ 
    #Iterate over the copy_table_queries list and execute them and populate the staging_songs
    #and staging_events table
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """  
  
    This function processes iterates over the queries listed in the insert_table_queries list.
    The goal of this function is to copy the relevant transformed data from the Staging Tables to the Analysis
    Tables.
    
    Parameters: 
    curr: Cursor variable with the currently connected DW
    conn: The connection variable passed into the function
    
    
    Returns: 
    None
    """ 
    
    #Iterate over the insert_table_queries list and execute them and populate the anaysis tables
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ 
    The function to runs the main function on this module. 
  
    This main function first loads the staging tables from the S3 bucket and then performs the ETL
    to populate the Analysis Tables.
  
    Parameters: 
    None
    
    Returns: 
    None
    """
    
    #Read the different configuration variables    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #Connect to the Data Warehouse (DW) from the values stored in config variable
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #Load the Staging Tables by copying data from the S3 Bucket
    load_staging_tables(cur, conn)
    #ETL the desired data from the staging tables into the analysis tables
    insert_tables(cur, conn)

    #Close the Connection    
    conn.close()


if __name__ == "__main__":
    main()