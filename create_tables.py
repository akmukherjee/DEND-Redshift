""" 
    This file contains the functions to create the DB connection along with creating and dropping the tables. 
"""
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ 
    The function to drop the tables on the connected DB. 
  
    This function executes the queries listed in the drop_table_queries list and drops the appropriate
    tables. 
  
    Parameters: 
    conn: The connection variable to the DW
    curr: Cursor variable with the currently connected DW
    
    Returns: 
    None
    """    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ 
    The function to create the tables on the connected DW. 
  
    This function executes the queries listed in the create_table_queries list and creates the appropriate
    tables. 
  
    Parameters: 
    conn: The connection variable to the DW
    curr: Cursor variable with the currently connected DW
    
    Returns: 
    None
    """ 
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ 
    The function to runs the main function on this module. 
  
    This main function first drops the tables and then creates new tables on the connected DW 
  
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
    
    #Drop if tables exist and Recreate the Tables
    drop_tables(cur, conn)
    create_tables(cur, conn)

    #Close the Connection
    conn.close()


if __name__ == "__main__":
    main()