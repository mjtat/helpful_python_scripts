import pymssql
import psycopg2
import pandas as pd

# Note: Runs on python 2.7

def run_query_pymssql(host, database, user, password, query):

    '''
    Opens a connection, creates a cursor, and runs the query, using pymssql.
    All results are fetched into a list, and the connection is then closed.

    params:
        host - host/server name
        database - db name (e.g., EDW_Generic)
        user - username
        password - password
        query - SQL query
    '''

    conn = pymssql.connect(host=host, database=database, user=user, password=password)
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    conn.close()

    return result

def run_query_pymssql_pandas(host, database, user, password, query):

    '''
    Opens a connection, and runs the query using pandas read_sql, using pymssql.
    All results are fetched into a pandas dataframe, and the connection is then closed.

    params:
        host - host/server name
        database - db name (e.g., EDW_Generic)
        user - username
        password - password
        query - SQL query
    '''

    conn = pymssql.connect(host=host, database=database, user=user, password=password)
    df = pd.read_sql(query, conn)
    conn.close()

    return df

def run_query_psycopg2(host, port, database, user, password, query):

    '''
    Opens a connection, creates a cursor, and runs the query, using psycopg2.
    All results are fetched into a list, and the connection is then closed.

    params:
        host - host/server name
        database - db name (e.g., EDW_Generic)
        user - username
        password - password
        query - SQL query
    '''

    conn = psycopg2.connect(database=database, user=user, host=host, password=password, port=port)
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    conn.close()

    return result

def run_query_psycopg2_pandas(host, port, database, user, password, query):

    '''
    Opens a connection, and runs the query using pandas read_sql, using psycopg2.
    All results are fetched into a pandas dataframe, and the connection is then closed.

    params:
        host - host/server name
        database - db name (e.g., EDW_Generic)
        user - username
        password - password
        query - SQL query
    '''

    conn = psycopg2.connect(database=database, user=user, host=host, password=password, port=port)
    df = pd.read_sql(query, conn)
    conn.close()

    return df

if __name__ == '__main__':

    # Query for pymssql
    pymssql_query = '''
    SELECT TOP 10 *
    FROM dbo.EMS_BikeCrashes
    '''

    # Query for psycopg2
    psycopg2_query = '''
    SELECT *
    FROM open_data.mbta_reliability_history
    LIMIT 10
    '''

    # Returns a list. Replace user and password with valid credentials
    query_result = run_query_pymssql(host='VSQL25',
                                     database='EDW_Generic',
                                     user='xxx',
                                     password='xxx',
                                     query=pymssql_query)
    # Returns a dataframe. Replace user and password with valid credentials
    query_result_df = run_query_pymssql_pandas(host='VSQL25',
                                               database='EDW_Generic',
                                               user='xxx',
                                               password='xxx',
                                               query=pymssql_query)

    # Returns a list. Replace user and password with valid credentials
    query_result_psql = run_query_psycopg2(database='boston_analytics',
                                           user='xxx',
                                           host='10.30.180.144',
                                           password="xxx",
                                           port='6666',
                                           query=psycopg2_query)


    # Returns a dataframe. Replace user and password with valid credentials
    query_result_psql_pandas = run_query_psycopg2_pandas(database='boston_analytics',
                                                         user='xxx',
                                                         host='10.30.180.144',
                                                         password="xxx",
                                                         port='6666',
                                                         query=psycopg2_query)

    print query_result
    print query_result_df
    print query_result_psql
    print query_result_psql_pandas
