import json
import psycopg2
from psycopg2 import Error


def add_table1():

    conn = psycopg2.connect(database="joseph",host="192.168.1.184",user="joseph",port="5432")
    cursor = conn.cursor()
    
    try:
        create_table = '''
            CREATE TABLE security_info (
            customer_id serial PRIMARY KEY,
            event_info VARCHAR (50),
            method VARCHAR (50),
            num1 int,
            num2 int,
            num3 int )
            '''

       
        cursor.execute(create_table)
        conn.commit()
        print("Table 'security_info' created successfully")

    except (Exception, Error) as error:
        print(f"Error creating table: {error}")

    finally:
        if conn:
            cursor.close()
            conn.close()

##################################################################################################


def add_table2():

    conn = psycopg2.connect(database="joseph",host="192.168.1.184",user="joseph",port="5432")
    cursor = conn.cursor()
    
    try:
        create_table = '''
            CREATE TABLE cust_score (
            customer_id INT REFERENCES
            security_info (customer_id),
            score_data jsonb)
            '''

       
        cursor.execute(create_table)
        conn.commit()
        print("Table 'cust_score' created successfully")

    except (Exception, Error) as error:
        print(f"Error creating table: {error}")

    finally:
        if conn:
            cursor.close()
            conn.close()

##################################################################################################



def get_table_secinfo():
    conn = psycopg2.connect(database="joseph",host="192.168.1.184",user="joseph",port="5432")
    cursor = conn.cursor()

    try:        
        query = "SELECT * from security_info"
        cursor.execute(query)
        
        # Fetch and print the result 
        rows = cursor.fetchall()
        for row in rows:
            print(f"{row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[5]}")
    
    except (Exception, psycopg2.Error) as error:
        print(f"Error querying data: {error}")
    
    finally:
        if conn:
            cursor.close()
            conn.close()

##################################################################################

def insert_column_data():
    
    conn = psycopg2.connect(database="joseph",host="192.168.1.184",user="joseph",port="5432")
    cursor = conn.cursor()

    try:
        # Define the SQL INSERT statement
        insert_query = """
            INSERT INTO security_info (event_info, method, num1, num2, num3)
            VALUES (%s, %s, %s, %s, %s)
        """

        # Define the data to be inserted as a list of tuples
        data_to_insert = [
            ('Event 2', 'Method B', 10, 20, 30)
            ]

        # Execute the INSERT statement with the data
        cursor.executemany(insert_query, data_to_insert)

        # Commit the transaction
        conn.commit()
        print("Data inserted successfully")

    except (Exception, psycopg2.Error) as error:
        print(f"Error inserting data: {error}")

    finally:
        if conn:
            cursor.close()
            conn.close()



###################################################################################

def get_table_columns(database_name, table_name):

    conn = psycopg2.connect(database=database_name, user="joseph", host="192.168.1.184", port="5432")
    cursor = conn.cursor()

    try:
        # Query to get the columns of the specified table
        query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        """

        cursor.execute(query)
        columns = cursor.fetchall()

        # Extract column names from the result
        column_names = [column[0] for column in columns]

        return column_names

    except (Exception, psycopg2.Error) as error:
        print(f"Error: {error}")
    finally:
        if conn:
            cursor.close()
            conn.close()


    if columns:
        print(f"Columns of table '{table_name}':")
        for column in columns:
            print(column)
    else:
        print(f"Table '{table_name}' not found or no columns.")


    
###########################################################################


def add_column():
    conn = psycopg2.connect(database="joseph",host="192.168.1.184",user="joseph",port="5432")
    cursor = conn.cursor()

    try:
        alter_query = """ALTER TABLE security_info
        ADD COLUMN event_info VARCHAR(50)"""

       
        cursor.execute(alter_query)
        conn.commit()
        print("Table 'security_info' altered successfully")

    except (Exception, Error) as error:
        print(f"Error creating table: {error}")

    finally:
        if conn:
            cursor.close()
            conn.close()

###############################################################################


def get_all_tables():
    conn = psycopg2.connect(database="joseph",host="192.168.1.184",user="joseph",port="5432")
    cursor = conn.cursor()

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")

    tables = cursor.fetchall()

    for table in tables:
         print(table[0])

    conn.close()


########################################################################################


def sum_columns_insert():
    conn = psycopg2.connect(database="joseph",host="192.168.1.184",user="joseph",port="5432")
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO cust_score (customer_id, score_data)
    SELECT customer_id, jsonb_build_object(
        'date', current_date,
        'sum_score', num1 + num2 + num3)
    FROM security_info;
    """)

    conn.commit()
    conn.close()


################################################################################

def get_table_cust_score():
    conn = psycopg2.connect(database="joseph",host="192.168.1.184",user="joseph",port="5432")
    cursor = conn.cursor()

    try:        
        query = "SELECT * from cust_score"
        cursor.execute(query)
        
        # Fetch and print the result 
        rows = cursor.fetchall()
        for row in rows:
            # Get the JSONB data as a dictionary
            json_data = row[1]

            # Manually format the JSON string without quotes around keys and values
            formatted_data = "{\n"
            for key, value in json_data.items():
                formatted_data += f"    {key}: {value},\n"
            formatted_data += "}"

            # Print the formatted JSON string
            print(f"{row[0]}, {formatted_data}")


    except (Exception, psycopg2.Error) as error:
        print(f"Error querying data: {error}")
    
    finally:
        if conn:
            cursor.close()
            conn.close()





if __name__ == "__main__":

#add_table1()
#add_table2()


 """database_name = "joseph"
    table_name = "security_info"
    columns = get_table_columns(database_name,table_name)
    if columns:
        print(f"Columns of table '{table_name}':")
        for column in columns:
            print(column)
    else:
        print(f"Table '{table_name}' not found or no columns.") 
 """

#get_table_secinfo()
#get_table_cust_score()
#insert_column_data()
#add_column()
#get_all_tables()
#sum_columns_insert()