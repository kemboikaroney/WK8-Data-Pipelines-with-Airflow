# -*- coding: utf-8 -*-
"""Data Pipelines with Airflow.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/11AqEz3Rj7RcdmzUPUfZhCpvO_IAqQkxk
"""

'''
Data Pipelines with Airflow for MTN Rwanda
'''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import logging

# Define the default arguments for the DAG
default_args = {
    'owner': 'MTN Rwanda Telecoms',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG('data_pipeline', 
          default_args=default_args, 
          schedule_interval=timedelta(days=1)
          )

# Define the function to extract the data from the CSV files
def extract_data():
    # Load the customer, order, and payment data from CSV files into Pandas dataframes
    customer_df = pd.read_csv('customer_data.csv')
    order_df = pd.read_csv('order_data.csv')
    payment_df = pd.read_csv('payment_data.csv')
    # Return the dataframes
    return customer_df, order_df, payment_df

# Define the function to transform the data
def transform_data(customer_df, order_df, payment_df):
    # Convert the date_of_birth field to datetime format
    customer_df['date_of_birth'] = pd.to_datetime(customer_df['date_of_birth'])

    # Merge the customer and order dataframes on the customer_id column
    customer_order_df = pd.merge(customer_df, order_df, on='customer_id')

    # Merge the payment dataframe with the merged dataframe on the order_id and customer_id columns
    customer_payment_df = pd.merge(customer_order_df, payment_df, on=['order_id', 'customer_id'])

    # Drop unnecessary columns like customer_id and order_id
    customer_payment_df.drop(columns=['customer_id', 'order_id'], inplace=True)

    # Group the data by customer and aggregate the amount paid using sum
    customer_grouped_df = customer_payment_df.groupby(['first_name', 'last_name', 'email', 'country', 'gender', 'date_of_birth'])['amount'].sum().reset_index()

    # Create a new column to calculate the total value of orders made by each customer
    customer_grouped_df['total_order_value'] = customer_payment_df.groupby(['first_name', 'last_name', 'email', 'country', 'gender', 'date_of_birth'])['price'].sum().values
    
    # Calculate the customer lifetime value using the formula CLV = (average order value) x (number of orders made per year) x (average customer lifespan)
    customer_grouped_df['average_order_value'] = customer_grouped_df['total_order_value'] / customer_grouped_df['amount']
    customer_grouped_df['number_of_orders_per_year'] = customer_grouped_df['amount'] / ((pd.to_datetime('now') - customer_grouped_df['date_of_birth']).dt.days / 365)
    customer_grouped_df['average_customer_lifespan'] = (pd.to_datetime('now') - customer_grouped_df['date_of_birth']).dt.days / 365
    customer_grouped_df['clv'] = customer_grouped_df['average_order_value'] * customer_grouped_df['number_of_orders_per_year'] * customer_grouped_df['average_customer_lifespan']
    
    # Return the transformed dataframe
    return customer_grouped_df


# Define the function to load the transformed data into a PostgreSQL database
# Use try catch block to catch any error in loading the data and log the error using python logging module

def load_data(transformed_df):
    '''
    Logs a success message when the data is loaded successfully into the PostgreSQL database, and an error message with the specific error when an error occurs. 
    '''
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host = "34.170.193.146",
            database = "mtnDB",
            user = "admin",
            password = "admin1"
        )

        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Create the customer_ltv table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customer_ltv (
                customer_id INTEGER PRIMARY KEY,
                total_orders INTEGER,
                total_amount NUMERIC(10,2),
                avg_order_value NUMERIC(10,2),
                ltv NUMERIC(10,2)
            )
        """)

        # Insert the transformed data into the customer_ltv table
        for index, row in transformed_df.iterrows():
            cur.execute("""
                INSERT INTO customer_ltv (customer_id, total_orders, total_amount, avg_order_value, ltv)
                VALUES (%s, %s, %s, %s, %s)
            """, (row['customer_id'], row['total_orders'], row['total_amount'], row['avg_order_value'], row['ltv']))

        # Commit the transaction
        conn.commit()

        # Close the cursor and connection to the database
        cur.close()
        conn.close()

        # Log a success message
        logging.info("Data loaded successfully into PostgreSQL database")

    except Exception as e:
        # Log an error message
        logging.error(f"Error loading data into PostgreSQL database: {str(e)}")
        raise e

# Define the extract data task
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

# Define the transform data task
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

# Define the load data task
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

# Define the task dependencies
extract_data_task >> transform_data_task >> load_data_task
