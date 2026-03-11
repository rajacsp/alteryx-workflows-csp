#!/usr/bin/env python3
"""
Airflow DAG: sample
Generated from Markdown: sample_1.md
Generated: 2026-03-11 00:45:02

This workflow contains 9 tools and 9 connections.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sample',
    default_args=default_args,
    description='This workflow contains 9 tools and 9 connections.',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['markdown', 'generated'],
)

    # Input Data - Tool 1
    def read_data_1(context):
        import pandas as pd
        ti = context['ti']
        
        # Read input file
        file_path = 'C:\Users\ash_s\Downloads\archive (8)\US_Accidents_March23.csv'
        
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_1 = PythonOperator(
        task_id='read_data_1',
        python_callable=read_data_1,
        dag=dag,
    )

    # Summarize - Tool 2
    def summarize_data_2(context):
        import pandas as pd
        ti = context['ti']
        
        json_data = ti.xcom_pull(key='data', task_ids='read_data_1')
        df = pd.read_json(json_data)
        
        # Aggregate data
        pass
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_2 = PythonOperator(
        task_id='summarize_data_2',
        python_callable=summarize_data_2,
        dag=dag,
    )

    # Browse - Tool 3
    def browse_data_3(context):
        import pandas as pd
        ti = context['ti']
        
        json_data = ti.xcom_pull(key='data', task_ids='read_data_2')
        df = pd.read_json(json_data)
        
        print(f"Browse Tool 3 - Shape: {df.shape}")
        print(df.head())
        
        return df.shape[0]
    
    task_3 = PythonOperator(
        task_id='browse_data_3',
        python_callable=browse_data_3,
        dag=dag,
    )

    # Browse - Tool 4
    def browse_data_4(context):
        import pandas as pd
        ti = context['ti']
        
        json_data = ti.xcom_pull(key='data', task_ids='read_data_1')
        df = pd.read_json(json_data)
        
        print(f"Browse Tool 4 - Shape: {df.shape}")
        print(df.head())
        
        return df.shape[0]
    
    task_4 = PythonOperator(
        task_id='browse_data_4',
        python_callable=browse_data_4,
        dag=dag,
    )

    # Filter - Tool 5
    def filter_data_5(context):
        import pandas as pd
        ti = context['ti']
        
        json_data = ti.xcom_pull(key='data', task_ids='read_data_2')
        df = pd.read_json(json_data)
        
        # Apply filter: Timezone.notnull()
        try:
            df = df.query('Timezone.notnull()')
        except:
            # Fallback for complex expressions
            pass
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_5 = PythonOperator(
        task_id='filter_data_5',
        python_callable=filter_data_5,
        dag=dag,
    )

    # Browse - Tool 7
    def browse_data_7(context):
        import pandas as pd
        ti = context['ti']
        
        json_data = ti.xcom_pull(key='data', task_ids='read_data_17')
        df = pd.read_json(json_data)
        
        print(f"Browse Tool 7 - Shape: {df.shape}")
        print(df.head())
        
        return df.shape[0]
    
    task_7 = PythonOperator(
        task_id='browse_data_7',
        python_callable=browse_data_7,
        dag=dag,
    )

    # Formula - Tool 16
    def apply_formula_16(context):
        import pandas as pd
        ti = context['ti']
        
        json_data = ti.xcom_pull(key='data', task_ids='read_data_5')
        df = pd.read_json(json_data)
        
        # Apply formulas
        
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_16 = PythonOperator(
        task_id='apply_formula_16',
        python_callable=apply_formula_16,
        dag=dag,
    )

    # Tool - Tool 17
    def process_17(context):
        import pandas as pd
        ti = context['ti']
        
        json_data = ti.xcom_pull(key='data', task_ids='read_data_16')
        df = pd.read_json(json_data)
        
        # Process data with Tool
        # TODO: Implement Tool logic
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_17 = PythonOperator(
        task_id='process_17',
        python_callable=process_17,
        dag=dag,
    )

    # Tool - Tool 22
    def process_22(context):
        import pandas as pd
        ti = context['ti']
        
        json_data = ti.xcom_pull(key='data', task_ids='read_data_17')
        df = pd.read_json(json_data)
        
        # Process data with Tool
        # TODO: Implement Tool logic
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_22 = PythonOperator(
        task_id='process_22',
        python_callable=process_22,
        dag=dag,
    )


# Define task dependencies
task_1 >> task_2
task_1 >> task_4
task_2 >> task_3
task_2 >> task_5
task_5 >> task_16
task_16 >> task_17
task_17 >> task_7
task_17 >> task_22
task_17 >> task_22


if __name__ == "__main__":
    dag.test()
