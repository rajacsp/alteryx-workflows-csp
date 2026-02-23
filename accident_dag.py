#!/usr/bin/env python3
"""
Airflow DAG generated from Alteryx workflow: Accident_Workflow
Source: Accident_Workflow.yxmd
Generated: 2026-02-23 18:32:36
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
    'accident_workflow',
    default_args=default_args,
    description='DAG generated from Alteryx workflow: Accident_Workflow',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['alteryx', 'generated'],
)

    # Read CSV file
    def read_1(context):
        import pandas as pd
        ti = context['ti']
        df = pd.read_csv('C:\Users\ash_s\Downloads\archive (8)\US_Accidents_March23.csv')
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_read_1 = PythonOperator(
        task_id='read_1',
        python_callable=read_1,
        dag=dag,
    )

    # Aggregate data
    def aggregate_data_2(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_1')
        df = pd.read_json(json_data)
        
        # Group by: []
        # Aggregations: []
pass
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_aggregate_data_2 = PythonOperator(
        task_id='aggregate_data_2',
        python_callable=aggregate_data_2,
        dag=dag,
    )

    # Browse/Print data
    def browse_3(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_2')
        df = pd.read_json(json_data)
        
        print(f"Browse 3 - Shape: {df.shape}")
        print(df.head())
        return df.shape[0]
    
    task_browse_3 = PythonOperator(
        task_id='browse_3',
        python_callable=browse_3,
        dag=dag,
    )

    # Browse/Print data
    def browse_4(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_1')
        df = pd.read_json(json_data)
        
        print(f"Browse 4 - Shape: {df.shape}")
        print(df.head())
        return df.shape[0]
    
    task_browse_4 = PythonOperator(
        task_id='browse_4',
        python_callable=browse_4,
        dag=dag,
    )

    # Filter data
    def filter_data_5(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_2')
        df = pd.read_json(json_data)
        
        # Apply filter: Timezone IsNotNull Serious
        try:
            df = df.query('Timezone IsNotNull Serious')
        except:
            # Fallback for complex expressions
            pass
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_filter_data_5 = PythonOperator(
        task_id='filter_data_5',
        python_callable=filter_data_5,
        dag=dag,
    )

    # Browse/Print data
    def browse_7(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_17')
        df = pd.read_json(json_data)
        
        print(f"Browse 7 - Shape: {df.shape}")
        print(df.head())
        return df.shape[0]
    
    task_browse_7 = PythonOperator(
        task_id='browse_7',
        python_callable=browse_7,
        dag=dag,
    )

    # Apply formulas
    def apply_formulas_16(context):
        import pandas as pd
        import re
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_5')
        df = pd.read_json(json_data)
        
        # Apply formulas

        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_apply_formulas_16 = PythonOperator(
        task_id='apply_formulas_16',
        python_callable=apply_formulas_16,
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
