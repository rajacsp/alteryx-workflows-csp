#!/usr/bin/env python3
"""
Airflow DAG generated from Alteryx workflow: Ted talk Workflow
Source: Ted talk Workflow.yxmd
Generated: 2026-02-23 18:37:17
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
    'ted_talk_workflow',
    default_args=default_args,
    description='DAG generated from Alteryx workflow: Ted talk Workflow',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['alteryx', 'generated'],
)

    # Read CSV file
    def read_2(context):
        import pandas as pd
        ti = context['ti']
        df = pd.read_csv('C:\Users\ash_s\Downloads\ted_main.csv')
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_read_2 = PythonOperator(
        task_id='read_2',
        python_callable=read_2,
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

    # Apply formulas
    def apply_formulas_4(context):
        import pandas as pd
        import re
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_2')
        df = pd.read_json(json_data)
        
        # Apply formulas

        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_apply_formulas_4 = PythonOperator(
        task_id='apply_formulas_4',
        python_callable=apply_formulas_4,
        dag=dag,
    )

    # Browse/Print data
    def browse_5(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_4')
        df = pd.read_json(json_data)
        
        print(f"Browse 5 - Shape: {df.shape}")
        print(df.head())
        return df.shape[0]
    
    task_browse_5 = PythonOperator(
        task_id='browse_5',
        python_callable=browse_5,
        dag=dag,
    )

    # Apply formulas
    def apply_formulas_6(context):
        import pandas as pd
        import re
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_4')
        df = pd.read_json(json_data)
        
        # Apply formulas

        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_apply_formulas_6 = PythonOperator(
        task_id='apply_formulas_6',
        python_callable=apply_formulas_6,
        dag=dag,
    )

    # Browse/Print data
    def browse_7(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_6')
        df = pd.read_json(json_data)
        
        print(f"Browse 7 - Shape: {df.shape}")
        print(df.head())
        return df.shape[0]
    
    task_browse_7 = PythonOperator(
        task_id='browse_7',
        python_callable=browse_7,
        dag=dag,
    )

    # Sort data
    def sort_data_8(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_6')
        df = pd.read_json(json_data)
        
        # Sort by: 
        if '':
            df = df.sort_values(by=[])
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_sort_data_8 = PythonOperator(
        task_id='sort_data_8',
        python_callable=sort_data_8,
        dag=dag,
    )

    # Sample data
    def sample_data_10(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_8')
        df = pd.read_json(json_data)
        
        # Sample: First 5 rows
        if 'First' == 'First':
            df = df.head(5)
        elif 'First' == 'Random':
            df = df.sample(n=min(5, len(df)), random_state=42)
        elif 'First' == 'Last':
            df = df.tail(5)
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_sample_data_10 = PythonOperator(
        task_id='sample_data_10',
        python_callable=sample_data_10,
        dag=dag,
    )

    # Sort data
    def sort_data_11(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_10')
        df = pd.read_json(json_data)
        
        # Sort by: 
        if '':
            df = df.sort_values(by=[])
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_sort_data_11 = PythonOperator(
        task_id='sort_data_11',
        python_callable=sort_data_11,
        dag=dag,
    )

    # Table composer
    def compose_table_12(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_11')
        df = pd.read_json(json_data)
        
        # Compose table with selected fields
        # This would format the data for display
        print(f"Table composed: {df.shape}")
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_compose_table_12 = PythonOperator(
        task_id='compose_table_12',
        python_callable=compose_table_12,
        dag=dag,
    )

    # Browse/Print data
    def browse_13(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_10')
        df = pd.read_json(json_data)
        
        print(f"Browse 13 - Shape: {df.shape}")
        print(df.head())
        return df.shape[0]
    
    task_browse_13 = PythonOperator(
        task_id='browse_13',
        python_callable=browse_13,
        dag=dag,
    )

    # Browse/Print data
    def browse_14(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_12')
        df = pd.read_json(json_data)
        
        print(f"Browse 14 - Shape: {df.shape}")
        print(df.head())
        return df.shape[0]
    
    task_browse_14 = PythonOperator(
        task_id='browse_14',
        python_callable=browse_14,
        dag=dag,
    )

    # Aggregate data
    def aggregate_data_15(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_6')
        df = pd.read_json(json_data)
        
        # Group by: []
        # Aggregations: []
pass
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_aggregate_data_15 = PythonOperator(
        task_id='aggregate_data_15',
        python_callable=aggregate_data_15,
        dag=dag,
    )

    # Sort data
    def sort_data_16(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_15')
        df = pd.read_json(json_data)
        
        # Sort by: 
        if '':
            df = df.sort_values(by=[])
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_sort_data_16 = PythonOperator(
        task_id='sort_data_16',
        python_callable=sort_data_16,
        dag=dag,
    )

    # Apply formulas
    def apply_formulas_17(context):
        import pandas as pd
        import re
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_16')
        df = pd.read_json(json_data)
        
        # Apply formulas

        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_apply_formulas_17 = PythonOperator(
        task_id='apply_formulas_17',
        python_callable=apply_formulas_17,
        dag=dag,
    )

    # Sample data
    def sample_data_20(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_18')
        df = pd.read_json(json_data)
        
        # Sample: First 5 rows
        if 'First' == 'First':
            df = df.head(5)
        elif 'First' == 'Random':
            df = df.sample(n=min(5, len(df)), random_state=42)
        elif 'First' == 'Last':
            df = df.tail(5)
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_sample_data_20 = PythonOperator(
        task_id='sample_data_20',
        python_callable=sample_data_20,
        dag=dag,
    )

    # Browse/Print data
    def browse_21(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_18')
        df = pd.read_json(json_data)
        
        print(f"Browse 21 - Shape: {df.shape}")
        print(df.head())
        return df.shape[0]
    
    task_browse_21 = PythonOperator(
        task_id='browse_21',
        python_callable=browse_21,
        dag=dag,
    )

    # Table composer
    def compose_table_22(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_20')
        df = pd.read_json(json_data)
        
        # Compose table with selected fields
        # This would format the data for display
        print(f"Table composed: {df.shape}")
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_compose_table_22 = PythonOperator(
        task_id='compose_table_22',
        python_callable=compose_table_22,
        dag=dag,
    )

    # Browse/Print data
    def browse_23(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_22')
        df = pd.read_json(json_data)
        
        print(f"Browse 23 - Shape: {df.shape}")
        print(df.head())
        return df.shape[0]
    
    task_browse_23 = PythonOperator(
        task_id='browse_23',
        python_callable=browse_23,
        dag=dag,
    )

    # Aggregate data
    def aggregate_data_24(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_6')
        df = pd.read_json(json_data)
        
        # Group by: []
        # Aggregations: []
pass
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_aggregate_data_24 = PythonOperator(
        task_id='aggregate_data_24',
        python_callable=aggregate_data_24,
        dag=dag,
    )

    # Sort data
    def sort_data_25(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_24')
        df = pd.read_json(json_data)
        
        # Sort by: 
        if '':
            df = df.sort_values(by=[])
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_sort_data_25 = PythonOperator(
        task_id='sort_data_25',
        python_callable=sort_data_25,
        dag=dag,
    )

    # Create chart
    def create_chart_26(context):
        import pandas as pd
        import matplotlib.pyplot as plt
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_25')
        df = pd.read_json(json_data)
        
        # Create chart based on configuration
        # This would implement the specific chart type
        print(f"Chart created: {df.shape}")
        
        return df.shape[0]
    
    task_create_chart_26 = PythonOperator(
        task_id='create_chart_26',
        python_callable=create_chart_26,
        dag=dag,
    )

    # Browse/Print data
    def browse_27(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_26')
        df = pd.read_json(json_data)
        
        print(f"Browse 27 - Shape: {df.shape}")
        print(df.head())
        return df.shape[0]
    
    task_browse_27 = PythonOperator(
        task_id='browse_27',
        python_callable=browse_27,
        dag=dag,
    )

    # Apply formulas
    def apply_formulas_28(context):
        import pandas as pd
        import re
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_6')
        df = pd.read_json(json_data)
        
        # Apply formulas
# field: 
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_apply_formulas_28 = PythonOperator(
        task_id='apply_formulas_28',
        python_callable=apply_formulas_28,
        dag=dag,
    )

    # Browse/Print data
    def browse_29(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_28')
        df = pd.read_json(json_data)
        
        print(f"Browse 29 - Shape: {df.shape}")
        print(df.head())
        return df.shape[0]
    
    task_browse_29 = PythonOperator(
        task_id='browse_29',
        python_callable=browse_29,
        dag=dag,
    )

    # Table composer
    def compose_table_30(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_28')
        df = pd.read_json(json_data)
        
        # Compose table with selected fields
        # This would format the data for display
        print(f"Table composed: {df.shape}")
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_compose_table_30 = PythonOperator(
        task_id='compose_table_30',
        python_callable=compose_table_30,
        dag=dag,
    )

    # Browse/Print data
    def browse_31(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_30')
        df = pd.read_json(json_data)
        
        print(f"Browse 31 - Shape: {df.shape}")
        print(df.head())
        return df.shape[0]
    
    task_browse_31 = PythonOperator(
        task_id='browse_31',
        python_callable=browse_31,
        dag=dag,
    )

    # Browse/Print data
    def browse_33(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_32')
        df = pd.read_json(json_data)
        
        print(f"Browse 33 - Shape: {df.shape}")
        print(df.head())
        return df.shape[0]
    
    task_browse_33 = PythonOperator(
        task_id='browse_33',
        python_callable=browse_33,
        dag=dag,
    )

    # Table composer
    def compose_table_34(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_32')
        df = pd.read_json(json_data)
        
        # Compose table with selected fields
        # This would format the data for display
        print(f"Table composed: {df.shape}")
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_compose_table_34 = PythonOperator(
        task_id='compose_table_34',
        python_callable=compose_table_34,
        dag=dag,
    )

    # Browse/Print data
    def browse_35(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_34')
        df = pd.read_json(json_data)
        
        print(f"Browse 35 - Shape: {df.shape}")
        print(df.head())
        return df.shape[0]
    
    task_browse_35 = PythonOperator(
        task_id='browse_35',
        python_callable=browse_35,
        dag=dag,
    )



# Define task dependencies
task_2 >> task_3
task_2 >> task_4
task_4 >> task_5
task_4 >> task_6
task_6 >> task_7
task_6 >> task_8
task_6 >> task_15
task_6 >> task_24
task_6 >> task_28
task_6 >> task_32
task_8 >> task_10
task_10 >> task_11
task_10 >> task_13
task_11 >> task_12
task_12 >> task_14
task_15 >> task_16
task_16 >> task_17
task_17 >> task_18
task_18 >> task_20
task_18 >> task_21
task_20 >> task_22
task_22 >> task_23
task_24 >> task_25
task_25 >> task_26
task_26 >> task_27
task_28 >> task_29
task_28 >> task_30
task_30 >> task_31
task_32 >> task_33
task_32 >> task_34
task_34 >> task_35

if __name__ == "__main__":
    dag.test()
