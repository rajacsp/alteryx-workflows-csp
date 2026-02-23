#!/usr/bin/env python3
"""
Alteryx yxmd to Airflow DAG Converter

Converts Alteryx workflow files (.yxmd) into Apache Airflow DAG Python files.
Supports various Alteryx tools and generates equivalent Airflow operators.
"""

import xml.etree.ElementTree as ET
import re
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path


class AlteryxNode:
    """Represents a node in the Alteryx workflow"""
    
    def __init__(self, node_elem: ET.Element):
        self.id = node_elem.get('ToolID')
        self.plugin = node_elem.find('.//GuiSettings').get('Plugin', '') if node_elem.find('.//GuiSettings') is not None else ''
        self.position = self._get_position(node_elem)
        self.properties = self._parse_properties(node_elem)
        self.engine_settings = self._parse_engine_settings(node_elem)
        self.connections = []
        
    def _get_position(self, node_elem: ET.Element) -> Tuple[int, int]:
        gui = node_elem.find('.//GuiSettings')
        if gui is not None:
            pos = gui.find('.//Position')
            if pos is not None:
                return (int(pos.get('x', 0)), int(pos.get('y', 0)))
        return (0, 0)
    
    def _parse_properties(self, node_elem: ET.Element) -> Dict:
        props = {}
        properties = node_elem.find('.//Properties')
        if properties is not None:
            config = properties.find('.//Configuration')
            if config is not None:
                props['Configuration'] = self._element_to_dict(config)
            props['Annotation'] = self._get_annotation(properties)
        return props
    
    def _get_annotation(self, properties: ET.Element) -> Dict:
        annotation = properties.find('.//Annotation')
        if annotation is not None:
            return {
                'Name': annotation.find('.//Name').text if annotation.find('.//Name') is not None else '',
                'DefaultAnnotationText': annotation.find('.//DefaultAnnotationText').text if annotation.find('.//DefaultAnnotationText') is not None else ''
            }
        return {}
    
    def _element_to_dict(self, element: ET.Element) -> Dict:
        """Convert XML element to dictionary"""
        result = {}
        for child in element:
            if len(child) > 0:
                result[child.tag] = self._element_to_dict(child)
            else:
                result[child.tag] = child.text
        return result
    
    def _parse_engine_settings(self, node_elem: ET.Element) -> Dict:
        engine = node_elem.find('.//EngineSettings')
        if engine is not None:
            return {
                'EngineDll': engine.get('EngineDll', ''),
                'EngineDllEntryPoint': engine.get('EngineDllEntryPoint', ''),
                'Macro': engine.get('Macro', '')
            }
        return {}


class AlteryxConnection:
    """Represents a connection between nodes"""
    
    def __init__(self, conn_elem: ET.Element):
        self.origin_tool_id = conn_elem.find('.//Origin').get('ToolID')
        self.origin_connection = conn_elem.find('.//Origin').get('Connection', 'Output')
        self.destination_tool_id = conn_elem.find('.//Destination').get('ToolID')
        self.destination_connection = conn_elem.find('.//Destination').get('Connection', 'Input')
        self.name = conn_elem.get('name', '')


class AlteryxWorkflow:
    """Represents an Alteryx workflow"""
    
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.tree = ET.parse(filepath)
        self.root = self.tree.getroot()
        self.nodes: Dict[str, AlteryxNode] = {}
        self.connections: List[AlteryxConnection] = []
        self.workflow_name = self._get_workflow_name()
        
    def _get_workflow_name(self) -> str:
        meta = self.root.find('.//MetaInfo')
        if meta is not None:
            name = meta.find('.//Name')
            if name is not None and name.text:
                return name.text
        return Path(self.filepath).stem
    
    def parse(self):
        """Parse the workflow file"""
        nodes_elem = self.root.find('.//Nodes')
        if nodes_elem is not None:
            for node_elem in nodes_elem.findall('.//Node'):
                node = AlteryxNode(node_elem)
                self.nodes[node.id] = node
        
        connections_elem = self.root.find('.//Connections')
        if connections_elem is not None:
            for conn_elem in connections_elem.findall('.//Connection'):
                self.connections.append(AlteryxConnection(conn_elem))
        
        # Link connections to nodes
        for conn in self.connections:
            if conn.origin_tool_id in self.nodes:
                self.nodes[conn.origin_tool_id].connections.append(conn)


class AlteryxToAirflowConverter:
    """Converts Alteryx workflows to Airflow DAGs"""
    
    # Tool mappings: Plugin -> Airflow operator
    TOOL_MAPPINGS = {
        'AlteryxBasePluginsGui.DbFileInput.DbFileInput': 'FileToTableOperator',
        'AlteryxBasePluginsGui.DbFileOutput.DbFileOutput': 'TableToFileOperator',
        'AlteryxBasePluginsGui.Formula.Formula': 'PythonOperator',
        'AlteryxBasePluginsGui.Filter.Filter': 'FilterOperator',
        'AlteryxBasePluginsGui.Sort.Sort': 'SortOperator',
        'AlteryxBasePluginsGui.Sample.Sample': 'SampleOperator',
        'AlteryxBasePluginsGui.BrowseV2.BrowseV2': 'PrintOperator',
        'AlteryxSpatialPluginsGui.Summarize.Summarize': 'AggregateOperator',
        'AlteryxBasePluginsGui.MultiRowFormula.MultiRowFormula': 'MultiRowOperator',
        'PortfolioPluginsGui.ComposerTable.PortfolioComposerTable': 'TableComposerOperator',
        'PlotlyCharting': 'ChartOperator',
        'AlteryxBasePluginsGui.MacroInput.MacroInput': 'MacroInputOperator',
        'AlteryxBasePluginsGui.MacroOutput.MacroOutput': 'MacroOutputOperator',
    }
    
    def __init__(self, workflow: AlteryxWorkflow):
        self.workflow = workflow
        self.dag_name = self._sanitize_name(self.workflow.workflow_name)
        self.task_definitions = []
        self.task_dependencies = []
        
    def _sanitize_name(self, name: str) -> str:
        """Convert name to valid Python identifier"""
        return re.sub(r'[^a-zA-Z0-9_]', '_', name).lower()
    
    def convert(self) -> str:
        """Convert workflow to Airflow DAG code"""
        self._generate_task_definitions()
        self._generate_task_dependencies()
        
        dag_code = self._generate_dag_header()
        dag_code += self._generate_task_code()
        dag_code += self._generate_dag_footer()
        
        return dag_code
    
    def _generate_task_definitions(self):
        """Generate task definitions for each node"""
        for node_id, node in self.workflow.nodes.items():
            task = self._create_task(node)
            if task:
                self.task_definitions.append(task)
    
    def _create_task(self, node: AlteryxNode) -> Optional[Dict]:
        """Create task definition from node"""
        task_type = self._get_task_type(node)
        
        if task_type == 'file_input':
            return self._create_file_input_task(node)
        elif task_type == 'file_output':
            return self._create_file_output_task(node)
        elif task_type == 'formula':
            return self._create_formula_task(node)
        elif task_type == 'filter':
            return self._create_filter_task(node)
        elif task_type == 'sort':
            return self._create_sort_task(node)
        elif task_type == 'sample':
            return self._create_sample_task(node)
        elif task_type == 'summarize':
            return self._create_summarize_task(node)
        elif task_type == 'macro_input':
            return self._create_macro_input_task(node)
        elif task_type == 'macro_output':
            return self._create_macro_output_task(node)
        elif task_type == 'browse':
            return self._create_browse_task(node)
        elif task_type == 'multi_row':
            return self._create_multi_row_task(node)
        elif task_type == 'table_composer':
            return self._create_table_composer_task(node)
        elif task_type == 'chart':
            return self._create_chart_task(node)
        
        return None
    
    def _get_task_type(self, node: AlteryxNode) -> str:
        """Determine task type from node plugin"""
        plugin = node.plugin
        
        if 'DbFileInput' in plugin:
            return 'file_input'
        elif 'DbFileOutput' in plugin:
            return 'file_output'
        elif 'Formula' in plugin:
            return 'formula'
        elif 'Filter' in plugin:
            return 'filter'
        elif 'Sort' in plugin:
            return 'sort'
        elif 'Sample' in plugin:
            return 'sample'
        elif 'Summarize' in plugin:
            return 'summarize'
        elif 'MacroInput' in plugin:
            return 'macro_input'
        elif 'MacroOutput' in plugin:
            return 'macro_output'
        elif 'Browse' in plugin:
            return 'browse'
        elif 'MultiRowFormula' in plugin:
            return 'multi_row'
        elif 'ComposerTable' in plugin:
            return 'table_composer'
        elif 'PlotlyCharting' in plugin:
            return 'chart'
        
        return 'python_operator'
    
    def _create_file_input_task(self, node: AlteryxNode) -> Dict:
        """Create file input task"""
        config = node.properties.get('Configuration', {})
        file_path = config.get('File', '')
        
        # Extract file extension
        ext = Path(file_path).suffix.lower()
        
        if ext in ['.csv']:
            operator = 'PythonOperator'
            task_code = f'''    # Read CSV file
    def read_{self._sanitize_name(node.id)}(context):
        import pandas as pd
        ti = context['ti']
        df = pd.read_csv('{file_path}')
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_read_{self._sanitize_name(node.id)} = PythonOperator(
        task_id='read_{self._sanitize_name(node.id)}',
        python_callable=read_{self._sanitize_name(node.id)},
        dag=dag,
    )
'''
        elif ext in ['.xlsx', '.xls']:
            operator = 'PythonOperator'
            task_code = f'''    # Read Excel file
    def read_{self._sanitize_name(node.id)}(context):
        import pandas as pd
        ti = context['ti']
        df = pd.read_excel('{file_path}')
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_read_{self._sanitize_name(node.id)} = PythonOperator(
        task_id='read_{self._sanitize_name(node.id)}',
        python_callable=read_{self._sanitize_name(node.id)},
        dag=dag,
    )
'''
        else:
            operator = 'PythonOperator'
            task_code = f'''    # Read file
    def read_{self._sanitize_name(node.id)}(context):
        import pandas as pd
        ti = context['ti']
        df = pd.read_csv('{file_path}')
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_read_{self._sanitize_name(node.id)} = PythonOperator(
        task_id='read_{self._sanitize_name(node.id)}',
        python_callable=read_{self._sanitize_name(node.id)},
        dag=dag,
    )
'''
        
        return {
            'id': node.id,
            'name': f'read_{self._sanitize_name(node.id)}',
            'type': 'file_input',
            'code': task_code,
            'operator': 'PythonOperator'
        }
    
    def _create_file_output_task(self, node: AlteryxNode) -> Dict:
        """Create file output task"""
        config = node.properties.get('Configuration', {})
        file_path = config.get('File', '')
        
        task_code = f'''    # Write to file
    def write_{self._sanitize_name(node.id)}(context):
        import pandas as pd
        import os
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_{self._get_input_task(node.id)}')
        df = pd.read_json(json_data)
        
        # Ensure directory exists
        os.makedirs(os.path.dirname('{file_path}'), exist_ok=True)
        
        # Write based on extension
        ext = Path('{file_path}').suffix.lower()
        if ext in ['.csv']:
            df.to_csv('{file_path}', index=False)
        elif ext in ['.xlsx', '.xls']:
            df.to_excel('{file_path}', index=False)
        else:
            df.to_csv('{file_path}', index=False)
        
        return df.shape[0]
    
    task_write_{self._sanitize_name(node.id)} = PythonOperator(
        task_id='write_{self._sanitize_name(node.id)}',
        python_callable=write_{self._sanitize_name(node.id)},
        dag=dag,
    )
'''
        
        return {
            'id': node.id,
            'name': f'write_{self._sanitize_name(node.id)}',
            'type': 'file_output',
            'code': task_code,
            'operator': 'PythonOperator'
        }
    
    def _create_formula_task(self, node: AlteryxNode) -> Dict:
        """Create formula task"""
        config = node.properties.get('Configuration', {})
        formula_fields = config.get('FormulaFields', {})
        
        # Extract formula expressions
        formulas = []
        if isinstance(formula_fields, dict):
            formula_field = formula_fields.get('FormulaField', {})
            if isinstance(formula_field, dict):
                formulas.append(formula_field)
            elif isinstance(formula_field, list):
                formulas = formula_field
        
        formula_code = '\n        '.join([
            f"# {f.get('field', 'field')}: {f.get('expression', '')}"
            for f in formulas
        ])
        
        task_code = f'''    # Apply formulas
    def apply_formulas_{self._sanitize_name(node.id)}(context):
        import pandas as pd
        import re
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_{self._get_input_task(node.id)}')
        df = pd.read_json(json_data)
        
        # Apply formulas
{formula_code}
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_apply_formulas_{self._sanitize_name(node.id)} = PythonOperator(
        task_id='apply_formulas_{self._sanitize_name(node.id)}',
        python_callable=apply_formulas_{self._sanitize_name(node.id)},
        dag=dag,
    )
'''
        
        return {
            'id': node.id,
            'name': f'apply_formulas_{self._sanitize_name(node.id)}',
            'type': 'formula',
            'code': task_code,
            'operator': 'PythonOperator'
        }
    
    def _create_filter_task(self, node: AlteryxNode) -> Dict:
        """Create filter task"""
        config = node.properties.get('Configuration', {})
        mode = config.get('Mode', 'Simple')
        
        if mode == 'Custom':
            expression = config.get('Expression', '')
        else:
            simple = config.get('Simple', {})
            operator = simple.get('Operator', '')
            field = simple.get('Field', '')
            operand = simple.get('Operands', {}).get('Operand', '')
            expression = f"{field} {operator} {operand}"
        
        # Clean up expression
        expression = expression.replace('&gt;', '>').replace('&lt;', '<')
        
        task_code = f'''    # Filter data
    def filter_data_{self._sanitize_name(node.id)}(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_{self._get_input_task(node.id)}')
        df = pd.read_json(json_data)
        
        # Apply filter: {expression}
        try:
            df = df.query('{expression.replace('"', '\\"')}')
        except:
            # Fallback for complex expressions
            pass
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_filter_data_{self._sanitize_name(node.id)} = PythonOperator(
        task_id='filter_data_{self._sanitize_name(node.id)}',
        python_callable=filter_data_{self._sanitize_name(node.id)},
        dag=dag,
    )
'''
        
        return {
            'id': node.id,
            'name': f'filter_data_{self._sanitize_name(node.id)}',
            'type': 'filter',
            'code': task_code,
            'operator': 'PythonOperator'
        }
    
    def _create_sort_task(self, node: AlteryxNode) -> Dict:
        """Create sort task"""
        config = node.properties.get('Configuration', {})
        sort_info = config.get('SortInfo', {})
        field_info = sort_info.get('Field', {})
        
        if isinstance(field_info, dict):
            fields = [field_info]
        elif isinstance(field_info, list):
            fields = field_info
        else:
            fields = []
        
        sort_fields = ', '.join([
            f"'{f.get('field', '')}': {f.get('order', 'ascending').lower()}"
            for f in fields
        ])
        
        task_code = f'''    # Sort data
    def sort_data_{self._sanitize_name(node.id)}(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_{self._get_input_task(node.id)}')
        df = pd.read_json(json_data)
        
        # Sort by: {sort_fields}
        if '{sort_fields}':
            df = df.sort_values(by=[{sort_fields}])
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_sort_data_{self._sanitize_name(node.id)} = PythonOperator(
        task_id='sort_data_{self._sanitize_name(node.id)}',
        python_callable=sort_data_{self._sanitize_name(node.id)},
        dag=dag,
    )
'''
        
        return {
            'id': node.id,
            'name': f'sort_data_{self._sanitize_name(node.id)}',
            'type': 'sort',
            'code': task_code,
            'operator': 'PythonOperator'
        }
    
    def _create_sample_task(self, node: AlteryxNode) -> Dict:
        """Create sample task"""
        config = node.properties.get('Configuration', {})
        mode = config.get('Mode', 'First')
        n = config.get('N', 10)
        group_fields = config.get('GroupFields', {})
        
        task_code = f'''    # Sample data
    def sample_data_{self._sanitize_name(node.id)}(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_{self._get_input_task(node.id)}')
        df = pd.read_json(json_data)
        
        # Sample: {mode} {n} rows
        if '{mode}' == 'First':
            df = df.head({n})
        elif '{mode}' == 'Random':
            df = df.sample(n=min({n}, len(df)), random_state=42)
        elif '{mode}' == 'Last':
            df = df.tail({n})
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_sample_data_{self._sanitize_name(node.id)} = PythonOperator(
        task_id='sample_data_{self._sanitize_name(node.id)}',
        python_callable=sample_data_{self._sanitize_name(node.id)},
        dag=dag,
    )
'''
        
        return {
            'id': node.id,
            'name': f'sample_data_{self._sanitize_name(node.id)}',
            'type': 'sample',
            'code': task_code,
            'operator': 'PythonOperator'
        }
    
    def _create_summarize_task(self, node: AlteryxNode) -> Dict:
        """Create summarize/aggregate task"""
        config = node.properties.get('Configuration', {})
        summarize_fields = config.get('SummarizeFields', {})
        
        # Parse summarize fields
        group_by = []
        aggregations = []
        
        if isinstance(summarize_fields, dict):
            fields = [summarize_fields.get('SummarizeField', {})]
        elif isinstance(summarize_fields, list):
            fields = summarize_fields.get('SummarizeField', [])
        else:
            fields = []
        
        if not isinstance(fields, list):
            fields = [fields] if fields else []
        
        for field in fields:
            if isinstance(field, dict):
                field_name = field.get('field', '')
                action = field.get('action', '')
                rename = field.get('rename', '')
                
                if action == 'GroupBy':
                    group_by.append(field_name)
                else:
                    agg_func = self._map_alteryx_agg(action)
                    if agg_func and field_name:
                        aggregations.append((field_name, agg_func, rename or f'{field_name}_{agg_func}'))
        
        # Build aggregation dictionary string
        if aggregations:
            agg_dict = ', '.join([f"'{a[0]}': '{a[1]}'" for a in aggregations])
        else:
            agg_dict = ''
        
        if group_by:
            agg_code = f"df = df.groupby({group_by}).agg({{{agg_dict}}}).reset_index()"
        else:
            agg_code = f"df = df.agg({{{agg_dict}}})" if agg_dict else 'pass'
        
        task_code = f'''    # Aggregate data
    def aggregate_data_{self._sanitize_name(node.id)}(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_{self._get_input_task(node.id)}')
        df = pd.read_json(json_data)
        
        # Group by: {group_by}
        # Aggregations: {aggregations}
{agg_code}
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_aggregate_data_{self._sanitize_name(node.id)} = PythonOperator(
        task_id='aggregate_data_{self._sanitize_name(node.id)}',
        python_callable=aggregate_data_{self._sanitize_name(node.id)},
        dag=dag,
    )
'''
        
        return {
            'id': node.id,
            'name': f'aggregate_data_{self._sanitize_name(node.id)}',
            'type': 'summarize',
            'code': task_code,
            'operator': 'PythonOperator'
        }
    
    def _map_alteryx_agg(self, action: str) -> str:
        """Map Alteryx aggregation to pandas"""
        mapping = {
            'Count': 'count',
            'CountDistinct': 'nunique',
            'Sum': 'sum',
            'Average': 'mean',
            'Min': 'min',
            'Max': 'max',
            'StdDev': 'std',
            'Variance': 'var',
            'First': 'first',
            'Last': 'last',
        }
        return mapping.get(action, 'count')
    
    def _create_macro_input_task(self, node: AlteryxNode) -> Dict:
        """Create macro input task"""
        config = node.properties.get('Configuration', {})
        name = config.get('Name', 'Input')
        
        task_code = f'''    # Macro input: {name}
    def macro_input_{self._sanitize_name(node.id)}(context):
        import pandas as pd
        ti = context['ti']
        
        # Get input from macro parameter or use default
        # This would be configured via macro parameters
        df = pd.DataFrame({{'Zone': ['Eastern', 'Central', 'Mountain', 'Pacific']}})
        
        ti.xcom_push(key='data', value=df.to_json())
        return len(df)
    
    task_macro_input_{self._sanitize_name(node.id)} = PythonOperator(
        task_id='macro_input_{self._sanitize_name(node.id)}',
        python_callable=macro_input_{self._sanitize_name(node.id)},
        dag=dag,
    )
'''
        
        return {
            'id': node.id,
            'name': f'macro_input_{self._sanitize_name(node.id)}',
            'type': 'macro_input',
            'code': task_code,
            'operator': 'PythonOperator'
        }
    
    def _create_macro_output_task(self, node: AlteryxNode) -> Dict:
        """Create macro output task"""
        config = node.properties.get('Configuration', {})
        name = config.get('Name', 'Output')
        
        task_code = f'''    # Macro output: {name}
    def macro_output_{self._sanitize_name(node.id)}(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_{self._get_input_task(node.id)}')
        df = pd.read_json(json_data)
        
        # Output data for macro
        return df.shape[0]
    
    task_macro_output_{self._sanitize_name(node.id)} = PythonOperator(
        task_id='macro_output_{self._sanitize_name(node.id)}',
        python_callable=macro_output_{self._sanitize_name(node.id)},
        dag=dag,
    )
'''
        
        return {
            'id': node.id,
            'name': f'macro_output_{self._sanitize_name(node.id)}',
            'type': 'macro_output',
            'code': task_code,
            'operator': 'PythonOperator'
        }
    
    def _create_browse_task(self, node: AlteryxNode) -> Dict:
        """Create browse/print task"""
        task_code = f'''    # Browse/Print data
    def browse_{self._sanitize_name(node.id)}(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_{self._get_input_task(node.id)}')
        df = pd.read_json(json_data)
        
        print(f"Browse {self._sanitize_name(node.id)} - Shape: {{df.shape}}")
        print(df.head())
        return df.shape[0]
    
    task_browse_{self._sanitize_name(node.id)} = PythonOperator(
        task_id='browse_{self._sanitize_name(node.id)}',
        python_callable=browse_{self._sanitize_name(node.id)},
        dag=dag,
    )
'''
        
        return {
            'id': node.id,
            'name': f'browse_{self._sanitize_name(node.id)}',
            'type': 'browse',
            'code': task_code,
            'operator': 'PythonOperator'
        }
    
    def _create_multi_row_task(self, node: AlteryxNode) -> Dict:
        """Create multi-row formula task"""
        config = node.properties.get('Configuration', {})
        expression = config.get('Expression', '')
        create_field = config.get('CreateField_Name', '')
        
        task_code = f'''    # Multi-row formula
    def multi_row_{self._sanitize_name(node.id)}(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_{self._get_input_task(node.id)}')
        df = pd.read_json(json_data)
        
        # Apply multi-row formula: {expression}
        # This would implement the specific multi-row logic
        df['{create_field}'] = 0  # Placeholder
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_multi_row_{self._sanitize_name(node.id)} = PythonOperator(
        task_id='multi_row_{self._sanitize_name(node.id)}',
        python_callable=multi_row_{self._sanitize_name(node.id)},
        dag=dag,
    )
'''
        
        return {
            'id': node.id,
            'name': f'multi_row_{self._sanitize_name(node.id)}',
            'type': 'multi_row',
            'code': task_code,
            'operator': 'PythonOperator'
        }
    
    def _create_table_composer_task(self, node: AlteryxNode) -> Dict:
        """Create table composer task"""
        config = node.properties.get('Configuration', {})
        table_fields = config.get('TableFields', {})
        
        task_code = f'''    # Table composer
    def compose_table_{self._sanitize_name(node.id)}(context):
        import pandas as pd
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_{self._get_input_task(node.id)}')
        df = pd.read_json(json_data)
        
        # Compose table with selected fields
        # This would format the data for display
        print(f"Table composed: {{df.shape}}")
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_compose_table_{self._sanitize_name(node.id)} = PythonOperator(
        task_id='compose_table_{self._sanitize_name(node.id)}',
        python_callable=compose_table_{self._sanitize_name(node.id)},
        dag=dag,
    )
'''
        
        return {
            'id': node.id,
            'name': f'compose_table_{self._sanitize_name(node.id)}',
            'type': 'table_composer',
            'code': task_code,
            'operator': 'PythonOperator'
        }
    
    def _create_chart_task(self, node: AlteryxNode) -> Dict:
        """Create chart/plotting task"""
        config = node.properties.get('Configuration', {})
        charting_fields = config.get('ChartingFields', {})
        
        task_code = f'''    # Create chart
    def create_chart_{self._sanitize_name(node.id)}(context):
        import pandas as pd
        import matplotlib.pyplot as plt
        ti = context['ti']
        json_data = ti.xcom_pull(key='data', task_ids='read_{self._get_input_task(node.id)}')
        df = pd.read_json(json_data)
        
        # Create chart based on configuration
        # This would implement the specific chart type
        print(f"Chart created: {{df.shape}}")
        
        return df.shape[0]
    
    task_create_chart_{self._sanitize_name(node.id)} = PythonOperator(
        task_id='create_chart_{self._sanitize_name(node.id)}',
        python_callable=create_chart_{self._sanitize_name(node.id)},
        dag=dag,
    )
'''
        
        return {
            'id': node.id,
            'name': f'create_chart_{self._sanitize_name(node.id)}',
            'type': 'chart',
            'code': task_code,
            'operator': 'PythonOperator'
        }
    
    def _get_input_task(self, node_id: str) -> str:
        """Get the input task ID for a node"""
        for conn in self.workflow.connections:
            if conn.destination_tool_id == node_id:
                return conn.origin_tool_id
        return 'start'
    
    def _generate_task_dependencies(self):
        """Generate task dependency code"""
        for conn in self.workflow.connections:
            origin = conn.origin_tool_id
            dest = conn.destination_tool_id
            
            if origin in self.workflow.nodes and dest in self.workflow.nodes:
                origin_task = f"task_{self._sanitize_name(origin)}"
                dest_task = f"task_{self._sanitize_name(dest)}"
                self.task_dependencies.append(f"{origin_task} >> {dest_task}")
    
    def _generate_dag_header(self) -> str:
        """Generate DAG header code"""
        return f'''#!/usr/bin/env python3
"""
Airflow DAG generated from Alteryx workflow: {self.workflow.workflow_name}
Source: {self.workflow.filepath}
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os

default_args = {{
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

dag = DAG(
    '{self.dag_name}',
    default_args=default_args,
    description='DAG generated from Alteryx workflow: {self.workflow.workflow_name}',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['alteryx', 'generated'],
)

'''
    
    def _generate_task_code(self) -> str:
        """Generate task definition code"""
        code = ''
        for task in self.task_definitions:
            code += task['code'] + '\n'
        return code
    
    def _generate_dag_footer(self) -> str:
        """Generate DAG footer with task dependencies"""
        deps_code = '\n'.join(self.task_dependencies) if self.task_dependencies else '# No dependencies defined'
        
        return f'''

# Define task dependencies
{deps_code}

if __name__ == "__main__":
    dag.test()
'''

    def save_to_file(self, output_path: str):
        """Save the generated DAG to a file"""
        dag_code = self.convert()
        with open(output_path, 'w') as f:
            f.write(dag_code)
        return output_path


def convert_yxmd_to_airflow(yxmd_path: str, output_path: Optional[str] = None) -> str:
    """
    Convert an Alteryx yxmd file to an Airflow DAG
    
    Args:
        yxmd_path: Path to the Alteryx workflow file
        output_path: Optional output path for the generated DAG
        
    Returns:
        Path to the generated DAG file
    """
    # Parse the workflow
    workflow = AlteryxWorkflow(yxmd_path)
    workflow.parse()
    
    # Convert to Airflow
    converter = AlteryxToAirflowConverter(workflow)
    
    # Generate output path if not provided
    if output_path is None:
        output_path = Path(yxmd_path).with_suffix('.py').as_posix()
    
    # Save to file
    converter.save_to_file(output_path)
    
    return output_path


def convert_all_yxmd_in_directory(directory: str, output_dir: Optional[str] = None) -> List[str]:
    """
    Convert all yxmd files in a directory to Airflow DAGs
    
    Args:
        directory: Directory containing yxmd files
        output_dir: Optional output directory for generated DAGs
        
    Returns:
        List of paths to generated DAG files
    """
    output_paths = []
    directory_path = Path(directory)
    
    for yxmd_file in directory_path.glob('*.yxmd'):
        output_path = None
        if output_dir:
            output_path = Path(output_dir) / (yxmd_file.stem + '.py')
        
        converted_path = convert_yxmd_to_airflow(str(yxmd_file), str(output_path))
        output_paths.append(converted_path)
    
    return output_paths


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python alterxy2airflow.py <yxmd_file> [output_file]")
        print("       python alterxy2airflow.py --all <directory> [output_directory]")
        sys.exit(1)
    
    if sys.argv[1] == '--all':
        if len(sys.argv) < 3:
            print("Error: --all requires a directory argument")
            sys.exit(1)
        
        input_dir = sys.argv[2]
        output_dir = sys.argv[3] if len(sys.argv) > 3 else None
        
        converted = convert_all_yxmd_in_directory(input_dir, output_dir)
        print(f"Converted {len(converted)} workflow(s) to Airflow DAG(s):")
        for path in converted:
            print(f"  - {path}")
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        
        converted = convert_yxmd_to_airflow(input_file, output_file)
        print(f"Converted {input_file} to {converted}")
