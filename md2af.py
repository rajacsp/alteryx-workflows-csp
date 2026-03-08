#!/usr/bin/env python3
"""
Markdown to Airflow DAG Converter

Converts Markdown documentation (generated from Alteryx workflows) 
into Apache Airflow DAG Python files by parsing Mermaid diagrams 
and tool details.
"""

import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path


class MarkdownWorkflow:
    """Represents a workflow parsed from Markdown"""
    
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.workflow_name = ''
        self.description = ''
        self.nodes: Dict[str, Dict] = {}
        self.connections: List[Tuple[str, str, str]] = []
        self.metadata = {}
        
    def parse(self):
        """Parse the markdown file"""
        with open(self.filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract workflow name
        name_match = re.search(r'^#\s+(.+)$', content, re.MULTILINE)
        if name_match:
            self.workflow_name = name_match.group(1).strip()
        else:
            self.workflow_name = Path(self.filepath).stem
        
        # Extract description from Overview section
        overview_match = re.search(r'##\s+Overview\s*\n\n(.+?)(?=\n##|\Z)', content, re.DOTALL)
        if overview_match:
            desc_text = overview_match.group(1).strip()
            # Get first paragraph
            first_para = desc_text.split('\n\n')[0]
            if not first_para.startswith('###') and not first_para.startswith('-'):
                self.description = first_para.strip()
        
        # Parse Mermaid diagram
        self._parse_mermaid_diagram(content)
        
        # Parse tool details
        self._parse_tool_details(content)
    
    def _parse_mermaid_diagram(self, content: str):
        """Parse Mermaid flowchart to extract nodes and connections"""
        mermaid_match = re.search(r'```mermaid\s*\n(.*?)\n```', content, re.DOTALL)
        if not mermaid_match:
            return
        
        mermaid_content = mermaid_match.group(1)
        
        # Parse nodes
        # Pattern: node_id[shape]"label"[shape] or node_id["label"]
        node_pattern = r'(\d+)[\[\{\(]+[/\\]*"([^"]+)"[/\\]*[\]\}\)]+'
        for match in re.finditer(node_pattern, mermaid_content):
            node_id = match.group(1)
            label = match.group(2)
            
            # Extract tool name from label (format: "label<br/><small>(ToolName)</small>")
            tool_match = re.search(r'\(([^)]+)\)', label)
            tool_name = tool_match.group(1) if tool_match else 'Tool'
            
            # Clean label
            clean_label = re.sub(r'<br/>.*', '', label).strip()
            
            self.nodes[node_id] = {
                'id': node_id,
                'tool_name': tool_name,
                'label': clean_label,
                'config': {}
            }
        
        # Parse connections
        # Pattern: node1 --> node2 or node1 -->|label| node2
        conn_pattern = r'(\d+)\s+(?:-->|-.->)\s*(?:\|([^|]+)\|)?\s*(\d+)'
        for match in re.finditer(conn_pattern, mermaid_content):
            origin = match.group(1)
            label = match.group(2) if match.group(2) else ''
            dest = match.group(3)
            
            self.connections.append((origin, dest, label))
    
    def _parse_tool_details(self, content: str):
        """Parse tool details section to extract configurations"""
        # Find all tool detail sections
        tool_pattern = r'###\s+Tool\s+(\d+):\s+([^\n]+)\n(.*?)(?=\n###|\n##|\Z)'
        
        for match in re.finditer(tool_pattern, content, re.DOTALL):
            tool_id = match.group(1)
            tool_name = match.group(2).strip()
            tool_content = match.group(3)
            
            if tool_id not in self.nodes:
                self.nodes[tool_id] = {
                    'id': tool_id,
                    'tool_name': tool_name,
                    'label': tool_name,
                    'config': {}
                }
            
            # Update tool name
            self.nodes[tool_id]['tool_name'] = tool_name
            
            # Parse configuration based on tool type
            config = {}
            
            # Extract description
            desc_match = re.search(r'\*\*Description:\*\*\s+(.+?)(?=\n\n|\*\*)', tool_content, re.DOTALL)
            if desc_match:
                config['description'] = desc_match.group(1).strip()
            
            # Parse Input Data configuration
            if 'Input Data' in tool_name:
                file_match = re.search(r'\*\*File:\*\*\s+`([^`]+)`', tool_content)
                if file_match:
                    config['file'] = file_match.group(1)
                
                delim_match = re.search(r'\*\*Delimiter:\*\*\s+`([^`]+)`', tool_content)
                if delim_match:
                    config['delimiter'] = delim_match.group(1)
            
            # Parse Output Data configuration
            elif 'Output Data' in tool_name:
                file_match = re.search(r'\*\*File:\*\*\s+`([^`]+)`', tool_content)
                if file_match:
                    config['file'] = file_match.group(1)
                
                multi_match = re.search(r'\*\*Multi-File Output:\*\*\s+(\w+)', tool_content)
                if multi_match:
                    config['multi_file'] = multi_match.group(1)
                
                group_match = re.search(r'\*\*Group By Field:\*\*\s+`([^`]+)`', tool_content)
                if group_match:
                    config['group_field'] = group_match.group(1)
            
            # Parse Formula configuration
            elif 'Formula' in tool_name:
                formulas = []
                formula_pattern = r'-\s+\*\*([^*]+)\*\*\s+\([^)]+\):\s+`([^`]+)`'
                for formula_match in re.finditer(formula_pattern, tool_content):
                    field = formula_match.group(1).strip()
                    expression = formula_match.group(2).strip()
                    formulas.append({'field': field, 'expression': expression})
                config['formulas'] = formulas
            
            # Parse Filter configuration
            elif 'Filter' in tool_name:
                # Check for custom expression
                expr_match = re.search(r'```\s*\n(.+?)\n```', tool_content, re.DOTALL)
                if expr_match:
                    config['expression'] = expr_match.group(1).strip()
                else:
                    # Parse simple filter
                    field_match = re.search(r'\*\*Field:\*\*\s+`([^`]+)`', tool_content)
                    op_match = re.search(r'\*\*Operator:\*\*\s+(\w+)', tool_content)
                    val_match = re.search(r'\*\*Value:\*\*\s+`([^`]+)`', tool_content)
                    
                    if field_match:
                        config['field'] = field_match.group(1)
                    if op_match:
                        config['operator'] = op_match.group(1)
                    if val_match:
                        config['value'] = val_match.group(1)
            
            # Parse Sort configuration
            elif 'Sort' in tool_name:
                sort_fields = []
                sort_pattern = r'-\s+`([^`]+)`\s+-\s+(\w+)'
                for sort_match in re.finditer(sort_pattern, tool_content):
                    field = sort_match.group(1)
                    order = sort_match.group(2)
                    sort_fields.append({'field': field, 'order': order})
                config['sort_fields'] = sort_fields
            
            # Parse Sample configuration
            elif 'Sample' in tool_name:
                mode_match = re.search(r'\*\*Mode:\*\*\s+(\w+)', tool_content)
                n_match = re.search(r'\*\*Number of Records:\*\*\s+(\d+)', tool_content)
                
                if mode_match:
                    config['mode'] = mode_match.group(1)
                if n_match:
                    config['n'] = int(n_match.group(1))
            
            # Parse Summarize configuration
            elif 'Summarize' in tool_name:
                group_match = re.search(r'\*\*Group By:\*\*\s+(.+?)(?=\n\n)', tool_content, re.DOTALL)
                if group_match:
                    group_by = [g.strip('`').strip() for g in group_match.group(1).split(',')]
                    config['group_by'] = group_by
                
                aggregations = []
                agg_pattern = r'-\s+`([^`]+)`\s+→\s+(\w+)\s+→\s+`([^`]+)`'
                for agg_match in re.finditer(agg_pattern, tool_content):
                    field = agg_match.group(1)
                    action = agg_match.group(2)
                    rename = agg_match.group(3)
                    aggregations.append({'field': field, 'action': action, 'rename': rename})
                config['aggregations'] = aggregations
            
            self.nodes[tool_id]['config'] = config


class MarkdownToAirflowConverter:
    """Converts Markdown workflow to Airflow DAG"""
    
    def __init__(self, workflow: MarkdownWorkflow):
        self.workflow = workflow
        self.dag_name = self._sanitize_name(self.workflow.workflow_name)
        
    def _sanitize_name(self, name: str) -> str:
        """Convert name to valid Python identifier"""
        return re.sub(r'[^a-zA-Z0-9_]', '_', name).lower()
    
    def convert(self) -> str:
        """Convert workflow to Airflow DAG code"""
        dag_code = self._generate_dag_header()
        dag_code += self._generate_tasks()
        dag_code += self._generate_dependencies()
        dag_code += self._generate_dag_footer()
        
        return dag_code
    
    def _generate_dag_header(self) -> str:
        """Generate DAG header"""
        description = self.workflow.description or f"DAG generated from {self.workflow.workflow_name}"
        
        return f'''#!/usr/bin/env python3
"""
Airflow DAG: {self.workflow.workflow_name}
Generated from Markdown: {Path(self.workflow.filepath).name}
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

{description}
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
    description='{description}',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['markdown', 'generated'],
)

'''
    
    def _generate_tasks(self) -> str:
        """Generate task definitions"""
        code = ''
        
        for node_id, node in self.workflow.nodes.items():
            tool_name = node['tool_name']
            config = node['config']
            
            if 'Input Data' in tool_name:
                code += self._create_input_task(node_id, config)
            elif 'Output Data' in tool_name:
                code += self._create_output_task(node_id, config)
            elif 'Formula' in tool_name:
                code += self._create_formula_task(node_id, config)
            elif 'Filter' in tool_name:
                code += self._create_filter_task(node_id, config)
            elif 'Sort' in tool_name:
                code += self._create_sort_task(node_id, config)
            elif 'Sample' in tool_name:
                code += self._create_sample_task(node_id, config)
            elif 'Summarize' in tool_name:
                code += self._create_summarize_task(node_id, config)
            elif 'Browse' in tool_name:
                code += self._create_browse_task(node_id, config)
            else:
                code += self._create_generic_task(node_id, tool_name, config)
            
            code += '\n'
        
        return code
    
    def _get_upstream_task(self, node_id: str) -> str:
        """Get the upstream task ID for a node"""
        for origin, dest, _ in self.workflow.connections:
            if dest == node_id:
                return origin
        return 'start'
    
    def _create_input_task(self, node_id: str, config: Dict) -> str:
        """Create input data task"""
        file_path = config.get('file', 'input.csv')
        
        return f'''    # Input Data - Tool {node_id}
    def read_data_{node_id}(context):
        import pandas as pd
        ti = context['ti']
        
        # Read input file
        file_path = '{file_path}'
        
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_{node_id} = PythonOperator(
        task_id='read_data_{node_id}',
        python_callable=read_data_{node_id},
        dag=dag,
    )
'''
    
    def _create_output_task(self, node_id: str, config: Dict) -> str:
        """Create output data task"""
        file_path = config.get('file', 'output.csv')
        upstream = self._get_upstream_task(node_id)
        
        return f'''    # Output Data - Tool {node_id}
    def write_data_{node_id}(context):
        import pandas as pd
        ti = context['ti']
        
        # Get data from upstream task
        json_data = ti.xcom_pull(key='data', task_ids='read_data_{upstream}')
        df = pd.read_json(json_data)
        
        # Write output file
        file_path = '{file_path}'
        os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)
        
        if file_path.endswith('.csv'):
            df.to_csv(file_path, index=False)
        elif file_path.endswith(('.xlsx', '.xls')):
            df.to_excel(file_path, index=False)
        else:
            df.to_csv(file_path, index=False)
        
        return df.shape[0]
    
    task_{node_id} = PythonOperator(
        task_id='write_data_{node_id}',
        python_callable=write_data_{node_id},
        dag=dag,
    )
'''
    
    def _create_formula_task(self, node_id: str, config: Dict) -> str:
        """Create formula task"""
        formulas = config.get('formulas', [])
        upstream = self._get_upstream_task(node_id)
        
        formula_code = '\n        '.join([
            f"# df['{f['field']}'] = {f['expression']}"
            for f in formulas
        ])
        
        return f'''    # Formula - Tool {node_id}
    def apply_formula_{node_id}(context):
        import pandas as pd
        ti = context['ti']
        
        json_data = ti.xcom_pull(key='data', task_ids='read_data_{upstream}')
        df = pd.read_json(json_data)
        
        # Apply formulas
        {formula_code}
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_{node_id} = PythonOperator(
        task_id='apply_formula_{node_id}',
        python_callable=apply_formula_{node_id},
        dag=dag,
    )
'''
    
    def _create_filter_task(self, node_id: str, config: Dict) -> str:
        """Create filter task"""
        upstream = self._get_upstream_task(node_id)
        
        if 'expression' in config:
            expression = config['expression']
        else:
            field = config.get('field', 'field')
            operator = config.get('operator', '==')
            value = config.get('value', '')
            
            # Map operator
            op_map = {
                'IsNotNull': 'notnull()',
                'IsNull': 'isnull()',
                'Equals': '==',
                'NotEqual': '!=',
                'GreaterThan': '>',
                'LessThan': '<',
                'GreaterThanOrEqual': '>=',
                'LessThanOrEqual': '<=',
            }
            
            op = op_map.get(operator, operator)
            
            if op in ['notnull()', 'isnull()']:
                expression = f"{field}.{op}"
            else:
                expression = f"{field} {op} '{value}'"
        
        return f'''    # Filter - Tool {node_id}
    def filter_data_{node_id}(context):
        import pandas as pd
        ti = context['ti']
        
        json_data = ti.xcom_pull(key='data', task_ids='read_data_{upstream}')
        df = pd.read_json(json_data)
        
        # Apply filter: {expression}
        try:
            df = df.query('{expression.replace("'", "\\'")}')
        except:
            # Fallback for complex expressions
            pass
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_{node_id} = PythonOperator(
        task_id='filter_data_{node_id}',
        python_callable=filter_data_{node_id},
        dag=dag,
    )
'''
    
    def _create_sort_task(self, node_id: str, config: Dict) -> str:
        """Create sort task"""
        sort_fields = config.get('sort_fields', [])
        upstream = self._get_upstream_task(node_id)
        
        if sort_fields:
            fields = [f['field'] for f in sort_fields]
            ascending = [f['order'].lower() == 'ascending' for f in sort_fields]
            sort_code = f"df = df.sort_values(by={fields}, ascending={ascending})"
        else:
            sort_code = "pass"
        
        return f'''    # Sort - Tool {node_id}
    def sort_data_{node_id}(context):
        import pandas as pd
        ti = context['ti']
        
        json_data = ti.xcom_pull(key='data', task_ids='read_data_{upstream}')
        df = pd.read_json(json_data)
        
        # Sort data
        {sort_code}
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_{node_id} = PythonOperator(
        task_id='sort_data_{node_id}',
        python_callable=sort_data_{node_id},
        dag=dag,
    )
'''
    
    def _create_sample_task(self, node_id: str, config: Dict) -> str:
        """Create sample task"""
        mode = config.get('mode', 'First')
        n = config.get('n', 10)
        upstream = self._get_upstream_task(node_id)
        
        return f'''    # Sample - Tool {node_id}
    def sample_data_{node_id}(context):
        import pandas as pd
        ti = context['ti']
        
        json_data = ti.xcom_pull(key='data', task_ids='read_data_{upstream}')
        df = pd.read_json(json_data)
        
        # Sample data: {mode} {n} rows
        if '{mode}' == 'First':
            df = df.head({n})
        elif '{mode}' == 'Random':
            df = df.sample(n=min({n}, len(df)), random_state=42)
        elif '{mode}' == 'Last':
            df = df.tail({n})
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_{node_id} = PythonOperator(
        task_id='sample_data_{node_id}',
        python_callable=sample_data_{node_id},
        dag=dag,
    )
'''
    
    def _create_summarize_task(self, node_id: str, config: Dict) -> str:
        """Create summarize task"""
        group_by = config.get('group_by', [])
        aggregations = config.get('aggregations', [])
        upstream = self._get_upstream_task(node_id)
        
        if aggregations:
            agg_dict = ', '.join([f"'{a['field']}': '{a['action'].lower()}'" for a in aggregations])
            
            if group_by:
                agg_code = f"df = df.groupby({group_by}).agg({{{agg_dict}}}).reset_index()"
            else:
                agg_code = f"df = df.agg({{{agg_dict}}})"
        else:
            agg_code = "pass"
        
        return f'''    # Summarize - Tool {node_id}
    def summarize_data_{node_id}(context):
        import pandas as pd
        ti = context['ti']
        
        json_data = ti.xcom_pull(key='data', task_ids='read_data_{upstream}')
        df = pd.read_json(json_data)
        
        # Aggregate data
        {agg_code}
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_{node_id} = PythonOperator(
        task_id='summarize_data_{node_id}',
        python_callable=summarize_data_{node_id},
        dag=dag,
    )
'''
    
    def _create_browse_task(self, node_id: str, config: Dict) -> str:
        """Create browse task"""
        upstream = self._get_upstream_task(node_id)
        
        return f'''    # Browse - Tool {node_id}
    def browse_data_{node_id}(context):
        import pandas as pd
        ti = context['ti']
        
        json_data = ti.xcom_pull(key='data', task_ids='read_data_{upstream}')
        df = pd.read_json(json_data)
        
        print(f"Browse Tool {node_id} - Shape: {{df.shape}}")
        print(df.head())
        
        return df.shape[0]
    
    task_{node_id} = PythonOperator(
        task_id='browse_data_{node_id}',
        python_callable=browse_data_{node_id},
        dag=dag,
    )
'''
    
    def _create_generic_task(self, node_id: str, tool_name: str, config: Dict) -> str:
        """Create generic task for unsupported tools"""
        upstream = self._get_upstream_task(node_id)
        
        return f'''    # {tool_name} - Tool {node_id}
    def process_{node_id}(context):
        import pandas as pd
        ti = context['ti']
        
        json_data = ti.xcom_pull(key='data', task_ids='read_data_{upstream}')
        df = pd.read_json(json_data)
        
        # Process data with {tool_name}
        # TODO: Implement {tool_name} logic
        
        ti.xcom_push(key='data', value=df.to_json())
        return df.shape[0]
    
    task_{node_id} = PythonOperator(
        task_id='process_{node_id}',
        python_callable=process_{node_id},
        dag=dag,
    )
'''
    
    def _generate_dependencies(self) -> str:
        """Generate task dependencies"""
        code = '\n# Define task dependencies\n'
        
        for origin, dest, label in self.workflow.connections:
            code += f'task_{origin} >> task_{dest}\n'
        
        return code + '\n'
    
    def _generate_dag_footer(self) -> str:
        """Generate DAG footer"""
        return '''
if __name__ == "__main__":
    dag.test()
'''
    
    def save_to_file(self, output_path: str):
        """Save the generated DAG to a file"""
        dag_code = self.convert()
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(dag_code)
        return output_path


def convert_markdown_to_airflow(md_path: str, output_path: Optional[str] = None) -> str:
    """
    Convert a Markdown file to an Airflow DAG
    
    Args:
        md_path: Path to the Markdown file
        output_path: Optional output path for the generated DAG
        
    Returns:
        Path to the generated DAG file
    """
    # Parse the markdown
    workflow = MarkdownWorkflow(md_path)
    workflow.parse()
    
    # Convert to Airflow
    converter = MarkdownToAirflowConverter(workflow)
    
    # Generate output path if not provided
    if output_path is None:
        output_path = Path(md_path).with_suffix('.py').as_posix()
    
    # Save to file
    converter.save_to_file(output_path)
    
    return output_path


def convert_all_md_in_directory(directory: str, output_dir: Optional[str] = None) -> List[str]:
    """
    Convert all markdown files in a directory to Airflow DAGs
    
    Args:
        directory: Directory containing markdown files
        output_dir: Optional output directory for generated DAGs
        
    Returns:
        List of paths to generated DAG files
    """
    output_paths = []
    directory_path = Path(directory)
    
    # Create output directory if specified
    if output_dir:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    for md_file in directory_path.glob('*.md'):
        # Skip README files
        if md_file.name.upper() == 'README.MD':
            continue
        
        output_path = None
        if output_dir:
            output_path = Path(output_dir) / (md_file.stem + '_dag.py')
        
        converted_path = convert_markdown_to_airflow(str(md_file), str(output_path) if output_path else None)
        output_paths.append(converted_path)
    
    return output_paths


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python md2af.py <markdown_file> [output_file]")
        print("       python md2af.py --all <directory> [output_directory]")
        sys.exit(1)
    
    if sys.argv[1] == '--all':
        if len(sys.argv) < 3:
            print("Error: --all requires a directory argument")
            sys.exit(1)
        
        input_dir = sys.argv[2]
        output_dir = sys.argv[3] if len(sys.argv) > 3 else None
        
        converted = convert_all_md_in_directory(input_dir, output_dir)
        print(f"Converted {len(converted)} markdown file(s) to Airflow DAG(s):")
        for path in converted:
            print(f"  - {path}")
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        
        converted = convert_markdown_to_airflow(input_file, output_file)
        print(f"Converted {input_file} to {converted}")
