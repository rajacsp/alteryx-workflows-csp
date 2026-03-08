#!/usr/bin/env python3
"""
Alteryx to Markdown Converter with Mermaid Diagrams

Converts Alteryx workflow files (.yxmd) into Markdown documentation
with Mermaid flowchart diagrams for visualization.
"""

import xml.etree.ElementTree as ET
import re
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
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
    
    def get_tool_name(self) -> str:
        """Get human-readable tool name"""
        plugin = self.plugin
        
        if 'DbFileInput' in plugin:
            return 'Input Data'
        elif 'DbFileOutput' in plugin:
            return 'Output Data'
        elif 'Formula' in plugin and 'MultiRow' not in plugin:
            return 'Formula'
        elif 'Filter' in plugin:
            return 'Filter'
        elif 'Sort' in plugin:
            return 'Sort'
        elif 'Sample' in plugin:
            return 'Sample'
        elif 'Summarize' in plugin:
            return 'Summarize'
        elif 'Browse' in plugin:
            return 'Browse'
        elif 'MultiRowFormula' in plugin:
            return 'Multi-Row Formula'
        elif 'ComposerTable' in plugin:
            return 'Table'
        elif 'PlotlyCharting' in plugin or 'Charting' in plugin:
            return 'Chart'
        elif 'MacroInput' in plugin:
            return 'Macro Input'
        elif 'MacroOutput' in plugin:
            return 'Macro Output'
        elif 'Join' in plugin:
            return 'Join'
        elif 'Union' in plugin:
            return 'Union'
        elif 'Select' in plugin:
            return 'Select'
        elif 'Append' in plugin:
            return 'Append Fields'
        elif 'Unique' in plugin:
            return 'Unique'
        elif 'RecordID' in plugin:
            return 'Record ID'
        elif 'Cleanse' in plugin:
            return 'Data Cleansing'
        elif 'TextInput' in plugin:
            return 'Text Input'
        elif 'RunningTotal' in plugin:
            return 'Running Total'
        elif 'Transpose' in plugin:
            return 'Transpose'
        elif 'CrossTab' in plugin:
            return 'Cross Tab'
        
        return 'Tool'
    
    def get_mermaid_shape(self) -> Tuple[str, str]:
        """Get Mermaid shape for this tool type"""
        plugin = self.plugin
        
        if 'DbFileInput' in plugin or 'TextInput' in plugin:
            return '[/', '/]'  # Parallelogram for input
        elif 'DbFileOutput' in plugin:
            return '[\\', '\\]'  # Parallelogram for output
        elif 'Filter' in plugin:
            return '{', '}'  # Diamond for decision
        elif 'Browse' in plugin:
            return '[(', ')]'  # Stadium for display
        elif 'Macro' in plugin:
            return '[[', ']]'  # Subroutine
        elif 'Charting' in plugin or 'ComposerTable' in plugin:
            return '[/', '/]'  # Parallelogram for output
        else:
            return '[', ']'  # Rectangle for processing


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
        self.description = self._get_description()
        self.metadata = self._get_metadata()
        
    def _get_workflow_name(self) -> str:
        meta = self.root.find('.//MetaInfo')
        if meta is not None:
            name = meta.find('.//Name')
            if name is not None and name.text:
                return name.text
        return Path(self.filepath).stem
    
    def _get_description(self) -> str:
        meta = self.root.find('.//MetaInfo')
        if meta is not None:
            desc = meta.find('.//Description')
            if desc is not None and desc.text:
                return desc.text
        return ''
    
    def _get_metadata(self) -> Dict:
        meta = self.root.find('.//MetaInfo')
        metadata = {}
        
        if meta is not None:
            for field in ['Author', 'Company', 'Copyright', 'SearchTags']:
                elem = meta.find(f'.//{field}')
                if elem is not None and elem.text:
                    metadata[field] = elem.text
        
        return metadata
    
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


class AlteryxToMarkdownConverter:
    """Converts Alteryx workflows to Markdown with Mermaid diagrams"""
    
    def __init__(self, workflow: AlteryxWorkflow):
        self.workflow = workflow
        
    def convert(self) -> str:
        """Convert workflow to Markdown"""
        md = self._generate_header()
        md += self._generate_overview()
        md += self._generate_mermaid_diagram()
        md += self._generate_tool_details()
        md += self._generate_data_flow()
        md += self._generate_footer()
        
        return md
    
    def _generate_header(self) -> str:
        """Generate markdown header"""
        return f"""# {self.workflow.workflow_name}

**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  
**Source:** `{Path(self.workflow.filepath).name}`

---

"""
    
    def _generate_overview(self) -> str:
        """Generate overview section"""
        md = "## Overview\n\n"
        
        if self.workflow.description:
            md += f"{self.workflow.description}\n\n"
        else:
            md += f"This workflow contains {len(self.workflow.nodes)} tools and {len(self.workflow.connections)} connections.\n\n"
        
        # Add metadata if available
        if self.workflow.metadata:
            md += "### Metadata\n\n"
            for key, value in self.workflow.metadata.items():
                md += f"- **{key}:** {value}\n"
            md += "\n"
        
        # Add statistics
        md += "### Statistics\n\n"
        md += f"- **Total Tools:** {len(self.workflow.nodes)}\n"
        md += f"- **Total Connections:** {len(self.workflow.connections)}\n"
        
        # Count tool types
        tool_types = {}
        for node in self.workflow.nodes.values():
            tool_name = node.get_tool_name()
            tool_types[tool_name] = tool_types.get(tool_name, 0) + 1
        
        md += f"- **Tool Types:** {len(tool_types)}\n\n"
        
        return md
    
    def _generate_mermaid_diagram(self) -> str:
        """Generate Mermaid flowchart diagram"""
        md = "## Workflow Diagram\n\n"
        md += "```mermaid\n"
        md += "flowchart TD\n"
        
        # Add nodes
        for node_id, node in self.workflow.nodes.items():
            tool_name = node.get_tool_name()
            annotation = node.properties.get('Annotation', {}).get('DefaultAnnotationText', '')
            
            # Create label
            if annotation and annotation.strip():
                # Truncate long annotations
                label = annotation[:50] + '...' if len(annotation) > 50 else annotation
                label = label.replace('"', "'").replace('\n', ' ')
            else:
                label = tool_name
            
            # Get shape
            shape_start, shape_end = node.get_mermaid_shape()
            
            # Add node with ID and label
            md += f"    {node_id}{shape_start}\"{label}<br/><small>({tool_name})</small>\"{shape_end}\n"
        
        md += "\n"
        
        # Add connections
        for conn in self.workflow.connections:
            origin = conn.origin_tool_id
            dest = conn.destination_tool_id
            
            # Determine arrow style based on connection type
            if conn.name and '#' in conn.name:
                # Action or special connection
                arrow = "-.->|action|"
            elif conn.origin_connection == 'True':
                arrow = "-->|True|"
            elif conn.origin_connection == 'False':
                arrow = "-->|False|"
            else:
                arrow = "-->"
            
            md += f"    {origin} {arrow} {dest}\n"
        
        md += "```\n\n"
        
        # Add styling note
        md += "> **Note:** The diagram shows the workflow structure with different shapes representing different tool types:\n"
        md += "> - Parallelograms: Input/Output tools\n"
        md += "> - Diamonds: Filter/Decision tools\n"
        md += "> - Rectangles: Processing tools\n"
        md += "> - Stadiums: Browse/Display tools\n\n"
        
        return md
    
    def _generate_tool_details(self) -> str:
        """Generate detailed tool information"""
        md = "## Tool Details\n\n"
        
        # Sort nodes by position (top to bottom, left to right)
        sorted_nodes = sorted(
            self.workflow.nodes.items(),
            key=lambda x: (x[1].position[1], x[1].position[0])
        )
        
        for node_id, node in sorted_nodes:
            tool_name = node.get_tool_name()
            annotation = node.properties.get('Annotation', {}).get('DefaultAnnotationText', '')
            
            md += f"### Tool {node_id}: {tool_name}\n\n"
            
            if annotation and annotation.strip():
                md += f"**Description:** {annotation}\n\n"
            
            # Add configuration details based on tool type
            config = node.properties.get('Configuration', {})
            
            if 'DbFileInput' in node.plugin:
                md += self._format_input_config(config)
            elif 'DbFileOutput' in node.plugin:
                md += self._format_output_config(config)
            elif 'Formula' in node.plugin and 'MultiRow' not in node.plugin:
                md += self._format_formula_config(config)
            elif 'Filter' in node.plugin:
                md += self._format_filter_config(config)
            elif 'Sort' in node.plugin:
                md += self._format_sort_config(config)
            elif 'Sample' in node.plugin:
                md += self._format_sample_config(config)
            elif 'Summarize' in node.plugin:
                md += self._format_summarize_config(config)
            elif 'MultiRowFormula' in node.plugin:
                md += self._format_multirow_config(config)
            
            md += "\n"
        
        return md
    
    def _format_input_config(self, config: Dict) -> str:
        """Format input tool configuration"""
        md = "**Configuration:**\n\n"
        
        file_path = config.get('File', '')
        if file_path:
            md += f"- **File:** `{file_path}`\n"
        
        format_opts = config.get('FormatSpecificOptions', {})
        if isinstance(format_opts, dict):
            if format_opts.get('HeaderRow'):
                md += f"- **Header Row:** {format_opts.get('HeaderRow')}\n"
            if format_opts.get('Delimeter'):
                md += f"- **Delimiter:** `{format_opts.get('Delimeter')}`\n"
        
        return md + "\n"
    
    def _format_output_config(self, config: Dict) -> str:
        """Format output tool configuration"""
        md = "**Configuration:**\n\n"
        
        file_path = config.get('File', '')
        if file_path:
            md += f"- **File:** `{file_path}`\n"
        
        multi_file = config.get('MultiFile')
        if multi_file:
            md += f"- **Multi-File Output:** {multi_file}\n"
            field = config.get('MultiFileField')
            if field:
                md += f"- **Group By Field:** `{field}`\n"
        
        return md + "\n"
    
    def _format_formula_config(self, config: Dict) -> str:
        """Format formula tool configuration"""
        md = "**Formulas:**\n\n"
        
        formula_fields = config.get('FormulaFields', {})
        if isinstance(formula_fields, dict):
            formula_field = formula_fields.get('FormulaField', [])
            
            if not isinstance(formula_field, list):
                formula_field = [formula_field] if formula_field else []
            
            for formula in formula_field:
                if isinstance(formula, dict):
                    field = formula.get('field', '')
                    expression = formula.get('expression', '')
                    field_type = formula.get('type', '')
                    
                    if field and expression:
                        md += f"- **{field}** ({field_type}): `{expression}`\n"
        
        return md + "\n"
    
    def _format_filter_config(self, config: Dict) -> str:
        """Format filter tool configuration"""
        md = "**Filter Condition:**\n\n"
        
        mode = config.get('Mode', 'Simple')
        
        if mode == 'Custom':
            expression = config.get('Expression', '')
            if expression:
                md += f"```\n{expression}\n```\n"
        else:
            simple = config.get('Simple', {})
            if isinstance(simple, dict):
                operator = simple.get('Operator', '')
                field = simple.get('Field', '')
                operands = simple.get('Operands', {})
                operand = operands.get('Operand', '') if isinstance(operands, dict) else ''
                
                md += f"- **Field:** `{field}`\n"
                md += f"- **Operator:** {operator}\n"
                if operand:
                    md += f"- **Value:** `{operand}`\n"
        
        return md + "\n"
    
    def _format_sort_config(self, config: Dict) -> str:
        """Format sort tool configuration"""
        md = "**Sort Order:**\n\n"
        
        sort_info = config.get('SortInfo', {})
        if isinstance(sort_info, dict):
            field_info = sort_info.get('Field', [])
            
            if not isinstance(field_info, list):
                field_info = [field_info] if field_info else []
            
            for field in field_info:
                if isinstance(field, dict):
                    field_name = field.get('field', '')
                    order = field.get('order', 'Ascending')
                    
                    if field_name:
                        md += f"- `{field_name}` - {order}\n"
        
        return md + "\n"
    
    def _format_sample_config(self, config: Dict) -> str:
        """Format sample tool configuration"""
        md = "**Sample Configuration:**\n\n"
        
        mode = config.get('Mode', 'First')
        n = config.get('N', 10)
        
        md += f"- **Mode:** {mode}\n"
        md += f"- **Number of Records:** {n}\n"
        
        group_fields = config.get('GroupFields', {})
        if isinstance(group_fields, dict):
            field = group_fields.get('Field', [])
            if field:
                if not isinstance(field, list):
                    field = [field]
                md += f"- **Group By:** {', '.join([f.get('name', '') if isinstance(f, dict) else str(f) for f in field])}\n"
        
        return md + "\n"
    
    def _format_summarize_config(self, config: Dict) -> str:
        """Format summarize tool configuration"""
        md = "**Aggregations:**\n\n"
        
        summarize_fields = config.get('SummarizeFields', {})
        if isinstance(summarize_fields, dict):
            fields = summarize_fields.get('SummarizeField', [])
            
            if not isinstance(fields, list):
                fields = [fields] if fields else []
            
            group_by = []
            aggregations = []
            
            for field in fields:
                if isinstance(field, dict):
                    field_name = field.get('field', '')
                    action = field.get('action', '')
                    rename = field.get('rename', '')
                    
                    if action == 'GroupBy':
                        group_by.append(field_name)
                    else:
                        aggregations.append((field_name, action, rename))
            
            if group_by:
                md += f"**Group By:** {', '.join([f'`{f}`' for f in group_by])}\n\n"
            
            if aggregations:
                md += "**Aggregations:**\n\n"
                for field, action, rename in aggregations:
                    md += f"- `{field}` → {action} → `{rename}`\n"
        
        return md + "\n"
    
    def _format_multirow_config(self, config: Dict) -> str:
        """Format multi-row formula configuration"""
        md = "**Multi-Row Formula:**\n\n"
        
        expression = config.get('Expression', '')
        create_field = config.get('CreateField_Name', '')
        num_rows = config.get('NumRows', 1)
        
        if create_field:
            md += f"- **New Field:** `{create_field}`\n"
        
        md += f"- **Number of Rows:** {num_rows}\n"
        
        if expression:
            md += f"- **Expression:** `{expression}`\n"
        
        group_by = config.get('GroupByFields', {})
        if isinstance(group_by, dict):
            field = group_by.get('Field', [])
            if field:
                if not isinstance(field, list):
                    field = [field]
                md += f"- **Group By:** {', '.join([f.get('field', '') if isinstance(f, dict) else str(f) for f in field])}\n"
        
        return md + "\n"
    
    def _generate_data_flow(self) -> str:
        """Generate data flow description"""
        md = "## Data Flow\n\n"
        
        # Find input nodes (nodes with no incoming connections)
        incoming = {conn.destination_tool_id for conn in self.workflow.connections}
        input_nodes = [node_id for node_id in self.workflow.nodes.keys() if node_id not in incoming]
        
        # Find output nodes (nodes with no outgoing connections)
        outgoing = {conn.origin_tool_id for conn in self.workflow.connections}
        output_nodes = [node_id for node_id in self.workflow.nodes.keys() if node_id not in outgoing]
        
        md += "### Input Sources\n\n"
        if input_nodes:
            for node_id in input_nodes:
                node = self.workflow.nodes[node_id]
                tool_name = node.get_tool_name()
                md += f"- **Tool {node_id}** ({tool_name})\n"
        else:
            md += "No input sources identified.\n"
        
        md += "\n### Output Destinations\n\n"
        if output_nodes:
            for node_id in output_nodes:
                node = self.workflow.nodes[node_id]
                tool_name = node.get_tool_name()
                md += f"- **Tool {node_id}** ({tool_name})\n"
        else:
            md += "No output destinations identified.\n"
        
        md += "\n"
        
        return md
    
    def _generate_footer(self) -> str:
        """Generate markdown footer"""
        return f"""---

*This documentation was automatically generated from the Alteryx workflow file.*
"""
    
    def save_to_file(self, output_path: str):
        """Save the generated markdown to a file"""
        md_content = self.convert()
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(md_content)
        return output_path


def convert_yxmd_to_markdown(yxmd_path: str, output_path: Optional[str] = None) -> str:
    """
    Convert an Alteryx yxmd file to Markdown with Mermaid diagram
    
    Args:
        yxmd_path: Path to the Alteryx workflow file
        output_path: Optional output path for the generated markdown
        
    Returns:
        Path to the generated markdown file
    """
    # Parse the workflow
    workflow = AlteryxWorkflow(yxmd_path)
    workflow.parse()
    
    # Convert to Markdown
    converter = AlteryxToMarkdownConverter(workflow)
    
    # Generate output path if not provided
    if output_path is None:
        output_path = Path(yxmd_path).with_suffix('.md').as_posix()
    
    # Save to file
    converter.save_to_file(output_path)
    
    return output_path


def convert_all_yxmd_in_directory(directory: str, output_dir: Optional[str] = None) -> List[str]:
    """
    Convert all yxmd files in a directory to Markdown
    
    Args:
        directory: Directory containing yxmd files
        output_dir: Optional output directory for generated markdown files
        
    Returns:
        List of paths to generated markdown files
    """
    output_paths = []
    directory_path = Path(directory)
    
    # Create output directory if specified
    if output_dir:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    for yxmd_file in directory_path.glob('*.yxmd'):
        output_path = None
        if output_dir:
            output_path = Path(output_dir) / (yxmd_file.stem + '.md')
        
        converted_path = convert_yxmd_to_markdown(str(yxmd_file), str(output_path) if output_path else None)
        output_paths.append(converted_path)
    
    return output_paths


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python alt2md.py <yxmd_file> [output_file]")
        print("       python alt2md.py --all <directory> [output_directory]")
        sys.exit(1)
    
    if sys.argv[1] == '--all':
        if len(sys.argv) < 3:
            print("Error: --all requires a directory argument")
            sys.exit(1)
        
        input_dir = sys.argv[2]
        output_dir = sys.argv[3] if len(sys.argv) > 3 else None
        
        converted = convert_all_yxmd_in_directory(input_dir, output_dir)
        print(f"Converted {len(converted)} workflow(s) to Markdown:")
        for path in converted:
            print(f"  - {path}")
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        
        converted = convert_yxmd_to_markdown(input_file, output_file)
        print(f"Converted {input_file} to {converted}")
