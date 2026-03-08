# Alteryx Workflow Repository

Welcome to the Alteryx Workflow Repository! This repository hosts a collection of Alteryx workflows designed to streamline data processing, analysis, and automation tasks.

## Overview

In this repository, you will find:

- **Alteryx Workflows**: .yxmd or .yxzp files containing data workflows for data preparation, blending, analysis, and reporting.
- **Data Files**: Sample data files or connections used within the workflows.
- **Documentation**: Guides, tutorials, and explanations on how to use, customize, and deploy the workflows.
- **Scripts**: Any additional scripts or tools used alongside Alteryx workflows.

## Workflow Conversion Tools

This repository includes three conversion tools for different workflows:

### 1. Alteryx to Airflow DAG (`alterxy2airflow.py`)

Directly converts Alteryx `.yxmd` files to Apache Airflow DAG Python files.

**Usage:**
```bash
# Convert a single workflow
python alterxy2airflow.py "Accident Workflow.yxmd" "accident_dag.py"

# Convert all workflows in a directory
python alterxy2airflow.py --all "." "output/dags"
```

### 2. Alteryx to Markdown (`alt2md.py`)

Converts Alteryx `.yxmd` files to Markdown documentation with Mermaid flowchart diagrams.

**Usage:**
```bash
# Convert a single workflow to markdown
python alt2md.py "Accident Workflow.yxmd" "accident_workflow.md"

# Convert all workflows in a directory
python alt2md.py --all "." "docs"
```

**Output:** Generates comprehensive documentation including:
- Workflow overview and metadata
- Mermaid flowchart diagram
- Detailed tool configurations
- Data flow analysis

### 3. Markdown to Airflow DAG (`md2af.py`)

Converts Markdown documentation (with Mermaid diagrams) to Apache Airflow DAG Python files.

**Usage:**
```bash
# Convert a single markdown file
python md2af.py "accident_workflow.md" "accident_dag.py"

# Convert all markdown files in a directory
python md2af.py --all "docs" "output/dags"
```

### Complete Conversion Pipeline

You can use these tools in sequence for a complete workflow:

```bash
# Step 1: Convert Alteryx to Markdown (for documentation/review)
python alt2md.py "Ted talk Workflow.yxmd" "ted_talk.md"

# Step 2: Review and edit the markdown if needed
# (Edit ted_talk.md to add notes, modify configurations, etc.)

# Step 3: Convert Markdown to Airflow DAG
python md2af.py "ted_talk.md" "ted_talk_dag.py"
```

Or convert directly:
```bash
# Direct conversion: Alteryx to Airflow
python alterxy2airflow.py "Ted talk Workflow.yxmd" "ted_talk_dag.py"
```

### Supported Alteryx Tools

The converters support the following Alteryx tool types:
- File Input/Output (CSV, Excel)
- Formula
- Filter
- Sort
- Sample
- Summarize/Aggregate
- Browse
- Multi-Row Formula
- Table Composer
- Charts
- Macro Input/Output
- Join, Union, Select
- Data Cleansing
- Running Total
- Transpose, Cross Tab

## Usage

To utilize the Alteryx workflows in this repository:

1. **Clone or Download**: Clone this repository to your local machine or download it as a ZIP file.
2. **Install Alteryx Designer**: If you haven't already, download and install [Alteryx Designer](https://www.alteryx.com/designer-trial).
3. **Open Workflow**: Open the desired Alteryx workflow (.yxmd or .yxzp file) using Alteryx Designer.
4. **Configure Input and Output**: If necessary, configure input connections to your data sources and output destinations.
5. **Run the Workflow**: Execute the workflow to perform data processing, blending, or analysis tasks.
6. **Review Results**: Review the output data and any generated reports or visualizations to gain insights.

## Formatting

To maintain consistency and readability across Alteryx workflows, please adhere to the following formatting guidelines:

- Organize the workflow with clear and logical workflows.
- Use meaningful names for tools, inputs, outputs, and macros.
- Include annotations and comments to explain complex workflows or logic.
- Follow best practices for efficient data processing and optimization.

## Git Commands for Alteryx Workflows

When collaborating on Alteryx workflows within this repository, consider using Git commands to manage changes efficiently:

- `git clone <repository_url>`: Clone the repository to your local machine.
- `git add <file>`: Add the modified Alteryx workflow file (.yxmd or .yxzp) to the staging area.
- `git commit -m "Your commit message"`: Commit the changes along with a descriptive message indicating the purpose of the modifications.
- `git push origin <branch>`: Push the committed changes to the remote repository, making them available to other team members.
- `git pull origin <branch>`: Pull the latest changes from the remote repository to your local machine.


## Feedback

Your feedback is valuable to us! If you have any questions, suggestions, or encounter any issues while using our Alteryx workflows, please don't hesitate to
[open an issue](https://github.com/nandita2000/Alteryx-workflows/issues) in this repository.

## License

This repository is licensed under the [MIT License](LICENSE). Feel free to use, modify, and distribute the workflows and resources as needed, but please attribute the original work appropriately.

## Contact

For any inquiries or further assistance, please contact [nanditasharma182@gmail.com].

Thank you for using our Alteryx workflows! We hope they streamline your data processing and analysis tasks effectively. Happy workflow building!
