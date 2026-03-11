Here is the enhanced Markdown documentation:

---
# Sample Workflow
Generated: 2026-03-11 00:38:55  
Source: `sample.yxmd`

---

## Overview

This workflow consists of 9 tools and 9 connections, processing a dataset from `US_Accidents_March23.csv`. The workflow includes a variety of tool types, including input/output tools, filter/decision tools, processing tools, and browse/display tools.

### Key Statistics

- **Total Tools:** 9
- **Total Connections:** 9
- **Tool Types:** 6 (Input/Output: 2, Filter/Decision: 3, Processing: 4)

## Workflow Diagram

```mermaid
flowchart TD
    1[/"US_Accidents_March23.csv<br/><small>(Input Data)</small>"/]
    2["Summarize<br/><small>(Summarize)</small>"]
    3[("Browse<br/><small>(Browse)</small>")]
    4[("Browse<br/><small>(Browse)</small>")]
    5{"!IsNull([Timezone])<br/><small>(Filter)</small>"}
    7[("Browse<br/><small>(Browse)</small>")]
    16["Timezone = REGEX_Replace([Timezone],'US/',' ') <br/><small>(Formula)</small>"]
    17["Tool<br/><small>(Tool)</small>"]
    22["Tool<br/><small>(Tool)</small>"]

    1 --> 2
    1 --> 4
    2 --> 3
    2 --> 5
    5 -->|True| 16
    16 --> 17
    17 --> 7
    17 --> 22
    17 --> 22
```

> **Note:** The diagram illustrates the workflow structure, with different shapes representing various tool types:
> - Parallelograms: Input/Output tools (`1`, `3`, `4`)
> - Diamonds: Filter/Decision tools (`5`, `16` and its successor)
> - Rectangles: Processing tools (`2`, `17`, `22`)
> - Stadiums: Browse/Display tools (`7`, `22`)

## Tool Details

### Tool 1: Input Data (US_Accidents_March23.csv)

**Description:** The input dataset, containing accident data for March 23.

**Configuration: