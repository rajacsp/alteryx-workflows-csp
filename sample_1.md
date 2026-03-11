# sample

**Generated:** 2026-03-11 00:44:56  
**Source:** `sample.yxmd`

---

## Overview

This workflow contains 9 tools and 9 connections.

### Statistics

- **Total Tools:** 9
- **Total Connections:** 9
- **Tool Types:** 6

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

> **Note:** The diagram shows the workflow structure with different shapes representing different tool types:
> - Parallelograms: Input/Output tools
> - Diamonds: Filter/Decision tools
> - Rectangles: Processing tools
> - Stadiums: Browse/Display tools

## Tool Details

### Tool 4: Browse


### Tool 3: Browse


### Tool 7: Browse


### Tool 16: Formula

**Description:** Timezone = REGEX_Replace([Timezone],"US/"," ")


**Formulas:**



### Tool 1: Input Data

**Description:** US_Accidents_March23.csv

**Configuration:**

- **File:** `C:\Users\ash_s\Downloads\archive (8)\US_Accidents_March23.csv`
- **Header Row:** True
- **Delimiter:** `,`


### Tool 2: Summarize

**Aggregations:**



### Tool 5: Filter

**Description:** !IsNull([Timezone])

**Filter Condition:**

- **Field:** `Timezone`
- **Operator:** IsNotNull
- **Value:** `Serious`


### Tool 17: Tool


### Tool 22: Tool


## Data Flow

### Input Sources

- **Tool 1** (Input Data)

### Output Destinations

- **Tool 3** (Browse)
- **Tool 4** (Browse)
- **Tool 7** (Browse)
- **Tool 22** (Tool)

---

*This documentation was automatically generated from the Alteryx workflow file.*
