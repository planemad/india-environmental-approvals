# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a data processing repository for environmental clearance applications in India. The system scrapes data from the [Parivesh](https://parivesh.nic.in/) portal and processes it into structured CSV datasets.

## Development Commands

### All States (Default)
- **Initialize data collection**: `bash initialize.sh` (initializes the list of projects to fetch)
- **Fetch project data**: `bash fetch.sh` (fetches detailed project information from Parivesh API)
- **Generate CSV datasets**: `python parse.py` (processes JSON files and outputs to csv/Projects.csv)

### State-Specific Collection (e.g., Goa)
- **Initialize for specific state**: `bash initialize.sh GOA` (initializes data collection for Goa only)
- **Fetch state-specific data**: `bash fetch.sh GOA` (fetches detailed information for Goa projects)
- **Generate state-specific CSV**: `python parse.py GOA` (processes Goa data and outputs to csv/Projects_GOA.csv)

### Complete Pipeline Examples
- **All states**: `bash initialize.sh && bash fetch.sh && python parse.py`
- **Goa only**: `bash initialize.sh GOA && bash fetch.sh GOA && python parse.py GOA`
- **Goa only (simplified)**: `bash run_goa.sh` (runs the complete Goa pipeline in one command)

## Project Architecture

### Data Processing Pipeline

The system follows a three-stage data processing pipeline:

1. **Initialize** (`initialize.sh`): Queries Parivesh API to get lists of projects by clearance type (1-4)
2. **Fetch** (`fetch.sh`): Downloads detailed project information for each proposal ID
3. **Parse** (`parse.py`): Processes raw JSON data into structured CSV format

### Directory Structure

#### Default (All States)
- **`raw/search/`** - Contains initial project lists by clearance type (1-4.json)
- **`raw/caf/`** - Contains detailed project JSON files organized by clearance type and proposal ID
- **`csv/Projects.csv`** - Final processed CSV dataset for all states

#### State-Specific (e.g., Goa)
- **`raw/search_goa/`** - Contains initial project lists for Goa by clearance type
- **`raw/caf_goa/`** - Contains detailed project JSON files for Goa organized by clearance type
- **`csv/Projects_GOA.csv`** - Final processed CSV dataset for Goa only

#### Scripts
- **`parse.py`** - Main data processing script using Polars for efficient data manipulation

### Data Flow

1. **API Queries**: The system queries 4 different clearance types from Parivesh
2. **Rate Limiting**: `2_fetch.sh` downloads in randomized batches (default 35 concurrent, 0.1-2s between batches; override with `MIN_BATCH_SIZE`, `MAX_BATCH_SIZE`, `MIN_DELAY`, `MAX_DELAY`, `MAX_CONCURRENT`)
3. **Incremental Updates**: The fetch script checks file modification dates and only re-fetches updated proposals
4. **Data Extraction**: Parser extracts 13 key fields from complex nested JSON structures

### Key Components

- **State Filtering**: Configurable state parameter filters data collection to specific states (e.g., GOA)
- **Dual Format Support**: Handles both JSON and XML response formats from Parivesh API
- **Polars Integration**: Uses Polars library for efficient data processing and CSV generation
- **Safe Navigation**: Implements `safe_get()` function to handle inconsistent JSON structures
- **Error Handling**: Comprehensive error handling for malformed JSON/XML and missing data
- **URL Generation**: Automatically generates Parivesh portal URLs for each project
- **Directory Management**: Automatically creates state-specific directories for organized data storage

## Data Schema

The processed CSV contains these fields:
- Project identification (Proposal Number, Application Date, Project Name)
- Geographic information (State, District)
- Project details (Description, Category, Organization Name)
- Financial data (Total Cost in Lakhs)
- Employment metrics (Construction and Operational)
- Land requirements (Project Land Requirement in Hectares)
- Reference URL (proposal_url)

## Dependencies

- **bash** - For shell scripts
- **curl** - For API requests
- **python** - For data processing
- **polars** - Python library for efficient data manipulation
- **jq** - Command-line JSON processor (used in shell scripts)

## Data Source

All data is sourced from the Parivesh portal (https://parivesh.nic.in/), India's single window environmental clearance system. The system processes applications for 4 different types of environmental clearances.