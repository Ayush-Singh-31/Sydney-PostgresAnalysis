# Greater Sydney Area Data Analysis Project

## Overview

This project involves the integration and analysis of various datasets to study the Greater Sydney Area. Using a PostgreSQL database with PostGIS extensions for spatial queries, the team explored datasets ranging from public transport stops to trees and parking spaces. The goal was to calculate a "bustling score" for different regions in Sydney based on several parameters.

## Project Structure

The repository contains:
- **Datasets**: Preprocessed and cleaned datasets used for the analysis.
- **SQL Scripts**: Queries for database creation, indexing, and geospatial analysis.
- **Python Code**: Scripts for data preprocessing, analysis, and visualization.
- **Reports**: Detailed analysis and findings.

## Datasets

1. **SA2 Digital Boundaries**  
   - Source: Australian Bureau of Statistics  
   - Description: Defines spatial boundaries for analysis.

2. **Businesses**  
   - Source: ABS  
   - Cleaned to retain unique rows with meaningful industry data.

3. **Public Transport Stops**  
   - Source: Transport for NSW  
   - Preprocessed for geospatial analysis.

4. **Polling Places**  
   - Source: Australian Electoral Commission  
   - Key columns extracted for regional analysis.

5. **School Catchments**  
   - Source: NSW Department of Education  
   - Focused on current data for analysis.

6. **Population and Income**  
   - Provided with assignment scaffold; preprocessed for data integrity.

7. **Mobility Parking, Trees, Stairs**  
   - Source: City of Sydney  
   - Cleaned for geospatial visualization.

## Analysis

### Data Schema
The datasets were joined on `SA2_Code` and `SA2_Name`. Geospatial data was indexed on the `geom` column for efficient querying.

### Bustling Score
Calculated using z-scores from multiple variables:
- **Businesses**
- **Public Transport Stops**
- **Polling Places**
- **Schools per 1000 under-19 population**
- **Trees, Parking Spaces, Stairs**

The sigmoid function was applied to standardize the scores, yielding a bustling score between 0 and 1.

### Correlation Analysis
Analyzed the relationship between bustling scores and median income. The correlation coefficient of -0.0101 indicated no significant relationship.

### Visualizations
- Geospatial overlays of bustling scores.
- Correlation scatter plot between bustling scores and median income.

## Key Findings

- **Top Bustling Area**: Sydney (North) - Millers Point  
- **Least Bustling Area**: Chippendale  
- Trees, parking spaces, and stairs significantly influence bustling scores.  

## Requirements

- **PostgreSQL** (with PostGIS extension)
- **Python 3.x**
- Python libraries: `pandas`, `matplotlib`, `geopandas`

## Contributors

- Ayush Singh  
- Devang Jindia  
- Raghav Dogra  
