# ATD Executive Dashboard

The purpose of this repo is to store the various scripts for moving data to places that are accessible to Power BI for the ATD Executive Dashboard.

# Data Sources

## AMANDA

AMANDA is a product used by the City of Austin to manage permitting and various other tasks. `amanda_to_s3.py` 
executes queries on the AMANDA read-replica database and deposits the results as a CSV file in a S3 bucket.

## SmartSheet

Smartsheet is used by the TPW Right of Way management division as a place to receive and manage some types of permit requests.
`smartsheet_to_s3.py` exports sheets to a CSV file in a S3 bucket.

## 3-1-1 Service Requests (CSR)

Austinites can call 3-1-1 or use the mobile app to submit service requests for city services. The 3-1-1 team has set up some
reports that we use to evaluate the department's performance. We use two ETL scripts to publish these reports to Socrata, 
so it can be accessed by BI tools. 

## Microstrategy (finance-reports)

We utilize a BI platform called microstrategy to run reporting on the department's finances. Specifically, for the department's 
revenue and expenses. These scripts run these reports and places the results in Socrata, so it can be accessed by BI tools.

## Socrata (socrata-metadata)

We utilize Socrata's dataset API to gather metrics about the department's assets published on the Open Data Portal. 
Such as the number of rows, when it was last updated, and the number of views. 
The results are stored in a [dataset](https://data.austintexas.gov/Transportation-and-Mobility/Data-and-Technology-Services-Datasets/28ys-ieqv/about_data).

# Processing Scripts

## active_permits_logging.py

Gets the count of all **currently active** permits from both AMANADA and SmartSheet and publishes it to a socrata dataset that 
contains a rolling log of the count of active permits.

## row_data_summary.py

Gets the count of all past permits that were processed from both AMANADA and SmartSheet and publishes it to a socrata dataset.
