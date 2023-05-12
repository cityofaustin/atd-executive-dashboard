# ATD Executive Dashboard

The purpose of this repo is to store the various scripts for moving data to places that are accessible to Power BI for the ATD Executive Dashboard.

# Data Sources

## AMANDA

AMANDA is a product used by the City of Austin to manage permitting and various other tasks. `amanda_to_s3.py` 
executes queries on the AMANDA read-replica database and deposits the results as a CSV file in a S3 bucket.

## SmartSheet

Smartsheet is used by the TPW Right of Way management division as a place to receive and manage some types of permit requests.
`smartsheet_to_s3.py` exports sheets to a CSV file in a S3 bucket.

# Processing Scripts

## active_permits_logging.py

Gets the count of all **currently active** permits from both AMANADA and SmartSheet and publishes it to a socrata dataset that 
contains a rolling log of the count of active permits.

## row_data_summary.py

Gets the count of all past permits that were processed from both AMANADA and SmartSheet and publishes it to a socrata dataset.
