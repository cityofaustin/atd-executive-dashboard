# ATD Executive Dashboard

The purpose of this repo is to store the various scripts for moving data to places that are accessible to Power BI for the ATD Executive Dashboard.

## AMANDA

AMANDA is a product used by the City of Austin to manage a lot permitting and various other tasks. `amanda_to_s3.py` executes queries on the AMANDA read-replica database and deposits the results as a CSV file in an S3 bucket. This file can now be accessed and refreshed daily by Power BI.