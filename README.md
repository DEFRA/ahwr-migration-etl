# ahwr-migration-etl

This app is meant to be run locally, to provide a way to combine JSON files and CSV data for AHWR main application migration

## Installation
To install the required dependencies, run:

``` npm install ```

## Pre-req for running

Create a .env file in the root directory with the following variables:
```
STATUS_ENVIRONMENT="test"
USE_FILE="data/teststatus.csv"
```

Where the environment is the environment you want to migrate data for, and USE_FILE is the path to the CSV file you want 
to use for the status history during the migration. the environment should be all lower case, and be one of the followign values:

- dev
- test
- pre
- prod

You will need a set of input files in the data/ folder in order to run the migration ETL, with names corresponding to 
the environment you are working with (in pascal case, matching the format shown below).
e.g for test environment, you will need:

- TestClaims.json
- TestHerds.json
- TestNewWorldApplications.json
- TestOldWorldApplications.json

These will be the outputs from the FCP Postgres database

You will also need to include the file that represents the status history entries which will have been exported from
Azure table storage, and this needs to match whatever name you supplied in the USE_FILE variable in the .env file.

## Running the app
Simply run by using the start target in the package.json

## Outputs

After running the app you will get a set of output files in the output/env folder corresponding to the env you are 
working with.
These files will be:

- TestClaims.json
- TestHerds.json
- TestNewWorldApplications.json
- TestOldWorldApplications.json

These correspond to your inputs, but will now have the status history entries added to each claim/application as appropriate
You will also have a file called:

- TestUnmatchedStatusHistoryEntries.json
  
Which contains a list of any status history entries that could not be matched to a claim or application during the migration process.
This is for validation purposes and should be empty for production
