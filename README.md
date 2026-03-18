# Claims Extraction and Eval Lab

## Required Setup

### 1. Create a Snowflake Trial Account
Create a new trial account [here](https://signup.snowflake.com), selecting 'Enterprise' as the Snowflake edition 'Azure' as the cloud provider, and 'East US-2' as the region. Note that it may take a few minutes to get the email from Snowflake to activate the account.

### 2. Run the `setup.sql` script
Once in the Snowsight UI for your trial account, click `+ Create` and choose `SQL Worksheet`.
Copy the contents of the `setup.sql` file in this repository and paste it into your new SQL worksheet in Snowflake.
Run the full script which will create a database and schema and load the `FinaNfipClaimsV2.parquet` data into a table so it's ready for the lab.

### 3. Set up the Notebook
- Go back to the home page
- Click `+ Create`, choose `Notebook`, then `From Git Repository`
- Click on `File location in repository`, then `Select Database` and choose the `CLAIMS_EXTRACTION_LAB`
- Now back on the notebook creation view, click 'Create' and you should be taken to the notebook!