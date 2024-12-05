# FUK-migration
Migration tool for migrating data from the old server to the Thingsboard Platform.


## Install
Clone the repository and cd into the directory. Then run the following command to install the dependencies:

```bash
    # Clone the repository
    cd fuk-migration

    # Copy the .env.example file to .env
    cp .env.example .env

    # Install the dependencies
    npm install
```

Change the values in the .env file to match your environment.

## Run Migration
To run the migration, run the following command:
```bash
    npm start
```
Migration process will be output to the console and saved to the `tmp/unixtime.json` file.

The migration process iterates over unique unix timestamps and fetches the data from the old server. The data is then uploaded to Thingsboard.