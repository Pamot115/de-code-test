# Data Engineering Code Test

This is a simple ETL pipeline that ingests a CSV file, and exports the data to multiple tables in a MySQL database.

The repository contains two folders, each representing an endpoint of the 'system', one being a MySQL container, with the second being the Python script that performs the ETL. Additionally, it contains the docker-compose file that allows everything to run in parallel.

## MySQL

The **mysql** folder contains only a .sql file to create the target database.

At this time, everything runs under the MySQL root account.

## Python

The **python** folder contains the below structure:
- data
    - This is where CSV files (that follow the original file structure) must be placed for the script to ingest them
- modules
    - In here we have the different parts of the script, each of which are explained under the **Process overview** section.

### Process Overview

The Python script is segmented in four elements:

- DB Operations:
    - This script creates a connection to the MySQL database.
- File Processing:
    - In here we have multiple functions that handle the processing of the CSV file, while generating multiple dataframes, which are then exported to MySQL as Dimensional and Fact tables.
- Setup:
    - This script reads the configuration for the process from a YAML file.
    If the configuration file does not exists, it will then prompt the user to input the settings to use.
- Main Script:
    - In here we import the previously-mentioned modules, generate their instances (which call the required functions upon instantiation), and call the export_data function to complete the process.

Additionally, there are two YAML files included:

- config_template:
    - This file should not be modified as it serves as a base when generating a new configuration (for a different database / user).

- config:
    - This is the current configuration for the process.
    - As including a plain-text password in the configuration is a bad practice, we're indicating the scripts to search for the **MYSQL_PASSWORD** environment variable, in a production environment, this variable would only be accessible to specific users, although different encryption methods can also be used to better secure the information.

To run this, you need to have Docker and Docker Compose installed, after that, ensure Docker is running, and from a terminal run:

```
docker-compose build
docker-compose up
```