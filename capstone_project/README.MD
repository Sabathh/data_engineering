# Capstone Project

Capstone project of the Data Engineering Nanodegree.

## Project description

The goal of the project is to create a data warehouse where files are stored in a parquet format and related via a star schema. The data warehouse stores information about people who entered the U.S., such as country of origin, destination, transportation mode, visa type, etc.

## How to run

Execute [etl.py](https://github.com/Sabathh/data_engineering/blob/master/capstone_project/etl.py) script. Configure the paths where the data comes from and where the parquet files will be saved directly in the script.

## Datasets used

- **I94 Immigration Data**: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. [This](https://travel.trade.gov/research/reports/i94/historical/2016.html) is where the data comes from. The [immigration_data_sample.csv](https://github.com/Sabathh/data_engineering/blob/master/capstone_project/data/immigration_data_sample.csv) file contains the sample data.
- **Airport Code Table**: This data comes from [ourairports](http://ourairports.com/data/airports.csv). It is a simple table of airport codes and corresponding cities. It contains the list of all airport codes, the attributes are identified in datapackage description. Some of the columns contain attributes identifying airport locations, other codes (IATA, local if exist) that are relevant to identification of an airport.

### Data cleaning steps

- The data available in the [I94_SAS_Labels_Descriptions.SAS](https://github.com/Sabathh/data_engineering/blob/master/capstone_project/data/I94_SAS_Labels_Descriptions.SAS) file was manually processed into a csv format in order to pre used by the ETL. The resulting files were [countries.csv](https://github.com/Sabathh/data_engineering/blob/master/capstone_project/data/countries.csv), [states.csv](https://github.com/Sabathh/data_engineering/blob/master/capstone_project/data/states.csv), [transport_mode.csv](https://github.com/Sabathh/data_engineering/blob/master/capstone_project/data/transport_mode.csv), [visa.csv](https://github.com/Sabathh/data_engineering/blob/master/capstone_project/data/visa.csv) and [ports.csv](https://github.com/Sabathh/data_engineering/blob/master/capstone_project/data/ports.csv).
- Airport data from [airport-codes_csv.csv](https://raw.githubusercontent.com/Sabathh/data_engineering/master/capstone_project/data/airport-codes_csv.csv) was filtered to show only the U.S. airports, as other airports are not relevant for the datase used.
- The immigration data from the *.sas7bdat* files was processed to have human-readable column names as well as coherent data types (e.g. numerical data was casted to numerical types). Invalid values, such as state codes that do not exist were replaced with easily identifiable invalid values.
  
### Schema

The resulting star-schema was the following:

![schema](./Schema.png)

The purpose of the model is to enable analysis about the immigration data to be performed, such as the connections between the arrival ports and the final destination of the people entering the country.

## Addressing Other Scenarios
The following sections describes a logical approach to this project under the predefined scienarios:

### The data was increased by 100x.
Since Spark was used as the main data-processing tool there wouldn't be any issues arising from an increase of 100x on the data size. The extra computation time could be compensated by running the ETL in a cluster scaled to match the dataset size.

### The pipeline would be run on a daily basis by 7am every day.
The ETL created could be trigerred via a scheduling tool such as *Apache Airflow*. In this scenarion the immigration data set would be updated and processed daily. The the rest of the data, such as airport data, could be processed less often, since they are less prone to change.

### The database needed to be accessed by 100+ people.
The data could be stored in an S3 bucket and adjusted for the number of people that need access to it. S3 was designed with scalability in mind, both in terms of size and access.

## Defending Decisions

The tool choson to process all the data was *Apache Spark*. This was due to Sparks capability of handling multiple data formats, such as *.csv* and *SAS*, as well as its capability of processing an amount of that that could easily overflow a machine's memory. The library [.sas7bdat](https://github.com/saurfang/spark-sas7bdat) was used for reading SAS data (*.sas7bdat* format) with Spark. A schema can be automatically inferred from metadata embedded in the SAS file, making the data extraction process straightforward. The processed data is saved in a parquet format for storage in either a local machine or an S3 bucket. Parquet was chosen due to its decent data compression ratio and due to how how easy it is to retrieve the data in the correct format for analysis when needed.
