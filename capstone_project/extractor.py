from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from typing import Dict

class Extractor:
    """
        Prepares the data for the Transform step
    """
    def __init__(self, spark:SparkSession, filepaths:Dict[str, str]):
        """
        Constructor

        Args:
            spark (SparkSession): Spark session
            filepaths (Dict[str]): Dictionary with paths (relative or absolute) for input data
                                   Must contain the following keys:
                                        - i94
                                        - airports
                                        - states
                                        - transport_modes
                                        - visa
                                        - cities
        """

        # Dict validation
        for key in ['i94', 'airports', 'states', 'transport_modes', 'visa', 'cities']:
            if key not in filepaths.keys():
                raise ValueError('Key ' + key + ' not available in filepaths.')

        self.spark = spark
        self.filepaths = filepaths

    def _load_csv(self, filepath:str, delimiter=','):
        """
        Creates Spark Dataframe from provided csv file

        Args:
            filepath (string): Relative or absolute path to csv file
            delimiter (str, optional): Delimiter used in the specified csv file. Defaults to ','.

        Returns:
            pyspark.sql.DataFrame: Spark Dataframe 
        """
        df_spark = self.spark.read \
                   .format('csv') \
                   .option("header", "true") \
                   .option("delimiter", delimiter) \
                   .load(filepath)

        return df_spark

    def _load_sas(self, filepath:str) -> DataFrame:
        """
        Creates Spark dataframe from provided sas7bdat file.

        Args:
            filepath (str): Relative or absolute path to sas7bdat file

        Returns:
            DataFrame: Spark Dataframe
        """

        df_spark = self.spark.read \
                   .format('com.github.saurfang.sas.spark') \
                   .load(filepath)

        return df_spark

    def get_i94_data(self) -> DataFrame:
        """
        Returns Spark Dataframe containing I94 data
        """
        return self._load_sas(self.filepaths['i94'])

    def get_airport_data(self) -> DataFrame:
        """
        Returns Spark Dataframe containing airports data
        """
        return self._load_csv(self.filepaths['airports'])

    def get_cities_data(self) -> DataFrame:
        """
        Returns Spark Dataframe containing cities data
        """
        return self._load_csv(self.filepaths['cities'], delimiter=';')

    def get_states_data(self) -> DataFrame:
        """
        Returns Spark Dataframe containing states data
        """
        return self._load_csv(self.filepaths['states'])

    def get_transp_mode_data(self) -> DataFrame:
        """
        Returns Spark Dataframe containing transportation mode data
        """
        return self._load_csv(self.filepaths['transport_modes'])

    def get_visa_data(self) -> DataFrame:
        """
        Returns Spark Dataframe containing visa data
        """
        return self._load_csv(self.filepaths['visa'])
