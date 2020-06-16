from pyspark.sql import DataFrame
import pyspark.sql.functions as f

class Validator:
    """
    Validates data located in warehouse
    """
    
    def __init__(self, spark_session):
      self.spark = spark_session

    def contain_data(self, df:DataFrame) -> bool:
        """
        Checks if provided Spark DataFrame contains data

        Args:
            df (DataFrame): Spark DataFrame to be tested

        Returns:
            bool: True if df contains data
        """
        return df.count() > 0

    def get_dataframe(self, path:str) -> DataFrame:
        """
        Returns Spark DataFrame with data located in the specified parquet

        Args:
            path (str): Path to the parquet containing the desired data

        Returns:
            DataFrame: Spark DataFrame containing data from parquet
        """
        return self.spark.read.parquet(path)

    def get_facts(self, i94_path:str) -> DataFrame:
        """
        Loads facts table (immigration_table)

        Args:
            i94_path (str): Path to the parquet containing the facts table

        Returns:
            DataFrame: Spark DataFrame containing data from parquet
        """
        df_i94 = self.get_dataframe(i94_path)

        return df_i94

    def get_dimensions(self, airport_path:str, 
                             cities_path:str, 
                             visas_path:str, 
                             transp_modes_path:str, 
                             countries_path:str,
                             states_path:str,
                             ports_path:str,):
        """
        Returns Spark DataFrames containing the dimensions tables

        Args:
            airport_path (str): Path to the parquet containing the airport_table
            cities_path (str): Path to the parquet containing the city_table
            visas_path (str): Path to the parquet containing the visa_table
            transp_modes_path (str): Path to the parquet containing the transport_modes_table
            countries_path (str): Path to the parquet containing the country_table
            states_path (str): Path to the parquet containing the states_table
            ports_path (str): Path to the parquet containing the ports_table

        Returns:
            DataFrame: Spark DataFrames containing the dimensions tables
        """

        df_airport = self.get_dataframe(airport_path)
        df_cities = self.get_dataframe(cities_path)
        df_visas = self.get_dataframe(visas_path)
        df_transp_modes = self.get_dataframe(transp_modes_path)
        df_countries = self.get_dataframe(countries_path)
        df_states = self.get_dataframe(states_path)
        df_ports = self.get_dataframe(ports_path)

        return df_airport, df_cities, df_visas, df_transp_modes, df_countries, df_states, df_ports
