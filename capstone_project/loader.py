from pyspark.sql import DataFrame

class Loader:
    """
    Loads data warehouse with parquet created from spark dataframes
    """
    
    def __init__(self, spark_session):
      self.spark = spark_session

    def _load_model(self, df:DataFrame, path:str):
        """
        Loads provided Spark DataFrame into specified path. Data is saved as parquet

        Args:
            df (DataFrame): Sparrk DataFrame to be loaded
            path (str): Path to load the DataFrame
        """
        df.write.mode('overwrite').parquet(path)

    def load_airports(self, df_airports:DataFrame, path:str):
        """
        Loads airports_table

        Args:
            df_airports (DataFrame): Spark DataFrame containing airports_table
            path (str): Path to load airports_table
        """
        self._load_model(df_airports, path)

    def load_i94(self, df_i94:DataFrame, path:str):
        """
        Loads immigration_table

        Args:
            df_i94 (DataFrame): Spark DataFrame containing immigration_table
            path (str): Path to load immigration_table
        """
        self._load_model(df_i94, path)

    def load_cities(self, df_cities:DataFrame, path:str):
        """
        Loads city_table

        Args:
            df_cities (DataFrame): Spark DataFrame containing city_table
            path (str): Path to load city_table
        """
        self._load_model(df_cities, path)

    def load_visas(self, df_visas:DataFrame, path:str):
        """
        Loads visa_table

        Args:
            df_visas (DataFrame): Spark DataFrame containing visa_table
            path (str): Path to load visa_table
        """
        self._load_model(df_visas, path)

    def load_trasnp_modes(self, df_trasnp_modes:DataFrame, path:str):
        """
        Loads transport_mode_table

        Args:
            df_trasnp_modes (DataFrame): Spark DataFrame containing transport_mode_table
            path (str): Path to load transport_mode_table
        """
        self._load_model(df_trasnp_modes, path)

    def load_countries(self, df_countries:DataFrame, path:str):
        """
        Loads country_table

        Args:
            df_countries (DataFrame): Spark DataFrame containing country_table
            path (str): Path to load country_table
        """
        self._load_model(df_countries, path)

    def load_states(self, df_states:DataFrame, path:str):
        """
        Loads states_table

        Args:
            df_states (DataFrame): Spark DataFrame containing states_table
            path (str): Path to load states_table
        """
        self._load_model(df_states, path)

    def load_ports(self, df_ports:DataFrame, path:str):
        """
        Loads ports_table

        Args:
            df_ports (DataFrame): Spark DataFrame containing ports_table
            path (str): Path to load ports_table
        """
        self._load_model(df_ports, path)

    