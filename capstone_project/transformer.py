
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

class Transformer:
    """
        Performs the cleaning and transformation for the data
    """

    @staticmethod
    def clean_airports(df_airports:DataFrame, df_states:DataFrame) -> DataFrame:
        """
        Cleans df_airports. Filters airports by location ('US') and type ('closed'). Creates 'state' column

        Args:
            df_airports (DataFrame): Spark Dataframe with airport data
            df_states (DataFrame): Spark Dataframe with state data

        Returns:
            DataFrame: Spark Dataframe with cleaned airport data
        """
        # Get list of states
        states_list = [x.state_code for x in df_states.select('state_code').distinct().collect()]

        # Filter for airports located in the US
        df_airports_clean = df_airports.filter(df_airports.iso_country == 'US')

        # Create 'state' column
        df_airports_clean = df_airports_clean.withColumn('state', F.substring(F.col('iso_region'), -2, 2)).drop('iso_region')

        # Replace invalid states with '99'
        df_airports_clean = df_airports_clean.withColumn('state', \
                                                         F.when(F.col('state').isin(states_list), F.col('state')) \
                                                         .otherwise('99'))
        
        # Filter closed airports
        df_airports_clean = df_airports_clean.filter(F.col('type') != 'closed')

        # Rename type column to airport_type
        df_airports_clean = df_airports_clean.withColumnRenamed('type','airport_type')

        return df_airports_clean
