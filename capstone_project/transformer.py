
import pyspark.sql.functions as F
from pyspark.sql.functions import date_add
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

        # Cast elevation_ft to float
        df_airports_clean = df_airports_clean.withColumn("elevation_ft", F.col("elevation_ft").cast("float"))

        return df_airports_clean

    @staticmethod
    def clean_i94_data(df_i94:DataFrame) -> DataFrame:
        """
        Cleans df_i94. Renames columns and converts values to the type specified in the schema. Creates arrival_date and departure_from_usa columns

        Args:
            df_i94 (DataFrame): Spark Dataframe with I94 SAS data

        Returns:
            DataFrame: Spark Dataframe with cleaned I94 SAS data
        """

        # Rename columns and set correct type
        df_i94_clean = df_i94.withColumn('cic_id', F.col('cicid').cast('integer')) \
                        .drop('cicid') \
                        .withColumn('year', F.col('i94yr').cast('integer')) \
                        .drop('i94yr') \
                        .withColumn('month', F.col('i94mon').cast('integer')) \
                        .drop('i94mon') \
                        .withColumn('country_birth_id', F.col('i94cit').cast('integer')) \
                        .drop('i94cit') \
                        .withColumn('country_residence_id', F.col('i94res').cast('integer')) \
                        .drop('i94res') \
                        .withColumn('port_code', F.col('i94port')) \
                        .drop('i94port') \
                        .withColumn('transport_mode_code', F.col('i94mode').cast('integer')) \
                        .drop('i94mode') \
                        .withColumn('state_code', F.col('i94addr')) \
                        .drop('i94addr') \
                        .withColumn('age', F.col('i94bir').cast('integer')) \
                        .drop('i94bir') \
                        .withColumn('visa_code', F.col('i94visa').cast('integer')) \
                        .drop('i94visa') \
                        .withColumn('count', F.col('count').cast('integer')) \
                        .withColumn('dtadfile', F.col('dtadfile').cast('integer')) \
                        .withColumn('birth_year', F.col('biryear').cast('integer')) \
                        .drop('biryear') \
                        .withColumn('allowed_to_stay_until', F.col('dtaddto')) \
                        .drop('dtaddto') \
                        .withColumn('ins_num', F.col('insnum')) \
                        .drop('insnum') \
                        .withColumn('admission_num', F.col('admnum').cast('integer')) \
                        .drop('admnum') \
                        .withColumn('flight_num', F.col('fltno').cast('integer')) \
                        .drop('fltno') \
                        .withColumn('arrival_date_sas', F.col('arrdate').cast('integer')) \
                        .drop('arrdate') \
                        .withColumn('departure_from_usa_sas', F.col('depdate').cast('integer')) \
                        .drop('depdate') \
                        .withColumn('visa_type', F.col('visatype')) \
                        .drop('visatype')
        
        # Convert arrival_date_sas and departure_from_usa_sas to date using a date baseline of 01/01/1960
        df_i94_clean = df_i94_clean \
                       .withColumn("date_baseline", F.to_date(F.lit("01/01/1960"), "MM/dd/yyyy")) \
                       .withColumn("arrival_date", F.expr("date_add(date_baseline, arrival_date_sas)")) \
                       .withColumn("departure_from_usa", F.expr("date_add(date_baseline, departure_from_usa_sas)"))  
        
        return df_i94_clean
