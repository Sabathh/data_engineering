from pyspark.sql import SparkSession

from extractor import Extractor
from transformer import Transformer
from loader import Loader
from validator import Validator

def create_spark_session():

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport()\
        .getOrCreate()
    
    return spark

def extract(spark):
    """
    Extracts data from various files
    """

    filepaths = {
        'i94' : './data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat',
        'airports' : './data/airport-codes_csv.csv',
        'states' : './data/states.csv',
        'transport_modes' : './data/transport_mode.csv',
        'visa' : './data/visa.csv',
        'cities' : './data/us-cities-demographics.csv',
        'ports' : './data/ports.csv',
        'countries' : './data/countries.csv'
    }

    extractor = Extractor(spark, filepaths)

    df_i94 = extractor.get_i94_data()
    df_airports = extractor.get_airport_data()
    df_cities = extractor.get_cities_data()
    df_states = extractor.get_states_data()
    df_trasnp_modes = extractor.get_transp_mode_data()
    df_visas = extractor.get_visa_data()
    df_ports = extractor.get_ports_data()
    df_countries = extractor.get_countries_data()

    return df_i94, df_airports, df_cities, df_states, df_trasnp_modes, df_visas, df_ports, df_countries

def transform(spark, df_i94, df_airports, df_cities, df_states, df_trasnp_modes, df_visas, df_countries):
    """
    Cleans the Spark DataFrames
    """

    df_i94_clean = Transformer.clean_i94_data(df_i94, df_states)
    df_airport_clean = Transformer.clean_airports(df_airports, df_states)
    df_cities_clean = Transformer.clean_city_data(df_cities)
    df_visas_clean = Transformer.clean_visa_data(df_visas)
    df_countries_clean = Transformer.clean_countries_data(df_countries)
    df_trasnp_modes_clean = Transformer.clean_trasnp_modes_data(df_trasnp_modes)

    return df_i94_clean, df_airport_clean, df_cities_clean, df_visas_clean, df_countries_clean, df_trasnp_modes_clean

def load(spark, df_i94_clean, df_airport_clean, df_cities_clean, df_visas_clean, df_countries_clean, df_trasnp_modes_clean, df_ports, df_states):
    """
    Loads the cleaned Spark DataFrames into the data warehouse
    """

    parquetpaths = {
        'i94' : 'parquets/i94.parquet',
        'airports' : 'parquets/airports.parquet',
        'cities' : 'parquets/cities.parquet',
        'visa' : 'parquets/visas.parquet',
        'countries' : 'parquets/countries.parquet',
        'transport_modes' : 'parquets/trasnp_modes.parquet',
        'ports' : 'parquets/ports.parquet',
        'states' : 'parquets/states.parquet'
    }

    loader = Loader(spark)

    loader.load_i94(df_i94_clean, parquetpaths['i94'])
    loader.load_airports(df_airport_clean, parquetpaths['airports'])
    loader.load_cities(df_cities_clean, parquetpaths['cities'])
    loader.load_visas(df_visas_clean, parquetpaths['visa'])
    loader.load_countries(df_countries_clean, parquetpaths['countries'])
    loader.load_trasnp_modes(df_trasnp_modes_clean, parquetpaths['transport_modes'])
    loader.load_ports(df_ports, parquetpaths['ports'])
    loader.load_states(df_states, parquetpaths['states'])
    
def validate(spark):
    """
    Validated the data loaded into the data warehouse
    """
    
    parquetpaths = {
        'i94' : 'parquets/i94.parquet',
        'airports' : 'parquets/airports.parquet',
        'cities' : 'parquets/cities.parquet',
        'visa' : 'parquets/visas.parquet',
        'countries' : 'parquets/countries.parquet',
        'transport_modes' : 'parquets/trasnp_modes.parquet',
        'ports' : 'parquets/ports.parquet',
        'states' : 'parquets/states.parquet'
    }

    validator = Validator(spark)

    df_i94_loaded = validator.get_facts(parquetpaths['i94'])
    df_airport_loaded, df_cities_loaded, df_visas_loaded, df_transp_modes_loaded, df_countries_loaded, df_states_loaded, df_ports_loaded = validator.get_dimensions(parquetpaths['airports'], 
                                                                                                                                                                    parquetpaths['cities'], 
                                                                                                                                                                    parquetpaths['visa'], 
                                                                                                                                                                    parquetpaths['transport_modes'], 
                                                                                                                                                                    parquetpaths['countries'],
                                                                                                                                                                    parquetpaths['states'],
                                                                                                                                                                    parquetpaths['ports'])

    assert(validator.contain_data(df_i94_loaded))
    assert(validator.contain_data(df_airport_loaded))
    assert(validator.contain_data(df_cities_loaded))
    assert(validator.contain_data(df_visas_loaded))
    assert(validator.contain_data(df_transp_modes_loaded))
    assert(validator.contain_data(df_countries_loaded))
    assert(validator.contain_data(df_states_loaded))
    assert(validator.contain_data(df_ports_loaded))

def main():

    spark = create_spark_session() 

    #Extract
    df_i94, df_airports, df_cities, df_states, df_trasnp_modes, df_visas, df_ports, df_countries = extract(spark)

    #Transform
    df_i94_clean, df_airport_clean, df_cities_clean, df_visas_clean, df_countries_clean, df_trasnp_modes_clean = transform(spark, df_i94, df_airports, df_cities, df_states, df_trasnp_modes, df_visas, df_countries)

    # Load
    load(spark, df_i94_clean, df_airport_clean, df_cities_clean, df_visas_clean, df_countries_clean, df_trasnp_modes_clean, df_ports, df_states)

    # Validate
    validate(spark)

if __name__ == "__main__":
    main()

    