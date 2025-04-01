"""
AIS Support Functions
This module provides functions to process AIS  data, 
including retrieving ship specifications and recognizing operational phases of vessels 
using methodologies from the IMO GHG 4th report.
"""

from ais import functions as af
import pyspark.sql.functions as F
from mtcc_ais_preparation._vessel_specs_ghg4 import find_folder_csv
import h3
from pyspark.sql.types import LongType,StringType

def ais_specs_version_ret(mmsi_values, spark):
    """
    Retrieves and matches ship specifications with the appropriate IHS data version.
    
    Parameters:
    ----------
    mmsi_values : pyspark.sql.DataFrame
        DataFrame containing MMSI numbers and vessel information with required columns:
        'mmsi', 'vessel_type_main', 'length', 'width', 'imo', 'version'.
    spark : SparkSession
        Active Spark session used for reading data.
    
    Returns:
    -------
    pyspark.sql.DataFrame
        DataFrame containing matched ship data with updated specifications and versioning.
    """
    # Check if all required columns are present in mmsi_values
    required_columns = {"mmsi", "vessel_type_main", "length", "width", "imo", "version"}
    mmsi_values_columns = set(mmsi_values.columns)
    
    if not required_columns.issubset(mmsi_values_columns):
        missing_columns = required_columns - mmsi_values_columns
        raise ValueError(f"mmsi_values is missing required columns: {', '.join(missing_columns)}")
    
    # Get the IHS versions and assign string versions
    versions = af.get_ihs_versions(spark, versions=True)
    versions = versions.assign(version=versions.version.apply(str))
    

    # Read ship data and join with versions for date ranges
    ship_data = af.read_ihs_shipdata(spark, versions_to_read=versions)
    # Drop unnecessary columns
    ship_data = ship_data.drop("from_date", "to_date")
    
    # Rename columns for consistency
    ship_data = ship_data.withColumnRenamed("MaritimeMobileServiceIdentityMMSINumber", "mmsi")\
                         .withColumnRenamed("LRIMOShipNo", "imo")\
                         .withColumnRenamed("version","version_mv")
    
    # Perform the join with conditional check, avoiding duplicate columns
    mmsi_values_selected = mmsi_values.select(
        F.col("mmsi").alias("mmsi_mv"),
        F.col("vessel_type_main"),
        F.col("length"),
        F.col("width"),
        F.col("imo").alias("imo_mv"),
        F.col("version")
    )

    ship_data = ship_data.join(
        F.broadcast(mmsi_values_selected),
        (ship_data["mmsi"] == mmsi_values_selected["mmsi_mv"]) &
        (ship_data["version_mv"] == mmsi_values_selected["version"]),
        "inner"
    )

    
    # Drop duplicate or conflicting columns
    ship_data = ship_data.drop("dt_pos_utc", "mmsi_mv", "imo_mv","version_mv")
    
    # Remove duplicate rows based on unique constraints
    ship_data = ship_data.dropDuplicates(["mmsi", "version"])

    return ship_data

def op_phase_recog(dfspark,spark):
    
    '''
    Recognizes a vessel's operational phase based on IMO GHG 4th report methodology, table 16.
    
    If 
    Linear intepolation of latitudes and longitudes every resample_interval minutes
    
    Explode function to create interpolated positions every 10 mins. Range between empty positions and
    arbitralrly defined as 2 hours are not interpolated.
    
    Exported as a spark.Dataframe with columns passed by the user or all columns
    
    Script from Gabriel Fuentes and the MTCC LatinAmerica

    Parameters
    ----------
    dfspark: Spark DataFrame

    refining_times: Spark DataFrame with ranges
        
    Returns
    -------
    spark data frame
        with operational phase between Anchorage, Berth, Slow transit, Normal cruising, Manoeuvring
  '''

    # Load H3-based polygon datasets for coast and port regions
    coast_1=spark.createDataFrame(find_folder_csv("polygons","h3_coast_1nm.csv"))
    coast_5=spark.createDataFrame(find_folder_csv("polygons","h3_coast_5nm.csv"))
    port_1=spark.createDataFrame(find_folder_csv("polygons","h3_wpi_port_polys_1NM.csv"))
    port_5=spark.createDataFrame(find_folder_csv("polygons","h3_wpi_port_polys_5NM.csv"))

    # Define UDFs for H3 indexing

    geo_to_h3 = F.udf(lambda latitude, longitude, resolution: 
                  h3.string_to_h3(h3.geo_to_h3(latitude,longitude, resolution)), LongType())                                       

    geo_to_h3_alpha = F.udf(lambda latitude, longitude, resolution: 
                    h3.geo_to_h3(latitude,longitude, resolution), StringType())  

    # Generate H3 indexes for spatial mapping
    dfspark=dfspark.withColumn("H3_int_4",geo_to_h3(F.col('latitude'),F.col("longitude"),F.lit(4)))\
                   .withColumn("H3_int_8",geo_to_h3(F.col('latitude'),F.col("longitude"),F.lit(8)))

     # Join with preloaded polygon datasets
    dfspark=dfspark.join(coast_1,["H3_int_4"],"left")\
                    .join(coast_5,["H3_int_4"],"left")\
                    .join(port_1,["H3_int_8"],"left")\
                    .join(port_5,["H3_int_8"],"left")

     # Define operational phase categories
    table_16_exc=["Oil tanker",'Chemical tanker','Liquified gas tanker','Other liquids tankers']
    anch="Anchorage"
    berth="Berth"
    slow="Slow transit"
    normal="Normal cruising"
    man="Manoeuvring"
    
    try:
        # Apply operational phase classification logic based on speed and location
        dfspark=dfspark.withColumn("op_phase",
            F.when(F.col("sog")<=1,
                   F.when(F.col("id_port_5").isNotNull(),berth).otherwise(anch)
                   ).otherwise(F.when(((F.col("sog")>1)&(F.col("sog")<=3)),anch)\
                    .otherwise(F.when(((F.col("sog")>3)&(F.col("sog")<=5)),
                                      F.when(((F.col("id_coast_5").isNull())&(F.col("id_port_5").isNull())),
                                            F.when(F.col("load")<=0.65,slow).otherwise(normal)).otherwise(man))\
                    .otherwise(F.when(F.col("sog")>5,
                                      F.when(F.col("id_port_1").isNotNull(),man)\
                                      .otherwise(F.when(F.col("load")<=0.65,slow).otherwise(normal))
                                     )
                              ))))
        dfspark=dfspark.withColumn("op_phase",
            F.when(F.col("StandardVesselType").isin(table_16_exc),
                   F.when(F.col("sog")>5,
                          F.when(F.col("id_port_5").isNotNull(),
                                 F.when(F.col("load")<=0.65,slow).otherwise(normal)
                                ).otherwise(F.col("op_phase"))
                         ).otherwise(F.col("op_phase"))
                  ).otherwise(F.col("op_phase")))
    except Exception as e:
        print(f'Have you merged the ais data with the hexagons dataframes for the ports and coast buffers?')
        print(f'Error: \n{e}\n')
    return dfspark



