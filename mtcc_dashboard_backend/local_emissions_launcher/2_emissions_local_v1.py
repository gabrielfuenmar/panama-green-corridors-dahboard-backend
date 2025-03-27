"""
Script: 2_emissions_local_v1.py
Author: Gabriel Fuentes

Description:
------------
The launcher for the method to generate emissions per hexagon size 6 within Panama local waters
The output is a aprquet file to be read by the dashboard and plotted as a heatmap

Dependencies:
-------------
- Python Packages: pandas, geopandas, h3, os, sys, re, datetime, pyspark
- Custom Modules: functions from ais, mtcc_ais_preparation,ais_support_func, s3_digest, emissions_generator,
- Spot Ocean API for Spark processing
"""
###---- Kernel default functions
from ais import functions as af
from mtcc_ais_preparation import functions as ef

###----- Python packages
import pandas as pd
import geopandas as gpd
import h3
import os
import sys
import re
from datetime import datetime

#####------ Custom functions
from s3_digest import open_s3_session, ais_read_data_range, write_to_s3, s3_file_read
from ais_support_func import ais_specs_version_ret
from emissions_generator import emissions_ghg_imo

###- Spot Ocean Spark packages -
import spot_api

###---------------
from pyspark.sql import functions as F


# Load geographical data for land EEZ and Panama Canal
land_eez = ef.find_folder_csv("polygons", "land_eez_wkt.csv")
panama_canal = ef.find_folder_csv("polygons", "panama_canal.csv")

# Convert data to GeoDataFrame with proper geometry
land_eez=gpd.GeoDataFrame(land_eez,geometry=gpd.GeoSeries.from_wkt(land_eez["WKT_geom"]))
panama_canal=gpd.GeoDataFrame(panama_canal,geometry=gpd.GeoSeries.from_wkt(panama_canal["WKT_geom"]),crs=4326)

# Filter to Panama's EEZ (Exclusive Economic Zone)
count_eez = land_eez[land_eez["TERRITORY1"] == 'Panama']


# Generate H3 indices for the coastline of Panama's EEZ
h3_indeces_int=set()
h3_indeces_alpha=set()

for ind, row in count_eez.iterrows():
    h3_inner_list=list(h3.polyfill(row.geometry.__geo_interface__, 5, geo_json_conformant=True))
    h3_indeces_alpha.update(h3_inner_list)
    
    ##Now in integer form
    h3_inner_list=[h3.string_to_h3(h) for h in h3_inner_list]
    h3_indeces_int.update(h3_inner_list)
    
h3_indeces_coast_int=list(h3_indeces_int)


# Connect to Spark session via Spot Ocean API
SPOT_TOKEN = os.environ.get("SPOT_TOKEN")
connect = spot_api.spot_connect(SPOT_TOKEN)
connect.submit()
connect.wait_til_running()
spark = connect.spark_session()


# Retrieve AWS access credentials from environment variables

P_ACCESS_KEY=os.environ.get("P_ACCESS_KEY")
P_SECRET_KEY=os.environ.get("P_SECRET_KEY")


# Get list of all files within the S3 bucket
base_files=open_s3_session(P_ACCESS_KEY,P_SECRET_KEY)

done_days = None

# Check for existing processed data
if any("transits_pc_in" in key for key in base_files):
    file_name = [x for x in base_files if "transits_pc_in" in x and "stops" not in x]
    log_name = [x for x in base_files if "log_emissions_local_panama" in x]

    start_dates = []
    end_dates = []

    # Extract date ranges from filenames
    for file in file_name:
        match = re.search(r"transits_pc_in_(\d{4}-\d{2}-\d{2})&(\d{4}-\d{2}-\d{2})\.parquet", file)
        if match:
            start_dates.append(datetime.fromisoformat(match.group(1)))  # Start date before '&'
            end_dates.append(datetime.fromisoformat(match.group(2)))    # End date after '&'

    if not start_dates or not end_dates:
        print("No valid date ranges found in file names. Exiting.")
        connect.delete()
        sys.exit(1)  # Stop execution if no valid data

    fr_rg = min(start_dates)
    to_rg = max(end_dates)

    # Generate list of dates to process
    range_to_do = pd.date_range(fr_rg, to_rg).to_list()

    if log_name:
        log_read = s3_file_read(log_name[0], P_ACCESS_KEY, P_SECRET_KEY)
        
        if log_read.shape[0]> 0:  # Ensure the log file is not empty
            log_read = log_read["date_done"].tolist()
            done_days = [datetime.fromisoformat(x) for x in log_read]

            # Remove already processed dates
            range_to_do = [x for x in range_to_do if x not in done_days]

    # **Ensure `range_to_do` is not empty before proceeding**
    if not range_to_do:
        print("All dates already processed. Exiting.")
        connect.delete()
        sys.exit(0)

    fr_date = min(range_to_do)
    to_date = max(range_to_do)
    vals_to_do = [x.strftime("%Y-%m-%d") for x in range_to_do]

    # **Retrieve AIS Data**
    ais_sample, fr_date, to_date = ais_read_data_range(
        P_ACCESS_KEY, P_SECRET_KEY, spark, connect, h3_int_list=h3_indeces_coast_int,
        dt_from=fr_date.strftime("%Y-%m-%d"), dt_to=to_date.strftime("%Y-%m-%d"), bypass_new_data_test=True
    )

    # **Break if AIS data is empty**
    if ais_sample.isEmpty():
        print("No AIS data to process. Exiting.")
        connect.delete()
        sys.exit(0)

    if done_days:
        ais_sample = ais_sample.withColumn("date_str", F.date_format(F.col("dt_pos_utc"), "yyyy-MM-dd"))
        ais_sample = ais_sample.filter(F.col("date_str").isin(set(vals_to_do)))

    # **Final Check if `ais_sample` is empty after filtering**
    if ais_sample.isEmpty():
        print("No valid data left after filtering. Exiting.")
        connect.delete()
        sys.exit(0)

# Retrieve vessel specifications
versions = af.get_ihs_versions(spark, versions=True)
versions = versions.assign(version=versions.version.apply(str))
versions = spark.createDataFrame(versions[["from_date", "to_date", "version"]])


# Join AIS data with vessel specifications
ais_sample = ais_sample.join(
    versions,
    (ais_sample.dt_pos_utc >= versions.from_date) & (ais_sample.dt_pos_utc <= versions.to_date),
    'left'
).select(ais_sample["*"])


mmsi_values = ais_sample.select("imo", "mmsi", "vessel_type_main", "length", "width", "version").dropDuplicates()
# # Retrieve vessel specifications
mmsi_values = ais_specs_version_ret(mmsi_values, spark)



# Adapt vessel specs for IMO GHG4 compliance
mmsi_values_all = ef.adapted_specs_imo(mmsi_values.toPandas())


mmsi_values_all=spark.createDataFrame(mmsi_values_all)


# Join vessel specs with AIS port records
ais_sample= ais_sample.join(F.broadcast(mmsi_values_all.drop("imo")), on=["mmsi"])


# ### Local emissions compendia for heatmap plots at Panama waters


emissions_local=emissions_ghg_imo(ais_sample,spark)


emissions_local = emissions_local.withColumn("year", F.year("dt_pos_utc")) \
                    .withColumn("month", F.month("dt_pos_utc")) \
                    .withColumn("resolution_id", F.col("H3_int_index_6")) \
                    .withColumn("h3resolution", F.lit(6)) \
                    .select("year", "month", "StandardVesselType", "resolution_id", "h3resolution",
                            "co2_t", "ch4_t", "n2o_t") \
                    .fillna({'co2_t': 0, 'ch4_t': 0, 'n2o_t': 0})

# Convert emission columns from wide to long format using the stack function.
# This will create two new columns: 'emission_type' and 'emission_value'
df_long = emissions_local.selectExpr(
    "year",
    "month",
    "StandardVesselType",
    "resolution_id",
    "h3resolution",
    "stack(3, 'co2_t', co2_t, 'ch4_t', ch4_t, 'n2o_t', n2o_t) as (emission_type, emission_value)"
)

# Group by the desired dimensions and aggregate the emission values.
em_aggregated = df_long.groupBy("year", "month","emission_type", 
                                "StandardVesselType", "h3resolution", "resolution_id") \
                       .agg(F.sum("emission_value").alias("emission_value"))


em_pd=em_aggregated.toPandas()
log_em=pd.DataFrame(vals_to_do,columns=["date_done"])

###Check if a file already exist and concatenate then groupby emissions. Also update the log file to avoid the 
### processed days in the future.
if any("emissions_local_panama_in" in key for key in base_files):
    em_file_name=[x for x in base_files if "emissions_local_panama_in" in x and "log" not in x]

    old_em=s3_file_read(em_file_name[0],P_ACCESS_KEY,P_SECRET_KEY)
    em_pd=pd.concat([old_em,em_pd])
    em_pd=em_pd.groupby(["year", "month","emission_type", 
                        "StandardVesselType", "h3resolution", "resolution_id"]).sum("emission_value")\
                .reset_index().sort_values(by=["year","month"]).reset_index(drop=True)
        
    log_name=[x for x in base_files if "log_emissions_local_panama" in x]
    old_log=s3_file_read(log_name[0],P_ACCESS_KEY,P_SECRET_KEY)
    log_em=pd.concat([old_log,log_em]).reset_index(drop=True)
    

##Write to S3
write_to_s3(em_pd,"emissions_local_panama",P_ACCESS_KEY,P_SECRET_KEY)
write_to_s3(log_em,"emissions_local_panama",P_ACCESS_KEY,P_SECRET_KEY,log=True)


spark.stop()

connect.delete()




