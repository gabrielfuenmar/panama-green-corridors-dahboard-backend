# -*- coding: utf-8 -*-
"""
Created on Wed Jan 29 15:51:00 2025

@author: gabri
"""

####---- Kernel default functions
from ais import functions as af
from mtcc_ais_preparation._vessel_specs_ghg4 import find_folder_csv, adapted_specs_imo
from mtcc_ais_preparation._interpolation import pos_interp

###----- Python packages
import pandas as pd
import geopandas as gpd
import h3
import os

#####------Custom functions
from s3_digest import open_s3_session, ais_read_data_range
from port_stops_recognition import port_stops_cutoff, get_berth_anchorage_records, merge_stop_segments
from ais_support_func import ais_specs_version_ret, op_phase_recog

###-Spot Ocean Spark packages-
import spot_api
from ocean_spark_connect.ocean_spark_session import OceanSparkSession

###---------------
from pyspark.sql.types import IntegerType, LongType, StringType, StructType, StructField, DoubleType
from pyspark.sql import functions as F

# Load Panama-specific polygon data
land_eez = find_folder_csv("polygons", "land_eez_wkt.csv")
panama_canal = find_folder_csv("polygons", "panama_canal.csv")

# Filter to Panama's EEZ (Exclusive Economic Zone)
count_eez = land_eez[land_eez["TERRITORY1"] == 'Panama']

# Extract unique H3 indices from polygons
h3_indeces_int = set()
h3_indeces_alpha = set()

for ind, row in count_eez.iterrows():
    h3_inner_list = list(h3.polyfill(row.geometry.__geo_interface__, 5, geo_json_conformant=True))
    h3_indeces_alpha.update(h3_inner_list)
    h3_inner_list = [h3.string_to_h3(h) for h in h3_inner_list]
    h3_indeces_int.update(h3_inner_list)

h3_indeces_int = list(h3_indeces_int)

# Extract H3 indices for the Panama Canal with higher resolution (10)
h3_indeces_panama = []
h3_indeces_panama_int = []

for ind, row in panama_canal.iterrows():
    h3_inner_list = list(h3.polyfill(row.geometry.__geo_interface__, 10, geo_json_conformant=True))
    for h in h3_inner_list:
        h3_indeces_panama.append([row.Name, h3.string_to_h3(h)])
        h3_indeces_panama_int.append(h3.string_to_h3(h))

# Connect to Spark session via Spot Ocean API
spot_token = os.environ.get('SPOT_TOKEN')
connect = spot_api.spot_connect(spot_token)
connect.submit()
connect.wait_til_running()
spark = connect.spark_session()

# Read AWS S3 credentials from environment variables
p_access_key = os.environ.get('S3_ACCESS_KEY_MTCC')
p_secret_key = os.environ.get('S3_SECRET_KEY_MTCC')

# Open S3 session
bucket_list = open_s3_session(p_access_key, p_secret_key)

# Retrieve AIS data from S3 within a specified time range
ais_sample = ais_read_data_range(p_access_key, p_secret_key, spark, h3_int_list=h3_indeces_panama_int, dt_from="2025-01-11", dt_to="2025-01-12", folder="stops_all")

# Get IHS versions and join with AIS data
versions = af.get_ihs_versions(spark, versions=True)
versions = versions.assign(version=versions.version.apply(str))
versions = spark.createDataFrame(versions[["from_date", "to_date", "version"]])

ais_sample = ais_sample.join(
    versions,
    (ais_sample.dt_pos_utc >= versions.from_date) & (ais_sample.dt_pos_utc <= versions.to_date),
    'left'
).select(ais_sample["*"])

# Filter distinct vessel records
mmsi_values = ais_sample.select("imo", "mmsi", "vessel_type_main", "length", "width", "version").dropDuplicates()

# Retrieve vessel specifications
mmsi_values = ais_specs_version_ret(mmsi_values, spark)

# Convert H3 indices of Panama Canal into a Spark DataFrame
schema = StructType([StructField("Name", StringType(), True), StructField("H3_int_index_10", LongType(), True)])
h3_indeces_panama = spark.createDataFrame(h3_indeces_panama, schema=schema).dropDuplicates(["H3_int_index_10"])

# Filter AIS data to only include vessels inside the Panama Canal
ais_sample_ports = ais_sample.join(h3_indeces_panama, ["H3_int_index_10"])

# Detect port stops and anchorage periods
ais_sample_ports = port_stops_cutoff(ais_sample_ports)

# Filter MMSI values related to port stops
port_mmsi = ais_sample_ports.select("mmsi").dropDuplicates(["mmsi"])

# Merge MMSI values with vessel specs for port-related vessels
mmsi_values_ports = mmsi_values.join(
    F.broadcast(port_mmsi), 
    on="mmsi", 
    how="inner"
)

# Adapt vessel specs for IMO GHG4 compliance
mmsi_values_ports = adapted_specs_imo(mmsi_values_ports.toPandas())
mmsi_values_ports = spark.createDataFrame(mmsi_values_ports)

# Join vessel specs with AIS port records
ais_sample_ports = ais_sample_ports.join(F.broadcast(mmsi_values_ports.drop("imo")), on=["mmsi"])
ais_sample_ports = ais_sample_ports.withColumn("load", F.col("sog") / F.col("Speed"))

# Detect operational phases (Berth, Anchorage, etc.)
ais_sample_ports = op_phase_recog(ais_sample_ports, spark)

# Merge Anchorage and Berth segments separately
ais_sample_ports = merge_stop_segments(ais_sample_ports, reference_phase="Anchorage", protected_phase="Berth", time_limit="1 hour")
ais_sample_ports = merge_stop_segments(ais_sample_ports, reference_phase="Berth", protected_phase="Anchorage", time_limit="1 hour")

# Retrieve berth and anchorage records
##### !!!! Falta iun refinement en esta funcion. Genera records del port call y todos los posibles anchoring precedentes al port call.
ais_sample_ports = get_berth_anchorage_records(ais_sample_ports)

# Get the date range of the AIS sample
to_date = ais_sample.select(F.max("dt_pos_utc")).collect()[0][0].strftime("%Y-%m-%d")
from_date = ais_sample.select(F.min("dt_pos_utc")).collect()[0][0].strftime("%Y-%m-%d")


# Save the processed data (if needed)
# ais_sample_ports.to_parquet("port_stops_{}_{}.parquet".format(from_date, to_date))

# Terminate Spark session
connect.delete()
