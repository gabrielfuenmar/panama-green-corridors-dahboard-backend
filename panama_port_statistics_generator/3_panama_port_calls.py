#!/usr/bin/env python
# coding: utf-8

# #### Panama Port Calls

####---- Kernel default functions
from ais import functions as af
from mtcc_ais_preparation import functions as ef

###----- Python packages
import pandas as pd
import geopandas as gpd
import h3
import os
import numpy as np
import re
from datetime import datetime
import sys

#####------Custom functions
from s3_digest import open_s3_session, ais_read_data_range, write_to_s3, s3_file_read
from port_stops_recognition import port_stops_cutoff, get_berth_anchorage_records, merge_stop_segments
from ais_support_func import ais_specs_version_ret, op_phase_recog

###-Spot Ocean Spark packages-
import spot_api
from ocean_spark_connect.ocean_spark_session import OceanSparkSession

###---------------
from pyspark.sql.types import IntegerType, LongType, StringType, StructType, StructField, DoubleType,BooleanType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql.window import Window


land_eez = ef.find_folder_csv("polygons", "land_eez_wkt.csv")
ports_panama = pd.read_csv("ports_panama.csv")

land_eez=gpd.GeoDataFrame(land_eez,geometry=gpd.GeoSeries.from_wkt(land_eez["WKT_geom"]),crs=4326)
ports_panama=gpd.GeoDataFrame(ports_panama,geometry=gpd.GeoSeries.from_wkt(ports_panama["WKT_geom"]),crs=4326)

# Filter to Panama's EEZ (Exclusive Economic Zone)
count_eez = land_eez[land_eez["TERRITORY1"] == 'Panama']


##Use set to remove duplicates, then transform to list to pass to af.get_ais
h3_indeces_int=set()
h3_indeces_alpha=set()

for ind, row in count_eez.iterrows():
    h3_inner_list=list(h3.polyfill(row.geometry.__geo_interface__, 5, geo_json_conformant=True))
    h3_indeces_alpha.update(h3_inner_list)
    
    ##Now in integer form
    h3_inner_list=[h3.string_to_h3(h) for h in h3_inner_list]
    h3_indeces_int.update(h3_inner_list)
    
h3_indeces_coast_int=list(h3_indeces_int)


h3_indeces_ports=list()

for ind, row in ports_panama.iterrows():
    h3_inner_list=list(h3.polyfill(row.geometry.__geo_interface__, 10, geo_json_conformant=True))
    
    for h in h3_inner_list:
        h3_indeces_ports.append([row.Name,h3.string_to_h3(h)])

# Connect to Spark session via Spot Ocean API
SPOT_TOKEN = os.environ.get("SPOT_TOKEN")
connect = spot_api.spot_connect(SPOT_TOKEN)
connect.submit()
connect.wait_til_running()
spark = connect.spark_session()



##Change with environmental variables

P_ACCESS_KEY=os.environ.get("P_ACCESS_KEY")
P_SECRET_KEY=os.environ.get("P_SECRET_KEY")



base_files=open_s3_session(P_ACCESS_KEY,P_SECRET_KEY)



###Find the track of the transit_pc. Based on file names.


dates = []
dates_ports = []

if any("transits_pc_in" in key for key in base_files):
    file_name = [x for x in base_files if "transits_pc_in" in x and "stops" not in x]
    
    for file in file_name:
        match = re.search(r"transits_pc_in_(\d{4}-\d{2}-\d{2})&(\d{4}-\d{2}-\d{2})\.parquet", file)
        if match:
            dates.append([match.group(1),  # Start date before '&'
                         match.group(2)])    # End date after '&'
                         
if any("port_stops_in" in key for key in base_files): 
    file_port_name = [x for x in base_files if "port_stops_in" in x]

    
    for file in file_port_name:
        port_match = re.search(r"port_stops_in_(\d{4}-\d{2}-\d{2})&(\d{4}-\d{2}-\d{2})\.parquet", file)
        if match:
            dates_ports.append([port_match.group(1),  # Start date before '&'
                         port_match.group(2)])    # End date after '&'

valid_dates=[x for x in dates if x not in dates_ports]


if not valid_dates:
    print("All dates already processed. Exiting.")
    connect.delete()
    sys.exit(0)


fr_date=valid_dates[0][0]
to_date=valid_dates[0][1]

# Retrieve AIS data from S3 within a specified time range
### RECUERDA AJUSTAR LAS FECHAS O REMOVERLAS PARA QUE IDENTIFIQUE ULTIMA FECAH DEL S3.
### Because the transits and emissions databases are created at the same time, we can assume that the ais retrieval
### will work both for transits and local emissions
### The folder name should be the start of the file name instead. instead of transit (folder name) it should be transit_pc(file name)
ais_sample,fr_date,to_date = ais_read_data_range(P_ACCESS_KEY, P_SECRET_KEY, spark, connect, h3_int_list=h3_indeces_coast_int, 
                                         dt_from=fr_date, dt_to=to_date, bypass_new_data_test=True)


schema = StructType([ \
    StructField("Name",StringType(),True), \
    StructField("H3_int_index_10",LongType(),True)])
                     
h3_indeces_ports=spark.createDataFrame(h3_indeces_ports,schema=schema).dropDuplicates(["H3_int_index_10"])

ais_sample=ais_sample.join(h3_indeces_ports,["H3_int_index_10"],how="left")


versions = af.get_ihs_versions(spark, versions=True)
versions = versions.assign(version=versions.version.apply(str))
versions = spark.createDataFrame(versions[["from_date", "to_date", "version"]])



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


mmsi_values_all=mmsi_values_all[["mmsi","ais_loa","ais_beam","StandardVesselType","GrossTonnage"]]
mmsi_values_all=mmsi_values_all.rename(columns={"ais_loa":"length","ais_beam":"beam"})


# #### Port prospects



ais_sample=ais_sample.drop("vessel_type_main", "length", "width", "version","cog","heading",
                            'H3_int_index_15','from_date', 'to_date')



df_val_in_ports=ais_sample.filter(F.col("Name").isNotNull())
df_val=ais_sample.filter(F.col("Name").isNull())


### Berth


# In[ ]:


# 1. Port events (sog <= 1)
# ===============================
window_spec = Window.partitionBy("imo", "mmsi").orderBy("dt_pos_utc")

df_val_in_ports = df_val_in_ports.filter(F.col("sog") <= 1)

df_val_in_ports = df_val_in_ports.withColumn("lag_name", F.lag("Name").over(window_spec)) \
                                 .withColumn("lag_dt_pos_utc", F.lag("dt_pos_utc").over(window_spec)) \
                                 .withColumn(
                                     "is_new_group",
                                     ((F.col("Name") != F.col("lag_name")) |
                                      (F.unix_timestamp("dt_pos_utc") - F.unix_timestamp("lag_dt_pos_utc") > 86400)
                                      ).cast("int")
                                 ) \
                                 .withColumn("group_id", F.sum("is_new_group").over(window_spec.rowsBetween(Window.unboundedPreceding, 0))) \
                                 .withColumn("anch1_port2", F.lit(2)) \
                                 .drop("lag_name", "lag_dt_pos_utc", "is_new_group")

# Group metadata for port
ports = df_val_in_ports.groupBy("imo", "mmsi", "group_id").agg(
    F.first("Name", ignorenulls=True).alias("port_name"),
    F.min("dt_pos_utc").alias("port_start"),
    F.max("dt_pos_utc").alias("port_end")
).withColumn("anch1_port2", F.lit(2))

# Rename group_id for clarity
ports = ports.withColumnRenamed("group_id", "port_group_id")


ports = ports.withColumn(
    "port_duration_sec",
    F.unix_timestamp("port_end") - F.unix_timestamp("port_start")
)

# Filter for port groups lasting at least 1 hour
ports = ports.filter(F.col("port_duration_sec") >= 3600).drop("port_duration_sec")


###Anchoring



# ===============================
# 2. Anchorage events (1 < sog <= 3)
# ===============================
df_val_in = df_val.filter((F.col("sog") > 1) & (F.col("sog") <= 3))

df_val_in = df_val_in.withColumn("lag_dt_pos_utc", F.lag("dt_pos_utc").over(window_spec)) \
                     .withColumn(
                         "is_new_group",
                         ((F.unix_timestamp("dt_pos_utc") - F.unix_timestamp("lag_dt_pos_utc") > 3600)
                          | F.col("lag_dt_pos_utc").isNull()).cast("int")
                     ) \
                     .withColumn("group_id", F.sum("is_new_group").over(window_spec.rowsBetween(Window.unboundedPreceding, 0))) \
                     .drop("lag_dt_pos_utc", "is_new_group")

# Group metadata for anchorage
anch = df_val_in.groupBy("imo", "mmsi", "group_id").agg(
    F.min("dt_pos_utc").alias("anch_start"),
    F.max("dt_pos_utc").alias("anch_end")
).withColumn("port_name", F.lit("anch")) \
 .withColumn("anch1_port2", F.lit(1)) \
 .withColumnRenamed("group_id", "anchorage_group_id")


# ===============================
# 3. Combine for sequential analysis
# ===============================
combined = ports.select("imo", "mmsi", F.col("port_group_id").alias("group_id"), "port_name", "port_start", "port_end", "anch1_port2") \
    .unionByName(
        anch.select("imo", "mmsi", F.col("anchorage_group_id").alias("group_id"), "port_name", F.col("anch_start").alias("port_start"), F.col("anch_end").alias("port_end"), "anch1_port2")
    )

w = Window.partitionBy("imo", "mmsi").orderBy("port_start")

combined = combined.withColumn("next_type", F.lead("anch1_port2").over(w)) \
                   .withColumn("next_group_id", F.lead("group_id").over(w)) \
                   .withColumn("next_port_start", F.lead("port_start").over(w)) \
                   .withColumn("next_port_name", F.lead("port_name").over(w)) \
                   .withColumn("time_to_next", F.unix_timestamp("next_port_start") - F.unix_timestamp("port_end"))


# ===============================
# 4. Find anchorage → port connections
# ===============================
anch_to_port_links = combined.filter(
    (F.col("anch1_port2") == 1) &  # current = anchorage
    (F.col("next_type") == 2) &    # next = port
    (F.col("time_to_next") <= 3600)  # ≤ 1 hourt
).select(
    "imo", "mmsi",
    F.col("group_id").alias("anchorage_group_id"),
    F.col("port_end").alias("anch_end"),
    F.col("port_start").alias("anch_start"),
    F.col("next_group_id").alias("port_group_id")
)

# ===============================
final_output = ports.join(
    anch_to_port_links,
    on=["imo", "mmsi", "port_group_id"],
    how="left"
).select(
    "imo",
    "mmsi",
    "port_group_id",
    "port_name",
    "port_start",
    "port_end",
    "anchorage_group_id",
    "anch_start",
    "anch_end"
)



final_output=final_output.toPandas()


final_output=pd.merge(final_output,mmsi_values_all,on="mmsi",how="left")


# ### Writing Transits_info


write_to_s3(final_output,"port_stops",P_ACCESS_KEY,P_SECRET_KEY,from_date=fr_date,to_date=to_date)


connect.delete()





