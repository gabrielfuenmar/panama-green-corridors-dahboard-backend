#!/usr/bin/env python
# coding: utf-8

# #### Transits PC

# In[1]:


####---- Kernel default functions
from ais import functions as af
from mtcc_ais_preparation import functions as ef

###----- Python packages
import pandas as pd
import geopandas as gpd
import h3
import os
import numpy as np

#####------Custom functions
from s3_digest import open_s3_session, ais_read_data_range, write_to_s3, s3_file_read
from port_stops_recognition import port_stops_cutoff, get_berth_anchorage_records, merge_stop_segments
from ais_support_func import ais_specs_version_ret, op_phase_recog
from panama_ports_var import tugs, ports_pol, anch_pol, lock_pol, stops
from transits_recognition import transits_prospects, dbscan_pandas
from emissions_generator import emissions_ghg_imo

###-Spot Ocean Spark packages-
import spot_api
from ocean_spark_connect.ocean_spark_session import OceanSparkSession

###---------------
from pyspark.sql.types import IntegerType, LongType, StringType, StructType, StructField, DoubleType,BooleanType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql.window import Window



land_eez = ef.find_folder_csv("polygons", "land_eez_wkt.csv")
panama_canal = ef.find_folder_csv("polygons", "panama_canal.csv")

land_eez=gpd.GeoDataFrame(land_eez,geometry=gpd.GeoSeries.from_wkt(land_eez["WKT_geom"]))
panama_canal=gpd.GeoDataFrame(panama_canal,geometry=gpd.GeoSeries.from_wkt(panama_canal["WKT_geom"]),crs=4326)

# Filter to Panama's EEZ (Exclusive Economic Zone)
count_eez = land_eez[land_eez["TERRITORY1"] == 'Panama']



panama_canal=gpd.GeoDataFrame(panama_canal,geometry=gpd.GeoSeries.from_wkt(panama_canal["WKT_geom"]),crs=4326)



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


##Some polygons fall short below res 10
h3_indeces_canal=list()
h3_indices_canal_int=list()

for ind, row in panama_canal.iterrows():
    h3_inner_list=list(h3.polyfill(row.geometry.__geo_interface__, 10, geo_json_conformant=True))
    
    for h in h3_inner_list:
        h3_indeces_canal.append([row.Name,h3.string_to_h3(h)])
        h3_indices_canal_int.append(h3.string_to_h3(h))



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


# Retrieve AIS data from S3 within a specified time range
### RECUERDA AJUSTAR LAS FECHAS O REMOVERLAS PARA QUE IDENTIFIQUE ULTIMA FECAH DEL S3.
### Because the transits and emissions databases are created at the same time, we can assume that the ais retrieval
### will work both for transits and local emissions
### The folder name should be the start of the file name instead. instead of transit (folder name) it should be transit_pc(file name)
ais_sample,fr_date,to_date = ais_read_data_range(P_ACCESS_KEY, P_SECRET_KEY, spark, connect, h3_int_list=h3_indeces_coast_int, 
                                          file_name="transits_pc_in")


schema = StructType([ \
    StructField("Name",StringType(),True), \
    StructField("H3_int_index_10",LongType(),True)])
                     
h3_indeces_canal=spark.createDataFrame(h3_indeces_canal,schema=schema).dropDuplicates(["H3_int_index_10"])



ais_sample=ais_sample.join(h3_indeces_canal,["H3_int_index_10"])



ais_sample=ais_sample.filter(~F.col("vessel_name").isin(tugs))



ais_sample=ais_sample.withColumn("Name",F.when(F.col("Name").isin(['Panama Canal - Atlantic - West Breakwater Inner Anchorage',
                                                           'Panama Canal - Atlantic - East Breakwater Inner Anchorage',
                                                            'Atlantic - West Breakwater Light Outer Anchorage']),"Atlantic Anchorage").otherwise(F.col("Name")))


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


# #### Transits prospects


mmsi_ais=transits_prospects(ais_sample,unique_id="mmsi")


imo_ais=transits_prospects(ais_sample,unique_id="imo")



#### Problema con draught en el unionbyname. etc.....


ais_transits=mmsi_ais.unionByName(imo_ais)
ais_transits=ais_transits.drop("vessel_type_main", "length", "width", "version","cog","heading",
                               'H3_int_index_15','from_date', 'to_date')


df_val_in_anch=ais_transits.filter(F.col("Name").isin(anch_pol))
df_val_in_stops=ais_transits.filter(F.col("Name").isin(ports_pol+["Gatun Anchorage"]))
df_val=ais_transits.filter(~F.col("Name").isin(anch_pol+ports_pol+["Gatun Anchorage"]))



window_spec_an_prt = Window.partitionBy("imo", "mmsi","segr", "flag").orderBy("dt_pos_utc") \
                           .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)


df_val_in_anch = df_val_in_anch.filter(F.col("sog") < 3) \
    .withColumn("anchoring_in", F.first("dt_pos_utc").over(window_spec_an_prt)) \
    .withColumn("anchoring_out", F.last("dt_pos_utc").over(window_spec_an_prt)) \
    .dropDuplicates(["imo", "mmsi", "segr", "flag"])

df_val_in_anch=df_val_in_anch.withColumn("anch_area", F.col("Name"))\
                             .select("imo", "flag", "mmsi", "anchoring_in", "anchoring_out", "anch_area")

df_val_in_stops = df_val_in_stops.filter(
                                        ((F.col("Name") == "Gatun Anchorage") & (F.col("sog") < 3)) |
                                        ((F.col("Name") != "Gatun Anchorage") & (F.col("sog") < 1))
                                    )\
                                     .withColumn("stop_time_in", F.first("dt_pos_utc").over(window_spec_an_prt))\
                                     .withColumn("stop_time_out", F.last("dt_pos_utc").over(window_spec_an_prt))

##Stops_in_ between transits
df_val_in_stops=df_val_in_stops.dropDuplicates(["imo", "mmsi", "segr", "flag"])
df_val_in_stops = df_val_in_stops.withColumn("stop_area", F.col("Name")) \
                                .select("imo", "mmsi", "flag", "stop_time_in", "stop_time_out", "stop_area")


# ### Transit refinement

# Define window specs
window_spec_first = Window.partitionBy("imo", "mmsi", "flag").orderBy("dt_pos_utc") \
                          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

window_mode = Window.partitionBy("imo", "mmsi", "flag").orderBy(F.desc("draught_freq"))

# Filter for lock areas
df_locks = df_val.filter(F.col("Name").isin(lock_pol))

# --------------------------
# Mode Draught per Transit
# --------------------------
df_draught_mode = df_locks.groupBy("imo", "mmsi", "flag", "draught") \
    .agg(F.count("*").alias("draught_freq")) \
    .withColumn("rank", F.row_number().over(window_mode)) \
    .filter(F.col("rank") == 1) \
    .select("imo", "mmsi", "flag", "draught")

# --------------------------
# Lock Times & Directions
# --------------------------
df_locks_summary = df_locks.withColumn("lock_in", F.first("dt_pos_utc").over(window_spec_first)) \
                           .withColumn("lock_out", F.last("dt_pos_utc").over(window_spec_first)) \
                           .withColumn("first_lock_name", F.first("Name").over(window_spec_first)) \
                           .withColumn("last_lock_name", F.last("Name").over(window_spec_first)) \
                           .select("imo", "mmsi", "flag", "neo_transit", "lock_in", "lock_out",
                                   "first_lock_name", "last_lock_name") \
                           .dropDuplicates(["imo", "mmsi", "flag"])  # still OK here

# Add direction column
df_locks_summary = df_locks_summary.withColumn(
    "direction",
    F.when(
        F.col("first_lock_name").isin(["Miraflores Locks Original", "Cocoli Locks"]),
        F.lit("North")
    ).otherwise(F.lit("South"))
)

# --------------------------
# Join Mode Draught
# --------------------------
df_locks = df_locks_summary.join(
    df_draught_mode,
    on=["imo", "mmsi", "flag"],
    how="left"
)

# --------------------------
# Join with Anchoring Data
# --------------------------
df_joined = df_locks.join(
    df_val_in_anch,
    on=["imo", "mmsi", "flag"],
    how="left"
)


test_dir=df_val_in_stops.dropDuplicates(["imo", "mmsi", "flag"]).withColumn("direct_transit",F.lit(0))
df_joined = df_joined.join(
            test_dir,
            on=["imo", "mmsi", "flag"],
            how="left"
            )
df_joined = df_joined.withColumn(
                        "direct_transit",
                        F.when(F.col("direct_transit") == 0, F.lit(0)).otherwise(F.lit(1))
                                )
df_joined=df_joined.select("imo","mmsi","flag","lock_in","lock_out","anchoring_in","anchoring_out",
                           "direct_transit","direction","neo_transit","draught")


###Pandas transfomr
df_joined=df_joined.toPandas()



df_joined=pd.merge(df_joined,mmsi_values_all,on="mmsi",how="left")


# ### Writing Transits_info


write_to_s3(df_joined,"transits_pc",P_ACCESS_KEY,P_SECRET_KEY,from_date=fr_date,to_date=to_date)



write_to_s3(df_val_in_stops.toPandas(),"stops_in_transits_pc",P_ACCESS_KEY,P_SECRET_KEY,from_date=fr_date,to_date=to_date)



spark.stop()


connect.delete()





