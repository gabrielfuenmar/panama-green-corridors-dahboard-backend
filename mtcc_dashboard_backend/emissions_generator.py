# -*- coding: utf-8 -*-
"""
Created on Mon Mar 10 22:36:16 2025

@author: gabri
"""

from pyspark.sql import functions as F
from pyspark.sql.types import LongType,StringType,IntegerType
from pyspark.sql.window import Window
import h3
from ais_support_func import  op_phase_recog
from mtcc_ais_preparation import functions as ef


def emissions_ghg_imo(df_spark_w_adapted_specs,spark):
    """
    Calculate greenhouse gas (GHG) emissions for vessels using AIS data, under IMO GHG4 methodology.
    The function estimates emissions based on vessel speed, operational phase, and engine power.
    
    Parameters:
    df_spark_w_adapted_specs (DataFrame): A PySpark DataFrame containing vessel specifications and movement data.
    spark (SparkSession): The active Spark session.
    
    Returns:
    DataFrame: A PySpark DataFrame containing estimated CO2, CH4, and N2O emissions per vessel.
    """
    # Calculate vessel load factor
    df_spark_w_adapted_specs= df_spark_w_adapted_specs.withColumn("load", F.col("sog") / F.col("Speed"))

    # Identify operational phase
    emissions_local=op_phase_recog(df_spark_w_adapted_specs,spark)

    # Define window specification
    window_spec = Window.partitionBy("imo").orderBy("dt_pos_utc")
    
    # Calculate time difference in hours
    emissions_local = emissions_local.withColumn(
        "freq",
        (F.unix_timestamp(F.col("dt_pos_utc")) - F.unix_timestamp(F.lag("dt_pos_utc").over(window_spec))) / 3600
    )
    
    # Remove records where time difference exceeds 1 hour
    emissions_local = emissions_local.withColumn("freq", F.when(F.col("freq") > 1, None).otherwise(F.col("freq")))
    
    emissions_local=emissions_local.filter(F.col("freq").isNotNull())
    
     # Load auxiliary tables for emission calculations
    ae_ab_pwr=spark.createDataFrame(ef.find_folder_csv("ghg4_tables","table_17.csv"))

    # Compute auxiliary engine (AE) and auxiliary boiler (AB) power usage
    emissions_local = emissions_local.withColumn(
                                        "ae_pow",
                                        F.when(F.col("Powerkwmax") <= 150, 0)  # Explicitly handling this case
                                         .when(F.col("Powerkwmax").between(151, 500), F.lit(0.05) * F.col("Powerkwmax") * F.col("freq"))
                                         .otherwise(None)  # Default case
                                    ).withColumn(
                                        "ab_pow",
                                        F.when(F.col("Powerkwmax") <= 150, 0).otherwise(None)  # Handles only the specified condition
                                    )

    # Merge auxiliary engine power table
    emissions_local=emissions_local.join(ae_ab_pwr,["StandardVesselType","imobin"],"left")
    
     # Adjust AE and AB power usage based on operational phase
    emissions_local=emissions_local.withColumn("ae_ene",F.when(F.col("ae_pow").isNull(),
                                                               F.when(F.col("op_phase").isin(["Slow transit","Normal cruising"]),F.col("ae_sea")*F.col("freq"))\
                                                               .otherwise(F.when(F.col("op_phase")=="Berth",F.col("ae_berth")*F.col("freq"))\
                                                               .otherwise(F.when(F.col("op_phase")=="Manoeuvring",F.col("ae_man")*F.col("freq"))\
                                                               .otherwise(F.when(F.col("op_phase")=="Anchorage",F.col("ae_anch")*F.col("freq"))\
                                                               .otherwise(F.col("ae_sea")*F.col("freq"))))))\
                                               .otherwise(F.col("ae_pow")))\
                                    .withColumn("ab_ene",F.when(F.col("ab_pow").isNull(),
                                                               F.when(F.col("op_phase").isin(["Slow transit","Normal cruising"]),F.col("ab_sea")*F.col("freq"))\
                                                               .otherwise(F.when(F.col("op_phase")=="Berth",F.col("ab_berth")*F.col("freq"))\
                                                               .otherwise(F.when(F.col("op_phase")=="Manoeuvring",F.col("ab_man")*F.col("freq"))\
                                                               .otherwise(F.when(F.col("op_phase")=="Anchorage",F.col("ab_anch")*F.col("freq"))\
                                                               .otherwise(F.col("ab_sea")*F.col("freq"))))))\
                                               .otherwise(F.col("ab_pow")))
                                        
   # Load weather and fouling factors                                     
    weath_foul=spark.createDataFrame(ef.find_folder_csv("ghg4_tables","table_44.csv"))

    emissions_local=emissions_local.join(weath_foul,["StandardVesselType","imobin"],"left")

    # Compute main engine power usage
    emissions_local=emissions_local.withColumn("me_pow",F.when(F.col("op_phase").isin(["Berth","Anchorage"]),0)\
                                               .otherwise(F.when(F.col("StandardVesselType")=="Cruise",
                                                          (F.lit(0.70)*F.col("Powerkwmax")*((F.col("draught")/F.col("SummerDraught"))**0.66)*F.col("load")**3)/(F.col("weather")*F.col("fouling")))\
                                                          .otherwise(F.when(F.col("StandardVesselType")=="Container",
                                                                            F.when(F.col("imobin").isin([8,9]),
                                                                                   (F.lit(0.75)*F.col("Powerkwmax")*((F.col("draught")/F.col("SummerDraught"))**0.66)*F.col("load")**3)/(F.col("weather")*F.col("fouling")))\
                                                                            .otherwise((F.col("Powerkwmax")*((F.col("draught")/F.col("SummerDraught"))**0.66)*F.col("load")**3)/(F.col("weather")*F.col("fouling"))))\
                                                                     .otherwise((F.col("Powerkwmax")*((F.col("draught")/F.col("SummerDraught"))**0.66)*F.col("load")**3)/(F.col("weather")*F.col("fouling")))
                                                                    )))    
    ###Transform Power to Energy
    emissions_local=emissions_local.withColumn("me_ene",F.col("me_pow")*F.col("freq"))
    
    # Load fuel consumption factors

    me_sf=spark.createDataFrame(ef.find_folder_csv("ghg4_tables","table_19_1.csv"))

    emissions_local=emissions_local.join(me_sf,["meType","fuel"],"left")
     
    ##Creates a column with Year of Build as an integer variable
    emissions_local=emissions_local.withColumn("YearOfBuild",F.col("DateOfBuild").cast(StringType()).substr(1,4).cast(IntegerType()))
    
    # Compute specific fuel consumption for main engines
    emissions_local=emissions_local.withColumn("sfc_me",F.when(F.col("meType").isin(["SSD","MSD","HSD"]),
                                                      F.when(F.col("YearOfBuild")<=1983,F.col("_83")*(F.lit(0.455)*(F.col("load")**2)-F.lit(0.710)*F.col("load")+F.lit(1.280)))\
                                                      .otherwise(F.when(F.col("YearOfBuild").between(1984,2000),F.col("84_2000")*(F.lit(0.455)*(F.col("load")**2)-F.lit(0.710)*F.col("load")+F.lit(1.280)))\
                                                      .otherwise(F.when(F.col("YearOfBuild")>=2001,F.col("2001_")*(F.lit(0.455)*(F.col("load")**2)-F.lit(0.710)*F.col("load")+F.lit(1.280)))\
                                                                )))\
                                               .otherwise(F.when(F.col("YearOfBuild")<=1983,F.col("_83"))\
                                               .otherwise(F.when(F.col("YearOfBuild").between(1984,2000),F.col("84_2000"))\
                                               .otherwise(F.when(F.col("YearOfBuild")>=2001,F.col("2001_"))))))
                
    ae_ab_sf=spark.createDataFrame(ef.find_folder_csv("ghg4_tables","table_19_2.csv"))

    ae_sf=ae_ab_sf.filter(F.col("meType")=="AE").drop("meType")
    ab_sf=ae_ab_sf.filter(F.col("meType")=="AB").drop("meType")

    ##AE fuel consumption
    emissions_local=emissions_local.join(ae_sf,["fuel"],how="left")
    
    emissions_local=emissions_local.withColumn("sfc_ae",F.when(F.col("YearOfBuild")<=1983,F.col("ae_ab_83"))\
                                               .otherwise(F.when(F.col("YearOfBuild").between(1984,2000), F.col("ae_ab_84_2000"))\
                                               .otherwise(F.when(F.col("YearOfBuild")>=2001, F.col("ae_ab_2001_"))\
                                               .otherwise(F.lit(0)))))
    
    
    emissions_local=emissions_local.drop(*("ae_ab_83","ae_ab_84_2000","ae_ab_2001_"))
    
    ##AB fuel consumption
    emissions_local=emissions_local.join(ab_sf,["fuel"],how="left")
    
    emissions_local=emissions_local.withColumn("sfc_ab",F.when(F.col("YearOfBuild")<=1983,F.col("ae_ab_83"))\
                                               .otherwise(F.when(F.col("YearOfBuild").between(1984,2000), F.col("ae_ab_84_2000"))\
                                               .otherwise(F.when(F.col("YearOfBuild")>=2001, F.col("ae_ab_2001_"))\
                                               .otherwise(F.lit(0)))))
        
    emissions_local=emissions_local.withColumn("me_con",F.col("me_ene")*F.col("sfc_me"))\
                           .withColumn("ae_con",F.col("ae_ene")*F.col("sfc_ae"))\
                           .withColumn("ab_con",F.col("ab_ene")*F.col("sfc_ab"))\
                            .withColumn("pilot_con",F.col("me_ene")*F.col("pilot_mdo"))  ###Pilot MDO for LNG fuel

                               
    emissions_local=emissions_local.withColumn("pilot_con",F.when(F.col("pilot_con").isNull(),0).otherwise(F.col("pilot_con")))

    ###Load emission factor for CO2 from fuel
    ef_co2=spark.createDataFrame(ef.find_folder_csv("ghg4_tables","table_21.csv")) 
    
    emissions_local=emissions_local.join(ef_co2,["fuel"],"left")
                                               
    emissions_local=emissions_local.withColumn("co2_g",F.col("me_con")*F.col("ef_co2")+\
                                                F.col("ae_con")*F.col("ef_co2")+\
                                                F.col("ab_con")*F.col("ef_co2")+\
                                                F.col("pilot_con")*F.lit(3.206))
    
    ##Load emission factor from energy to CH4
    ef_ch4=spark.createDataFrame(ef.find_folder_csv("ghg4_tables","table_55_56.csv"))
    ef_ch4_ae=ef_ch4.filter(F.col("meType")=="AE").drop("meType").withColumnRenamed("ef_ch4","ef_ch4_ae")
    ef_ch4_ab=ef_ch4.filter(F.col("meType")=="AB").drop("meType").withColumnRenamed("ef_ch4","ef_ch4_ab")
    
    emissions_local=emissions_local.join(ef_ch4,["meType","fuel"],"left")\
                                    .join(ef_ch4_ae,["fuel"],"left")\
                                    .join(ef_ch4_ab,["fuel"],"left")
    
    ##Low load factor
    emissions_local=emissions_local.withColumn("ef_ch4",F.when(F.col("load")<0.02,F.lit(21.18))\
                                               .otherwise(F.when(F.col("load").between(0.02,0.1),2.18)\
                                               .otherwise(F.when(F.col("load").between(0.1001,0.2),1.0).otherwise(F.col("ef_ch4")))))
                                                      

    emissions_local=emissions_local.withColumn("ch4_g",F.col("me_ene")*F.col("ef_ch4")+\
                                               F.col("ae_ene")*F.col("ef_ch4_ae")+\
                                               F.col("ab_ene")*F.col("ef_ch4_ab"))
        
    ef_n2o=spark.createDataFrame(ef.find_folder_csv("ghg4_tables","table_59_60.csv"))
    ef_n2o_ae=ef_n2o.filter(F.col("meType")=="AE").drop("meType").withColumnRenamed("ef_n2o","ef_n2o_ae")
    ef_n2o_ab=ef_n2o.filter(F.col("meType")=="AB").drop("meType").withColumnRenamed("ef_n2o","ef_n2o_ab")
    
    emissions_local=emissions_local.join(ef_n2o,["meType","fuel"],"left")\
                                    .join(ef_n2o_ae,["fuel"],"left")\
                                    .join(ef_n2o_ab,["fuel"],"left")
    
    ##Adjust energy use on low load factor
    emissions_local=emissions_local.withColumn("ef_n2o",F.when(F.col("load")<0.02,F.lit(4.63))\
                                               .otherwise(F.when(F.col("load").between(0.02,0.1),F.lit(1.22))\
                                               .otherwise(F.when(F.col("load").between(0.1001,0.2),1.0).otherwise(F.col("ef_n2o")))))
                                                      

    emissions_local=emissions_local.withColumn("n2o_g",F.col("me_ene")*F.col("ef_n2o")+\
                                               F.col("ae_ene")*F.col("ef_n2o_ae")+\
                                               F.col("ab_ene")*F.col("ef_n2o_ab"))
    # Convert emissions from grams to tonnes
    emissions_local=emissions_local.withColumn("n2o_t",F.col("n2o_g")*F.lit(1e-6))\
                            .withColumn("co2_t",F.col("co2_g")*F.lit(1e-6))\
                            .withColumn("ch4_t",F.col("ch4_g")*F.lit(1e-6))
    return emissions_local