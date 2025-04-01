"""
Functions to support the transits recognition of vessels transitinig the Panama Canal
Gabriel Fuentes gabriel.fuentes@nhh.no
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from panama_ports_var import  ports_pol, anch_pol, stops
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN

def transits_prospects(ais_sample_pc,unique_id="imo"):
    """
    Process AIS sample data to identify transit prospects.
    The function segments vessel movements into transit groups based on anchoring locations,
    time gaps, and specific navigation conditions.
    
    Parameters:
    ais_sample_pc (DataFrame): A PySpark DataFrame containing AIS data.
    unique_id (str, optional): The identifier type for the vessel. Defaults to "imo".

    Returns:
    DataFrame: Processed DataFrame with identified transits and associated attributes.
    """
    if unique_id=="imo":
    ##MMSI unique number record
        id_val=ais_sample_pc.filter(F.col("imo").isNotNull())
    else:
        id_val=ais_sample_pc.filter(F.col("imo").isNull())
        unique_id="mmsi"
    
    w_id = Window.partitionBy(unique_id).orderBy(F.col("dt_pos_utc"))
    id_val=id_val.withColumn("row",F.row_number().over(w_id)) ###Exported id_val for mmsi driven numbers
    
    ##Index used at vessel_specs
    mmsi_ind=id_val.filter(F.col("row") == 1).drop("row")
    
    ##Get time difference between order rows groupby mmsi
    id_val=id_val.withColumn("dt_pos_utc_lag",F.lag(F.col("dt_pos_utc")).over(w_id))\
                     .withColumn("Name_lag",F.lag(F.col("Name")).over(w_id))
    
    timeFmt = "yyyy-MM-dd' 'HH:mm:ss.S"                             
    id_val = id_val.withColumn("time_diff", F.when(F.isnull(F.unix_timestamp("dt_pos_utc",format=timeFmt)  - F.unix_timestamp("dt_pos_utc_lag",format=timeFmt)), 0)
                                  .otherwise((F.unix_timestamp("dt_pos_utc",format=timeFmt)  - F.unix_timestamp("dt_pos_utc_lag",format=timeFmt))/3600))
    
    # ###Segregate long differences(72 hours)
    id_val=id_val.withColumn("segr",F.sum(F.when(((F.col("time_diff")>=72)|(F.col("Name")!=F.col("Name_lag"))),1).otherwise(0)).over(w_id))
    
    ###New division. Test if first values of every segregation is an Anchorage (Pacific or Atlantic)
    w_id_test1=Window.partitionBy([unique_id,"segr"]).orderBy(F.col("dt_pos_utc"))
    
    id_val=id_val.withColumn("row",F.row_number().over(w_id_test1))
    
    id_val=id_val.withColumn("seg_first_anch",F.when(((F.col("row")==1)&(F.col("Name").isin(['Atlantic Anchorage','Pacific Anchorage']))),1)).drop("row")
    
    # # ###New division. Test if sections should be joined based on consecutive anchorage.
    w_id_test2=Window.partitionBy([unique_id,"seg_first_anch"]).orderBy(F.col("dt_pos_utc"))
    
    # ##If they are equal then keep on same group. Calculate difference between first anchoring on each group. 
    id_val=id_val.withColumn("Name_lag_sec",F.when(F.col("seg_first_anch").isNotNull(),F.lag(F.col("Name")).over(w_id_test2)))\
                     .withColumn("dt_pos_utc_lag_sec",F.when(F.col("seg_first_anch").isNotNull(),F.lag(F.col("dt_pos_utc")).over(w_id_test2)))
    
    id_val=id_val.withColumn("index_test",F.sum(F.when(F.col("Name")!=F.col("Name_lag_sec"),1).otherwise(0)).over(w_id))
    
    ##Test if ports in between
    id_val=id_val.withColumn("time_diff_sec",F.when(((F.col("dt_pos_utc_lag_sec").isNotNull())&(F.col("Name")==F.col("Name_lag_sec"))),
                                                        (F.unix_timestamp("dt_pos_utc",format=timeFmt)  - F.unix_timestamp("dt_pos_utc_lag_sec",format=timeFmt))/3600))
    
    id_val=id_val.withColumn("settler",F.sum(F.when(((F.col("index_test")!=F.lag(F.col("index_test")).over(w_id))\
                                                         |(F.lag(F.col("time_diff_sec").isNotNull()).over(w_id))),1).otherwise(0)).over(w_id))
    
    ##I had to rverse this such that the notnull time_diff_sec is on top and could populate the rest of the group.
    ##This test 1) There are two initial anchoring with the same name, one after the other and filters out: 
    ### 2) Groups starting after the last anchorage 
    #### 3) Different anchorage combination in the group
    w_id_test3=Window.partitionBy([unique_id,"settler"])
    
    id_val=id_val.withColumn("test",F.max(F.col("time_diff_sec").isNotNull()).over(w_id_test3.orderBy(F.col("dt_pos_utc").desc())))
    
    id_val=id_val.withColumn("test_port",F.when(F.col("test")=="true",F.max(F.col("Name").isin(ports_pol)).over(w_id_test3.orderBy(F.col("dt_pos_utc")))))
    
    id_val=id_val.withColumn("test_port",F.lag(F.col("test_port"),offset=-1).over(w_id_test2))
    
    id_val=id_val.withColumn("seg_first_anch",F.when(((F.col("test_port")=="true")&(F.col("seg_first_anch")==1)),None).otherwise(F.col("seg_first_anch")))\
                    .drop("settler","test","time_diff_sec","index_test","Name_lag_sec","dt_pos_utc_lag_sec","row","time_diff","dt_pos_utc_lag_sec")
                                 
    ###Sequences removed continuos visit between anchorage and port. Kept the last one.
    ##Get potential transit by checking same name one after the other. We have ruled out this is due to several visits, 
    ###we merge those that belong together probably cutted on movements in close areas. So the remaining (based on seg>72 hours) is that continous name are on different transits:
    ###Redo this with new seg_first_anch
    id_val=id_val.withColumn("Name_lag_sec",F.when(F.col("seg_first_anch").isNotNull(),F.lag(F.col("Name")).over(w_id_test2)))
    
    id_val=id_val.withColumn("index_test",F.sum(F.when(F.col("Name")==F.col("Name_lag_sec"),1).otherwise(0)).over(w_id))  
    
    w_id_test4=Window.partitionBy([unique_id,"seg_first_anch","index_test"])
    
    id_val=id_val.withColumn("row",F.when(F.col("seg_first_anch").isNotNull(),F.row_number().over(w_id_test4.orderBy(F.col("dt_pos_utc")))))
    
    ##Remove groups with one transit flag.
    id_val=id_val.withColumn("test_size",F.when(F.col("seg_first_anch").isNotNull(),F.max(F.col("row")>1).over(w_id_test4.orderBy(F.col("dt_pos_utc").desc()))))
    id_val=id_val.withColumn("seg_first_anch",F.when(((F.col("seg_first_anch").isNotNull())&(F.col("test_size")=="true")),1))
    
    # ###Keep groups with 2 flags and flag the groups with more than 2 groups for refining
    id_val=id_val.withColumn("test_size",F.when(F.col("seg_first_anch").isNotNull(),F.max(F.col("row")>2).over(w_id_test4.orderBy(F.col("dt_pos_utc").desc()))))
    id_val=id_val.withColumn("seg_first_anch",F.when(((F.col("seg_first_anch").isNotNull())&(F.col("test_size")=="true")),2).otherwise(F.col("seg_first_anch")))
    
    # ###Set time difference, only for groups not defined. More than 2 flags.
    id_val=id_val.withColumn("time_diff_not_def",F.when(F.col("seg_first_anch")==2,
                                                            (F.unix_timestamp("dt_pos_utc",format=timeFmt)  - F.lag(F.unix_timestamp("dt_pos_utc",format=timeFmt))\
                                                             .over(w_id_test4.orderBy(F.col("dt_pos_utc"))))/3600))
    
    id_val=id_val.withColumn("time_diff_not_def_lag",F.when(F.col("seg_first_anch")==2,F.lag(F.col("time_diff_not_def"),offset=-1).over(w_id_test4.orderBy(F.col("dt_pos_utc")))))
    id_val=id_val.withColumn("seg_first_anch",F.when(((F.col("seg_first_anch")==2)&(F.col("time_diff_not_def_lag")>96)),None).otherwise(F.col("seg_first_anch")))
    
    ##Clear the newly constructed group of 2.
    id_val=id_val.withColumn("row",F.when(F.col("seg_first_anch")==2,F.row_number().over(w_id_test4.orderBy(F.col("dt_pos_utc")))).otherwise(F.col("row")))
    
    id_val=id_val.withColumn("test_size",F.when(F.col("seg_first_anch")==2,F.max(F.col("row")>1).over(w_id_test4.orderBy(F.col("dt_pos_utc").desc()))).otherwise(F.col("test_size")))
    
    id_val=id_val.withColumn("seg_first_anch",F.when(((F.col("seg_first_anch")==2)&(F.col("test_size")=="false")),None).otherwise(F.col("seg_first_anch")))
    
    id_val=id_val.withColumn("test_size",F.when(F.col("seg_first_anch")==2,F.max(F.col("row")>2).over(w_id_test4.orderBy(F.col("dt_pos_utc").desc()))).otherwise(F.col("test_size")))
    
    id_val=id_val.withColumn("seg_first_anch",F.when(((F.col("seg_first_anch")==2)&(F.col("test_size")=="false")),1).otherwise(F.col("seg_first_anch")))
    
    ####Remove all non compliants. I can still choose to make a new test and keep the pair with the lowest range. Lazy filter here
    id_val=id_val.withColumn("seg_first_anch",F.when(F.col("seg_first_anch")==2,None).otherwise(F.col("seg_first_anch")))
    
    ###Individual group flags
    w_id_test5=Window.partitionBy([unique_id,"seg_first_anch"]).orderBy(["dt_pos_utc"])
    
    id_val=id_val.withColumn("flag",F.when(F.col("seg_first_anch")==1,F.sum(F.when(((F.col(unique_id)!=F.lag(F.col(unique_id)).over(w_id_test5))|\
                                                    (F.col("index_test")!=F.lag(F.col("index_test")).over(w_id_test5))),1).otherwise(0)).over(w_id_test5)
                                ))
    
    id_val=id_val.withColumn("flag",F.sum(F.when(F.col("flag").isNotNull(),1).otherwise(0)).over(w_id))
    
    ##Keep both limits within group
    id_val=id_val.withColumn("flag",F.when(((F.col("seg_first_anch")==1)&(F.col("row")==2)),F.lag(F.col("flag")).over(w_id_test4.orderBy(F.col("dt_pos_utc"))))\
                                               .otherwise(F.col("flag")))
    ###Filter just the complete groups
    w_id_test6=Window.partitionBy([unique_id,"flag"])
    
    id_val=id_val.withColumn("flag_test",F.max(F.col("seg_first_anch")>0).over(w_id_test6.orderBy(["dt_pos_utc"])))
    id_val=id_val.filter(F.col("flag_test")=="true")
    
    ###Transit must have at least some polygons visits ""Access determine a transit start""
    id_val=id_val.withColumn("transit_test",F.when(F.col("Name").isin(["Atlantic Access","Pacific Access"]),1))
    id_val=id_val.withColumn("transit_test",F.max(F.col("transit_test")>0).over(w_id_test6.orderBy(F.col("transit_test").desc())))
    id_val=id_val.filter(F.col("transit_test")=="true")
    
    ###Check if passed through locks. Regular
    id_val=id_val.withColumn("lock_test_r",F.when(F.col("Name").isin(["Miraflores Locks Original","Gatun Locks Original"]),1))
    
    w_id_test7=Window.partitionBy([unique_id,"flag","lock_test_r"])
    
    id_val=id_val.withColumn("transit_test_l_r",F.when(F.col("lock_test_r").isNotNull(),
                                                          F.sum(F.when(F.col("Name")!=F.lag(F.col("Name")).over(w_id_test7.orderBy(["dt_pos_utc"])),1).otherwise(0))\
                                                           .over(w_id_test7.orderBy(["dt_pos_utc"]))))
    
    ###If more than 0 then is valid. If not, then mix of 1 is on the same lock polygon different periods (Invalid transit)
    id_val=id_val.withColumn("transit_test_l_r",F.max(F.col("transit_test_l_r")>0).over(w_id_test6.orderBy(F.col("transit_test_l_r").desc())))
    
    ###Neo locks
    id_val=id_val.withColumn("lock_test_n",F.when(F.col("Name").isin(["Gatun Locks Expanded","Cocoli Locks"]),1))
    
    w_id_test8=Window.partitionBy([unique_id,"flag","lock_test_n"])
    
    id_val=id_val.withColumn("transit_test_l_n",F.when(F.col("lock_test_n").isNotNull(),
                                                          F.sum(F.when(F.col("Name")!=F.lag(F.col("Name")).over(w_id_test8.orderBy(["dt_pos_utc"])),1).otherwise(0))\
                                                           .over(w_id_test8.orderBy(["dt_pos_utc"]))))
    
    ###If more than 0 then is valid. If not, then mix of 1 is on the same lock polygon different periods (Invalid transit)
    id_val=id_val.withColumn("transit_test_l_n",F.max(F.col("transit_test_l_n")>0).over(w_id_test6.orderBy(F.col("transit_test_l_n").desc())))
    
    ###Filter out flags with no valid pass through locks
    id_val=id_val.withColumn("full_transit",F.when(((F.col("transit_test_l_r").isNotNull())|(F.col("transit_test_l_n").isNotNull())),"true"))
    id_val=id_val.filter(F.col("full_transit").isNotNull())
    
    ##Type of transit. Neo or regular
    id_val=id_val.withColumn("neo_transit",F.when(F.col("transit_test_l_n").isNotNull(),1).otherwise(0))
    
    ##Get anchoring group
    id_val=id_val.withColumn("anchoring_group",F.when(F.col("Name").isin(['Atlantic Anchorage','Pacific Anchorage','']),1))
    w_id_test9=Window.partitionBy([unique_id,"flag","anchoring_group"])
    
    id_val=id_val.withColumn("anchoring_group",F.when(F.col("anchoring_group").isNotNull(),
                                                          F.sum(F.when(F.col("Name")!=F.lag(F.col("Name")).over(w_id_test9.orderBy(["dt_pos_utc"])),1).otherwise(0))\
                                                           .over(w_id_test9.orderBy(["dt_pos_utc"]))))
    
    id_val=id_val.withColumn("anchoring_group",F.when(F.col("anchoring_group")==0,1))
    id_val=id_val.withColumn("port_group",F.when(F.col("Name").isin(ports_pol+stops),1))
    
    ##Transit direction
    id_val=id_val.withColumn("dir",F.when(F.col("anchoring_group").isNotNull(),
                                             F.when(F.col("Name")=="Atlantic Anchorage",1).otherwise(2)))
    
    id_val=id_val.withColumn("dir",F.when(F.max(F.col("dir")>1).over(w_id_test6.orderBy(F.col("transit_test_l_n").desc())),"North").otherwise("South"))
    
    id_val=id_val.withColumn("segr",F.sum(F.when(F.col("Name")!=F.col("Name_lag"),1).otherwise(0)).over(w_id_test6.orderBy("dt_pos_utc")))

        
    id_val=id_val.drop("dt_pos_utc_lag",'seg_first_anch','test_port','Name_lag_sec','index_test','row','test_size',
                           'time_diff_not_def', 'time_diff_not_def_lag','flag_test','transit_test','lock_test_r',
                           'transit_test_l_r','lock_test_n','transit_test_l_n','full_transit',"Name_lag",
                           'vessel_name', 'callsign','vessel_type','vessel_type_code','vessel_type_cargo',
                           'vessel_class','flag_country','flag_code','rot','nav_status','nav_status_code',
                           'source','ts_pos_utc','ts_static_utc','ts_insert_utc','dt_static_utc','dt_insert_utc',
                           'vessel_type_sub','message_type','eeid',"rightgeometry","destination",
                           "eta",'H3_int_index_0', 'H3_int_index_1', 'H3_int_index_2', 'H3_int_index_3',
                            'H3_int_index_4', 'H3_int_index_6', 'H3_int_index_7', 'H3_int_index_8', 'H3_int_index_9',
                            'H3_int_index_11', 'H3_int_index_12', 'H3_int_index_13', 'H3_int_index_14',"H3index_0",
                           "source_filename",'H3_int_index_10','H3_int_index_5')
    return id_val
    

def dbscan_pandas(data):
    """
    Perform DBSCAN clustering on vessel position data to detect anchorages and port stops.

    Parameters:
    -----------
    data : pandas.DataFrame
        Dataframe containing vessel position data with latitude and longitude.

    Returns:
    --------
    pandas.DataFrame
        Dataframe with an additional "cluster" column representing detected clusters.

    Notes:
    ------
    - The epsilon (eps) parameter is adjusted based on vessel direction.
    - Clusters are identified to detect stops at anchorages and ports.
    """
    cols = data.columns.tolist()

    # Adjust clustering sensitivity based on direction of travel
    if data["dir"].iloc[0] == "South":
        eps = 0.0028
    else:
        eps = 0.0014

    # Perform clustering on latitude and longitude
    data["cluster"] = DBSCAN(eps=eps, min_samples=3).fit_predict(data[["longitude", "latitude"]])

    return data[cols]
