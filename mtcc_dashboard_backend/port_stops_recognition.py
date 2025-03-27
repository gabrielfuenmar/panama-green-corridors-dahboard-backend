# -*- coding: utf-8 -*-
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import panama_ports_var
from ais import functions as af
from pyspark.sql.types import TimestampType
import pandas as pd

def port_stops_cutoff(ais_df): ###TEST
    '''
    ais_df : PySpark Dataframe
    '''
    ports_pol=panama_ports_var.ports_pol
    anch_pol=panama_ports_var.anch_pol

    w_mmsi = Window.partitionBy("mmsi").orderBy(F.col("dt_pos_utc"))
    
    ## Step 1: Add row numbers to identify first record per MMSI
    ais_cuts = ais_df.withColumn("row", F.row_number().over(w_mmsi))

   # Step 2: Filter for visits to ports or anchorages
    ais_cuts = ais_cuts.filter(F.col("Name").isin(ports_pol + anch_pol))

    # Step 3: Calculate time difference and lagged columns
    timeFmt = "yyyy-MM-dd' 'HH:mm:ss.S"

    ais_cuts = ais_cuts.withColumn("dt_pos_utc_lag", F.lag("dt_pos_utc").over(w_mmsi)) \
                    .withColumn("Name_lag", F.lag("Name").over(w_mmsi)) \
                    .withColumn(
                        "time_diff",
                        F.when(
                            F.isnull(F.unix_timestamp("dt_pos_utc", format=timeFmt) - 
                                        F.unix_timestamp("dt_pos_utc_lag", format=timeFmt)), 
                            0
                        ).otherwise(
                            (F.unix_timestamp("dt_pos_utc", format=timeFmt) - 
                                F.unix_timestamp("dt_pos_utc_lag", format=timeFmt)) / 3600
                        )
                    )

    # Step 4: Flag long time differences (>= 72 hours)
    ais_cuts = ais_cuts.withColumn(
        "flag",
        F.sum(F.when(F.col("time_diff") >= 72, 1).otherwise(0)).over(w_mmsi)
    )

    # Step 5: Flag polygon changes
    ais_cuts = ais_cuts.withColumn(
        "segr",
        F.sum(F.when(F.col("Name") != F.col("Name_lag"), 1).otherwise(0)).over(w_mmsi)
    )


    # Step 6: Update flag for transitions between ports and anchorages
    ais_cuts = ais_cuts.withColumn("flag_lag", F.lag("flag").over(w_mmsi)) \
                    .withColumn(
                        "flag",
                        F.sum(
                            F.when(
                                (F.col("flag") != F.col("flag_lag")) |
                                ((F.col("Name").isin(anch_pol)) & (F.col("Name_lag").isin(ports_pol))),
                                1
                            ).otherwise(0)
                        ).over(w_mmsi)
                    )

    # Step 7: Check port visits
    w_mmsi_2 = Window.partitionBy("mmsi", "flag")
    ais_cuts = ais_cuts.withColumn("visit_test", F.when(F.col("Name").isin(ports_pol), 1)) \
                    .withColumn(
                        "visit_test",
                        F.max(F.col("visit_test") > 0).over(w_mmsi_2)
                    ).filter(F.col("visit_test") == True)

    # Step 8: Check anchorage visits
    ais_cuts = ais_cuts.withColumn("visit_test", F.when(F.col("Name").isin(anch_pol), 1)) \
                    .withColumn(
                        "visit_test",
                        F.max(F.col("visit_test") > 0).over(w_mmsi_2)
                    ).filter(F.col("visit_test") == True)

    # Step 9: Drop unnecessary columns
    columns_to_drop = [
        "dt_pos_utc_lag", "seg_first_anch", "test_port", "Name_lag_sec", "index_test", "row", "test_size",
        "time_diff_not_def", "time_diff_not_def_lag", "flag_test", "transit_test", "lock_test_r",
        "transit_test_l_r", "lock_test_n", "transit_test_l_n", "full_transit", "vessel_name", "callsign",
        "vessel_type", "vessel_type_code", "vessel_type_cargo", "vessel_class", "flag_country", "flag_code",
        "rot", "nav_status", "nav_status_code", "source", "ts_pos_utc", "ts_static_utc", "ts_insert_utc",
        "dt_static_utc", "dt_insert_utc", "vessel_type_sub", "message_type", "eeid", "rightgeometry",
        "length", "width", "destination", "eta", "H3_int_index_0", "H3_int_index_1", "H3_int_index_2",
        "H3_int_index_3", "H3_int_index_4", "H3_int_index_6", "H3_int_index_7", "H3_int_index_8",
        "H3_int_index_9", "H3_int_index_11", "H3_int_index_12", "H3_int_index_13", "H3_int_index_14",
        "H3index_0", "source_filename", "H3_int_index_10", "H3_int_index_5"
    ]
    ais_cuts = ais_cuts.drop(*columns_to_drop)

    return ais_cuts


def merge_stop_segments(df_spark, reference_phase, protected_phase, time_limit='1H'):
    """
    Merge groups in a PySpark DataFrame based on a reference phase while protecting another phase.
    
    Parameters:
    - df_spark (DataFrame): Input PySpark DataFrame with columns ['flag', 'mmsi', 'op_phase', 'dt_pos_utc', 'segr'].
    - reference_phase (str): The operation phase used as a reference for merging (e.g., 'Anchorage').
    - protected_phase (str): The operation phase that should not be absorbed (e.g., 'Berth').
    - time_limit (str): Maximum time gap (e.g., '1H' for 1 hour) for merging segments.

    Returns:
    - DataFrame: Updated PySpark DataFrame with corrected segment (`segr`) values.
    """

    # Convert time limit to seconds
    time_limit_seconds = pd.Timedelta(time_limit).total_seconds()

    # Define a window partitioned by flag and mmsi, ordered by dt_pos_utc
    window_spec = Window.partitionBy("flag", "mmsi").orderBy("dt_pos_utc")

    # Get previous segment details
    df_spark = df_spark.withColumn("prev_op_phase", F.lag("op_phase").over(window_spec)) \
                       .withColumn("prev_segr", F.lag("segr").over(window_spec)) \
                       .withColumn("prev_dt_pos_utc", F.lag("dt_pos_utc").over(window_spec))

    # Compute time difference (avoiding unnecessary Unix timestamp conversion)
    df_spark = df_spark.withColumn(
        "time_diff",
        (F.col("dt_pos_utc").cast("long") - F.col("prev_dt_pos_utc").cast("long"))
    )

    # Step 1: Merge segments for the reference phase
    df_spark = df_spark.withColumn(
        "new_segr",
        F.when(
            (F.col("op_phase") == reference_phase) & 
            (F.col("prev_op_phase") == reference_phase) & 
            (F.col("time_diff") <= time_limit_seconds),
            F.col("prev_segr")  # Assign the previous segment ID if conditions are met
        ).otherwise(F.col("segr"))
    )

    # Step 2: Propagate segment assignments (merging)
    df_spark = df_spark.withColumn(
        "merged_segr",
        F.last("new_segr", ignorenulls=True).over(window_spec.rowsBetween(Window.unboundedPreceding, 0))
    )

    # Step 3: Ensure the protected phase is NOT merged
    df_spark = df_spark.withColumn(
        "final_segr",
        F.when(F.col("op_phase") == protected_phase, F.col("segr")).otherwise(F.col("merged_segr"))
    )

    # Clean up temporary columns
    df_spark = df_spark.drop("prev_op_phase", "prev_segr", "prev_dt_pos_utc", "time_diff", "new_segr", "merged_segr","segr")
    
    return df_spark.withColumnRenamed("final_segr", "segr")


def get_berth_anchorage_records(df_spark):
    """
    Retrieve all berth groups and their preceding anchorage groups using PySpark.
    Additionally, include the mode of 'Name' for the 'flag' and 'mmsi' group,
    but only if 'op_phase' is 'Berth'.
    If no preceding anchorage exists, return None for those values.
    
    Parameters:
    - df_spark (DataFrame): PySpark DataFrame with columns ['flag', 'mmsi', 'Name', 'op_phase', 'dt_pos_utc', 'segr'].

    Returns:
    - DataFrame: PySpark DataFrame with berth and preceding anchorage details.
    """

    # Define a window partitioned by flag and mmsi, ordered by dt_pos_utc
    window_spec = Window.partitionBy("flag", "mmsi").orderBy("dt_pos_utc")

    df_mode_name = df_spark.filter(F.col("op_phase") == "Berth") \
        .groupby("flag", "mmsi") \
        .agg(F.first("Name", ignorenulls=True).alias("mode_name"))

    # Identify the minimum and maximum timestamps for Berth segments
    berth_df = df_spark.filter(F.col("op_phase") == "Berth") \
        .groupby("flag", "mmsi", "segr") \
        .agg(F.min("dt_pos_utc").alias("berth_min_dt_pos_utc"),
             F.max("dt_pos_utc").alias("berth_max_dt_pos_utc"))

    # Identify the preceding Anchorage group for each Berth segment
    anchorage_df = df_spark.filter(F.col("op_phase") == "Anchorage") \
        .withColumn("next_berth_dt", 
                    F.lead("dt_pos_utc").over(window_spec)) \
        .filter(F.col("dt_pos_utc") < F.col("next_berth_dt")) \
        .groupby("flag", "mmsi", "segr") \
        .agg(F.min("dt_pos_utc").alias("anchorage_min_dt_pos_utc"),
             F.max("dt_pos_utc").alias("anchorage_max_dt_pos_utc"))

    # Join the berth dataframe with the anchorage dataframe
    result_df = berth_df.join(
        anchorage_df, 
        on=["flag", "mmsi"], 
        how="left"
    ).join(
        df_mode_name, 
        on=["flag", "mmsi"], 
        how="left"
    ).select(
        "flag", "mmsi", "mode_name",
        "berth_min_dt_pos_utc", "berth_max_dt_pos_utc",
        "anchorage_min_dt_pos_utc", "anchorage_max_dt_pos_utc"
    )

    return result_df