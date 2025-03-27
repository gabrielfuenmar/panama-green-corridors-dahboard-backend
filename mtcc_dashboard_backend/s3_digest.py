# -*- coding: utf-8 -*-
import h3
import boto3
from datetime import datetime, timedelta
import re
from ais import functions as af
import pyspark.sql.functions as F
from io import StringIO, BytesIO
import pandas as pd

def open_s3_session(p_access_key,p_secret_key,bucket_name="mtcclatam",folder_name="dash"):
    '''
    Read bucket information
    '''
    try:
        session = boto3.Session(aws_access_key_id=p_access_key,
                                aws_secret_access_key=p_secret_key)

        s3 = session.resource('s3')

        bucket_list=[]
        for file in  s3.Bucket(bucket_name).objects.filter(Prefix='{}/'.format(folder_name)):
            file_name=file.key
            bucket_list.append(file.key)
        return bucket_list    

    except Exception as e:
        print(f'Have you typed in the correct folder and file name? Check out the s3 credetianls.')
        print(f'Error: \n{e}\n') 


def ais_read_data_range(p_access_key, p_secret_key, spark, connect, h3_int_list=[],
                        dt_from=None, dt_to=None, bypass_new_data_test=False, file_name="transits_pc_in"):
    """Bypass overrides the test of new """
    # Open the S3 session and get the list of files
    bucket_list = open_s3_session(p_access_key, p_secret_key)

    # Filter files based on the folder name
    test_list = [x for x in bucket_list if file_name in x]
    
    # Default end date is yesterday
    default_end_date = datetime.now() - timedelta(days=1)

    if test_list:
        try:
            # Extract all start and end dates from file names
            start_dates = []
            end_dates = []

            for file in test_list:
                match = re.search(rf"{file_name}_(\d{{4}}-\d{{2}}-\d{{2}})&(\d{{4}}-\d{{2}}-\d{{2}})\.parquet", file)
                if match:
                    start_dates.append(datetime.fromisoformat(match.group(1)))  # Start date before '&'
                    end_dates.append(datetime.fromisoformat(match.group(2)))    # End date after '&'

            if start_dates and end_dates:
                # Get the maximum datetime from the extracted dates
                last_date = max(max(start_dates), max(end_dates) - timedelta(days=15))
            else:
                raise ValueError("No valid date ranges found in file names.")

        except Exception as e:
            print(f"Error processing file names: {e}")
            return 
    else:
        # No files found, set the default start date to 15 days ago
        last_date = default_end_date - timedelta(days=15)

    # Determine the date range
    try:
        start_date = datetime.fromisoformat(dt_from) if dt_from else last_date
        end_date = datetime.fromisoformat(dt_to) if dt_to else default_end_date

        if start_date >= end_date:
            raise ValueError("Start date must be earlier than end date.")
    except ValueError as e:
        print(f"Invalid date format or range: {e}. Connection stopped")
        connect.delete()
        return

    # Fetch AIS data
    if len(h3_int_list)==0:
        ais_sample = af.get_ais(
            spark,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )
    else:
        ais_sample = af.get_ais(
            spark,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            h3_list=h3_int_list,
        )
    # Check if there are new AIS positions
    if ais_sample.select(F.max(F.col("dt_pos_utc"))).collect()[0][0] < last_date and bypass_new_data_test== False:
        print("No new AIS positions.")
        connect.delete()
        return

    print(start_date.isoformat(),end_date.isoformat())
    print("AIS data fetched successfully.")

    return ais_sample,start_date.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d")
    
def s3_file_read(file_name, p_access_key, p_secret_key):
    bucket_name = "mtcclatam"
    
    # Create an S3 resource session
    s3 = boto3.resource('s3', aws_access_key_id=p_access_key, aws_secret_access_key=p_secret_key)
    bucket = s3.Bucket(bucket_name)
    
    # Check if the file exists in the bucket by filtering objects with the given prefix
    if not any(obj.key == file_name for obj in bucket.objects.filter(Prefix=file_name)):
        print(f"File {file_name} not found in bucket {bucket_name}.")
        return None
    
    try:
        # Retrieve the S3 object
        s3_object = s3.Object(bucket_name, file_name)
        # Read the file content as raw bytes
        file_content = s3_object.get()['Body'].read()
        # Create a BytesIO stream for the parquet file
        file_stream = BytesIO(file_content)
        # Load the parquet file into a DataFrame
        old_file_df = pd.read_parquet(file_stream)
        return old_file_df

    except Exception as e:
        print(f"Error reading file: {e}")
        return None
        
        
def write_to_s3(dataframe, folder_name, p_access_key, p_secret_key, from_date=None, to_date=None, log=False):
    dataframe.attrs = {}
    # Establish an AWS session
    session = boto3.Session(aws_access_key_id=p_access_key,
                            aws_secret_access_key=p_secret_key)
    s3 = session.resource('s3')

    # Use BytesIO to handle binary data for Parquet output
    buffer = BytesIO()
    # Writing the DataFrame to Parquet; ensure you have either pyarrow or fastparquet installed.
    dataframe.to_parquet(buffer, index=False)
    buffer.seek(0)  # Reset buffer pointer to the beginning

    # Define S3 object key with the appropriate file extension
    if from_date and to_date:
        file_key = f'dash/{folder_name}/{folder_name}_in_{from_date}&{to_date}.parquet'
    else:
        file_key = f'dash/{folder_name}/{folder_name}_in.parquet'

    if log:
        file_key = f'dash/{folder_name}/log_{folder_name}.parquet'

    s3_obj = s3.Object("mtcclatam", file_key)
    s3_obj.put(Body=buffer.getvalue())

def delete_file_from_s3(file_name, p_access_key, p_secret_key):
    bucket_name = "mtcclatam"
    
    # Create an S3 resource session
    s3 = boto3.resource('s3', aws_access_key_id=p_access_key, aws_secret_access_key=p_secret_key)
    bucket = s3.Bucket(bucket_name)
    
    # Check if the file exists
    if not any(obj.key == file_name for obj in bucket.objects.filter(Prefix=file_name)):
        print(f"File {file_name} not found in bucket {bucket_name}.")
        return False  # Indicating file not found
    
    try:
        # Delete the file
        s3.Object(bucket_name, file_name).delete()
        print(f"File {file_name} successfully deleted from {bucket_name}.")
        return True  # Indicating successful deletion
    
    except Exception as e:
        print(f"Error deleting file: {e}")
        return False  # Indicating failure


# def overlap_merge(first_df, second_df, setup=):

#     merged_df=pd.concat([first_df,second_df])

#     if setup = "transit_info_pc_in":

#         merged_df=merged_df.groupby(["imo","mmsi", "neo_transit","direction",'StandardVesselType', 'GrossTonnage'], as_index=False).apply(
#         lambda d: d.sort_values(["lock_out", "lock_in"])
#         .assign(
#             grp=lambda d: (
#                 ~(d["lock_in"] <= (d["lock_out"].shift()))
#             ).cumsum()
#         )
#         .groupby(["imo",  "mmsi","neo_transit","direction","grp",'StandardVesselType', 'GrossTonnage'], as_index=False)
#         .agg({"lock_in": "min", "lock_out": "max","anchoring_in":"min", 
#             "anchoring_out":"max","direct_transit":"min"})).reset_index(drop=True)

#         merged_df=merged_df.groupby(["imo", "mmsi","neo_transit","direction",'StandardVesselType', 'GrossTonnage'], as_index=False).apply(
#             lambda d: d.sort_values(["anchoring_out", "anchoring_in"])
#             .assign(
#                 grp=lambda d: (
#                     ~(d["anchoring_in"] <= (d["anchoring_out"].shift()))
#                 ).cumsum()
#             )
#             .groupby(["imo","mmsi","neo_transit","direction", "grp",'StandardVesselType', 'GrossTonnage'], as_index=False)
#             .agg({"lock_in": "min", "lock_out": "max","anchoring_in":"min", 
#                 "anchoring_out":"max","direct_transit":"min"})).reset_index(drop=True)
#     else:
        
    # return merged_df