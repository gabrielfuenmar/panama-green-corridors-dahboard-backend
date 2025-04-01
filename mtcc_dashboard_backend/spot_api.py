# -*- coding: utf-8 -*-
import os  
import json
import requests
import logging
import time

from ocean_spark_connect.ocean_spark_session import OceanSparkSession

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

class spot_connect:
    
    def __init__(
        self,
        token: str
    ):
        self.cluster_id = "osc-b22517f3"
        self.acct_id = "act-f5fd9572"
        self.token = token
        self.payload = None
        self.app_id = None
        self.app_url = None
        self.config_name = "spark-connect-dev"
        
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        
    def get_spark_connect_payload(self):
        
        config_template_id = "spark-connect"
        url = f'https://api.spotinst.io/ocean/spark/cluster/{self.cluster_id}/configTemplate/spark-connect?accountId={self.acct_id}'
        response = requests.request("GET", url, headers=self.headers)
        if response.status_code == 200:
            response_json = response.json()
            for item in response.json()['response']['items']:
                if item['displayName'] == self.config_name:
                    payload ={}
                    payload["jobId"]="spark-connect"
                    payload["configOverrides"] = item['config']
                    self.payload = json.dumps(payload)
                    break
        else:
             assert "Check config template name"
        return None
    
    

    def submit(self):
        url = f'https://api.spotinst.io/ocean/spark/cluster/{self.cluster_id}/app?accountId={self.acct_id}'
        if not self.payload:
            self.get_spark_connect_payload()
            
        response = requests.request("POST", url, headers=self.headers, data=self.payload)
        response_json = response.json() 
        app_id= response_json.get('response', {}).get('items', [{}])[0].get('id', None)
        
        if app_id:
            self.app_id = app_id
            self.app_url = f"https://console.spotinst.com/ocean/spark/apps/clusters/{self.cluster_id}/apps/{self.app_id}/overview"
            logger.info(f"App link: {self.app_url}")
            
        else: 
            logger.info(f"Request failed with status code : {response.status_code}")
            logger.info(f"{response.text}")
            
        return response
    
    def appstate(self):
        url = f"https://api.spotinst.io/ocean/spark/cluster/{self.cluster_id}/app/{self.app_id}?accountId={self.acct_id}"
        response = requests.request("GET", url, headers=self.headers)
        
        if response.status_code == 200:
            response_json = response.json()
            status = response_json.get('response', {}).get('items', [{}])[0].get('appState', None)
            logger.info(f"App State: {status}")
            
        else: 
            status="REQUEST FAILED"
            logger.info(f"Request failed with status code : {response.status_code}")
            logger.info(f"{response.text}")
        
        return status
    
    def wait_til_running(self, wait_seconds=10, max_wait_seconds=240):
        total_wait = 0
        status = "PENDING"
        while status ==  "PENDING":
            status = self.appstate()
            if status != "PENDING":
                break
            if total_wait == max_wait_seconds:
                logger.info(f"Maximum waiting time reached. App State:{status}")
                break
            if status == "REQUEST FAILED ":
                break
            time.sleep(wait_seconds)
            total_wait+=wait_seconds
        return None
        
        
    
    def delete(self):
        
        if self.app_id:
            url = f'https://api.spotinst.io/ocean/spark/cluster/{self.cluster_id}/app/{self.app_id}'
            response = requests.request("DELETE", url, headers=self.headers)
            
            if response.status_code == 200:
                response_json = response.json()  # Parse the JSON response
                logger.info(f"Killed App ID {self.app_id}")
            else:
                print(f'Request failed with status code {response.status_code}')
                plogger.info(f": {response.text}")

        else:
            logger.info(f"Provide App ID")
            
        return response
    
    def spark_session(self):
        spark = OceanSparkSession.Builder() \
            .account_id(self.acct_id) \
            .cluster_id(self.cluster_id) \
            .appid(self.app_id) \
            .token(self.token) \
            .getOrCreate()
        
        return spark
