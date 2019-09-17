# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 11:15:54 2019

@author: A0606
"""
import argparse
import datetime
import json
import logging

import mysql.connector
import pandas as pd

import cloud_connector_api
import config
import fuel_api_v2

#command line arguments sys.argv[1] is client_id
logging.basicConfig(level=logging.INFO)
logging.info("---------SFMC wrapper started----------")
cli_parser = argparse.ArgumentParser(description='SFMC Integration')
cli_parser.add_argument('-c', '--ClientId', type=int, default=8,
                        help='The ID of Client for which data needs to be extracted')
args = cli_parser.parse_args()
sql_connection = mysql.connector.connect(user=config.sql_user, password=config.sql_password, host=config.sql_host,
                                         port=config.sql_port, database=config.sql_database)
cursor = sql_connection.cursor(buffered=True)
logging.info("Successfully made connection to SQL database")
cursor.execute(f"Select Custom_Column_Value, Value from vw_client_config where client_id={args.ClientId}")
df_client_config = pd.DataFrame(cursor.fetchall(), columns=['KEY','VALUE'])
cursor.execute(f"Select Name, Environment_Flag from Clients where Id={args.ClientId}")
client_name, environment_flag = cursor.fetchone()
#cursor.execute(f"Select Name, Environment_Flag from Clients where Id={sys.argv[1]}")
#environment_flag = str(cursor.fetchone()[1]).upper()

cursor = sql_connection.cursor(buffered=True)
cursor.execute("Select cvc_id, Custom_Column_Value, cvc_val from vw_client_vendor_config "
               f" where Client_Id={args.ClientId} and custom_column_name='API' and vendor_name='SFMC'")
df_vendor_config = pd.DataFrame(cursor.fetchall(), columns=['ID', 'KEY', 'VALUE'])
sfmc_subdomain = df_vendor_config.loc[df_vendor_config['KEY']=='SUBDOMAIN', 'VALUE'].values[0]
sfmc_custom_config = json.loads(df_vendor_config.loc[df_vendor_config['KEY']=='CUSTOM_CONFIG', 'VALUE'].values[0])
#Store {sfmc_custom_config_id} to update the record later
sfmc_custom_config_id = df_vendor_config.loc[df_vendor_config['KEY']=='CUSTOM_CONFIG', 'ID'].values[0]
sfmc_creds = {'clientid':df_vendor_config.loc[df_vendor_config['KEY']=='CLIENT_ID', 'VALUE'].values[0],
              'clientsecret':df_vendor_config.loc[df_vendor_config['KEY']=='CLIENT_SECRET', 'VALUE'].values[0],
              'authenticationurl':f'https://{sfmc_subdomain}.auth.marketingcloudapis.com/v1/requestToken',
              'baseapiurl':f'https://{sfmc_subdomain}.rest.marketingcloudapis.com',
              'soapendpoint':f'https://{sfmc_subdomain}.soap.marketingcloudapis.com/Service.asmx',
              'defaultwsdl':f'https://{sfmc_subdomain}.soap.marketingcloudapis.com/etframework.wsdl',
              }
cloud_root_addr = df_client_config.loc[df_client_config['KEY']=='CLIENT_ROOT_ADDRESS', 'VALUE'].values[0]
cloud_config = {'TENANT_ID':config.tenant_id,
                'AZURE_SECRET_KEY':df_client_config.loc[df_client_config['KEY']=='AZURE_TOKEN', 'VALUE'].values[0],
                'AZURE_CLIENT_ID':df_client_config.loc[df_client_config['KEY']=='AZURE_APP_ID', 'VALUE'].values[0],
                'AZURE_RESOURCE':config.resource,
                'STORE_NAME':config.store_name,
                'AWS_ACCESS_ID':df_client_config.loc[df_client_config['KEY']=='AWS_ACCESS_ID', 'VALUE'].values[0],
                'AWS_SECRET_KEY':df_client_config.loc[df_client_config['KEY']=='AWS_SECRET_KEY', 'VALUE'].values[0],
                }
staging_address = f'{cloud_root_addr}{client_name}/{config.staging_addr}/{config.staging_raw_addr}/'
current_date = datetime.datetime.today().strftime("%Y-%m-%d")
sfmc_conn_obj = fuel_api_v2.SFMCConnector(sfmc_creds)
logging.info("Successfully made connection with SFMC")
cloud_conn_obj = cloud_connector_api.CloudConnector(environment_flag, **cloud_config)
logging.info(f"Successfully made connection with cloud environment: {environment_flag}")
#sfmc_custom_config = [{"dataExtName":"Endo_SMAcr_Click", "dataExtColName":"EventDate", "dataExtWaterMark":"2019-01-01"}]
for index, record in enumerate(sfmc_custom_config):
     file_path = f"{staging_address}{record['dataExtName']}_{current_date}.csv"
     logging.info(f"Extracting record for dataextension {record['dataExtName']} for date after {record['dataExtWaterMark']}")
     temp_df = sfmc_conn_obj.get_de_rows_filter(record['dataExtName'], record['dataExtColName'],
                                                record['dataExtWaterMark'], 'greaterThan')
     if type(temp_df).__name__ == 'DataFrame':
         logging.info(f"Records successfully extracted from {record['dataExtName']}")
         cloud_conn_obj.upload_df(temp_df, file_path)
     else:
         logging.info(f"Records can not be extracted from {record['dataExtName']}")
     sfmc_custom_config[index]['dataExtWaterMark'] = current_date

cursor.execute(f"update client_vendor_configurations set value='{json.dumps(sfmc_custom_config)}' where id={sfmc_custom_config_id}")
logging.info("Database updated successfully and releasing connections.........")
sql_connection.commit()
cursor.close()
sql_connection.close()
logging.info("Connection Released")

