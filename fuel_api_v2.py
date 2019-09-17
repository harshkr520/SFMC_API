# -*- coding: utf-8 -*-
"""
Created on Tue Jun 18 13:01:31 2019

@author: A0606
"""


import FuelSDK
import pandas as pd
import logging
logging.basicConfig(level=logging.INFO)

class SFMCConnector():
    def __init__(self, sfmc_creds):
        """
        Initialize ET_Client object for the client
        """
        self.sfmc_client = FuelSDK.ET_Client(params=sfmc_creds)
        logging.info("Authentication success with SFMC........")
    
    def _get_col(self, de_name):
        """
        Get all columns for a Data Extension
        """
        dataextensioncol = FuelSDK.ET_DataExtension_Column()
        dataextensioncol.auth_stub = self.sfmc_client
        dataextensioncol.props = ["Name"]
        dataextensioncol.search_filter = {'Property':'DataExtension.CustomerKey', 'SimpleOperator':'equals', 'Value':de_name}
        response = dataextensioncol.get()
        return [i['Name'] for i in response.results]
        
    def get_de_rows_filter(self, de_name, col_name, filter_value, operation, value_type='date'):
        """
        Get filtered records from data extension based on the filter applied
        on the column
        Use value_type != 'date' for filtering values other than date type
        Operation: SFMC supported operations
        """
        dataextensionrow = FuelSDK.ET_DataExtension_Row()
        dataextensionrow.auth_stub = self.sfmc_client
        dataextensionrow.Name = de_name
        col_names = self._get_col(de_name)
        if col_name not in col_names: #Check for column presence
            logging.error(f"{col_name} column not present in data extension {de_name}")
            return False
        if value_type.lower() == 'date':
            logging.debug('Date value selected for filtering')
            dataextensionrow.search_filter = {'Property':col_name, 'SimpleOperator':operation, 'DateValue':filter_value}
        else:
            logging.debug('Non date value selected for filtering')
            dataextensionrow.search_filter = {'Property':col_name, 'SimpleOperator':operation, 'Value':filter_value}
        df_in_file = pd.DataFrame()
        for col in col_names: #Get data column wise
            dataextensionrow.props = [col]
            results = dataextensionrow.get()
            col_data = [i['Properties']['Property'][0]['Value'] for i in results.results]
            while results.more_results:
                results = dataextensionrow.getMoreResults()
                col_data = col_data + [i['Properties']['Property'][0]['Value'] for i in results.results]
            df_in_file[col] = col_data
        logging.info(f"Total rows gathered from data extension:{de_name} = {df_in_file.shape[0]}")
        return df_in_file

if __name__ == '__main__':
    client_config = {'clientid':'15umqlu90r1hxaaqtnvms321',
                     'clientsecret':'W64fTNH19blDhq3YAZm7Pkzx',
                     'authenticationurl':'https://mcq-zr27nhr19938skh14fm170zm.auth.marketingcloudapis.com/v1/requestToken',
                     'baseapiurl':'https://mcq-zr27nhr19938skh14fm170zm.rest.marketingcloudapis.com',
                     'soapendpoint':'https://mcq-zr27nhr19938skh14fm170zm.soap.marketingcloudapis.com/Service.asmx',
                     'defaultwsdl':'https://mcq-zr27nhr19938skh14fm170zm.soap.marketingcloudapis.com/etframework.wsdl',
                    }
    sfmc_obj = SFMCConnector(client_config)
    de_name = 'Endo_SMAcr_Click'
    sfmc_obj.get_de_rows_filter(de_name, 'EventDate', '2019-03-03', 'greaterThan')
    
    
