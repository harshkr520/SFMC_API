# -*- coding: utf-8 -*-
"""
Created on Fri Feb 22 15:55:43 2019

@author: A0606
"""
# =============================================================================
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText
# from email.mime.base import MIMEBase
# from email import encoders
# =============================================================================
import logging
import os
import smtplib

from azure.datalake.store import  core, lib, multithread
import numpy as np
import pandas as pd
import pyodbc
import pysftp
import s3fs
import sqlalchemy

class CloudConnector():
    def __init__(self, client_env, **kwargs):
        if 'azure' in client_env.lower():
            self.cloud_obj = azure_cloud_connector(**kwargs)
        elif 'aws' in client_env.lower():
            self.cloud_obj = aws_cloud_connector(**kwargs)
        elif 'sftp' in client_env.lower():
            self.cloud_obj = sftp_connector(**kwargs)
        elif 'email' in client_env.lower():
            self.cloud_obj = email_connector(**kwargs)
        elif 'mssql' in client_env.lower():
            self.cloud_obj = mssql_server_connector(**kwargs)
    
    def move_folder(self, source_folder_path, dest_folder_path):
        source_folder_path = self._modify_folder_path(source_folder_path)
        dest_folder_path = self._modify_folder_path(dest_folder_path)
        self.cloud_obj.move_folder(source_folder_path, dest_folder_path)
    
    def move_file(self, source, destination):
        self.cloud_obj.move_file(source, destination)
    
    def list_files(self, folder_path):
        folder_path = self._modify_folder_path(folder_path)
        return self.cloud_obj.list_files(folder_path)
        
    def delete_folder(self, folder_path):
        folder_path = self._modify_folder_path(folder_path)
        self.cloud_obj.delete_folder(folder_path)
    
    def download_folder(self, source_folder_path, dest_folder_path):
        source_folder_path = self._modify_folder_path(source_folder_path)
        dest_folder_path = self._modify_folder_path(dest_folder_path)
        self.cloud_obj.download_folder(source_folder_path, dest_folder_path)
    
    def download_file(self, source, destination):
        self.cloud_obj.download_file(source, destination)
        
    def upload_df(self, df, dest_folder_path):
        self.cloud_obj.upload_df(df, dest_folder_path)
        logging.info(f"Total number of rows uploaded = {df.shape[0]}")
    
    def download_df(self, source):
        return self.cloud_obj.download_df(source)
        
    def upload_file(self, source_folder_path, dest_folder_path):
        self.cloud_obj.upload_file(source_folder_path, dest_folder_path)
        
    def upload_folder(self, source_folder_path, dest_folder_path):
        source_folder_path = self._modify_folder_path(source_folder_path)
        dest_folder_path = self._modify_folder_path(dest_folder_path)
        self.cloud_obj.upload_folder(source_folder_path, dest_folder_path)
    
    def send_mail(self, source_file_path=''):
        self.cloud_obj.send_mail(source_file_path)
    
    def _modify_folder_path(self, folder_path):
        if folder_path[-1] != '/':
            folder_path = folder_path + '/'
        return folder_path
        

class azure_cloud_connector(object):
    def __init__(self, **kwargs):
        adlCreds = lib.auth(tenant_id=kwargs['TENANT_ID'], client_secret=kwargs['AZURE_SECRET_KEY'], client_id=kwargs['AZURE_CLIENT_ID'], resource=kwargs['AZURE_RESOURCE'])
        self.adl_conn_obj = core.AzureDLFileSystem(adlCreds, store_name=kwargs['STORE_NAME'])
        
    def move_folder(self, source_folder_path, dest_folder_path):
        self.adl_conn_obj.mkdir(dest_folder_path)
        self.adl_conn_obj.mv(source_folder_path, dest_folder_path)
        self.adl_conn_obj.mkdir(source_folder_path)
    
    def list_files(self, folder_path):
        return self.adl_conn_obj.ls(folder_path)
        
    def delete_folder(self, folder_path):
        if self.adl_conn_obj.exists(folder_path):
            self.adl_conn_obj.rm(folder_path, recursive=True)
    
    def download_folder(self, source_folder_path, dest_folder_path):
        if not os.path.exists(dest_folder_path):
            os.makedirs(dest_folder_path)
        multithread.ADLDownloader(self.adl_conn_obj, lpath=dest_folder_path, rpath=source_folder_path, nthreads=64, overwrite=True, buffersize=4194304, blocksize=4194304)
        
    def download_file(self, source, destination):
        self.adl_conn_obj.get(source, destination)
        
    def upload_df(self, df, destination):
        with self.adl_conn_obj.open(destination,'w',blocksize=2**20) as f:
            df.to_csv(f, header=True, index=False, encoding='utf-8', line_terminator='\n')
            
    def download_df(self, source):
        f = self.adl_conn_obj.open(source, 'r', blocksize=2**20)
        temp_df = pd.read_csv(f, dtype=object, keep_default_na=False)
        f.close()
        return temp_df
        
    def upload_file(self, source, destination):
        multithread.ADLUploader(self.adl_conn_obj, lpath=source, rpath=destination, nthreads=64, overwrite=True, buffersize=4194304, blocksize=4194304)
        
    def upload_folder(self, source_folder_path, dest_folder_path):
        self.upload_file(source_folder_path, dest_folder_path)
    
        
class aws_cloud_connector(object):
    def __init__(self, **kwargs):
        self.s3_conn_obj = s3fs.S3FileSystem(key=kwargs['AWS_ACCESS_ID'], secret=kwargs['AWS_SECRET_KEY'])
        
    def move_folder(self, source_folder_path, dest_folder_path):
        heirarchy_num = len(source_folder_path.split('/')) - 2
        for file_key in self.s3_conn_obj.walk(source_folder_path):
            self.s3_conn_obj.mv(file_key, dest_folder_path + file_key.split('/',heirarchy_num)[heirarchy_num])
    
    def list_files(self, folder_path):
        return self.s3_conn_obj.ls(folder_path)
        
    def delete_folder(self, folder_path):
        if self.s3_conn_obj.exists(folder_path):
            self.s3_conn_obj.rm(folder_path, recursive=True)
    
    def download_folder(self, source_folder_path, dest_folder_path):
        for file_key in self.s3_conn_obj.walk(source_folder_path):
            if not os.path.exists(os.path.split(dest_folder_path + file_key)[0]):
                os.makedirs(os.path.split(dest_folder_path + file_key)[0])
            self.s3_conn_obj.get(file_key, dest_folder_path + file_key)
    
    def download_file(self, source, destination):
        self.s3_conn_obj.get(source, destination)
        
    def upload_df(self, df, destination):
        with self.s3_conn_obj.open(destination, mode='w') as f:
            df.to_csv(f, header=True, index=False, encoding='utf-8', line_terminator='\n')
    
    def download_df(self, source):
        f = self.s3_conn_obj.open(source, mode='r')
        temp_df = pd.read_csv(f, dtype=object, keep_default_na=False)
        f.close()
        return temp_df
            
    def upload_file(self, source, destination):
        self.s3_conn_obj.put(source, destination)
        
    def upload_folder(self, source, destination):
        for path, dirs, files in os.walk(source):
            for file_name in files:
                self.s3_conn_obj.put(os.path.join(path, file_name), destination + file_name)


class sftp_connector(object):
    def __init__(self, **kwargs):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        self.sftp_conn_obj = pysftp.Connection(host=kwargs['SFTP_IP'], username=kwargs['SFTP_USER'], password=kwargs['SFTP_PASSWORD'], cnopts=cnopts)
        
    def upload_file(self, source, destination):
        self.sftp_conn_obj.put(source, destination)
        
    def list_files(self, folder_path):
        return self.sftp_conn_obj.listdir(folder_path)
        
    def download_file(self, source, destination):
        self.sftp_conn_obj.get(source, destination)
        
    def move_file(self, source, destination):
        self.sftp_conn_obj.rename(source, destination)
        
class mssql_server_connector():
    def __init__(self, **kwargs):
        self.sql_engine = sqlalchemy.create_engine(f'mssql+pyodbc://{kwargs["DATABASE_USER"]}:{kwargs["DATABASE_PASSWORD"]}'
                                              f'@{kwargs["DATABASE_HOST"]}/{kwargs["DATABASE_NAME"]}?driver={kwargs["DATABASE_DRIVER"]}',
                                              fast_executemany=True)

# =============================================================================
#     @sqlalchemy.event.listens_for(self.sql_engine, 'before_cursor_execute')
#     def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
#         if executemany:
#             cursor.fast_executemany = True
# =============================================================================
    
    def download_df(self, sql, **kwargs):
        return pd.read_sql(sql, self.sql_engine, **kwargs)
        
    def upload_df(self, df, table_name):
        chunks = np.split(df, df.shape[0]//10**6 + 1) #Divides the array into 10**6 rows chunks, +1 to handle rows less than 10**6
        for index, chunk in enumerate(chunks):
            chunk.to_sql(name=table_name, if_exists='append', con=self.sql_engine, index=False,
                         chunksize=10**5)
            logging.info("{chunk.shape[0]} rows of {index} partition written to database")


class email_connector(object):
    def __init__(self, **kwargs):
        self.from_email_address = kwargs['from_email_address']
        self.from_email_password = kwargs['from_email_password']
        self.recipients = kwargs['recipients']
        self.email_subject = kwargs['email_subject']
        self.body = kwargs['body']
        
    def send_mail(self, source_file_path):
        msg = MIMEMultipart()
        msg['Subject'] = self.email_subject
        msg['From'] = self.from_email_address
        msg['To'] = ', '.join(self.recipients)
        
        if self.body[0]=='<':
            msg.attach(MIMEText(self.body, 'html'))
        else:
            msg.attach(MIMEText(self.body, 'plain'))
        
        if source_file_path != '':
            filename = os.path.split(source_file_path)[1]
            attachment = open(source_file_path, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
            msg.attach(part)
     
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(self.from_email_address, self.from_email_password)
        server.sendmail(self.from_email_address, self.recipients, msg.as_string())
        server.quit()