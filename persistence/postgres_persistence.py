# -*- coding: utf-8 -*-
"""
Created on Fri Jul 21 10:39:30 2023

@author: doylef
"""
import psycopg2
# import the error handling libraries for psycopg2
from psycopg2 import OperationalError, errorcodes, errors
import datetime
import pandas as pd
import logging
logger = logging.getLogger(__name__)

class PostgresPersistence:
    def __init__(self, config, sensors_df):
        self.sensors_df = sensors_df
        self.dbname = config.get('DATABASE', 'DatabaseName')       
        self.dbhost = config.get('DATABASE', 'DatabaseHost')
        self.dbport = config.get('DATABASE', 'DatabasePort')
        self.dbuser = config.get('DATABASE', 'DatabaseUser')
        self.dbpass = config.get('DATABASE', 'DatabasePass')
        self.dt_name = config.get('DEFAULT', 'DigitalTwinIdentifier')
        self.successfully_initialized = False
        
        
        try:
            self.conn = psycopg2.connect(
                host=self.dbhost,
                port=self.dbport,
                database=self.dbname,
                user=self.dbuser,
                password=self.dbpass)
        except psycopg2.Error as e:
            print ("Unable to connect!")
            print (e.pgerror)
            print (e.diag.message_detail)
            return
        
        self.config_output_table()
        
    def config_columns(self):
        
        for idx in self.sensors_df.index:
            col_name = self.sensors_df['PersistenceName'][idx]
            col_type = self.sensors_df['DataType'][idx]
            sql_stmt = "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {} NULL;".format(self.dt_name, col_name, col_type)
            cur = self.conn.cursor()
            try:
                cur.execute(sql_stmt)
                cur.close()
                self.conn.commit()
            except psycopg2.Error as e:
                print ("Unable to create table!")
                print (e.pgerror)
                print (e.diag.message_detail)
                return
            
        self.successfully_initialized = True
                
    def make_table(self):
        cur = self.conn.cursor()
        try:
            #sql_stmt = "CREATE TABLE {} (key_id serial PRIMARY KEY, id varchar(20) NOT NULL, time timestamp null);".format(self.dt_name)
            sql_stmt = "CREATE TABLE {} (time TIMESTAMPTZ PRIMARY KEY);".format(self.dt_name)
            cur.execute(sql_stmt)
            cur.close()
            self.conn.commit()
        except psycopg2.Error as e:
            print ("Unable to create table!")
            print (e.pgerror)
            print (e.diag.message_detail)
            return
        print("Table'{}' created.".format(self.dt_name))
                   
            
    def config_output_table(self):
        '''
        Confirms that table representing the digital twin instance exists and that
        all sensors to be read have associated columns and meta data

        Parameters
        ----------
        None.

        Returns
        -------
        None.

        '''
        cur = self.conn.cursor()
        try:
            cur.execute("select exists(select * from information_schema.tables where table_name='{}');".format(self.dt_name))
            table_exists = cur.fetchone()[0]
            cur.close()
            if table_exists:
                self.config_columns()
            else:
                self.make_table()
                self.config_columns()
        except psycopg2.Error as e:
            print ("Unable to create table!")
            print (e.pgerror)
            print (e.diag.message_detail)
            return
            
    def generate_varchar_id(self):
        current_time = datetime.datetime.now()
        microseconds = current_time.strftime("%f")
        millis = str(int(int(microseconds)/1000)).zfill(4)
        new_id = current_time.strftime("WO%Y%m%d%H%M%S")+millis
        return new_id
        
    def persist(self, timestamp):
        #varchar_id = self.generate_varchar_id()
        columns_for_insert = "time"
        val_holders = "%s"
        
        #values_for_insert = [varchar_id,timestamp]
        values_for_insert = [timestamp]
        for idx in self.sensors_df.index:
            col_name = self.sensors_df['PersistenceName'][idx]
            columns_for_insert = columns_for_insert+","+col_name
            values_for_insert.append(self.sensors_df['current_val'][idx])
            val_holders = val_holders+",%s"
        cur = self.conn.cursor()
        try:
            sql_stmt = "INSERT INTO {} ({}) VALUES({});".format(self.dt_name, columns_for_insert, val_holders)
            cur.execute(sql_stmt, values_for_insert)
            cur.close()
            self.conn.commit()
        except psycopg2.Error as e:
            print ("Unable to persist for time{}!".format(timestamp))
            print (e.pgerror)
            print (e.diag.message_detail)
            return        
            