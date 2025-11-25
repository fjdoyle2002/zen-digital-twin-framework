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
        
                
    def make_table(self):
        cur = self.conn.cursor()
        try:
            sql_stmt = "CREATE TABLE {} (key_id serial PRIMARY KEY, time TIMESTAMPTZ, signal_name VARCHAR(255) not null, value real);".format(self.dt_name)
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
                pass
            else:
                self.make_table()
        except psycopg2.Error as e:
            print ("Unable to create table!")
            print (e.pgerror)
            print (e.diag.message_detail)
            return
            
        
    def persist(self, timestamp):
        #varchar_id = self.generate_varchar_id()
        val_holders = "%s"
        
        #values_for_insert = [varchar_id,timestamp]
        
        for idx in self.sensors_df.index:
            curr_signal_name = self.sensors_df['PersistenceName'][idx]
            curr_value = self.sensors_df['current_val'][idx]
            values_for_insert = [timestamp]
            values_for_insert.append(curr_signal_name)
            values_for_insert.append(curr_value)
            try:
                cur = self.conn.cursor()
                sql_stmt = "INSERT INTO {} (time,signal_name,value) VALUES({});".format(self.dt_name, "%s,%s,%s" )
                cur.execute(sql_stmt, values_for_insert)
                cur.close()
                self.conn.commit()
            except psycopg2.Error as e:
                print ("Unable to persist signal('{}')for time{}!".format(curr_signal_name,timestamp))

                print (e.pgerror)
                print (e.diag.message_detail)
                       
            