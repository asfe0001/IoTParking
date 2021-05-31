#!python3

##################################################################################################
# Adapted by Danial Haris Limi Hawari, LiDa, dannyalharris@gmail.com                             #
# MQTT Energy Management System for IKKU Lab 043, Hochschule Karlsruhe, HsKA                     #
##################################################################################################

import sqlite3
from sqlite3 import Error



class MQTT_SQLite_Logger():
    def __init__(self,db_file):
        try:
            self.conn = sqlite3.connect(db_file,isolation_level=None)
        except Error as e:
            print(e)

        self.conn.execute('pragma foreign_keys = on')
        self.conn.execute('pragma journal_mode=wal')
        self.conn.commit()
        self.cur = self.conn.cursor()
        self.verbose=False


    def Log_input(self,sql_query, args=()):
        r=self.cur.execute(sql_query, args)
        if self.verbose:
            print('inserted ',r)
     
        self.conn.commit()

    def Log_message(self,table_name,data): #This function we do not use anymore
        #print("table ",table_name)
        #print("data to log ",data)
        broker = data["broker"]
        time_taken = data["time_taken"]
        time = data["time"]
        #interval =data["interval"]
        count=data["count"]

        r=self.cur.execute('''INSERT INTO '''+ \
        table_name +'''(broker, time_taken, time, count) 
        VALUES(?,?,?,?)''', (broker,time_taken,time, count))
        if self.verbose:
            print('inserted ',r)
     
        self.conn.commit()
    def drop_table(self,table_name):
        """ drop a table """
        sql_drop_table="DROP TABLE "+table_name +";"        
        try:
            c = self.conn.cursor()
            c.execute(sql_drop_table)
            if self.verbose:
                print ("table ",table_name," dropped")
        except Error as e:
            print(" error ",e)

    def create_table(self,table_name,t_fields):

        com="\n ("

        for key,value in t_fields.items():
            com=com+ key+" "+value +",\n"
        com=com[:-2]
        com=com+"\n);"

        sql_create_table='''CREATE TABLE IF NOT EXISTS '''+table_name+com        
        try:
            c = self.conn.cursor()
            c.execute(sql_create_table)
            if self.verbose:
                print ("table created")
        except Error as e:
            print(" error ",e)

