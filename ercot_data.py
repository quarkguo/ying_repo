import pandas as pd
import numpy as np
import pickle
import time
import datetime
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import os

class ErcotDB:
    def __init__(self):
        self.dbhost='psedataproduction.crkajpgzxx4n.us-east-1.redshift.amazonaws.com:5439/'
        self.database='psepowerstore'
        self.user='psedatamaster'
        self.password='Ruby8.24RedWood'
        self.protocol='postgresql://'
        print('current location is :',self.makeDBURL())

    def makeDBURL(self):
        url=self.protocol+self.user+':'+self.password+'@'+self.dbhost+self.database
        return url
    
    def makeEngine(self):
        self.engine= create_engine(self.makeDBURL())
    
    def readForeCastdata2(self,filename):
        sql_select='''
        select opdate, he, north, south, west, houston, totalsystem,cutoff
        from ercot_base.ercot_lz_mtlf where cutoff >=18 and cutoff <= 72
        order by opdate, he, cutoff       
        '''  
        conn=self.engine.connect()
        resultset=conn.execute(sql_select)
        recs=[]
        
        for rec in resultset:
            recs.append({
                "opdate":rec['opdate'],
                "he":rec['he'],
                "north":rec["north"],
                "south":rec["south"],
                "west":rec["west"],
                "houston":rec["houston"],
                "totalsystem":rec["totalsystem"],
                "cutoff":rec["cutoff"]
            })
        print(len(recs))
        df=pd.DataFrame()
        df=df.append(recs,ignore_index=True)
        df.to_csv(filename)
    def readForecastData(self,filename):
        sql_select='''
        select opdate, he, totalsystem as system_mtlf, cutoff as cutoff
        from ercot_base.ercot_wz_mtlf where cutoff >= 18  and cutoff <= 72
        order by opdate, he, cutoff       
        '''
        conn=self.engine.connect()
        resultset=conn.execute(sql_select)
        recs=[]
        
        for rec in resultset:
            recs.append({
                "opdate":rec['opdate'],
                "he":rec['he'],
                "forecast":rec["system_mtlf"],
                "cutoff":rec["cutoff"]
            })
        print(len(recs))
        df=pd.DataFrame()
        df=df.append(recs,ignore_index=True)
        df.to_csv(filename)
    def readLoadData(self,filename):
        sql_select='''
        select opdate, he, totalsystem  from  ercot_base.analysis_v_wz_load order by opdate,he
        '''
        conn=self.engine.connect()
        resultset=conn.execute(sql_select)
        recs=[]
        for rec in resultset:
            recs.append({
                "opdate":rec['opdate'],
                "he":rec['he'],
                "load":rec["totalsystem"]
            })
        print(len(recs))
        df=pd.DataFrame()
        df=df.append(recs,ignore_index=True)
        df.to_csv(filename)

if (__name__ == "__main__"):
    print("connect to DB")
    outfile="E:\\quan_ml\\ercot\\data\\forecast3.csv"
    db=  ErcotDB()
    db.makeEngine()
    #df=db.readLoadData(outfile)
    db.readForeCastdata2(outfile)
    