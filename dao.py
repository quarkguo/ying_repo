import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt
from datetime import date, timedelta,datetime
import pymysql
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String,DateTime,BigInteger,Float
import mplfinance as mpf

def initDBContext():
    _cnx = create_engine('mysql+pymysql://quark:Quark618$@localhost/marketdata')   
    _meta = MetaData(_cnx)
    return _cnx,_meta

class MarketData:
    def __init__(self):
        return
    
    def getTop100Stock(self,context):
        large_tickers=pd.read_sql_query("select * from marketdata.tickerdef where type='stock' order by reccount desc,marketcap desc limit 100 ",context)
        return large_tickers
    
    
class TickerData:
    def __init__(self, tickername,ty):
        self.ticker=tickername
        self.type=ty

    def loadData(self,ctx, dateStart=None,dateEnd=None):
        # default the database table store the minute data
        _start=datetime.now()
        _query="select * from "+self.type+"_"+self.ticker 
        if (dateStart is not None) or (dateEnd is not None):
            _query = _query +" where 17 = 17 "
            if dateStart is not None:
                _query = _query +" and datetime >='"+dateStart +"'"
            if dateEnd is not None:
                _query = _query +" and datetime <='"+dateEnd+"'"
        _query=_query+" order by timestamp asc"
        self.data=pd.read_sql_query(_query,ctx)
        self.data=self.data[['datetime', 'open', 'high', 'low', 'close', 'volume']]
        self.data=self.data.set_index(pd.DatetimeIndex(self.data['datetime']))
        print("loading time...",datetime.now()-_start)
        return self.data
    
    def getData(self, freq="1m", dtrange=None):
        if freq == '1m':
            _df_data=self.data.copy()
        else:
            _df_data=self.data.resample(freq,label='right',closed="right").agg({
                'open':'first',
                'high':np.max,
                'low':np.min,
                'close':'last',
                'volume':'sum'
                })
            _df_data.dropna(inplace=True)
        if(dtrange is not None):
            dt_start=dtrange[0]
            dt_end=dtrange[1]
            _df_data=_df_data.loc[dt_start:dt_end]
        return _df_data;

    def getEnrichData(self,freq='1D' , dtrange=None):
        _data_1m=self.getData()
        _data_1m['pc_avg']=_data_1m.close*_data_1m.volume
        _data_1m['pc_std']=_data_1m.close
        _data_1m['po_avg']=_data_1m.open*_data_1m.volume
        _data_1m['po_std']=_data_1m.open
        _data_res=_data_1m.resample(freq,label='right',closed='right').agg({
            'open':'first',
            'high':np.max,
            'low':np.min,
            'close':'last',
            'volume':'sum',
            'pc_avg':'sum',
            'pc_std':'std',
            'po_avg':'sum',
            'po_std':'std',
        })
        _data_res['pc_dol_in_mil']= _data_res.pc_avg/1000000.0
        _data_res.pc_avg=_data_res.pc_avg/(_data_res.volume+0.1)
        _data_res['po_dol_in_mil']= _data_res.po_avg/1000000.0
        _data_res.po_avg=_data_res.po_avg/(_data_res.volume+0.1)
        _data_res.dropna(inplace=True)
        if(dtrange is not None):
            dt_start=dtrange[0]
            dt_end=dtrange[1]
            _data_res=_data_res.loc[dt_start:dt_end]
        return _data_res
        
