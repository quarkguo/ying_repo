import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt
from datetime import date, timedelta,datetime
import mplfinance as mpf

# this module have the visualization tool for the time series data
# provide the dataset have standard data elements

class VisTool:
    def __init__(self, data):
        self._data=data
        print(len(self._data))
        self.region_width=168
        return
    
    def plot(self):
        ndownsample=round(len(self._data)/1200)
        if ndownsample > 1 :
            print("down sampling",ndownsample)
            mpf.plot(self._data.iloc[::ndownsample,:], figratio=(20,8),type='candle',style='charles',mav=(7,23,123),volume=True)
        else:
            mpf.plot(self._data, figratio=(20,8),type='candle',style='charles',mav=(7,23,123),volume=True)
    
    # plot focus region
    def plotRegion(self,ts):
        pos=self._data.index.get_loc(ts)
        start=max(pos-self.region_width,0)
        end=min(pos+self.region_width,len(self._data)-1)
        _df_tmp=self._data[start:end]
        signals=[]
        close=self._data['close'][pos]
        print(close)
        tpos=_df_tmp.index.get_loc(ts)
        print(tpos)
        for i in range(len(_df_tmp)):
            if(i==tpos):
                signals.append(close*.98)
            else:
                signals.append(np.nan)
        print(len(_df_tmp),len(signals))
        adp=mpf.make_addplot(signals, scatter=True,markersize=120, color='green',marker='|')
        mpf.plot(_df_tmp, figratio=(20,8),addplot=adp,type='candle',style='charles',mav=(7,23,123),volume=True)
    
    # plot focus region
    def plotRange(self,ts,te):
        p_start=self._data.index.get_loc(ts)
        p_end=self._data.index.get_loc(te)
        start=max(p_start-60,0)
        end=min(p_end+60,len(self._data)-1)
        _df_tmp=self._data[start:end]
        # marker
        signals=[]
        p_start=_df_tmp.index.get_loc(ts)
        p_end=_df_tmp.index.get_loc(te)
        for i in range(len(_df_tmp)):
            if (i==p_start) or (i==p_end):
                print("add signal")
                signals.append(_df_tmp.high[i]*1.05)
            else:
                signals.append(np.nan)
        print(len(_df_tmp),len(signals))
        adp=mpf.make_addplot(signals, scatter=True,markersize=120, color='green',marker='*')
        mpf.plot(_df_tmp, figratio=(20,8),addplot=adp,type='candle',style='charles',mav=(7,23,123),volume=True)        
        
    # sell signal is list of object with timestamps and sell price
    # buy signal is list of timestamp with buy price
    def plotSignals(self,sells=[],buys=[]):
        if sells is not None and len(sells)>0:
            ts=sells[0]
            te=sells[0]
            ts=min(ts,min(sells))
            te=max(te,max(sells))
        if buys is not None and len(buys)>0:
            if(ts is None) :
                ts=buys[0]
            if(te is None) :
                te=buys[0]
            te=max(te,max(buys))
            ts=min(ts,min(buys))
        print(ts,te)
        p_start=self._data.index.get_loc(ts)
        p_end=self._data.index.get_loc(te)
        start=max(p_start-60,0)
        end=min(p_end+60,len(self._data)-1)
        _df_tmp=self._data[start:end]
        adp=[]
        if buys is not None and len(buys)>0:
            pos_buys=[]
            for tt in buys:
                pos_buys.append(_df_tmp.index.get_loc(tt))
            sig_buys=[]
            for ix in range(len(_df_tmp)):
                if ix in pos_buys:
                    sig_buys.append(_df_tmp.iloc[ix].low*0.95)
                else:
                    sig_buys.append(np.nan)
            adp.append(mpf.make_addplot(sig_buys, scatter=True,markersize=120, color='green',marker='^'))
        if sells is not None and len(sells)>0:
            pos_sells=[]
            for tt in sells:
                pos_sells.append(_df_tmp.index.get_loc(tt))
            sig_sells=[]
            for ix in range(len(_df_tmp)):
                if ix in pos_sells:
                    sig_sells.append(_df_tmp.iloc[ix].high*1.05)
                else:
                    sig_sells.append(np.nan)
            adp.append(mpf.make_addplot(sig_sells, scatter=True,markersize=120, color='red',marker='v'))               
        mpf.plot(_df_tmp, figratio=(20,8),addplot=adp,type='candle',style='charles',mav=(7,23,123),volume=True)  
