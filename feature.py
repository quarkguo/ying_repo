import numpy as np
import pandas as pd
from datetime import date, timedelta,datetime
        
class FeatureBuidler:
    # change, changepercent, dailyfluc, dailyfluc percent
    # rollingwindow(ema,std on close and avg) 10,20,120
    # imballance bars, run bars
    def __init__(self):
        return

    def buildFeatures(self,df_input,includevolume=True):
        df_res=self._buildBasicFeature(df_input);
        df_res=self._buildDateTimeFeature(df_res);
        df_res=self._buildChangeFeature(df_res)
        df_res=self._buildRollingFeature(df_res)
        df_res=self.buildDiffFractionFeatures(df_res,includevolume=includevolume)
        df_res=self._buildTriggerChangeFeature(df_res,includevolume=includevolume)
        return df_res
   
    def _buildBasicFeature(self,df_input):
        df_res=df_input.copy()
        df_res['feature_open']=df_res.open
        df_res['feature_high']=df_res.high
        df_res['feature_low']=df_res.low
        df_res['feature_close']=df_res.close
        df_res['feature_volume']=df_res.volume
        df_res['feature_close_avg']=df_res.pc_avg  
        df_res['feature_closs_std']=df_res.pc_std 
        df_res['feature_open_avg']=df_res.po_avg
        df_res['feature_open_std']=df_res.po_std
        df_res['feature_cap']=df_res.pc_dol_in_mil
        return  df_res
        
    def _buildDateTimeFeature(self,df_input):
        df_res=df_input.copy()
        df_res['feature_month']=df_res.index.month
        df_res['feature_day']=df_res.index.day
        df_res['feature_weekday']=df_res.index.weekday
        df_res['feature_hour']=df_res.index.hour
        return df_res;
        
    def _buildChangeFeature(self, df_input):
        df_res=df_input.copy()
        df_res['feature_hl_del']=df_res.high-df_res.low
        df_res['feature_hl_del_per']=df_res.feature_hl_del/df_res.close
        df_res['feature_pc_del']=df_res.close-df_res.close.shift()
        df_res['feature_pc_del_per']=df_res.feature_pc_del/df_res.close.shift()
        df_res['feature_vol_del']=df_res.volume-df_res.volume.shift()
        df_res['feature_vol_del_per']=df_res.feature_vol_del/df_res.volume.shift()
        df_res['feature_high_del']=df_res.high-df_res.high.shift()
        df_res['feature_high_del_per']=df_res.feature_high_del/df_res.high.shift()
        df_res['feature_pc_del_flag']=1.0
        df_res.loc[df_res.feature_pc_del<0,'feature_pc_del_flag']=-1.0
        df_res['feature_vol_ba']=df_res.volume*df_res.feature_pc_del_flag
        df_res['feature_dollar_ba']=df_res.feature_pc_del_flag*df_res.pc_dol_in_mil
        
        return df_res
        
    def _buildRollingFeature(self, df_input):
        df_res=df_input.copy()
        df_res['feature_high_std10']=df_res.high.rolling(10, min_periods=1).std()
        df_res['feature_high_std20']=df_res.high.rolling(20, min_periods=1).std()
        df_res['feature_high_std60']=df_res.high.rolling(60, min_periods=1).std()
        df_res['feature_high_std120']=df_res.high.rolling(120, min_periods=1).std()
        df_res['feature_low_std10']=df_res.low.rolling(10, min_periods=1).std()
        df_res['feature_low_std20']=df_res.low.rolling(20, min_periods=1).std()
        df_res['feature_low_std60']=df_res.low.rolling(60, min_periods=1).std()
        df_res['feature_low_std120']=df_res.low.rolling(120, min_periods=1).std()
        df_res['feature_close_std10']=df_res.close.rolling(10, min_periods=1).std()
        df_res['feature_close_std20']=df_res.close.rolling(20, min_periods=1).std()
        df_res['feature_close_std60']=df_res.close.rolling(60, min_periods=1).std()
        df_res['feature_close_std120']=df_res.close.rolling(120, min_periods=1).std()
        df_res['feature_pc_avg_std10']=df_res.pc_avg.rolling(10, min_periods=1).std()
        df_res['feature_pc_avg_std20']=df_res.pc_avg.rolling(20, min_periods=1).std()
        df_res['feature_pc_avg_std60']=df_res.pc_avg.rolling(60, min_periods=1).std()
        df_res['feature_pc_avg_std120']=df_res.pc_avg.rolling(120, min_periods=1).std()
        df_res['feature_pc_avg_sma10']=df_res.pc_avg.rolling(10, min_periods=1).mean()
        df_res['feature_pc_avg_sma20']=df_res.pc_avg.rolling(20, min_periods=1).mean()
        df_res['feature_pc_avg_sma60']=df_res.pc_avg.rolling(60, min_periods=1).mean()
        df_res['feature_pc_avg_sma120']=df_res.pc_avg.rolling(120, min_periods=1).mean()
        df_res['feature_pc_sma10']=df_res.close.rolling(10, min_periods=1).mean()
        df_res['feature_pc_sma20']=df_res.close.rolling(20, min_periods=1).mean()
        df_res['feature_pc_sma60']=df_res.close.rolling(60, min_periods=1).mean()
        df_res['feature_pc_sma120']=df_res.close.rolling(120, min_periods=1).mean()
        df_res['feature_vol_sma10']=df_res.volume.rolling(10, min_periods=1).mean()
        df_res['feature_vol_sma20']=df_res.volume.rolling(20, min_periods=1).mean()
        df_res['feature_vol_sma60']=df_res.volume.rolling(60, min_periods=1).mean()
        df_res['feature_vol_sma120']=df_res.volume.rolling(120, min_periods=1).mean()
        df_res['feature_pc_std_pt10']=df_res.feature_close_std10/df_res.feature_pc_avg_sma10
        df_res['feature_pc_std_pt20']=df_res.feature_close_std20/df_res.feature_pc_avg_sma20
        df_res['feature_pc_std_pt60']=df_res.feature_close_std60/df_res.feature_pc_avg_sma60
        df_res['feature_pc_std_pt120']=df_res.feature_close_std120/df_res.feature_pc_avg_sma120
        df_res['feature_pc_imba']=df_res.feature_pc_del_flag.expanding().sum()
        df_res['feature_vol_imba']=df_res.feature_vol_ba.expanding().sum()
        df_res['feature_dollar_imba']=df_res.feature_dollar_ba.expanding().sum()
        df_res['feature_pc_run']=df_res.feature_vol_imba.expanding().max()
        df_res['feature_vol_run']=df_res.feature_vol_imba.expanding().max()
        df_res['feature_dollar_run']=df_res.feature_dollar_imba.expanding().max()
        
        # drift from mean
        df_res['feature_avg_drift']=df_res.feature_close-df_res.feature_pc_avg_sma120
        df_res['feature_std_drift']=df_res.feature_closs_std-df_res.feature_pc_avg_std120
        df_res['feature_volume_drift']=df_res.feature_volume-df_res.feature_vol_sma120
        return df_res
    
    def buildDiffFractionFeatures(self, df_input, includevolume=True):
        df_res=df_input.copy()    
        df_res['feature_close_ratio']=df_res['close']/df_res['close'].shift()
        df_res['feature_close_ratio_log']=np.log(df_res['feature_close_ratio'])
        df_res['feature_open_ratio']=df_res['open']/df_res['open'].shift()
        df_res['feature_open_ratio_log']=np.log(df_res['feature_open_ratio'])
        df_res['feature_high_ratio']=df_res['high']/df_res['high'].shift()
        df_res['feature_high_ratio_log']=np.log(df_res['feature_high_ratio'])
        df_res['feature_low_ratio']=df_res['low']/df_res['low'].shift()
        df_res['feature_low_ratio_log']=np.log(df_res['feature_low_ratio'])  
        if (includevolume):            
            df_res['feature_volume_ratio']=df_res['volume']/df_res['volume'].shift()
            df_res['feature_volume_ratio_log']=np.log(df_res['feature_volume_ratio'])
        return df_res
    
    def buildShiftFeature(self, df_input, shift_configs):
        df_res=df_input.copy()
        for sc in shift_configs:
            col=sc["featurename"]
            for shift in sc["shifts"]:
                shift_col=col+"_shift_"+str(shift)
                df_res[shift_col]=df_res[col].shift(shift)
        return df_res
    

    def _buildTriggerChangeFeature(self,df_input, includevolume=True):
        df_res=df_input.copy()
        df_res['feature_close_del_per']=(df_res['close']-df_res['close'].shift(1))/df_res['close'].shift(1)
        df_res['feature_close_del_per_3']=(df_res['close']-df_res['close'].shift(3))/df_res['close'].shift(3)
        df_res['feature_close_del_per_5']=(df_res['close']-df_res['close'].shift(5))/df_res['close'].shift(5)
        df_res['feature_close_del_per_10']=(df_res['close']-df_res['close'].shift(10))/df_res['close'].shift(10)
        if includevolume:
            df_res['feature_volume_del_per']=np.log(df_res['volume']/df_res['volume'].shift(1))
            df_res['feature_volume_del_per_3']=np.log(df_res['volume']/df_res['volume'].shift(3))
            df_res['feature_volume_del_per_5']=np.log(df_res['volume']/df_res['volume'].shift(5))    
            df_res['feature_volume_del_per_10']=np.log(df_res['volume']/df_res['volume'].shift(10))
        return df_res

