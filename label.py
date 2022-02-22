import numpy as np
import pandas as pd
from datetime import date, timedelta,datetime

# tagging need to comes up with better naming standard
class LabelBuilder:
    def __init__(self):
        return
    
    # this function will build lbl (-1,0,1) three state where (-tgtpercent, 0, tgtpercent) 3 scenarios 
    def buildF1(self,df_input,tgt, winloss):
        df_res=df_input.copy()
        tgt_profit=tgt*winloss[0]
        tgt_loss=tgt*winloss[1]
        df_res['lbl_pc_del_per']=df_res['feature_pc_del_per'].shift(-1)
        df_res['lbl_tgt_pc_del_per']=0
        df_res.loc[df_res.lbl_pc_del_per>=tgt_profit,'lbl_tgt_pc_del_per']=1
        df_res.loc[df_res.lbl_pc_del_per<=tgt_loss,'lbl_tgt_pc_del_per']=2
        return df_res
    
        
    # the 3 barrier (holdTick, winTick, lossTick) (holdGain, winGain, LossGain]
    def build3Barrier(self,df_input,maxholdtics,tgt,winloss):
        df_input_copy=df_input.copy()
        tgt_profit=tgt*winloss[0]
        tgt_loss=tgt*winloss[1]
        df_input_copy[['t_maxhold','t_win','t_loss','hold_gain','win_gain','loss_gain']]=pd.DataFrame([[np.nan, np.nan, np.nan,np.nan,np.nan,np.nan]], index=df_input_copy.index)
        for idx,row in df_input.iterrows():
            pc=row.close
            pc_tgt_win=pc*(1.0+tgt_profit)
            pc_tgt_loss=pc*(1.0+tgt_loss) 
            pos=df_input.index.get_loc(idx)
            _df_tmp=df_input[pos:pos+maxholdtics]
            if((_df_tmp is not None) and (len(_df_tmp)>0)):
                tmaxhold=_df_tmp[-1:].index[0]
                t_win=_df_tmp.loc[_df_tmp.high>=pc_tgt_win].index.min()
                t_loss=_df_tmp.loc[_df_tmp.low<=pc_tgt_loss].index.min()
                holdgain=(_df_tmp.iloc[-1].close-pc)/pc
                win_gain=tgt_profit
                loss_gain=tgt_loss
                df_input_copy.at[idx,'t_maxhold']=tmaxhold
                df_input_copy.at[idx,'t_win']=t_win
                df_input_copy.at[idx,'t_loss']=t_loss
                df_input_copy.at[idx,'hold_gain']=holdgain
                df_input_copy.at[idx,'win_gain']=win_gain    
                df_input_copy.at[idx,'loss_gain']=loss_gain
        df_input_copy['lbl_3ba']=0
        df_input_copy.loc[(df_input_copy["t_win"].notnull() ) & (df_input_copy['t_loss'].isnull()),'lbl_3ba']=1
        df_input_copy.loc[(df_input_copy.t_win.notnull()) & ((df_input_copy.t_loss.notnull()) & (df_input_copy.t_win<df_input_copy.t_loss)),'lbl_3ba']=1
        df_input_copy.loc[(df_input_copy.t_loss.notnull()) & (df_input_copy.t_win.isnull()),'lbl_3ba']=2
        df_input_copy.loc[(df_input_copy.t_loss.notnull()) & ((df_input_copy.t_win.notnull()) & (df_input_copy.t_loss<df_input_copy.t_win)),'lbl_3ba']=2        
        return df_input_copy
   
    def buildPriceChangeTag(self,df_input,pricechangePer):
        df_res=df_input.copy()
        df_res['tag_price_rise']=0
        df_res['tag_price_rise_3']=0
        df_res['tag_price_rise_5']=0
        df_res['tag_price_rise_10']=0
        df_res.loc[df_res.feature_close_del_per>pricechangePer,'tag_price_rise']=1
        df_res.loc[df_res.feature_close_del_per_3>pricechangePer,'tag_price_rise_3']=1
        df_res.loc[df_res.feature_close_del_per_5>pricechangePer,'tag_price_rise_5']=1
        df_res.loc[df_res.feature_close_del_per_10>pricechangePer,'tag_price_rise_10']=1
        df_res['tag_price_drop']=0
        df_res['tag_price_drop_3']=0
        df_res['tag_price_drop_5']=0
        df_res['tag_price_drop_10']=0
        df_res.loc[df_res.feature_close_del_per<-pricechangePer,'tag_price_drop']=1
        df_res.loc[df_res.feature_close_del_per_3<-pricechangePer,'tag_price_drop_3']=1
        df_res.loc[df_res.feature_close_del_per_5<-pricechangePer,'tag_price_drop_5']=1
        df_res.loc[df_res.feature_close_del_per_10<-pricechangePer,'tag_price_drop_10']=1        
        return df_res
    
    def buildVolumeChangeTag(self,df_input,volumeChangePer):
        df_res=df_input.copy()
        df_res['tag_vol_rise']=0
        df_res['tag_vol_rise_3']=0
        df_res['tag_vol_rise_5']=0
        df_res['tag_vol_rise_10']=0
        df_res.loc[df_res.feature_close_del_per>volumeChangePer,'tag_vol_rise']=1
        df_res.loc[df_res.feature_close_del_per_3>volumeChangePer,'tag_vol_rise_3']=1
        df_res.loc[df_res.feature_close_del_per_5>volumeChangePer,'tag_vol_rise_5']=1
        df_res.loc[df_res.feature_close_del_per_10>volumeChangePer,'tag_vol_rise_10']=1
        df_res['tag_vol_drop']=0
        df_res['tag_vol_drop_3']=0
        df_res['tag_vol_drop_5']=0
        df_res['tag_vol_drop_10']=0
        df_res.loc[df_res.feature_close_del_per<-volumeChangePer,'tag_vol_drop']=1
        df_res.loc[df_res.feature_close_del_per_3<-volumeChangePer,'tag_vol_drop_3']=1
        df_res.loc[df_res.feature_close_del_per_5<-volumeChangePer,'tag_vol_drop_5']=1
        df_res.loc[df_res.feature_close_del_per_10<-volumeChangePer,'tag_vol_drop_10']=1        
        return df_res