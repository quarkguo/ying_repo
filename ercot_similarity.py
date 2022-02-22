import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import datetime
import matplotlib.pyplot as plt
from datetime import date, timedelta
from sklearn import preprocessing
from scipy.spatial import distance

def cal_distant(t_op,t_ref):
    v1=t_op.flatten()
    v2=t_ref.flatten()
    if(len(v1)!=len(v2)):
        return 9999.99
    return np.linalg.norm(v1-v2)
def cal_cosinedistant(t_op,t_ref):
    v1=t_op.flatten()
    v2=t_ref.flatten()
    if(len(v1)!=len(v2)):
        return 9999.99
    return distance.cosine(v1,v2)

if (__name__ == "__main__"):
    engine = create_engine('postgresql://psedatamaster:Woodup71RedWood@psedataproduction.crkajpgzxx4n.us-east-1.redshift.amazonaws.com:5439/psepowerstore')
    sql_config="select * from ercot_base.cfg_silimar_day_search"
    df_config = pd.read_sql_query(sql_config, engine)
    res=[]
    for index,row in df_config.iterrows():
        cfg=row
        sql_op="Select opdate, he, "+cfg.data_tag+ " from "+cfg.data_set+" where opdate = '"+str(cfg.target_opdate)+"' order by he"
        df_op=pd.read_sql_query(sql_op,engine)
        cols=[x.strip() for x in cfg.data_tag.split(",")]
        sql_ref="Select opdate, he, "+cfg.data_tag+ " from "+cfg.data_set+" where opdate between '" + str(cfg.date_range_low) +"' and '" +str(cfg.date_range_high) + "' order by opdate desc ,he" 
        print(sql_ref)
        df_data = pd.read_sql_query(sql_ref,engine)
        df_merge=df_op.append(df_data)
        _data=df_merge.copy()
        min_max_scaler = preprocessing.MinMaxScaler()
        for col in cols:
            _data[col]=min_max_scaler.fit_transform(pd.DataFrame(_data[col]))
        l_dates=list(df_merge.opdate.unique())
        t_op_v=_data.loc[_data.opdate==cfg.target_opdate,cols].values
        for dt_ref in l_dates:
            if dt_ref !=cfg.target_opdate:
                t_ref_v=_data.loc[_data.opdate == dt_ref,cols].values
                dis_eu=cal_distant(t_op_v,t_ref_v)
                dis_cosine=cal_cosinedistant(t_op_v,t_ref_v)
                res.append({"cfg_id":cfg.cfg_id,"opdate":cfg.target_opdate,"col":cols,"refdate":dt_ref,"dis_eu":dis_eu,"dis_cosine":dis_cosine})
    df_res=pd.DataFrame(res)
    df_res.to_csv("similarity_output.csv")