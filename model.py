import numpy as np
import pandas as pd
import os
import sys
import matplotlib.pyplot as plt
from datetime import date, timedelta,datetime
import pymysql
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String,DateTime,BigInteger,Float
from sqlalchemy.orm import sessionmaker
import mplfinance as mpf
%matplotlib inline
module_path = os.path.abspath(os.path.join('data'))
if module_path not in sys.path:
    sys.path.append(module_path)
    from data import dao,feature,label
import importlib
from fastai.tabular import * 
from sklearn.metrics import classification_report,confusion_matrix
from fastai import metrics as fmetrics
from numpy import mean
from sklearn.datasets import make_classification
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import RepeatedStratifiedKFold
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score,roc_auc_score

# now trying to loop through this build models for 3 barriars 5, 10,20 days for both fastai and xgb modles
def runDailyModel(ticker,tt,context):
    start=datetime.now()
    print("start on ",ticker," ",start)
    days=[10,15,20]
    fe_builder=feature.FeatureBuidler()
    label_builder=label.LabelBuilder()
    _data=dao.TickerData(ticker,tt)
    _data.loadData(context)
    _data_r=_data.getEnrichData('1D')
 
    procs = [FillMissing, Categorify, Normalize]
    res=[];
    for day in days:       
        fe_data_r=fe_builder.buildFeatures(_data_r)
        fe_data_r=label_builder.build3Barrier(fe_data_r,day,0.06,[1,-1])
        df_learn=fe_data_r[f_cols].sample(frac=1.0).dropna()
        n_train=(int)(len(df_learn)*0.8)
        valid_idx2=range(n_train,len(df_learn))
        data = TabularDataBunch.from_df(".", df_learn, 'lbl_100hr_3ba',valid_idx=valid_idx2,procs=procs,bs=32)
        learn = tabular_learner(data, layers=[32,32],metrics=fmetrics.accuracy)
        learn.model=torch.nn.DataParallel(learn.model)
        learn.fit_one_cycle(8, 0.005)
        learn.recorder.silent= True
        y_pred,y_true=learn.get_preds()
        y_pred_label=np.argmax(y_pred, axis=1)
        score=accuracy_score(y_true,y_pred_label)
        res.append({'ticker':ticker,'model':'fastai-32x32-'+str(day),'score':score})
        # xgb
        n_train=(int)(len(df_learn)*0.8)
        df_train=df_learn[:n_train]
        df_test=df_learn[n_train:]
        x_train=df_train[df_learn.columns[:-1]]
        y_train=df_train['lbl_100hr_3ba']
        x_test=df_test[df_learn.columns[:-1]]
        y_test=df_test['lbl_100hr_3ba']
        model = XGBClassifier()
        model.fit(x_train, y_train)
        # make predictions for test data
        y_pred = model.predict(x_test)
        predictions = [round(value) for value in y_pred]
        # evaluate predictions
        accr = accuracy_score(y_test, predictions)
        res.append({'ticker':ticker,'model':'xgb-'+str(day),'score':accr})
        diff=datetime.now()-start
        print("time cost --->",diff)
    return res
# now trying to loop through this build models for 3 barriars 5, 10,20 days for both fastai and xgb modles
def runHourlyModel(ticker,tt,context):
    start=datetime.now()
    print("start on ",ticker," ",start)
    hours=[72,120,200]
    fe_builder=feature.FeatureBuidler()
    label_builder=label.LabelBuilder()
    _data=dao.TickerData(ticker,tt)
    _data.loadData(context)
    _data_r=_data.getEnrichData('1h')
 
    procs = [FillMissing, Categorify, Normalize]
    res=[];
    for hour in hours:       
        fe_data_r=fe_builder.buildFeatures(_data_r)
        fe_data_r=label_builder.build3Barrier(fe_data_r,hour,0.06,[1,-1])
        df_learn=fe_data_r[f_cols].sample(frac=1.0).dropna()
        n_train=(int)(len(df_learn)*0.8)
        valid_idx2=range(n_train,len(df_learn))
        data = TabularDataBunch.from_df(".", df_learn, 'lbl_100hr_3ba',valid_idx=valid_idx2,procs=procs,bs=32)
        learn = tabular_learner(data, layers=[32,32],metrics=fmetrics.accuracy)
        learn.model=torch.nn.DataParallel(learn.model)
        learn.fit_one_cycle(8, 0.005)
        learn.recorder.silent= True
        y_pred,y_true=learn.get_preds()
        y_pred_label=np.argmax(y_pred, axis=1)
        score=accuracy_score(y_true,y_pred_label)
        res.append({'ticker':ticker,'model':'fastai-32x32-'+str(hour),'score':score})
        # xgb
        n_train=(int)(len(df_learn)*0.8)
        df_train=df_learn[:n_train]
        df_test=df_learn[n_train:]
        x_train=df_train[df_learn.columns[:-1]]
        y_train=df_train['lbl_100hr_3ba']
        x_test=df_test[df_learn.columns[:-1]]
        y_test=df_test['lbl_100hr_3ba']
        model = XGBClassifier()
        model.fit(x_train, y_train)
        # make predictions for test data
        y_pred = model.predict(x_test)
        predictions = [round(value) for value in y_pred]
        # evaluate predictions
        accr = accuracy_score(y_test, predictions)
        res.append({'ticker':ticker,'model':'xgb-'+str(hour),'score':accr})
        diff=datetime.now()-start
        print("time cost --->