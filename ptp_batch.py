from multiprocessing import Pool, TimeoutError
import datetime
import time
import os
from ercot.ptp import PTPProcessor
from itertools import product
import pandas as pd
import numpy

def process_pp_batch(loc,job,nlimit):
    t_start=time.time()
    print('starting....:',job,'...at...',str(datetime.datetime.now()))
    ptp= PTPProcessor(loc)
    ptp.process_ptp_pp_job(job,rec_limit=nlimit)
    print('finish job:',job,'... using...',time.time()-t_start)
    print('finish at:',str(datetime.datetime.now()))

def process_year_batch( loc,job,nlimit):
    t_start=time.time()
    print('starting....:',job,'...at...',str(datetime.datetime.now()))
    ptp= PTPProcessor(loc)
    ptp.process_ptp_year_job(job,rec_limit=nlimit)
    print('finish job:',job,'... using...',time.time()-t_start)
    print('finish at:',str(datetime.datetime.now()))

def convert2CSV(files):
    loc= "E:/quan_ml/ercot/"
    ptp=PTPProcessor(loc)
    ptp.convertOutput2CSV(files)

if (__name__ == "__main__"):
    loc= "E:\\quan_ml\\ercot\\"
    #process_pp_batch(loc,'0.job',400)   
    # convert2CSV(['0.job.pp.out'])
    files=[]
    for ii in range(45):
        outfile=str(ii)+'.job.pp.out'
        files.append(outfile)
    convert2CSV(files)
    '''
    for job in jobs:
        process_year_batch(loc,job,100)    
    ''' 
  
    #jobs=['0.job','1.job','2.job','3.job','4.job','5.job','6.job','7.job','8.job','9.job']
    #jobs=['0.job','1.job','2.job','3.job','5.job']
    '''
    jobs=[]
    for i in range(45):
        jobs.append(str(i)+'.job')
    params=[]
    for job in jobs:
        params.append((loc,job,-1))
    print(params)
    print(str(datetime.datetime.now()))
    with Pool(processes=10) as pool:     # 10 thread feel a little lag
        pool.starmap(process_pp_batch,params) 
    '''
    '''
    files=['1.job.year.out','2.job.year.out','3.job.year.out','4.job.year.out', \
    '5.job.year.out','6.job.year.out','7.job.year.out','8.job.year.out','9.job.year.out']
    ptp=PTPProcessor(loc)
    ptp.convertOutput2CSV(files)
    '''