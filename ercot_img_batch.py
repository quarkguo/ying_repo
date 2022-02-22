from multiprocessing import Pool, TimeoutError
import datetime
import time
import os
import sys
import pandas as pd
import numpy
from ercot.ercot_img import ErcotImageProcessor

def createJobFiles(inputLoc,packedSize):
    list_files=os.listdir(inputLoc)
    nsize=(int)(len(list_files)/packedSize)
    for ii in range(nsize+1):
        nlowbound=ii*packedSize
        nupbound=min((len(list_files),(ii+1)*packedSize))
        print(str(ii)," ",nlowbound,"--",nupbound)
        # create job input files
        fn="job.fn."+str(ii)
        file=open("ercot/map/"+fn,"w+")
        for ele in list_files[nlowbound:nupbound]:
            file.write(ele+"\n")
        file.close()
    return

def process_batch(job,nlimit):
    t_start=time.time()
    grid_def=[]
    input_loc="E:/quan_ml/ercot/data/ERCOTImages_20191002/ERCOTImages_20191002/"
    grid_def.append({"cx":155,"cy":328,"r":60})
    grid_def.append({"cx":200,"cy":230,"r":30})
    grid_def.append({"cx":240,"cy":100,"r":70})
    grid_def.append({"cx":280,"cy":340,"r":50})
    grid_def.append({"cx":360,"cy":450,"r":60})
    grid_def.append({"cx":400,"cy":210,"r":60})
    grid_def.append({"cx":420,"cy":330,"r":60})
    print('starting....:',job,'...at...',str(datetime.datetime.now()))
    proc=ErcotImageProcessor("ercot/",input_loc,grid_def)
    proc.processJob(job,nlimit)
    print('finish job:',job,'... using...',time.time()-t_start)
    print('finish at:',str(datetime.datetime.now()))

def mergeDataFile(foldname):
    return
if (__name__ == "__main__"):
    #createJobFiles(input_loc,1200)
    #readJob("ercot/map/job.fn.0")
    params=[]
    for i in range(0,56):
        params.append(("job.fn."+str(i),-1))
    print(params)
    print('start at:',str(datetime.datetime.now()))
    with Pool(processes=6) as pool:     # 10 thread feel a little lag
        pool.starmap(process_batch,params) 