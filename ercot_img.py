import datetime
import time
import os
import sys
import pandas as pd
import numpy
import cv2

class ErcotImageProcessor:
    def __init__(self,loc,imageRoot,epiccenters):
        self.loc=loc
        self.imageroot=imageRoot
        self.epiccenters=epiccenters
        print('current location is :',self.loc)

    def calFeatures(self,img):
        f_def=self.epiccenters
        res=[]
        for fp in f_def:
            ele={"cx":fp["cx"], "cy":fp["cy"], "r":fp["r"],"ncount":0,"blue":0,"green":0,"red":0}
            res.append(ele)
        for px in range(img.shape[0]):
            for py in range(img.shape[1]):
                pix=img[py,px]
                if(pix[0]==0 & pix[1]==0 & pix[2]==0):
                    continue
                for fp in res:
                    cx=fp["cx"]
                    cy=fp["cy"]
                    r=fp["r"]
                    if((px-cx)*(px-cx)+(py-cy)*(py-cy)<r*r):
                        fp["ncount"]=fp["ncount"]+1
                        fp["blue"]= fp["blue"]+pix[0]
                        fp["green"]=fp["green"]+pix[1]
                        fp["red"]= fp["red"]+pix[2]
        for fp in res:
            if(fp["ncount"]>0):
                fp["blue"]= fp["blue"]/fp["ncount"]
                fp["green"]=fp["green"]/fp["ncount"]
                fp["red"]=fp["red"]/fp["ncount"]
        return res
        
    def calFeaturesRect(self,img,filename):
        f_def=self.epiccenters
        res=[]
        for fp in f_def:
            ele={"filename":filename,"cx":fp["cx"], "cy":fp["cy"], "r":fp["r"],"ncount":0,"blue":0,"green":0,"red":0}
            res.append(ele)
        for fp in res:
            cx=fp["cx"]
            cy=fp["cy"]
            r=fp["r"]
            for px in range(cx-r,cx+r):
                for py in range(cy-r,cy+r):
                    pix=img[py,px]
                    if(pix[0]==0 & pix[1]==0 & pix[2]==0):
                        continue
                    fp["ncount"]=fp["ncount"]+1
                    fp["blue"]= fp["blue"]+pix[0]
                    fp["green"]=fp["green"]+pix[1]
                    fp["red"]= fp["red"]+pix[2]
        for fp in res:
            if(fp["ncount"]>0):
                fp["blue"]= fp["blue"]/fp["ncount"]
                fp["green"]=fp["green"]/fp["ncount"]
                fp["red"]=fp["red"]/fp["ncount"]
        return res

    def flatten(self,res):
        ele={}
        ele['filename']=res[0]['filename']
        index=0
        for row in res:
            index=index+1
            bn="p"+str(index)+"_blue"
            gn="p"+str(index)+"_green"
            rn="p"+str(index)+"_red"
            ele[bn]=row['blue']
            ele[gn]=row['green']
            ele[rn]=row['red']
        return ele

    def jobLoc(self,jobfn):
        return self.loc+"map/"+jobfn

    def outputFile(self,jobfn):
        return self.loc+"output/"+jobfn+".out"
    
    def imageLoc(self,filename):
        return self.imageroot+filename 

    def readJob(self,fn):
        file=open(self.jobLoc(fn),'r')
        lines=file.readlines()
        res=[]
        for ln in lines:
            ln=ln.strip()
            if(ln!=''):
                res.append(ln)
        #print(len(res))
        return res

        # nlimit is to test out during development
    def processJob(self,jobfn,nLimit=-1):
        res=self.readJob(jobfn)
        if(nLimit!=-1):
            res=res[:nLimit]
        result=[]
        for ff in res:
            #print(self.imageLoc(ff))
            img=cv2.imread(self.imageLoc(ff))
           # print(img)
            ele=self.flatten(self.calFeaturesRect(img,ff))
            result.append(ele)
        # now writing files
        df=pd.DataFrame(result)
        df.to_csv(self.outputFile(jobfn))
        return