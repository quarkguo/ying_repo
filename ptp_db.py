import pandas as pd
import numpy as np
import pickle
import time
import datetime
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import os
from multiprocessing import Pool, TimeoutError

class PTPJobRec:

    def __init__(self, batchName, seq, par1, par2, jobType, statType):
        self.batchName=batchName
        self.seq=seq
        self.par1=par1
        self.par2=par2
        self.jobType=jobType
        self.statType=statType
        self.startTime=None
        self.completeTime=None
        self.agentName=''
        self.host='localhost'
        self.nprocessed=0
        self.outputfile=''
        self.tileid=''
    def printMe(self):
        print(self.batchName, self.seq,self.par1,self.par2,self.jobType,self.statType)

class PTPDBScheduler:
    #url <- "jdbc:redshift://psedataproduction.crkajpgzxx4n.us-east-1.redshift.amazonaws.com:5439/psepowerstore?user=psedatamaster&password=Woodup71RedWood"
    
    # constructor for DB processor
    def __init__(self,loc):
        self.dbhost='psedataproduction.crkajpgzxx4n.us-east-1.redshift.amazonaws.com:5439/'
        self.database='psepowerstore'
        self.user='psedatamaster'
        self.password='Ruby8.24RedWood'
        self.protocol='postgresql://'
        self.loc=loc
        print("PTP procoessing location:",self.loc)
        print('current location is :',self.makeDBURL())
    
    def mapfilename(self,batchname,partition):
        res=self.loc+'/map/'+batchname+'_'+str(partition)+'.map'
        return res
    
    def ptp_outputfile(self,jobRec):
        res=self.loc+'/output/'+jobRec.batchName+'.ptp.'+str(jobRec.seq)+'.'+str(jobRec.statType)+'.out'
        return res
    
    def ptp_outputcsv(self,jobRec):
        res=self.loc+'/output/'+jobRec.batchName+'.ptp.'+str(jobRec.seq)+'.'+str(jobRec.statType)+'.csv'
        return res

    def makeDBURL(self):
        url=self.protocol+self.user+':'+self.password+'@'+self.dbhost+self.database
        return url
    
    def makeEngine(self):
        self.engine= create_engine(self.makeDBURL())

    def readTileDef(self,tileID):
        sql_select = text("""
            select tileid,tileseq,ptp_da_lower,ptp_da_upper from ercot_ops.ptp_tile_def where
            tileid = :tileid order by tileseq
        """)
        conn=self.engine.connect()
        res=conn.execute(sql_select,{"tileid":tileID})
        defs=[]
        for rec in res:
            defs.append({
                "tileid":rec['tileid'],
                "tileseq":rec['tileseq'],
                "lowerbound":rec["ptp_da_upper"],
                "upperbound":rec['ptp_da_lower']
            })
        conn.close()
        return defs
    def createTileConfig(self,tileID,boundData):
        sql_insert = text("""
          insert into ercot_ops.ptp_tile_def (tileid,tileseq,ptp_da_lower,ptp_da_upper,desc_details) 
          values (:tileid, :tileseq, :ptp_da_lower, :ptp_da_upper, :desc_details)
        """)
        # now prepare the insert params
        params=[]
        seq=0
        for b_low,b_up in boundData:
            params.append({
                "tileid":tileID,
                "tileseq":seq,
                "ptp_da_lower":b_low,
                "ptp_da_upper":b_up,
                "desc_details":"description"
            })
            seq=seq+1
        conn=self.engine.connect()
        conn.execute(sql_insert,params)
        conn.close()

    # preprocess
    def createPatition(self,nPartition):
        t_start=time.time()
        sql_loccodes='select loc, loc_id from ercot_ops.loc_nodes'
        conn = self.engine.connect()
        res_1=conn.execute(sql_loccodes)
        idx=0
         
        sql_update1=text("""update ercot_ops.loc_nodes set par= :par where loc_id= :loc_id""")
        sql_update2=text(""" update ercot_ops.data_input set par= :par  where loc_id= :loc_id""")
        params=[]
        for rec in res_1:
            vpar=idx%nPartition
            params.append({"par":vpar,"loc_id":rec['loc_id']})
            idx=idx+1
        print(params[0])
        
        conn.execute(sql_update1,params)
        print('finish loc nodes update:',time.time()-t_start)
        conn.execute(sql_update2,params)
        print('finish data input update:',time.time()-t_start)
        conn.close()
    
    # this function assume all partition has been created correctly
    def generateJobPairs(self):
        # first truncate the job pair tables
        sql_truncate='truncate table ercot_ops.job_ptp_pairs'
        sql_insertpair = text(""" 
        insert into ercot_ops.job_ptp_pairs (loc_id_1,par1,loc_id_2,par2,selectionflag,status) 
        (select a.loc_id as loc_id_1, a.par as par1, b.loc_id as loc_id_2, b.par as par2, 'Y' as selectionflag, 
        'A' as status from ercot_ops.loc_nodes a, ercot_ops.loc_nodes b where a.loc_id > b.loc_id
        )
        """)
        t_start=time.time()
        conn = self.engine.connect()
        print ('execute truncate command...', sql_truncate)
        conn.execute(sql_truncate)
        print ('create the job pair records', str(sql_insertpair))
        conn.execute(sql_insertpair)
        print('total time:',time.time()-t_start)
        conn.close()
    
    # batchname is referrence name for mananging batch
    # jobType can be normal and adhoc, which will take different type of data input
    # stattype will create data output for overall, yearly based, adhoc
    def buildJobSchedule(self,batchName,jobType,statType, tileID=''):
        sql_querypairs = 'select count(loc_id_1) as nrec, par1, par2 from ercot_ops.job_ptp_pairs group by par1 , par2 order by par1, par2'
        sql_insert=text(
        """
          insert into ercot_ops.job_scheduler (batchName, seq, par1, par2, jobType, statType, scheduleTime,status, nPTP_Recs, tileid)
          values (:batchname, :seq, :par1, :par2, :jobType, :statType, :scheduleTime, :status, :npairs, :tileid )
        """)
        print(str(sql_insert))
        conn = self.engine.connect()
        res= conn.execute(sql_querypairs)
        params=[]
        seq=0
        for rec in res:
            #print(rec['nrec']," ",rec['par1'], ' ', rec['par2'])
            dt=datetime.datetime.now()
            params.append({
                "batchname": batchName,
                "seq": seq,
                "par1": rec['par1'],
                "par2": rec['par2'],
                "jobType": jobType,
                "statType": statType,
                "scheduleTime":str(dt),
                "status":'created',
                "npairs": rec['nrec'],
                "tileid": tileID
            })
            seq=seq+1
        # now execute the insertion
        conn.execute(sql_insert,params)
        conn.close()

    def checkOutNextJob(self, batchName, agentname="agent", hostname="localhost"):
        sql_select=text("""
          select top 1 * from ercot_ops.job_scheduler where batchname = :batchname and status = 'created' order by seq
        """)
        sql_update=text("""
          update ercot_ops.job_scheduler set status=:status, agentname=:agent, serverhost=:server where batchname=:batchname 
          and seq=:seq and par1=:par1 and par2=:par2
        """)
        conn = self.engine.connect() 
        jobRec = None
        res=conn.execute(sql_select,{"batchname":batchName})
        rjob = None
        for rec in res:
            jobRec=rec
            print(' find record seq', rec['seq'], rec['batchname'], batchName)
            break
        if(jobRec!= None):
            # first update the job record
            conn.execute(sql_update,
            {"batchname":batchName,"seq":jobRec['seq'],"par1":jobRec['par1'],"par2":jobRec['par2'],"status":"processing"
            , "agent":agentname,"server":hostname})
            rjob = PTPJobRec(batchName,jobRec['seq'],jobRec['par1'],jobRec['par2'],jobRec['jobtype'],
            jobRec['stattype'])
            rjob.agentName=agentname
            rjob.host=hostname
            rjob.tileid=jobRec['tileid']
        conn.close()
 
        return rjob
    
    def markJobComplete(self,jobRec):
        sql_update = text("""
            update ercot_ops.job_scheduler set status= :status, starttime =:tstart, completedtime=:tcomplete, 
            n_processed_recs = :nprocessed, outputfile= :outputfile
            where batchname = :batchname and seq=:seq and par1 =:par1 and par2=:par2
        """)
        params= {"batchname":jobRec.batchName,"seq":jobRec.seq,"par1":jobRec.par1,"par2":jobRec.par2,
         "status":"complete","tstart":jobRec.startTime,"tcomplete":jobRec.completeTime,
         "nprocessed":jobRec.nprocessed,"outputfile":jobRec.outputfile}
        with self.engine.connect() as conn:
            conn.execute(sql_update,params)
            conn.close()
    # return list of ptp pairs
    def readJobPTPPairs(self,jobrec):
        sql_select=text("""
          select loc_id_1,loc_id_2 from ercot_ops.job_ptp_pairs where par1 = :par1 and par2 =:par2 order by loc_id_1,loc_id_2
        """)
        conn=self.engine.connect()
        resultset=conn.execute(sql_select,{"par1":jobrec.par1,"par2":jobrec.par2})
        res=[]
        for rec in resultset:
            res.append({'source':rec['loc_id_1'],'target':rec['loc_id_2']})
        return res
    
    # this will return a hashmap or dict of pandas
    def readDataInputMap(self,batchname,partition):
        sql_select= 'select loc_id,opdate,he,da_price,rt_price from ercot_ops.data_input where par = '+str(partition)
        print(datetime.datetime.now())
        t_start=time.time()
        rmap = {}
        mapfilename=self.mapfilename(batchname,partition)
        if(os.path.exists(mapfilename)):
            # if file exist using pickle readit and return
            print("existing local cached map found and loading from local")
            with open(mapfilename,'rb') as istream:
                rmap=pickle.load(istream)
        else:
            print("no cache.... reading from DB:")
            conn=self.engine.connect()
            df_res=pd.read_sql(sql_select,conn)
            print("len:",len(df_res),df_res.columns)
            print("reading into pandas:",time.time()-t_start)
            locids=list(df_res['loc_id'].unique())
            print(datetime.datetime.now(),len(locids))
            for locid in locids:
                rmap[locid]=df_res.loc[df_res['loc_id']==locid]
            print ("map size:",len(rmap.keys()))
            print("saving cache for future use:",time.time()-t_start)
            with open(mapfilename,'wb') as os_output:
                pickle.dump(rmap,os_output,-1)
        print(datetime.datetime.now())
        print("build map....:",time.time()-t_start)
        return rmap

    def cal_stat(self,source,target,df_input,hour):
        df_tmp=df_input.loc[df_input['he'] == hour]
        res = {}
        res['a_source']=source
        res['a_target']=target
        res['a_he']=hour
        res['count']=len(df_tmp)
        res['max_date']=df_tmp['opdate'].max()
        res['mean_ptp_da']=df_tmp['ptp_da'].mean()
        res['mean_ptp_rt']=df_tmp['ptp_rt'].mean()
        res['mean_ptp_dart']=df_tmp['ptp_dart'].mean()
        res['median_ptp_dart']=df_tmp['ptp_dart'].median()
        res['min_ptp_dart']=df_tmp['ptp_dart'].min()
        res['max_ptp_dart']=df_tmp['ptp_dart'].max()
        res['min_ptp_da']=df_tmp['ptp_da'].min()
        res['max_ptp_da']=df_tmp['ptp_da'].max()
        res['min_ptp_rt']=df_tmp['ptp_rt'].min()
        res['max_ptp_rt']=df_tmp['ptp_rt'].max()
        res['offer_ops']=len(df_tmp.loc[df_tmp['ptp_dart']>0.0])
        res['bid_ops']=res['count']-res['offer_ops']
        res['bid_x_ops']=len(df_tmp.loc[df_tmp['ptp_dart']<-30.0])
        res['offer_win']=df_tmp.loc[df_tmp['ptp_dart']>0.0,'ptp_dart'].sum()
        res['bid_win']=df_tmp.loc[df_tmp['ptp_dart']<0.0,'ptp_dart'].sum()
        res['bid_x_win']=df_tmp.loc[df_tmp['ptp_dart']<-30.0,'ptp_dart'].sum()
        return res
        
    # this method process the source and target and append to an array 'output'
    def process_ptp_overall(self,map_src,map_tgt,source,target,output):
        df_src=map_src[source]
        df_src=df_src.rename(index=str, columns={"loc_id": "loc_p1", "da_price": "price_da_p1",'rt_price':'price_rt_p1'}) 
        df_target=map_tgt[target]
        df_target=df_target.rename(index=str, columns={"loc_id": "loc_p2", "da_price": "price_da_p2",'rt_price':'price_rt_p2'})
        #print(".... READOUT",time.time()-t_start)
        df_res=pd.merge(df_src,df_target,on=['opdate','he'])
        #print(df_res.columns)
        #print(".... merge",time.time()-t_start)
        df_res['ptp_da']=df_res['price_da_p1']-df_res['price_da_p2']
        df_res['ptp_rt']=df_res['price_rt_p1']-df_res['price_rt_p2']
        df_res['ptp_dart']=df_res['ptp_rt']-df_res['ptp_da']

        for hr in range(24):
            hh=hr+1
            rec=self.cal_stat(source,target,df_res,hh)
            output.append(rec)
        return output

    # calculate PTP stat with year category    
    def process_ptp_year(self,map_src,map_tgt,source,target,output):
        df_src=map_src[source]
        df_src=df_src.rename(index=str, columns={"loc_id": "loc_p1", "da_price": "price_da_p1",'rt_price':'price_rt_p1'}) 
        df_target=map_tgt[target]
        df_target=df_target.rename(index=str, columns={"loc_id": "loc_p2", "da_price": "price_da_p2",'rt_price':'price_rt_p2'})
        #print(".... READOUT",time.time()-t_start)
        df_res=pd.merge(df_src,df_target,on=['opdate','he'])
        df_res['year']=pd.to_datetime(df_res['opdate']).dt.year
        #print(df_res.columns)
        #print(".... merge",time.time()-t_start)
        df_res['ptp_da']=df_res['price_da_p1']-df_res['price_da_p2']
        df_res['ptp_rt']=df_res['price_rt_p1']-df_res['price_rt_p2']
        df_res['ptp_dart']=df_res['ptp_rt']-df_res['ptp_da']
        # now list all the years:
        year_list=list(df_res['year'].unique())
        #print(year_list)
        for yr in year_list:
            df_year=df_res.loc[df_res['year']==yr]
            for hr in range(24):
                hh=hr+1
                rec=self.cal_stat(source,target,df_year,hh)
                rec['a_year']=yr
                output.append(rec)
        return output 

    def fillPercentile(self,df_input,colname,pcol,per_bounds=[0.8,0.6,0.4,0.2]):
        df_res=df_input.sort_values(by=[colname])
        n_key=len(df_res)
        df_res[pcol]=1.0
        tmp=[1.0]*n_key
        for pp in per_bounds:
            posi=(int)(n_key*(1.0-pp)+0.5)
            tmp[posi:]=[pp]*(n_key-posi)
        df_res[pcol]=tmp
        #print(df_res[-20:])
        return df_res    
    def process_ptp_tile(self,map_src,map_tgt,source,target,output,tiledefs):
        df_src=map_src[source]
        df_src=df_src.rename(index=str, columns={"loc_id": "loc_p1", "da_price": "price_da_p1",'rt_price':'price_rt_p1'}) 
        df_target=map_tgt[target]
        df_target=df_target.rename(index=str, columns={"loc_id": "loc_p2", "da_price": "price_da_p2",'rt_price':'price_rt_p2'})
        #print(".... READOUT",time.time()-t_start)
        df_res=pd.merge(df_src,df_target,on=['opdate','he'])
        df_res['year']=pd.to_datetime(df_res['opdate']).dt.year
        #print(df_res.columns)
        #print(".... merge",time.time()-t_start)
        df_res['ptp_da']=df_res['price_da_p1']-df_res['price_da_p2']
        df_res['ptp_rt']=df_res['price_rt_p1']-df_res['price_rt_p2']
        df_res['ptp_dart']=df_res['ptp_rt']-df_res['ptp_da']
        # now process tiles
        for tile in tiledefs:
            df_tile=df_res.loc[(df_res['ptp_da']>=tile['lowerbound']) & (df_res['ptp_da']<tile['upperbound'])]
            for hr in range(24):
                hh=hr+1
                rec=self.cal_stat(source,target,df_tile,hh)
                rec['a_tileid']=tile['tileid']
                rec['a_tileseq']=tile['tileseq']
                output.append(rec)
        return output
    def process_ptp_pp(self,map_src,map_tgt,source,target,output,per_bounds=[0.8,0.6,0.4,0.2]):
        df_src=map_src[source]
        df_src=df_src.rename(index=str, columns={"loc_id": "loc_p1", "da_price": "price_da_p1",'rt_price':'price_rt_p1'}) 
        df_target=map_tgt[target]
        df_target=df_target.rename(index=str, columns={"loc_id": "loc_p2", "da_price": "price_da_p2",'rt_price':'price_rt_p2'})
        #print(".... READOUT",time.time()-t_start)
        df_res=pd.merge(df_src,df_target,on=['opdate','he'])
        df_res['year']=pd.to_datetime(df_res['opdate']).dt.year
        #print(df_res.columns)
        #print(".... merge",time.time()-t_start)
        df_res['ptp_da']=df_res['price_da_p1']-df_res['price_da_p2']
        df_res['ptp_rt']=df_res['price_rt_p1']-df_res['price_rt_p2']
        df_res['ptp_dart']=df_res['ptp_rt']-df_res['ptp_da']
        df_res=self.fillPercentile(df_res,'ptp_da','pp_tile',per_bounds)
        #print(df_res[-10:])
        # now loop through percentile
        for pp in per_bounds:
            df_pp=df_res.loc[df_res['pp_tile']<=pp]
            for hr in range(24):
                hh=hr+1
                rec=self.cal_stat(source,target,df_pp,hh)
                rec['a_pp_tile']=pp
                output.append(rec)
        return output   

    # this is method is the broker method
    def process_ptp_job(self, jobRec, rec_limit=-1):
        if(jobRec.jobType=='full'): # this is the logic to take data_input table as input
            if(jobRec.statType == 'overall'):
                self.process_ptp_job_overall(jobRec,rec_limit)
            elif (jobRec.statType =='year'):
                self.process_ptp_job_year(jobRec,rec_limit)
            elif (jobRec.statType =='pp'):
                self.process_ptp_job_pp(jobRec,rec_limit)
            elif (jobRec.statType == 'tile'):
                self.process_ptp_job_tile(jobRec,rec_limit)
            else:
                print('not support Stat type:',jobRec.statType)
        elif(jobRec.jobType=='adhoc'):
            print("not yet implemented")

    def process_ptp_job_tile(self,jobRec,rec_limit=-1):
        jobRec.startTime=datetime.datetime.now()
        pairs=self.readJobPTPPairs(jobRec)
        tiledefs=self.readTileDef(jobRec.tileid)
        #print(job['ptp_pair'])
        # load src dict and target dict
        srcmap=self.readDataInputMap(jobRec.batchName,jobRec.par1)
        if(jobRec.par1==jobRec.par2):
            tgtmap=srcmap
        else:
            tgtmap=self.readDataInputMap(jobRec.batchName,jobRec.par2)
        # now start processing
        out_ary=[]
        i_rec=0
        for rec in pairs:
            src=rec['source']
            tgt=rec['target']
            self.process_ptp_tile(srcmap,tgtmap,src,tgt,out_ary,tiledefs)
            i_rec=i_rec+1
            if(rec_limit>-1 and i_rec>rec_limit):
                break
        #print(out_ary)
        outfile=self.ptp_outputcsv(jobRec)
        print(outfile)
        pd.DataFrame().append(out_ary).to_csv(outfile)
        jobRec.completeTime=datetime.datetime.now()
        jobRec.printMe()
        # update database indicate job completed
        jobRec.nprocessed=len(out_ary)
        jobRec.outputfile=outfile
        # self.markJobComplete(jobRec)
        print("completed")
        return jobRec

    def process_ptp_job_pp(self,jobRec,rec_limit=-1):
        # first read jobfile
        jobRec.startTime=datetime.datetime.now()
        pairs=self.readJobPTPPairs(jobRec)
        #print(job['ptp_pair'])
        # load src dict and target dict
        srcmap=self.readDataInputMap(jobRec.batchName,jobRec.par1)
        if(jobRec.par1==jobRec.par2):
            tgtmap=srcmap
        else:
            tgtmap=self.readDataInputMap(jobRec.batchName,jobRec.par2)
        # now start processing
        out_ary=[]
        i_rec=0
        for rec in pairs:
            src=rec['source']
            tgt=rec['target']
            self.process_ptp_pp(srcmap,tgtmap,src,tgt,out_ary)
            i_rec=i_rec+1
            if(rec_limit>-1 and i_rec>rec_limit):
                break
        #print(out_ary)
        outfile=self.ptp_outputcsv(jobRec)
        print(outfile)
        pd.DataFrame().append(out_ary).to_csv(outfile)
        jobRec.completeTime=datetime.datetime.now()
        jobRec.printMe()
        # update database indicate job completed
        jobRec.nprocessed=len(out_ary)
        jobRec.outputfile=outfile
        # self.markJobComplete(jobRec)
        print("completed")
        return jobRec
    def process_ptp_job_year(self, jobRec, rec_limit=-1):
        # first read jobfile
        jobRec.startTime=datetime.datetime.now()
        pairs=self.readJobPTPPairs(jobRec)
        #print(job['ptp_pair'])
        # load src dict and target dict
        srcmap=self.readDataInputMap(jobRec.batchName,jobRec.par1)
        if(jobRec.par1==jobRec.par2):
            tgtmap=srcmap
        else:
            tgtmap=self.readDataInputMap(jobRec.batchName,jobRec.par2)
        # now start processing
        out_ary=[]
        # add the record limit
        i_rec=0
        for rec in pairs:
            src=rec['source']
            tgt=rec['target']
            self.process_ptp_year(srcmap,tgtmap,src,tgt,out_ary)
            i_rec=i_rec+1
            if(rec_limit>-1 and i_rec>rec_limit):
                break
        #print(out_ary)
        outfile=self.ptp_outputcsv(jobRec)
        print(outfile)
        pd.DataFrame().append(out_ary).to_csv(outfile)
        jobRec.completeTime=datetime.datetime.now()
        jobRec.printMe()
        # update database indicate job completed
        jobRec.nprocessed=len(out_ary)
        jobRec.outputfile=outfile
        # self.markJobComplete(jobRec)
        print("completed")
        return jobRec


    def process_ptp_job_overall(self,jobRec,rec_limit=-1):
        # first read jobfile
        jobRec.startTime=datetime.datetime.now()
        pairs=self.readJobPTPPairs(jobRec)
        #print(job['ptp_pair'])
        # load src dict and target dict
        srcmap=self.readDataInputMap(jobRec.batchName,jobRec.par1)
        if(jobRec.par1==jobRec.par2):
            tgtmap=srcmap
        else:
            tgtmap=self.readDataInputMap(jobRec.batchName,jobRec.par2)
        # now start processing
        out_ary=[]
        # add the record limit
        i_rec=0
        for rec in pairs:
            src=rec['source']
            tgt=rec['target']
            self.process_ptp_overall(srcmap,tgtmap,src,tgt,out_ary)
            i_rec=i_rec+1
            if(rec_limit>-1 and i_rec>rec_limit):
                break
        #print(out_ary)
        outfile=self.ptp_outputcsv(jobRec)
        print(outfile)
        pd.DataFrame().append(out_ary).to_csv(outfile)
        jobRec.completeTime=datetime.datetime.now()
        jobRec.printMe()
        # update database indicate job completed
        jobRec.nprocessed=len(out_ary)
        jobRec.outputfile=outfile
        # self.markJobComplete(jobRec)
        print("completed")
        return jobRec


def process_job(loc,jobrec,nlimit):
    dbproc=PTPDBScheduler(loc)
    dbproc.makeEngine()
    jobrec.printMe()
    dbproc.process_ptp_job(jobrec,rec_limit=nlimit)
    return jobrec

def executeBatch(loc,batchname,njob,nprocess,nlimit):
    # first retrieve record and build params
    dbproc=PTPDBScheduler(loc)
    dbproc.makeEngine()
    params=[]
    for ii in range(njob):
        print("trying to get job:",ii)
        rec=dbproc.checkOutNextJob(batchname)
        if(rec == None):
            print("no job avaliable")
            break
        else:
            params.append((loc,rec,nlimit))
    # now using process pool to execute tasks
    with Pool(processes=4) as pool:     # 10 thread feel a little lag
        rmap=pool.starmap(process_job,params) 
    
    # now update db job status
    # reconnect engine
    dbproc.makeEngine()
    for jobrec in rmap:
        dbproc.markJobComplete(jobrec)
    
if (__name__ == "__main__"):
    # here we are testing database connection etc
    loc='E:/python/ying_python_workspace/ercot'
    batchname='testbatchpp'

    #executeBatch(loc,batchname,100,4,-1)
    dbproc=PTPDBScheduler(loc)
    #print (dbproc.makeDBURL())
    dbproc.makeEngine()
    #dbproc.createTileConfig("tile1",[(-99999,-30.0),(-30.0,-5.0),(-5.0,-2.0),(-2.0,2.0),(2.0,5.0),(5.0,30.0),(30.0,99999.0)])
    tiledefs=dbproc.readTileDef('tile1')
    for tile in tiledefs:
        print(tile['tileid'],tile['tileseq'],tile['lowerbound'],tile['upperbound'])
    #dbproc.buildJobSchedule('testbatchpp','full','pp')
    #nextjob=dbproc.checkOutNextJob('testbatch')
    #nextjob.printMe()
    #dbproc.process_ptp_job(nextjob,10)
    #dbproc.createPatition(8)
    #dbproc.generateJobPairs()
    #dbproc.buildJobSchedule('testbatch','full','overall')
 #   session=sessionmaker(bind=engine)
 #   mysession=session()
 #   dao=ErcotOpsDAO(mysession)
 #   print('get count:',dao.getLocNodesCount())
 #   list=dao.getLocList()
#     dao.updatePartition(list,8)
  #  count=mysession.query(func.count(ErcotOpsDataInput.loc_id)).scalar() 
  #  print("count location nodes:",count)

