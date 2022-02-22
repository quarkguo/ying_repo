# this is the library for ercot PTP processing
import pandas as pd
import numpy as np
import pickle
import time

class PTPProcessor:

    # constructor specify the the current execute location
    #  folder --data
    #           --matrix
    #              --job
    #              --output
    def __init__(self,loc):
        self.loc=loc
        print('current location is :',self.loc)
    
    # calculate the statistics metrics
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
    def process_ptp_local(self,map_src,map_tgt,source,target,output):
        df_src=map_src[source]
        df_target=map_tgt[target]
        df_target=df_target.rename(index=str, columns={"loc_p1": "loc_p2", "price_da_p1": "price_da_p2",'price_rt_p1':'price_rt_p2'})
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

    # this method process source target ptp stats group by year
    def process_ptp_year_local(self,map_src,map_tgt,source,target,output):
        df_src=map_src[source]
        df_target=map_tgt[target]
        df_target=df_target.rename(index=str, columns={"loc_p1": "loc_p2", "price_da_p1": "price_da_p2",'price_rt_p1':'price_rt_p2'})
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
        return output        #print ('over all time spend per node pair:',time.time()-t_start)
        
    # local process through batch
    def process_batch_ptp(self,t_start,map_src,map_tgt,pairs):
        outary=[]
        for p in pairs:
            self.process_ptp_local(map_src,map_tgt,p[0],p[1],outary)
        return outary

    def process_ptp_job(self,jobfile,rec_limit=-1):
        # first read jobfile
        with open(self.loc+'data/matrix/job/'+jobfile,'rb') as istream:
            job=pickle.load(istream)
        print('partition...:',job['partition'])
        print('record limit:',rec_limit)
        #print(job['ptp_pair'])
        # load src dict and target dict
        psrc,ptgt=job['partition']
        srcmapfile=self.loc+'data/matrix/'+str(psrc)+'.pk'
        with open(srcmapfile,'rb') as in_src:
            srcmap=pickle.load(in_src)
        if(psrc==ptgt) :
            tgtmap=srcmap
        else:
            tgtmapfile=self.loc+'data/matrix/'+str(ptgt)+'.pk'
            with open(tgtmapfile,'rb') as in_tgt:
                tgtmap=pickle.load(in_tgt)
        # now start processing
        out_ary=[]
        # add the record limit
        i_rec=0
        for src,tgt in job['ptp_pair']:
            self.process_ptp_local(srcmap,tgtmap,src,tgt,out_ary)
            i_rec=i_rec+1
            if(rec_limit>-1 and i_rec>rec_limit):
                break
        #print(out_ary)
        outfile=self.loc+'data/matrix/output/'+jobfile+'.out'
        print(outfile)
        with open(outfile,'wb') as os_output:
            pickle.dump(out_ary,os_output,-1)

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

    def process_ptp_pp_local(self,map_src,map_tgt,source,target,output,per_bounds=[0.8,0.6,0.4,0.2]):
        df_src=map_src[source]
        df_target=map_tgt[target]
        df_target=df_target.rename(index=str, columns={"loc_p1": "loc_p2", "price_da_p1": "price_da_p2",'price_rt_p1':'price_rt_p2'})
        #print(".... READOUT",time.time()-t_start)
        df_res=pd.merge(df_src,df_target,on=['opdate','he'])
        df_res['year']=pd.to_datetime(df_res['opdate']).dt.year
        #print(df_res.columns)
        #print(".... merge",time.time()-t_start)
        df_res['ptp_da']=df_res['price_da_p1']-df_res['price_da_p2']
        df_res['ptp_rt']=df_res['price_rt_p1']-df_res['price_rt_p2']
        df_res['ptp_dart']=df_res['ptp_rt']-df_res['ptp_da']
        # now set percentile
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

    def process_ptp_pp_job(self,jobfile,rec_limit =-1 ,per_bounds=[0.8,0.6,0.4,0.2]):
        with open(self.loc+'data/matrix/job/'+jobfile,'rb') as istream:
            job=pickle.load(istream)
        print('partition...:',job['partition'])
        print('record limit:',rec_limit)
        print('percentile:',per_bounds)
        #print(job['ptp_pair'])
        # load src dict and target dict
        psrc,ptgt=job['partition']
        srcmapfile=self.loc+'data/matrix/'+str(psrc)+'.pk'
        with open(srcmapfile,'rb') as in_src:
            srcmap=pickle.load(in_src)
        if(psrc==ptgt) :
            tgtmap=srcmap
        else:
            tgtmapfile=self.loc+'data/matrix/'+str(ptgt)+'.pk'
            with open(tgtmapfile,'rb') as in_tgt:
                tgtmap=pickle.load(in_tgt) 
        # now start processing
        out_ary=[]
        # add the record limit
        i_rec=0
        for src,tgt in job['ptp_pair']:
            self.process_ptp_pp_local(srcmap,tgtmap,src,tgt,out_ary,per_bounds)
            i_rec=i_rec+1
            if(rec_limit>-1 and i_rec>rec_limit):
                break
        #print(out_ary)
        outfile=self.loc+'data/matrix/output/'+jobfile+'.pp.out'
        print(outfile)
        with open(outfile,'wb') as os_output:
            pickle.dump(out_ary,os_output,-1)
        return

    ## process stat for yearly distribution
    def process_ptp_year_job(self,jobfile,rec_limit=-1):
        # first read jobfile
        with open(self.loc+'data/matrix/job/'+jobfile,'rb') as istream:
            job=pickle.load(istream)
        print('partition...:',job['partition'])
        print('record limit:',rec_limit)
        #print(job['ptp_pair'])
        # load src dict and target dict
        psrc,ptgt=job['partition']
        srcmapfile=self.loc+'data/matrix/'+str(psrc)+'.pk'
        with open(srcmapfile,'rb') as in_src:
            srcmap=pickle.load(in_src)
        if(psrc==ptgt) :
            tgtmap=srcmap
        else:
            tgtmapfile=self.loc+'data/matrix/'+str(ptgt)+'.pk'
            with open(tgtmapfile,'rb') as in_tgt:
                tgtmap=pickle.load(in_tgt) 
        # now start processing
        out_ary=[]
        # add the record limit
        i_rec=0
        for src,tgt in job['ptp_pair']:
            self.process_ptp_year_local(srcmap,tgtmap,src,tgt,out_ary)
            i_rec=i_rec+1
            if(rec_limit>-1 and i_rec>rec_limit):
                break
        #print(out_ary)
        outfile=self.loc+'data/matrix/output/'+jobfile+'.year.out'
        print(outfile)
        with open(outfile,'wb') as os_output:
            pickle.dump(out_ary,os_output,-1)

    # this method help create partition for the keys
    def partition(self, keylist, partition_chunksize=120):
        partition_ary=[]
        for ii in range(len(keylist)):
            npart=(int)(ii/partition_chunksize)
            partition_ary.append((keylist[ii],npart))
        return partition_ary

    # build partition cache and store them in data/matrix
    def buildPartitionDataCache(self,df_data,keycolumn,partition_keys):
        # now build all matrix
        df_matrix=[]
        for key,par in partition_keys:
            while(len(df_matrix) <=par):
                df_matrix.append({})
                print('create partition ...',str(par))
            df_matrix[par][key]=df_data.loc[df_data[keycolumn]==key]
        # now persist to the harddrive
        ipar=0
        for dict_par in df_matrix:
            cachefilename=self.loc+'data/matrix/'+str(ipar)+'.pk'
            print('saving cache file:',cachefilename)
            with open(self.loc+'data/matrix/'+str(ipar)+'.pk', 'wb') as output:  # Overwrites any existing file.
                pickle.dump(dict_par, output, pickle.HIGHEST_PROTOCOL)
            ipar=ipar+1

    # this method help build the partition to help calculate the corelation
    def buildPatitionJob(self,keyPartition):
        # first build the partition map
        pairmap={}
        for i in range(len(keyPartition)-1):
            src,pa_src=keyPartition[i]
            for j in range(i+1,len(keyPartition)):
                tar,pa_tar=keyPartition[j]
                if(not ((pa_src,pa_tar) in pairmap)):
                    pairmap[(pa_src,pa_tar)]=[]
                pairmap[(pa_src,pa_tar)].append((src,tar))
        
        #output the job file
        jobnum=0
        for psrc,ptar in pairmap.keys():
            job={}
            job['partition']=(psrc,ptar)
            job['ptp_pair']=pairmap[(psrc,ptar)]
            jobfilename=self.loc+'data/matrix/job/'+str(jobnum)+'.job'
            print('create and saving job:',jobfilename)
            with open(jobfilename,'wb') as output:
                pickle.dump(job,output,-1)
            jobnum=jobnum+1
    def buildPartition(self,keylist,df_data,npar):
        n_chunk=(int)((len(keylist)+1)/npar)
        print(' print partition:chunk',npar,' :',n_chunk)
        keylist_par=self.partition(keylist,n_chunk)
        self.buildPartitionDataCache(df_data,'loc_p1',keylist_par)
        self.buildPatitionJob(keylist_par)

    def convertOutput2CSV(self,inputfiles):
        for ifile in inputfiles:
            print('reading... ',ifile)
            with open(self.loc+'data/matrix/output/'+ifile,'rb') as istream:
                ary=pickle.load(istream)
                print('total recodes:',len(ary))
                ofile=ifile+'.csv'
                print('save to:',ofile)
                pd.DataFrame().append(ary).to_csv(self.loc+'data/matrix/csv/'+ofile)