from celery.task import task
import pandas as pd
from datetime import datetime
import paramiko
import math
import urllib, shutil
import sys
from itertools import groupby
from operator import itemgetter
sys.path.append('/code/task_config')
from config import ftp_username, ftp_password
from config_SEV import ftp_username1, ftp_password1

#@task
def check_na_values(teco_spruce1):
    if(teco_spruce1.isnull().values.any()):
        #gives the rows which contain NaN values
        na_df=teco_spruce1[pd.isnull(teco_spruce1).any(axis=1)]
        #getting the index of the NaN rows
        data=na_df.index.values
	#data=[12,13,14,45,46,47,48,49,50]
	for k, g in groupby(enumerate(data), lambda (i, x): i-x):
        	print map(itemgetter(1),g) 
	temp=[]
	for k, g in groupby(enumerate(data), lambda (i, x): i-x):
        	temp.append(map(itemgetter(1),g))
	len(temp)  
	#for consec in temp:
   	#print(consec,len(consec))
	for consec in temp:
           if len(consec)>5:
        	for a in consec:
            		teco_spruce1.loc[a,('Tair')]=teco_spruce1.iloc[a-24]['Tair']
            		#teco_spruce1.Tsoil.fillna(teco_spruce1.Tsoil.shift(24), inplace=True)
            		teco_spruce1.loc[a,('Tsoil')]=teco_spruce1.iloc[a-24]['Tsoil']
            		#teco_spruce1.RH.fillna(teco_spruce1.RH.shift(24), inplace=True)
            		teco_spruce1.loc[a,('RH')]=teco_spruce1.iloc[a-24]['RH']
            		#teco_spruce1.VPD.fillna(teco_spruce1.VPD.shift(24), inplace=True)
            		teco_spruce1.loc[a,('VPD')]=teco_spruce1.iloc[a-24]['VPD']
            		#teco_spruce1.Rain.fillna(teco_spruce1.Rain.shift(24), inplace=True)
            		teco_spruce1.loc[a,('Rain')]=teco_spruce1.iloc[a-24]['Rain']
            		#teco_spruce1.WS.fillna(teco_spruce1.WS.shift(24), inplace=True)
            		teco_spruce1.loc[a,('WS')]=teco_spruce1.iloc[a-24]['WS']
            		#teco_spruce1.PAR.fillna(teco_spruce1.PAR.shift(24), inplace=True)
            		teco_spruce1.loc[a,('PAR')]=teco_spruce1.iloc[a-24]['PAR']
                   
     	   else:
           	for a in consec:
            		teco_spruce1.loc[a,('Tair')]=teco_spruce1.iloc[a-1]['Tair']
            		#teco_spruce1.Tsoil.fillna(teco_spruce1.Tsoil.shift(24), inplace=True)
            		teco_spruce1.loc[a,('Tsoil')]=teco_spruce1.iloc[a-1]['Tsoil']
            		#teco_spruce1.RH.fillna(teco_spruce1.RH.shift(24), inplace=True)
            		teco_spruce1.loc[a,('RH')]=teco_spruce1.iloc[a-1]['RH']
            		#teco_spruce1.VPD.fillna(teco_spruce1.VPD.shift(24), inplace=True)
            		teco_spruce1.loc[a,('VPD')]=teco_spruce1.iloc[a-1]['VPD']
            		#teco_spruce1.Rain.fillna(teco_spruce1.Rain.shift(24), inplace=True)
            		teco_spruce1.loc[a,('Rain')]=teco_spruce1.iloc[a-1]['Rain']
            		#teco_spruce1.WS.fillna(teco_spruce1.WS.shift(24), inplace=True)
            		teco_spruce1.loc[a,('WS')]=teco_spruce1.iloc[a-1]['WS']
            		#teco_spruce1.PAR.fillna(teco_spruce1.PAR.shift(24), inplace=True)
            		teco_spruce1.loc[a,('PAR')]=teco_spruce1.iloc[a-1]['PAR'] 
           
    
@task()
def teco_spruce_pulldata(destination='/data/local/spruce_data'):
    initial_text=open("{0}/initial_v2.txt".format(destination),"r")
    #pulling data from the url
    print('trying to pull datapppp')   
    url = 'ftp://{0}:{1}@sprucedata.ornl.gov/DataFiles/EM1_Table1.dat'.format(ftp_username,ftp_password)
    try:
	
	sp_data=pd.read_csv(url,low_memory=False,skiprows=5)
	print ('I am in try loop')
	columnnames = ["TIMESTAMP","RECORD","Tair","RH","AirTCHumm_Avg","RH_Humm_Avg","VPD","Rain","WS","WindDir_D1_WVT","WindDir_SD1_WVT","WSDiag_Tot","SmplsF_Tot","Axis1Failed_Tot","Axis2Failed_Tot","BothAxisFailed_Tot","NVMerror_Tot","ROMerror_Tot","MaxGain_Tot","NNDF_Tot","HollowSurf_Avg","Hollow5cm_Avg","Tsoil","Hollow40cm_Avg","Hollow80cm_Avg","Hollow160cm_Avg","Hollow200cm_Avg","HummockSurf_Avg","Hummock5cm_Avg","Hummock20cm_Avg","Hummock40cm_Avg","Hummock80cm_Avg","Hummock160cm_Avg","Hummock200cm_Avg","PAR","PAR_NTree1_Avg","PAR_NTree2_Avg","PAR_SouthofHollow1_Avg","PAR_SouthofHollow2_Avg","PAR_NorthofHollow1_Avg","PAR_NorthofHollow2_Avg","PAR_Srub1_Avg","PAR_Srub2_Avg","PAR_Srub3_Avg","PAR_Srub4_Avg","TopofHummock_Avg","MidofHummock_Avg","Surface1_Avg","Surface2_Avg","D1-20cm_Avg","D2-20cm_Avg","TopH_Avg","MidH_Avg","S1_Avg","S2_Avg","Deep-20cm_Avg","short_up_Avg","short_dn_Avg","long_up_Avg","long_dn_Avg","CNR4_Temp_C_Avg","CNR4_Temp_K_Avg","long_up_corr_Avg","long_dn_corr_Avg","Rs_net_Avg","Rl_net_Avg","albedo_Avg","Rn_Avg","SPN1_Total_Avg","SPN1_Diffuse_Avg","Water_Height_Avg","Water_Temp_Avg","Watertable","Dewpoint","Dewpoint_Diff"]
	sp_data.columns = columnnames
    
	#trying to bring the timestamp into a format
	df=sp_data
	data=df['TIMESTAMP']
	df['Date_Time']=pd.to_datetime(df['TIMESTAMP'])    
    
	#Trim columns
	teco_spruce =df[['Date_Time','Tair','Tsoil','RH','VPD','Rain','WS','PAR']]
	#adding it to the existing data frame
	df['year']=df['Date_Time'].dt.year
	df['doy']=df['Date_Time'].dt.dayofyear
	df['hour']=df['Date_Time'].dt.hour

	teco_spruce=df[['year','doy','hour','Tair','Tsoil','RH','VPD','Rain','WS','PAR']]
	#getting the mean of 'Tair','Tsoil','RH','VPD','WS','PAR' n sum of ,'Rain' by combining half n full hour(e.i.12 & 12:30)
	group_treat=teco_spruce.groupby(['year','doy','hour'])
	tair=group_treat['Tair'].mean()
	tsoil=group_treat['Tsoil'].mean()
	rh=group_treat['RH'].mean()
	vpd=group_treat['VPD'].mean()
	rain=group_treat['Rain'].sum()
	ws=group_treat['WS'].mean()
	par=group_treat['PAR'].mean()
	#Taking only the even coulums(as half hourly details not required) i.e. 12:30 not required only 12 required 
	teco_spruce1=teco_spruce.iloc[::2]
	#need to reset the index number[from 0 2 4 8] [to 0 1 2 3]
	teco_spruce1=teco_spruce1.reset_index(drop=True)
	#setting the mean of 'Tair','Tsoil','RH','VPD','WS','PAR' n sum of ,'Rain' to this new dataframe teco_spruce1
	teco_spruce1['Tair']=tair.reset_index(drop=True)	    
	teco_spruce1['Tsoil']=tsoil.reset_index(drop=True)
	teco_spruce1['RH']=rh.reset_index(drop=True)
	teco_spruce1['VPD']=vpd.reset_index(drop=True)
	teco_spruce1['Rain']=rain.reset_index(drop=True)
	teco_spruce1['WS']=ws.reset_index(drop=True)
	teco_spruce1['PAR']=par.reset_index(drop=True)
	#file which contain earlier data(2011-2015)
	j1=pd.read_csv(initial_text,'\t')
    
	#file which contain the new data
	#j2=pd.read_csv('teco_spruce.txt','\t')
	#print "I found you############################################################3"
	#joining both the files together and removing the duplicate rows
	j3=pd.concat([j1,teco_spruce1]).drop_duplicates().reset_index(drop=True)
	#checking for na values
        print('now I will check na values')
	check_na_values(teco_spruce1)
	print('I have finished checking the na values')
	time_now=datetime.now()
        time_now =time_now.strftime("%Y_%m_%d_%H_%M_%S")
        #writing it to a file
	print('now I am writing to the file')
	j3.to_csv('{0}/SPRUCE_v2_0_forcing.txt'.format(destination),'\t',index=False) 
       	#teco_spruce1.to_csv('final.txt','\t',index=False)  
        j3.to_csv('{0}/SPRUCE_v2_0_forcing_{1}.txt'.format(destination,time_now),'\t',index=False) 
	#teco_spruce1.to_csv('final_{0}.txt'.format(time_now),'\t',index=False)
	print('finished writing to the file')
	
        
    except Exception,e:
	#raise Exception('the ftp site is down..Using the old sprucing file...')    
	print(e)
	print('the ftp site is down..Using the old sprucing file...')   
   
#teco_spruce_pulldata()
    
@task()
def teco_spruce_pulldata_old(destination='/data/local/spruce_data'):
    initial_text=open("{0}/initial_v1.txt".format(destination),"r")
    #pulling data from the url
    print('trying to pull datapppp')
    url = 'ftp://{0}:{1}@sprucedata.ornl.gov/DataFiles/EM1_Table1.dat'.format(ftp_username,ftp_password)
    try:

        sp_data=pd.read_csv(url,low_memory=False,skiprows=5)
        print ('I am in try loop')
	columnnames = ["TIMESTAMP","RECORD","Tair","RH","AirTCHumm_Avg","RH_Humm_Avg","VPD","Rain","WS","WindDir_D1_WVT","WindDir_SD1_WVT","WSDiag_Tot","SmplsF_Tot","Axis1Failed_Tot","Axis2Failed_Tot","BothAxisFailed_Tot","NVMerror_Tot","ROMerror_Tot","MaxGain_Tot","NNDF_Tot","HollowSurf_Avg","Hollow5cm_Avg","Tsoil","Hollow40cm_Avg","Hollow80cm_Avg","Hollow160cm_Avg","Hollow200cm_Avg","HummockSurf_Avg","Hummock5cm_Avg","Hummock20cm_Avg","Hummock40cm_Avg","Hummock80cm_Avg","Hummock160cm_Avg","Hummock200cm_Avg","PAR","PAR_NTree1_Avg","PAR_NTree2_Avg","PAR_SouthofHollow1_Avg","PAR_SouthofHollow2_Avg","PAR_NorthofHollow1_Avg","PAR_NorthofHollow2_Avg","PAR_Srub1_Avg","PAR_Srub2_Avg","PAR_Srub3_Avg","PAR_Srub4_Avg","TopofHummock_Avg","MidofHummock_Avg","Surface1_Avg","Surface2_Avg","D1-20cm_Avg","D2-20cm_Avg","TopH_Avg","MidH_Avg","S1_Avg","S2_Avg","Deep-20cm_Avg","short_up_Avg","short_dn_Avg","long_up_Avg","long_dn_Avg","CNR4_Temp_C_Avg","CNR4_Temp_K_Avg","long_up_corr_Avg","long_dn_corr_Avg","Rs_net_Avg","Rl_net_Avg","albedo_Avg","Rn_Avg","SPN1_Total_Avg","SPN1_Diffuse_Avg","Water_Height_Avg","Water_Temp_Avg","Watertable","Dewpoint","Dewpoint_Diff"]

        sp_data.columns = columnnames

        #trying to bring the timestamp into a format
        df=sp_data
        data=df['TIMESTAMP']
        df['Date_Time']=pd.to_datetime(df['TIMESTAMP'])

        #Trim columns
        teco_spruce =df[['Date_Time','Tair','Tsoil','RH','VPD','Rain','WS','PAR']]
        #adding it to the existing data frame
        df['year']=df['Date_Time'].dt.year
        df['doy']=df['Date_Time'].dt.dayofyear
        df['hour']=df['Date_Time'].dt.hour

        teco_spruce=df[['year','doy','hour','Tair','Tsoil','RH','VPD','Rain','WS','PAR']]
        #getting the mean of 'Tair','Tsoil','RH','VPD','WS','PAR' n sum of ,'Rain' by combining half n full hour(e.i.12 & 12:30)
        group_treat=teco_spruce.groupby(['year','doy','hour'])
        tair=group_treat['Tair'].mean()
        tsoil=group_treat['Tsoil'].mean()
        rh=group_treat['RH'].mean()
        vpd=group_treat['VPD'].mean()
        rain=group_treat['Rain'].mean()
        ws=group_treat['WS'].mean()
        par=group_treat['PAR'].mean()
        #Taking only the even coulums(as half hourly details not required) i.e. 12:30 not required only 12 required
        teco_spruce1=teco_spruce.iloc[::2]
        #need to reset the index number[from 0 2 4 8] [to 0 1 2 3]
        teco_spruce1=teco_spruce1.reset_index(drop=True)
        #setting the mean of 'Tair','Tsoil','RH','VPD','WS','PAR' n sum of ,'Rain' to this new dataframe teco_spruce1
        teco_spruce1['Tair']=tair.reset_index(drop=True)
        teco_spruce1['Tsoil']=tsoil.reset_index(drop=True)
        teco_spruce1['RH']=rh.reset_index(drop=True)
        teco_spruce1['VPD']=vpd.reset_index(drop=True)
        teco_spruce1['Rain']=rain.reset_index(drop=True)
        teco_spruce1['WS']=ws.reset_index(drop=True)
        teco_spruce1['PAR']=par.reset_index(drop=True)
        #file which contain earlier data(2011-2015)
        j1=pd.read_csv(initial_text,'\t')

        #file which contain the new data
        #j2=pd.read_csv('teco_spruce.txt','\t')
        #print "I found you############################################################3"
        #joining both the files together and removing the duplicate rows
        j3=pd.concat([j1,teco_spruce1]).drop_duplicates().reset_index(drop=True)
        #checking for na values
        print('now I will check na values')
        check_na_values(teco_spruce1)
        print('I have finished checking the na values')
        time_now=datetime.now()
        time_now =time_now.strftime("%Y_%m_%d_%H_%M_%S")
        #writing it to a file
        print('now I am writing to the file')
        j3.to_csv('{0}/SPRUCE_old_forcing.txt'.format(destination),'\t',index=False)
        #teco_spruce1.to_csv('final.txt','\t',index=False)
        j3.to_csv('{0}/SPRUCE_old_forcing_{1}.txt'.format(destination,time_now),'\t',index=False)
        #teco_spruce1.to_csv('final_{0}.txt'.format(time_now),'\t',index=False)
        print('finished writing to the file')


    except Exception,e:
        #raise Exception('the ftp site is down..Using the old sprucing file...')
        print(e)
        print('the ftp site is down..Using the old sprucing file...')

def check_na_values_SEV(teco_SEV1):
    if(teco_SEV1.isnull().values.any()):
        #gives the rows which contain NaN values
        na_df=teco_SEV1[pd.isnull(teco_SEV1).any(axis=1)]
        #getting the index of the NaN rows
        data=na_df.index.values
	#data=[12,13,14,45,46,47,48,49,50]
	for k, g in groupby(enumerate(data), lambda (i, x): i-x):
        	print map(itemgetter(1),g) 
	temp=[]
	for k, g in groupby(enumerate(data), lambda (i, x): i-x):
        	temp.append(map(itemgetter(1),g))
	len(temp)  
	#for consec in temp:
   	#print(consec,len(consec))
	for consec in temp:
           if len(consec)>5:
        	for a in consec:
            		teco_SEV1.loc[a,('Tair')]=teco_SEV1.iloc[a-24]['Tair']
            		#teco_SEV1.Tsoil.fillna(teco_SEV1.Tsoil.shift(24), inplace=True)
            		teco_SEV1.loc[a,('Tsoil')]=teco_SEV1.iloc[a-24]['Tsoil']
            		#teco_SEV1.RH.fillna(teco_SEV1.RH.shift(24), inplace=True)
            		teco_SEV1.loc[a,('RH')]=teco_SEV1.iloc[a-24]['RH']
            		#teco_SEV1.VPD.fillna(teco_SEV1.VPD.shift(24), inplace=True)
            		teco_SEV1.loc[a,('VPD')]=teco_SEV1.iloc[a-24]['VPD']
            		#teco_SEV1.Rain.fillna(teco_SEV1.Rain.shift(24), inplace=True)
            		teco_SEV1.loc[a,('Rain')]=teco_SEV1.iloc[a-24]['Rain']
            		#teco_SEV1.WS.fillna(teco_SEV1.WS.shift(24), inplace=True)
            		teco_SEV1.loc[a,('WS')]=teco_SEV1.iloc[a-24]['WS']
            		#teco_SEV1.PAR.fillna(teco_SEV1.PAR.shift(24), inplace=True)
            		teco_SEV1.loc[a,('PAR')]=teco_SEV1.iloc[a-24]['PAR']
                   
     	   else:
           	for a in consec:
            		teco_SEV1.loc[a,('Tair')]=teco_SEV1.iloc[a-1]['Tair']
            		#teco_SEV1.Tsoil.fillna(teco_SEV1.Tsoil.shift(24), inplace=True)
            		teco_SEV1.loc[a,('Tsoil')]=teco_SEV1.iloc[a-1]['Tsoil']
            		#teco_SEV1.RH.fillna(teco_SEV1.RH.shift(24), inplace=True)
            		teco_SEV1.loc[a,('RH')]=teco_SEV1.iloc[a-1]['RH']
            		#teco_SEV1.VPD.fillna(teco_SEV1.VPD.shift(24), inplace=True)
            		teco_SEV1.loc[a,('VPD')]=teco_SEV1.iloc[a-1]['VPD']
            		#teco_SEV1.Rain.fillna(teco_SEV1.Rain.shift(24), inplace=True)
            		teco_SEV1.loc[a,('Rain')]=teco_SEV1.iloc[a-1]['Rain']
            		#teco_SEV1.WS.fillna(teco_SEV1.WS.shift(24), inplace=True)
            		teco_SEV1.loc[a,('WS')]=teco_SEV1.iloc[a-1]['WS']
            		#teco_SEV1.PAR.fillna(teco_SEV1.PAR.shift(24), inplace=True)
            		teco_SEV1.loc[a,('PAR')]=teco_SEV1.iloc[a-1]['PAR'] 
           
    
#caculate VPD -chang_101718					
def calculate_VPD(df):
	a=0.611
	b=17.502
	c=240.97
	
	es = a*math.exp(b*df['Tair']/(df['Tair']+c)) 
	ea = es*df['RH1']
	VPD = es-ea
	return VPD
		   
@task()
def teco_SEV_pulldata(destination='/data/local/SEV_data'):
    initial_text=open("{0}/initial_SEV.txt".format(destination),"r") #chang_101718
    #pulling data from the SFTP
    print('trying to pull datapppp')
    #connect
    transport = paramiko.Transport(('socorro.unm.edu', 22))
    transport.connect(username = ftp_username1, password = ftp_password1)
    sftp=paramiko.SFTPClient.from_transport(transport)
    filepath = '/net/ladron/export/db/work/wireless/nmufn/newgland/NMUFN_NEWGLAND_CR5000_flux.dat'
    localpath = "{0}/socorro/NMUFN_NEWGLAND_CR5000_flux.dat".format(destination)
    sftp.get(filepath, localpath)
    sftp.close()
    transport.close()
    try:
	
	sp_data=pd.read_csv("{0}/socorro/NMUFN_NEWGLAND_CR5000_flux.dat".format(destination),skiprows=4)

	print ('I am in try loop')
	columnnames = ["TIMESTAMP","RECORD","GPS_Time","Fc_wpl","LE_wpl","Hc","tau","u_star","n_Tot","Ux_Avg","Uy_Avg","Uz_Avg","Ts_Avg","co2_mean_Avg","h2o_mean_Avg","co2_um_m_Avg","h2o_mm_m_Avg","rho_a_mean_Avg","press_Avg","wnd_dir_compass","wnd_dir_csat3","wnd_spd","WS","std_wnd_dir","stdev_Ts","stdev_co2","stdev_h2o","stdev_Ux","stdev_Uy","stdev_Uz","cov_Ts_Ux","cov_Ts_Uy","cov_Ts_Uz","cov_Ux_Uy","cov_Ux_Uz","cov_Uy_Uz","cov_co2_Ux","cov_co2_Uy","cov_co2_Uz","cov_h2o_Ux","cov_h2o_Uy","cov_h2o_Uz","panelT_5000_in_Avg","batt_5000_Avg","battbank_Avg","panelV_Avg","low12V_5000_Tot","watchdog_5000_Tot","csat_warnings","irga_warnings","del_T_f_Tot","sig_lck_f_Tot","amp_h_f_Tot","amp_l_f_Tot","chopper_f_Tot","detector_f_Tot","pll_f_Tot","sync_f_Tot","agc_Avg","Rad_short_Up_Avg","Rad_short_Dn_Avg","Rad_long_Up__Avg","Rad_long_Dn__Avg","CG3UpCo_Avg","CG3DnCo_Avg","NetTot_Avg","NetRs_Avg","NetRl_Avg","CNR1TC_Avg","ppdf","par_facedown_Avg","Tair","RH1","RTD_50cm_Avg","RTD_150cm_Avg","RTD_250cm_Avg","TargmV_Avg","SBTempC_Avg","TargTempC_Avg","Rain","e_hmp_mean_Avg","e_sat_Avg","h2o_hmp_mean_Avg","SWC_G1_2p5_Avg","SWC_G1_12p5_Avg","SWC_G1_22p5_Avg","SWC_G1_37p5_Avg","SWC_G1_52p5_Avg","SWC_G2_2p5_Avg","SWC_G2_12p5_Avg","SWC_G2_22p5_Avg","SWC_G2_37p5_Avg","SWC_G2_52p5_Avg","SWC_O1_2p5_Avg","SWC_O1_12p5_Avg","SWC_O1_22p5_Avg","SWC_O1_37p5_Avg","SWC_O1_52p5_Avg","SWC_O2_2p5_Avg","SWC_O2_12p5_Avg","SWC_O2_22p5_Avg","SWC_O2_37p5_Avg","SWC_O2_52p5_Avg","SoilT_G1_2p5_Avg","Tsoil1","SoilT_G1_22p5_Avg","SoilT_G1_37p5_Avg","SoilT_G1_52p5_Avg","SoilT_G2_2p5_Avg","Tsoil2","SoilT_G2_22p5_Avg","SoilT_G2_37p5_Avg","SoilT_G2_52p5_Avg","SoilT_O1_2p5_Avg","Tsoil3","SoilT_O1_22p5_Avg","SoilT_O1_37p5_Avg","SoilT_O1_52p5_Avg","SoilT_O2_2p5_Avg","Tsoil4","SoilT_O2_22p5_Avg","SoilT_O2_37p5_Avg","SoilT_O2_52p5_Avg","Soil_CO2_G1_5_Avg","Soil_CO2_G1_10_Avg","Soil_CO2_G1_30_Avg","Soil_CO2_G2_5_Avg","Soil_CO2_G2_10_Avg","Soil_CO2_G2_30_Avg","Soil_CO2_G3_5_Avg","Soil_CO2_G3_10_Avg","Soil_CO2_G3_30_Avg","Soil_CO2_Surface_Avg","grass_1_Avg","grass_2_Avg","open_1_Avg","open_2_Avg","TCAV_grass_Avg","TCAV_open_Avg","battbank8100_Avg","panelV8100_Avg"]
	sp_data.columns = columnnames
    
	#trying to bring the timestamp into a format
	df=sp_data
	df['Date_Time']=pd.to_datetime(df['TIMESTAMP'])    

	#Trim columns
	#adding it to the existing data frame
	df['year']=df['Date_Time'].dt.year
	df['doy']=df['Date_Time'].dt.dayofyear
	df['hour']=df['Date_Time'].dt.hour
	
	df=df.apply(pd.to_numeric, errors='coerce')
	
	#average_cal
	df['Tsoil']=df[['Tsoil1','Tsoil2','Tsoil3','Tsoil4']].mean(axis=1)
	df.ix[df.ppdf < 0, 'ppdf']=0
	df['PAR']=df['ppdf'].divide(4.6)
	df['RH']=df['RH1'].multiply(100)
	df['VPD']=df[['Tair','RH1']].apply(calculate_VPD, axis=1)
	
	teco_SEV=df[['year','doy','hour','Tair','Tsoil','RH','VPD','Rain','WS','PAR']]

	#getting the mean of 'Tair','Tsoil','RH','VPD','WS','PAR' n sum of ,'Rain' by combining half n full hour(e.i.12 & 12:30)
	group_treat=teco_SEV.groupby(['year','doy','hour'])
	tair=group_treat['Tair'].mean()
	tsoil=group_treat['Tsoil'].mean()
	rh=group_treat['RH'].mean()
	vpd=group_treat['VPD'].mean()
	rain=group_treat['Rain'].sum()
	ws=group_treat['WS'].mean()
	par=group_treat['PAR'].mean()
	#Taking only the even coulums(as half hourly details not required) i.e. 12:30 not required only 12 required 
	teco_SEV1=teco_SEV.iloc[::2]
	#need to reset the index number[from 0 2 4 8] [to 0 1 2 3]
	teco_SEV1=teco_SEV1.reset_index(drop=True)
	#setting the mean of 'Tair','Tsoil','RH','VPD','WS','PAR' n sum of ,'Rain' to this new dataframe teco_SEV1
	teco_SEV1['Tair']=tair.reset_index(drop=True)	    
	teco_SEV1['Tsoil']=tsoil.reset_index(drop=True)
	teco_SEV1['RH']=rh.reset_index(drop=True)
	teco_SEV1['VPD']=vpd.reset_index(drop=True)
	teco_SEV1['Rain']=rain.reset_index(drop=True)
	teco_SEV1['WS']=ws.reset_index(drop=True)
	teco_SEV1['PAR']=par.reset_index(drop=True)
	#file which contain earlier data(2011-2017)
	j1=pd.read_csv(initial_text,'\t') #chang_101718
    
	#file which contain the new data
	#j2=pd.read_csv('teco_SEV.txt','\t')
	#print "I found you############################################################3"
	#joining both the files together and removing the duplicate rows
	j3=pd.concat([j1,teco_SEV1]).drop_duplicates().reset_index(drop=True) 
	
	#checking for na values
        print('now I will check na values')
	check_na_values_SEV(teco_SEV1)
	print('I have finished checking the na values')
	time_now=datetime.now()
        time_now =time_now.strftime("%Y_%m_%d_%H_%M_%S")
        #writing it to a file
	print('now I am writing to the file')
	j3.to_csv('{0}/SEV_forcing.txt'.format(destination),'\t',index=False) 
       	#teco_SEV1.to_csv('final.txt','\t',index=False)  
        j3.to_csv('{0}/SEV_forcing_{1}.txt'.format(destination,time_now),'\t',index=False) 
	#teco_SEV1.to_csv('final_{0}.txt'.format(time_now),'\t',index=False)
	print('finished writing to the file')
	
        
    except Exception,e:
	#raise Exception('the ftp site is down..Using the old sprucing file...')    
	print(e)
	print('the ftp site is down..Using the old SEV file...')   
