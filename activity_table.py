# -*- coding: utf-8 -*-
"""
Created on Fri Aug 19 13:57:32 2022

@author: shubh
"""


import pandas as pd
import numpy as np
import sqlalchemy
from datetime import timedelta
from pandas.tseries.offsets import MonthBegin
from datetime import date
from dateutil import relativedelta

from tqdm import tqdm

import warnings
warnings.filterwarnings("ignore")

import json

with open("configuration.json") as json_data_file:
    data = json.load(json_data_file)

username=data['db']['readDb']['dbUserName']
password=data['db']['readDb']['dbUserPassword']
host=data['db']['readDb']['dbHost']
database=data['db']['readDb']['dbName']

engine = sqlalchemy.create_engine('mysql+pymysql://'+username+':'+password+'@'+host+'/'+database)




class make_activity:
    
    def __init__(self,end_date):
        
        self.end=end_date
        
    def find_category(self,cat_num):
        
        cat_query = "SELECT * FROM categories"
        cat = pd.read_sql_query(cat_query, con = engine)
        
        temp = cat_num
        temp = temp.rstrip(']')
        temp = temp.lstrip('[')
        if ',' in temp:
            temp = temp.split(',')
            lst = []
            for x in temp:
                x = int(x)
                lst.append(x)
        else:
            lst = []
            if temp != '':
                lst.append(int(temp))
            else:
                lst.append('None')
            
        cat_name = ''
        if len(lst) > 1:
            x=lst[0]
            sel = cat[cat.id == x]['categoryName'].reset_index(drop=True)[0]
            if cat_name == '':
                cat_name = cat_name + sel
            else:
                cat_name = cat_name + ', ' + sel
        elif len(lst) == 1:
            if lst[0] != 'None':
                sel = cat[cat.id == lst[0]]['categoryName'].reset_index(drop=True)[0]
                cat_name = cat_name + sel
            else:
                cat_name = 'No_Category'
        
        return cat_name


    def load_retailer_data(self):
        
        # SQL Query
        ret_query = "SELECT * FROM retailers"
        
        # Dataframe
        ret_df = pd.read_sql_query(ret_query, con = engine)
       
        # Add category Name to retailer data
        ret_df['retailerCategory'] = ret_df['retailerCategoryId'].apply(lambda x: self.find_category(x))
        
        # Rename ID to retailerId
        ret_df.rename({'id': 'retailerId', 'mallId' : 'retailerMallId'}, 
                      axis = 1, inplace = True)
        
        # Select columns
        ret_cols = ['retailerId', 'retailerName', 'retailerMallId',
                    'retailerCategoryId', 'retailerCategory']
        ret_df = ret_df[ret_cols]
        
        # Add retailer details for NaN's
        ret_df.loc[len(ret_df)] = [99999999, 'Not Available', np.NaN,
                                   np.NaN, 'Not Available']
        ret_df.loc[len(ret_df)] = [99999998, 'Promotional', np.NaN,
                                   np.NaN, 'Promotional']
        print('Retailers done')
        
        return ret_df
    
    def save_file(self,df, date_col):
        """
        
        Parameters
        ----------
        df : pd.DataFrame
            Pandas Dataframe that includes the date column.
        date_col : str
            Date column in the dataframe for which new values are
            to be generated.
        Returns
        -------
        df : pd.DataFrame
            Returns the new dataframe with all the new values added
            against the new date col.
        """
        df[date_col]=pd.to_datetime(df[date_col])
        df['Date'] = pd.to_datetime(df[date_col]).dt.date
        df['Time'] = pd.to_datetime(df[date_col]).dt.time
        df['Year'] = pd.to_datetime(df[date_col]).dt.year
        df['Month_Number'] = pd.to_datetime(df[date_col]).dt.month_name()
        df['Month-Year'] = df['Month_Number'].astype(str) + '-' + df['Year'].astype(str)
        df['Week_Number'] = pd.to_datetime(df[date_col]).dt.isocalendar().week
        df['Day_of_Week'] = pd.to_datetime(df[date_col]).dt.day_name()


        return df
    
    def preffered_behave(self,df,pref_level,metric):
        cat_s=df.groupby(['customerId',pref_level])[metric].count().reset_index()
        pst=cat_s.sort_values(['customerId',metric],ascending=[False,False]).groupby('customerId').head(3)
        fs=pst.groupby('customerId')[pref_level].apply(list)
        
        
        return fs
    
    
        
    
    def scan_related(self):
        
        ret_df=self.load_retailer_data()
        final=[]
        icr=[]
        
        query = "SELECT * FROM customerScans where response!=6 and date(createdAt)<='"+str(self.end)+"'"
        df = pd.read_sql_query(query, con = engine)
        print('Scan Load Successful! \n\n')
        df['createdAt'] = pd.to_datetime(df['createdAt'], dayfirst = True)
        df=self.save_file(df,'createdAt')
        
        # Add processed amount
        df['scan_amount_proc'] = df['billTotal']
        df['scan_amount_proc']=df['scan_amount_proc'].replace('',np.nan).fillna(0).astype(float)
        
        # Replace empty retailers with NaNs
        df.loc[(df['retailerId'] == '') | (df['retailerId'].isnull()),['retailerId']] = 99999999
        df['retailerId'] = df['retailerId'].astype(np.int64)
        
        # Add retailer data
        df = pd.merge(df, ret_df, on = 'retailerId')

        #summary for icr
        f_icr=pd.pivot_table(df,values='id',index='customerId',columns='response',aggfunc='count').fillna(0)
        resp_dict={1:'directSuccess',
                   2:'directReject',
                   3:'refuteSuccess',
                   4:'refuteReject',
                   5:'manualSuccess',
                   0:'refute'}
        f_icr=f_icr.rename(columns=resp_dict)
        icr.append(f_icr)
        all_sc=pd.pivot_table(df,values=['id','Date'],index='customerId',aggfunc='nunique').fillna(0)
        all_sc=all_sc.rename(columns={'id':'scanCount','Date':'userVisitCount'})
        icr.append(all_sc)
        pss=pd.pivot_table(df,values='id',index=['customerId','Date'],aggfunc='count').reset_index()
        pss['diff_days']=(pss.sort_values('Date').groupby('customerId').Date.shift() - pss.Date).dt.days.abs().dropna()
        pff = pss.groupby('customerId')['diff_days'].median().round().reset_index().rename(columns={'diff_days':'shoppingInterval'}).set_index('customerId')
        print(pff)
        #pff=pff.rename(columns={'diff_days':'shoppingInterval'})
        icr.append(pff)
        pret=all_sc.reset_index()
        pret=pret[pret['userVisitCount']>2]
        rel_cust=pret['customerId'].to_list()
        first_scan=df[['customerId','createdAt']].sort_values(['customerId','createdAt']).groupby('customerId').head(1).set_index('customerId').rename(columns={'createdAt':'firstScan'})
        icr.append(first_scan)
        last_scan=df[['customerId','createdAt']].sort_values(['customerId','createdAt']).groupby('customerId').tail(1).set_index('customerId').rename(columns={'createdAt':'lastScan'})
        icr.append(last_scan)
        acc_scan=df[df['response'].isin([1,3,5])]
        scan_amount=pd.pivot_table(acc_scan,values=['id','retailerId','retailerCategoryId','mallId'],index='customerId',aggfunc={'id':'nunique','retailerId':'nunique','retailerCategoryId':'nunique','mallId':'nunique'}).rename(columns={'id':'Accepted_scans','retailerId':'totalRetailers','retailerCategoryId':'totalCategories','mallId':'numberMalls'}).fillna(0)
        icr.append(scan_amount)
        t_s=pd.pivot_table(acc_scan,values='scan_amount_proc',index='customerId',aggfunc=np.sum).rename(columns={'scan_amount_proc':'totalScanAmount'}).fillna(0)
        icr.append(t_s)
        a_s=pd.pivot_table(acc_scan,values='scan_amount_proc',index='customerId',aggfunc='mean').rename(columns={'scan_amount_proc':'averageScanAmount'}).fillna(0)
        icr.append(a_s)
        dl=pd.concat(icr,axis=1)
        dl=dl.fillna(0)
        #dl=dl.drop(columns=[['Accepted_scans']])
        dl['isRisky']=0
        dl['refuteSuccessRate']=(dl['refuteSuccess']/dl['scanCount']).round(2)
        dl['refuteRejectPercent']=(dl['refuteReject']/dl['scanCount']).round(2)
        dl['isRisky']=np.where((dl['refuteRejectPercent']>=0.25)&(dl['userVisitCount']<=3),1,dl['isRisky'])
        
        final.append(dl)
        #extrpolate behaviour
        #revisit=acc_scan[acc_scan['customerId'].isin(rel_cust)]
        #sc_visit=pd.pivot_table(revisit,index=['customerId','Date'],values='id',aggfunc='count').reset_index()
        #a_t=pd.DataFrame()
        pref=[]
        #Average revisit difference
        #for i in tqdm(rel_cust):
            #visits = sc_visit[(sc_visit['customerId'] == i)]
            #visits["Date_Diff"] = np.nan
            #for j in range(1,len(visits)):
                #visits["Date_Diff"].iloc[j] = (visits["Date"].iloc[j] - visits["Date"].iloc[j-1]).days
            #avg = round(visits["Date_Diff"].mean(),0)
            #sc_visit.loc[i, 'shop_inteval'] = avg
            
            
        #sc_visit=sc_visit.set_index('customerId')
        #sc_visit=sc_visit.drop(columns=['Date'])
        #pref.append(sc_visit)
        #visit=acc_scan[['customerId','Date']]
        #v_l = visit.groupby(['customerId']).datetime.apply(lambda x: x.sort_values().diff().dt.days).fillna(0).reset_index(name='timediff in days')
        
        
        cat=acc_scan[['customerId','retailerCategory','retailerName','scan_amount_proc','id']]
        cat_p=self.preffered_behave(cat,'retailerCategory','id')
        alt=cat_p.reset_index()
        s_f=[]
        s_f.append(alt)
        
        
        new_df=pd.DataFrame(alt['retailerCategory'].to_list(),columns=['preferredCategory1','preferredCategory2','preferredCategory3'])
        s_f.append(new_df)
        s_t=pd.concat(s_f,axis=1)
        s_t=s_t.set_index('customerId').drop(columns=['retailerCategory'])
        pref.append(s_t)
        ret_p=self.preffered_behave(cat,'retailerName','id')
        p_s=[]
        alter=ret_p.reset_index()
        p_s.append(alter)
        new_df2=pd.DataFrame(alter['retailerName'].to_list(),columns=['preferredretailer1','preferredretailer2','preferredretailer3'])
        p_s.append(new_df2)
        p_t=pd.concat(p_s,axis=1)
        p_t=p_t.set_index('customerId').drop(columns=['retailerName'])
        #ret_p['retailerName']=ret_p['retailerName'].astype(str)
        pref.append(p_t)
        click=pd.concat(pref,axis=1)
        final.append(click)
        scan_ret=pd.concat(final,axis=1)
       #scan_ret['Month_diff']=(((scan_ret['Last_scan']).dt.date - (scan_ret['First_scan']).dt.date)/np.timedelta64(1, 'M')).round(0)       
       #scan_ret['Regularity']=(scan_ret['month_scanned']/scan_ret['Month_diff'])*100
       #scan_ret=scan_ret.drop(columns=['Month_diff','month_scanned'])
        print("Heavy stuff done")
        return scan_ret
    
    def fcm_related(self):
        frames=[]
        query = "SELECT customerId,count(distinct device) As Number_devices,Sum(case reason when 'Token Invalidated By Script' then 1 else 0 END) AS uninstallCount FROM customerFCM where date(createdAt)<='"+str(self.end)+"' GROUP BY customerId"
        df = pd.read_sql_query(query, con = engine)
        df['isMultiSource']=0
        df['isMultiSource']=np.where((df['Number_devices']>=2),1,df['isMultiSource'])
        
        df=df.set_index('customerId')
        
        return df
    
    def coupon_related(self):
        frames=[]
        
        query = "SELECT * FROM couponTransactions where couponId in (select id from coupon where categorization=1) and date(createdAt)<='"+str(self.end)+"'"
        df_ct = pd.read_sql_query(query, con = engine)
        c_un=pd.pivot_table(df_ct,values='id',index='customerId',aggfunc='count').fillna(0)
        c_un=c_un.rename(columns={'id':'couponUnlockCount'})
        df_cu=df_ct[df_ct['couponRedeemDateTime'].notnull()]
        df_cus=df_ct[df_ct['couponRedeemDateTime'].isnull()]
        c_us=pd.pivot_table(df_cu,values='id',index='customerId',aggfunc='count').fillna(0)
        c_us=c_us.rename(columns={'id':'couponUsedCount'})
        print('Load Successful! \n\n')
        frames.append(c_un)
        frames.append(c_us)

        query = "SELECT * FROM coupon"
        df_c = pd.read_sql_query(query, con = engine)
        df_c=df_c.rename(columns={'id':'couponId'})
        df_cl=pd.merge(df_cus,df_c[['couponId','endDate']],how='left',on='couponId')
        df_exp=df_cl[(df_cl['endDate'])<=date.today()]
        df_ex_c=pd.pivot_table(df_exp,values='id',index='customerId',aggfunc='count').fillna(0)
        df_ex_c=df_ex_c.rename(columns={'id':'Coupon Expired'})
        print('Load Successful! \n\n')
        frames.append(df_ex_c)
        
        
        pf=pd.concat(frames,axis=1)
        
        return pf
    
    def reward_related(self):
        frames=[]
        
        query = "SELECT * FROM rewardTransactions where rewardId in (select id from rewards where categorization=1) and date(createdAt)<='"+str(self.end)+"'"
        df_rt = pd.read_sql_query(query, con = engine)
        r_un=pd.pivot_table(df_rt,values='id',index='customerId',aggfunc='count').fillna(0)
        r_un=r_un.rename(columns={'id':'rewardUnlockCount'})
        df_ru=df_rt[df_rt['rewardRedeemDateTime'].notnull()]
        df_rus=df_rt[df_rt['rewardRedeemDateTime'].isnull()]
        r_us=pd.pivot_table(df_ru,values='id',index='customerId',aggfunc='count').fillna(0)
        r_us=r_us.rename(columns={'id':'rewardUsedCount'})
        print('Load Successful! \n\n')
        frames.append(r_un)
        frames.append(r_us)


        query = "SELECT * FROM rewards"
        df_r = pd.read_sql_query(query, con = engine)
        df_r=df_r.rename(columns={'id':'rewardId'})
        df_rl=pd.merge(df_rus,df_r[['rewardId','endDate']],how='left',on='rewardId')
        df_exp2=df_rl[(df_rl['endDate'])<=date.today()]
        df_ex_r=pd.pivot_table(df_exp2,values='id',index='customerId',aggfunc='count').fillna(0)
        df_ex_r=df_ex_r.rename(columns={'id':'Reward Expired'})
        print('Load Successful! \n\n')
        frames.append(df_ex_r)
        
        pg=pd.concat(frames,axis=1)
        
        return pg
    
    def gamification_related(self):
        frames=[]
        
        query = "SELECT * FROM scratchCardTransactions where date(createdAt)<='"+str(self.end)+"'"
        df_sct = pd.read_sql_query(query, con = engine)
        sc_un=pd.pivot_table(df_sct,values='id',index='customerId',aggfunc='count').fillna(0)
        sc_un=sc_un.rename(columns={'id':'scardUsed'})
        print('Load Successful! \n\n')
        frames.append(sc_un)

        query = "SELECT * FROM spinWheelTransactions where date(createdAt)<='"+str(self.end)+"'"
        df_spt = pd.read_sql_query(query, con = engine)
        sp_un=pd.pivot_table(df_spt,values='id',index='customerId',aggfunc='count').fillna(0)
        sp_un=sp_un.rename(columns={'id':'swheelUsed'})
        print('Load Successful! \n\n')
        frames.append(sp_un)
        
        pg=pd.concat(frames,axis=1)
        return pg
    
    def scan_win(self):
        
        query = "SELECT customerId,count(*) AS S_W_participation,sum(isAwarded) AS S_W_completion FROM scanWinTransactions Group by customerId"
        df = pd.read_sql_query(query, con = engine)
        df=df.set_index('customerId')
        print('Load Successful! \n\n')
        
        return df
    
    
    def notification_clicked(self):
        query = "SELECT userId as customerId,count(*) as notificationClicked FROM notificationClick group by userId"
        df= pd.read_sql_query(query, con = engine)
        df=df.set_index('customerId')
        return df

    def transferred_delights(self):
        query =" SELECT customerId,count(id) As delightTransfer from transferDelights group by customerId"
        df= pd.read_sql_query(query, con = engine)
        df=df.set_index('customerId')
        return df
    
    def news_feed(self):
        query='SELECT userId as customerId,count(id) as newsFeedInteraction from newsFeedActivity group by userId'
        df= pd.read_sql_query(query, con = engine)
        df=df.set_index('customerId')
        return df
    
    def gems(self):
        query='SELECT customerId,sum(value) as gemsUsed  FROM `customerGemTransactions` WHERE type=2 and debitType!=1 group by customerId'
        df= pd.read_sql_query(query, con = engine)
        df=df.set_index('customerId')
        return df
    
    def referred(self):
        query='select customerId, count(id) as usersReferred from customerPointsTransactions where creditType=12 group by customerId'
        df= pd.read_sql_query(query, con = engine)
        df=df.set_index('customerId')
        return df
    
    
    def main(self):
        frames=[]
        query = "SELECT id as customerId, TIMESTAMPDIFF(DAY,date(createdAt), CURRENT_DATE) AS customerLifetime, preferredMallId,`userName`,`firstName`,`lastName`,`lastLogin`,`email`,`gender`,`dob`,`lastLogin`,source,tierId,isActive FROM customerUser"
        df_main = pd.read_sql_query(query, con = engine)
        #df_main=df_main.set_index('customerId')
        #frames.append(df_main)
        sec_1=self.scan_related()
        if isinstance(sec_1, pd.DataFrame):
            print('chill')
        else:
            print('its scan')
        frames.append(sec_1)
        
        sec_2=self.fcm_related()
        if isinstance(sec_2, pd.DataFrame):
            print('chill')
        else:
            print('its fcm')
        frames.append(sec_2)
        sec_3=self.coupon_related()
        if isinstance(sec_3, pd.DataFrame):
            print('chill')
        else:
            print('its coupon')
        frames.append(sec_3)
        sec_4=self.reward_related()
        if isinstance(sec_4, pd.DataFrame):
            print('chill')
        else:
            print('its reward')
        frames.append(sec_4)
        sec_5=self.gamification_related()
        if isinstance(sec_5, pd.DataFrame):
            print('chill')
        else:
            print('its game')
        frames.append(sec_5)
        sec_6=self.scan_win()
        if isinstance(sec_6, pd.DataFrame):
            print('chill')
        else:
            print('its scan and win')
        frames.append(sec_6)
        sec_7=self.notification_clicked()
        if isinstance(sec_7, pd.DataFrame):
            print('chill')
        else:
            print('its notification')
        frames.append(sec_7)
        sec_8=self.transferred_delights()
        if isinstance(sec_8, pd.DataFrame):
            print('chill')
        else:
            print('its delights')
        frames.append(sec_8)
        sec_9=self.news_feed()
        if isinstance(sec_9, pd.DataFrame):
            print('chill')
        else:
            print('its newsfeed')
        frames.append(sec_9)
        sec_10=self.gems()
        if isinstance(sec_10, pd.DataFrame):
            print('chill')
        else:
            print('its gems')
        frames.append(sec_10)
        sec_11=self.referred()
        if isinstance(sec_11, pd.DataFrame):
            print('chill')
        else:
            print('its referred')
        frames.append(sec_11)
        
        plt=pd.concat(frames,axis=1).replace([np.inf, -np.inf], np.nan).reset_index()
        plt=pd.merge(df_main,plt,how='inner',on='customerId')
        plt=plt.set_index('customerId')
        
        return plt
        
        
    
        
        
        
        
        
        
if __name__ == '__main__':
    a=make_activity(date(2022,8,17))
    b=a.scan_related()
    print(b)
    print('Start writing')
    b.to_sql('customerActivity', con=engine, if_exists='replace',index_label='customerId')
    print('End writing')   
        