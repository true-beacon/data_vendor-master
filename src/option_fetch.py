import datetime
import json
import pandas
import pandas as pd
from blp import blp
from dateutil.relativedelta import relativedelta, TH
from pathlib import Path
import calendar
import os
import datetime

class BBData:
    def __init__(self):
        self.bquery = blp.BlpQuery().start()
        self.bb_symbol_map={'NIFTY':'NIFTY','BANKNIFTY':'NSEBANK'}
        self.bb_weekmap={"BANKNIFTY":["A","B","C","D","E"],"NIFTY":["C","D","E","F","G"]}

    def getOptionsBBPrices(self, secList,outputfileName,eventType='TRADE',FromDate = 20230102, tillDays=1,interval=1):
        StartDate = pd.to_datetime(FromDate, format="%Y%m%d")
        EndDate = StartDate + pd.Timedelta(days=tillDays)
        for security,nseName,strike,expirydate in zip(secList['BBSymbol'],secList['NSESymbol'],secList['strikes'],secList['expirydates']):
            try:
                res = self.bquery.bdib(security,eventType,interval,str(datetime.datetime.strptime(str(FromDate),"%Y%m%d")),str(EndDate))
                tempoutputDf = res
                tempoutputDf['Symbol']=nseName
                tempoutputDf['strike']=strike
                tempoutputDf['expirydate']=expirydate
                tempoutputDf.to_csv(outputfileName, mode='a', header=True, index=False)
            except Exception as e:
                print("not done for " + security )
                print(e)
    def fetch_strikes(self,underlying_name,strike_range,expiry,interval=100):
        try:
            exp_date=expiry
            month =str(expiry.month)
            last_day=datetime.date(expiry.year,expiry.month,calendar.monthrange(expiry.year,expiry.month)[1])
            last_thurs=last_day + relativedelta(weekday=TH(-1))
            if expiry>datetime.datetime.today().date() or expiry.month==datetime.datetime.today().month:
                if expiry!=last_thurs:
                    bb_symbol = self.bb_symbol_map[underlying_name] + self.bb_weekmap[underlying_name][(expiry.day - 1) // 7]
                    expiry=expiry.strftime("%y")+month+expiry.strftime("%d")
                    symbols = [[underlying_name+str(expiry)+str(i)+'PE',underlying_name+str(expiry)+str(i)+'CE'] for i in range(strike_range[0],strike_range[1],interval)]
                    strikes=[[i,i] for i in range(strike_range[0],strike_range[1],interval)]
                else:
                    bb_symbol = underlying_name+' IS '
                    expiry=str(int(expiry.strftime("%m")))
                    symbols = [[underlying_name+str(expiry)+str(i)+'PE',underlying_name+str(expiry)+str(i)+'CE'] for i in range(strike_range[0],strike_range[1],interval)]
                    strikes=[[i,i] for i in range(strike_range[0],strike_range[1],interval)]
                b_symbols= [[bb_symbol+str(expiry)+' C'+str(i)+' Equity',bb_symbol+str(expiry)+' P'+str(i)+' Equity'] for i in range(strike_range[0],strike_range[1],interval)]
            else:
                if expiry!=last_thurs:
                    bb_symbol = self.bb_symbol_map[underlying_name]+ self.bb_weekmap[underlying_name][(expiry.day - 1) // 7]+" "+str(expiry.month)+"/"+expiry.strftime("%y")
                    expiry=expiry.strftime("%y")+month+expiry.strftime("%d")
                    symbols = [[underlying_name+str(expiry)+str(i)+'PE',underlying_name+str(expiry)+str(i)+'CE'] for i in range(strike_range[0],strike_range[1],interval)]
                    strikes=[[i,i] for i in range(strike_range[0],strike_range[1],interval)]
                else:
                    bb_symbol = underlying_name+' IS'+" "+str(expiry.month)+"/"+expiry.strftime("%y")
                    expiry=expiry.strftime("%y")+expiry.strftime("%b").upper()
                    symbols = [[underlying_name+str(expiry)+str(i)+'PE',underlying_name+str(expiry)+str(i)+'CE'] for i in range(strike_range[0],strike_range[1],interval)]
                    strikes=[[i,i] for i in range(strike_range[0],strike_range[1],interval)]
                b_symbols= [[bb_symbol+' P'+str(i)+' Equity',bb_symbol +' C'+str(i)+' Equity'] for i in range(strike_range[0],strike_range[1],interval)]
            symbols =[val for sublist in symbols for val in sublist]
            b_symbols =[val for sublist in b_symbols for val in sublist]
            strikes=[val for sublist in strikes for val in sublist]
            fetch_df=pd.DataFrame()
            
            fetch_df['BBSymbol']=b_symbols
            fetch_df['NSESymbol']=symbols
            fetch_df['strikes']=strikes
            fetch_df['expirydates']=str(exp_date)
            return fetch_df
        except Exception as e:
            print(e)



dirname = os.path.dirname(__file__)

obj = BBData()
data=obj.fetch_strikes("TCS",(3200,3400),expiry=datetime.date(2023,1,26),interval=20)
outputfileName = os.path.join(str(Path(dirname).parent) ,"Output","OptionsInformation"+datetime.datetime.now().strftime('%Y%d%m%H%M%S')+'.csv')
obj.getOptionsBBPrices(data,outputfileName,FromDate=20230108,tillDays=10)
print(data)
