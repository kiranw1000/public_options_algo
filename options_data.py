import config,datetime,math
import pandas as pd
from polygon import RESTClient

pclient = RESTClient(config.POLYGON_KEY)

def get_options_high_low(tickers:dict,path,df:pd.DataFrame=pd.DataFrame(columns=['bought_price','high','low','percent_high','percent_low'])):
    for x,y in tickers.items():
        bought = y[1]
        h,l = pd.DataFrame(pclient.get_aggs('O:'+x,390,'minute',y[0],datetime.datetime.now()))[['high','low']].values[0]
        hp,lp = ((h/bought)-1.00),(1.00-(l/bought))
        df.loc[f"{x} ({datetime.datetime.today().strftime('%m-%d-%Y')})"] = [bought,h,l,hp,lp]
    df.to_csv(path)
    return df
