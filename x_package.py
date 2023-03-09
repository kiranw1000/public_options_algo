import sys
sys.path.append('Xavier')
#Install modules and mount drive
import alpaca_trade_api as apca
import websocket, json, asyncio, datetime, time
import matplotlib.pyplot as plt
import mplfinance.original_flavor as mplf
import matplotlib.dates as mpdates
import pandas as pd
import imp, math
import polygon as p
import yahoo_fin.options as yfo
import tda
import accounts, config, tickers
from tda.orders import options
import accounts, tickers, config, watchlist
from alpaca_trade_api import TimeFrame
from tda import orders
from options_data import get_options_high_low
client = tda.auth.client_from_token_file(f'Xavier/tokens/kiran.json',config.TD_CLIENT_ID)

#Create Trade Account Class
class trade_account:
    name = ''
    account_num = 0
    account_value = 0
    percent = 0
    allocation = 0
    contract_counts = {}
    def __init__(self,name,account_num,percent=0.9):
      self.name = name
      self.account_num = account_num
      self.percent = percent
      client = tda.auth.client_from_token_file(f'Xavier/tokens/{name}.json',config.TD_CLIENT_ID)
      self.account_value = client.get_account(self.account_num).json()['securitiesAccount']['currentBalances']['cashAvailableForTrading']
      self.contract_counts = {}
    def add_to_list(self):
      accounts.accounts[self.name] = self.account_num
    def get_account_value(self):
      client = tda.auth.client_from_token_file(f'Xavier/tokens/{self.name}.json',config.TD_CLIENT_ID)
      self.account_value = client.get_account(self.account_num).json()['securitiesAccount']['currentBalances']['cashAvailableForTrading']
      return self.account_value
    def get_allocation(self,numstocks):
      self.allocation = math.floor(math.floor(self.account_value*self.percent)/numstocks)
    def get_raw_balances(self):
      client = tda.auth.client_from_token_file(f'Xavier/tokens/{self.name}.json',config.TD_CLIENT_ID)
      return client.get_account(self.account_num).json()
    def buy_option(self,symbol:str,price:float,verbose:bool):
        numcontracts = math.floor(self.allocation/(price*100))
        try:
          print(f'Numcontracts {numcontracts}')
          self.contract_counts[symbol] = numcontracts
          if numcontracts!=0:
            orderspec = options.option_buy_to_open_limit(symbol,numcontracts,price=price)
            client = tda.auth.client_from_token_file(f'Xavier/tokens/{self.name}.json',config.TD_CLIENT_ID)
            message = client.create_saved_order(self.account_num,orderspec)
            #message = client.place_order(self.account_num,orderspec).content
            if verbose: 
              print(message,f'Bought {symbol} for price {price} at {datetime.datetime.now().strftime("%m%d%Y")}')
          return price, numcontracts
        except:
          print(f'{self.name} too small allocation to buy option {symbol} for {numcontracts*price} with allocation {self.allocation}')
    def sell_option(self,symbol:str,verbose:bool,price:float,boughtprice:float):
        numcontracts = self.contract_counts[symbol]
        runners = 0
        if numcontracts>20 and price>boughtprice:
            runners = 2
        print(symbol,numcontracts,f'runners: {runners}')
        orderspec = options.option_sell_to_close_limit(symbol,numcontracts-runners,price=price)
        client = tda.auth.client_from_token_file(f'Xavier/tokens/{self.name}.json',config.TD_CLIENT_ID)
        message = client.create_saved_order(self.account_num,orderspec)
        #message = client.place_order(self.account_num,orderspec).json()
        if verbose:
          print(message)
    def cancel_orders(self):
        client = tda.auth.client_from_token_file(f'Xavier/tokens/{self.name}.json',config.TD_CLIENT_ID)
        orderlist = [x['orderId'] for x in client.get_orders_by_query(statuses = [client.Order.Status('WORKING')]).json()]
        for x in orderlist:
            client.cancel_order(x,self.account_num)

class paper_trade_account:
    name = ''
    account_value = 0
    percent = 0
    allocation = 0
    contract_counts = {}
    def __init__(self,name,account_value,percent=0.9):
      self.name = name
      self.percent = percent
      self.account_value = account_value
      self.contract_counts = {}
    def get_balance(self):
      return self.account_value
    def get_allocation(self,numstocks):
      self.allocation = math.floor(math.floor(self.account_value*self.percent)/numstocks)
    def get_raw_balances(self):
      return self.account_value
    def buy_option(self,symbol:str,price:float,verbose:bool):
        numcontracts = math.floor(self.allocation/(price*100))
        try:
          print(f'Numcontracts {numcontracts}')
          self.contract_counts[symbol] = numcontracts
          if numcontracts!=0:
            if verbose: 
              print(f'Bought {symbol} for price {price} at {datetime.datetime.now().strftime("%m%d%Y")}')
          self.account_value -= price*numcontracts*100
          return price, numcontracts
        except:
          print(f'{self.name} too small allocation to buy option {symbol} for {numcontracts*price} with allocation {self.allocation}')
    def sell_option(self,symbol:str,verbose:bool,price:float,boughtprice:float):
        numcontracts = self.contract_counts[symbol]
        runners = 0
        if numcontracts>20 and price>boughtprice:
            runners = 0
        print(symbol,numcontracts,f'runners: {runners}')
        self.contract_counts[symbol] = runners
        self.account_value+= price*numcontracts*100
    def get_account_value(self):
        value = float(self.account_value)
        for x,y in self.contract_counts.items():
            client = tda.auth.client_from_token_file(f'Xavier/tokens/kiran.json',config.TD_CLIENT_ID)
            price = client.get_quote(x).json()[x]['mark']
            value+=y*price*100
        return value
    def cancel_orders(self):
        pass
def get_td_option_name(ticker:str,expiration:str,cp:str,strike:int):
    assert cp.upper() in ('C','P')
    return tda.orders.options.OptionSymbol(ticker,datetime.datetime.strptime(expiration,'%B %d, %Y').strftime('%m%d%y'),cp.upper(),str(strike)).build()

#Create Stock Class
apcarest = apca.REST(config.APCA_KEY_ID,config.APCA_SECRET_KEY)
class stock:
    ticker = ""
    stream = ""
    key = "c7i7ajqad3if83qgf1l0"
    today = datetime.datetime.now().date()
    startdate = today-datetime.timedelta(days=7)
    startts = int(time.mktime(startdate.timetuple()))
    tstoday = int(time.mktime(today.timetuple()))
    candleone = {}
    candletwo = {}
    minutecount = 0
    toplimit, bottomlimit = 0,0
    optionName = ""
    td_option_name = ""
    callBoughtPrice,putBoughtPrice = 0,0
    stopLoss,takeProfit = 0,0
    soldCallPrice, soldPutPrice = 0,0
    putStrike, callStrike = 0,0
    TPpct, SLpct = 0,0
    expiration = 0
    ioc = False
    oib = False
    ttl = False
    tbl = False
    oacot = False
    oacub = False
    sldict = {}
    checklist = {
        "InOutCandles":ioc,
        "Opened in between":oib,
        "Touches Top Line":ttl,
        "Touches Bottom Line":tbl,
        "Open and close over top":oacot,
        "Open and close under bottom":oacub
    }
    candleplot = 0
    plotdata = pd.DataFrame()
    fig,ax = (0,0)
    dateformat = mpdates.DateFormatter("%d-%m-%Y")
    contract = {"b":"","t":"","s":""}
    candlesdf = pd.DataFrame(columns=  ["Symbol","Open","High","Low","Close","Volume","Start time","End time","Start datetime","End datetime"])
    def from_old(self,old):
        for x in ['toplimit','bottomlimit','optionName','td_option_name','TPpct','SLpct','callBoughtPrice','putBoughtPrice','soldCallPrice','soldPutPrice','callStrike','putStrike','ioc','oib','tbl','oacub','ttl','oacot']:
            setattr(self,x,getattr(old,x))
    def get_ioc(self,logs=[]):
        logs.append(datetime.datetime.now().timestamp(), f'{self.ticker} ioc : {self.ioc}')
        return self.ioc
    def get_oib(self,logs=[]):
        logs.append(datetime.datetime.now().timestamp(), f'{self.ticker} oib : {self.oib}')
        return self.oib
    def get_ttl(self,logs=[]):
        logs.append(datetime.datetime.now().timestamp(), f'{self.ticker} ttl : {self.ttl}')
        return self.ttl
    def get_tbl(self,logs=[]):
        logs.append(datetime.datetime.now().timestamp(), f'{self.ticker} tbl : {self.tbl}')
        return self.tbl
    def get_oacot(self,logs=[]):
        logs.append(datetime.datetime.now().timestamp(), f'{self.ticker} oacot : {self.oacot}')
        return self.oacot
    def get_oacub(self,logs=[]):
        logs.append(datetime.datetime.now().timestamp(), f'{self.ticker} oacub : {self.oacub}')
        return self.oacub
    def set_ioc(self,val,logs=[],verbose = False):
        if verbose and self.ioc!=val:
            print(datetime.datetime.now(), f'{self.ticker} set ioc : {val}')
        self.ioc = val
        logs.append(datetime.datetime.now().timestamp(), f'{self.ticker} set ioc : {val}')
    def set_oib(self,val,logs=[],verbose = False):
        if verbose and self.oib!=val:
            print(datetime.datetime.now(), f'{self.ticker} set oib : {val}')
        self.oib = val
        logs.append(datetime.datetime.now().timestamp(), f'{self.ticker} set oib : {val}')
    def set_ttl(self,val,logs=[],verbose = False):
        if verbose and self.ttl!=val:
            print(datetime.datetime.now(), f'{self.ticker} set ttl : {val}')
        self.ttl = val
        logs.append(datetime.datetime.now().timestamp(), f'{self.ticker} set ttl : {val}')
    def set_tbl(self,val,logs=[],verbose = False):
        if verbose and self.tbl!=val:
            print(datetime.datetime.now(), f'{self.ticker} set tbl : {val}')
        self.tbl = val
        logs.append(datetime.datetime.now().timestamp(), f'{self.ticker} set tbl : {val}')
    def set_oacot(self,val,logs=[],verbose = False):
        if verbose and self.oacot!=val:
            print(datetime.datetime.now(), f'{self.ticker} set oacot : {val}')
        self.oacot = val
        logs.append(datetime.datetime.now().timestamp(), f'{self.ticker} set oacot : {val}')
    def set_oacub(self,val,logs=[],verbose = False):
        if verbose and self.oacub!=val:
            print(datetime.datetime.now(), f'{self.ticker} set oacub : {val}')
        self.oacub = val
        logs.append(datetime.datetime.now().timestamp(), f'{self.ticker} set oacub : {val}')
    def set_SLpct(self,val,logs=[],verbose = False):
        if verbose and self.SLpct!=val:
            print(datetime.datetime.now(), f'{self.ticker} set SLpct : {val}')
        self.SLpct = 1.0-val
        logs.append(datetime.datetime.now().timestamp(), f'{self.ticker} set SLpct : {val}')
    def set_TPpct(self,val,logs=[],verbose = False):
        if verbose and self.TPpct!=val:
            print(datetime.datetime.now(), f'{self.ticker} set TPpct : {val}')
        self.TPpct = 1.0+val
        logs.append(datetime.datetime.now().timestamp(), f'{self.ticker} set TPpct : {val}')
    def __init__(self,ticker,TPpct=.3,SLpct=.2, sldict={5:.15,10:.1,20:.5}) -> None:
        self.ticker = ticker.upper()
        self.stream = "AM."+ticker.upper()
        self.TPpct = 1.0+TPpct
        self.SLpct = 1.0-SLpct
        self.sldict = sldict
    def addCandle(self,candle):
        self.candlesdf = self.candlesdf.append(candle)
    def getPastTwoDaysCandles(self,sameday=False):
        if sameday:
            end = datetime.datetime.today().strftime(f'%Y-%m-%d')
        else:
            end = (datetime.datetime.today()-datetime.timedelta(1)).strftime(f'%Y-%m-%d')
        resp = apcarest.get_bars(self.ticker,TimeFrame.Day,start=(datetime.datetime.today()-datetime.timedelta(7)).strftime(f'%Y-%m-%d'),end=end).df
        self.candleone = resp.iloc[-2]
        self.candletwo = resp.iloc[-1]
        self.plotdata = pd.DataFrame(resp)
    def showplot(self):
        self.fig,self.ax = plt.subplots()
        self.ax.grid(True)
        plt.title(self.ticker)
        self.ax.xaxis.set_major_formatter(self.dateformat)
        plotdf = self.plotdata
        plotdf ['t'] = plotdf["t"].map(datetime.datetime.fromtimestamp)
        plotdf ['t'] = plotdf["t"].map(mpdates.date2num)
        mplf.candlestick_ohlc(self.ax,self.plotdata.values,width=0.2,colorup="green",colordown="red",alpha=0.8)
        self.fig.autofmt_xdate()
        self.fig.tight_layout()
    def checkPastTwoDays(self,updateself=True,logs=[]):
        #assert self.candleone!={} and self.candletwo!={},"Run getPastTwoDaysCandles function first"
        candleone, candletwo = self.candleone,self.candletwo
        #set limits if its an inside outside candle
        if (candletwo['low']<candleone['low'] and candletwo['high']>candleone['high']) or (candletwo['low']>candleone['low'] and candletwo['high']<candleone['high']):
            templist = [candletwo['low'],candletwo['high']]
            templist.sort()
            self.bottomlimit,self.toplimit = templist
            logs.append(datetime.datetime.now().timestamp(),f"Limits for {self.ticker}: {self.toplimit} | {self.bottomlimit}")
            if updateself:
                self.set_ioc(True,verbose=True,logs=logs)
            return True
        else:
            return False
    def checkOpen(self, logs):
        assert self.toplimit!=self.bottomlimit 
        opents = int(datetime.datetime.combine(datetime.datetime.today().date(),datetime.time(hour = 9,minute = 30)).timestamp())
        with apca.REST(config.APCA_KEY_ID,config.APCA_SECRET_KEY) as apcarest:
            high,low = apcarest.get_bars(symbol = self.ticker, timeframe=apca.TimeFrame.Day).df[['high','low']].values[0]
            logs.append(datetime.datetime.now().timestamp(),f"{self.ticker} opened in between: {low<self.toplimit and high> self.bottomlimit}")
            return low<self.toplimit and high> self.bottomlimit
    def updateChecklist(self,ropen,rhigh,rlow,rclose,logs=[]):
        beginChklst = self.checklist.copy()
        if self.get_oib(logs=logs) == False:
            self.set_oib(rhigh<self.toplimit and rlow>self.bottomlimit,logs=logs,verbose=True)
        self.set_oacot(ropen>self.toplimit and rclose>self.toplimit,verbose=True,logs=logs)
        self.set_oacub(ropen<self.bottomlimit and rclose<self.bottomlimit,verbose=True,logs=logs)
        self.set_ttl(rhigh>self.toplimit,verbose=True,logs=logs)
        self.set_tbl(rlow<self.bottomlimit,verbose=True,logs=logs)
        self.checklist = {
            "InOutCandles":self.ioc,
            "Opened in between":self.oib,
            "Touches Top Line":self.ttl,
            "Touches Bottom Line":self.tbl,
            "Open and close over top":self.oacot,
            "Open and close under bottom":self.oacub
        }
        if beginChklst!=self.checklist:
            return True
        else:
            return False
    def updateChecklistBase(self,logs=[]):
        _,ropen,rhigh,rlow,rclose,volume,_,_ = apcarest.get_latest_bar(self.ticker)._raw.values()
        ts = datetime.datetime.now().timestamp()
        self.addCandle(pd.DataFrame([[self.ticker,ropen,rhigh,rlow,rclose,volume,ts,ts,datetime.datetime.fromtimestamp(ts/1000),datetime.datetime.fromtimestamp(ts/1000)]],columns = ["Symbol","Open","High","Low","Close","Volume","Start time","End time","Start datetime","Edn datetime"]))
        beginChklst = self.checklist.copy()
        if self.get_oib(logs=logs) == False:
            self.set_oib(rhigh<self.toplimit and rlow>self.bottomlimit,logs=logs,verbose=True)
        self.set_oacot(ropen>self.toplimit and rclose>self.toplimit,verbose=True,logs=logs)
        self.set_oacub(ropen<self.bottomlimit and rclose<self.bottomlimit,verbose=True,logs=logs)
        self.set_ttl(rhigh>self.toplimit,verbose=True,logs=logs)
        self.set_tbl(rlow<self.bottomlimit,verbose=True,logs=logs)
        self.checklist = {
            "InOutCandles":self.ioc,
            "Opened in between":self.oib,
            "Touches Top Line":self.ttl,
            "Touches Bottom Line":self.tbl,
            "Open and close over top":self.oacot,
            "Open and close under bottom":self.oacub
        }
        if beginChklst!=self.checklist:
            return True
        else:
            return False
    def reload_checklist(self):
        self.checklist = {
            "InOutCandles":self.ioc,
            "Opened in between":self.oib,
            "Touches Top Line":self.ttl,
            "Touches Bottom Line":self.tbl,
            "Open and close over top":self.oacot,
            "Open and close under bottom":self.oacub
        }
    def updateStopLoss(self,cp,logs,verbose:bool=True):
        if cp.lower() == "call":
            client = tda.auth.client_from_token_file(f'Xavier/tokens/kiran.json',config.TD_CLIENT_ID)
            price = client.get_quote(self.td_option_name).json()[self.td_option_name]['mark']
            newsl = self.stopLoss
            for x,y in self.sldict.items():
                if self.minutecount>=x: newsl = self.callBoughtPrice*(1-y)
            if newsl>self.stopLoss and newsl>self.callBoughtPrice and round(price/self.callBoughtPrice,2)/.05>0:
                logs.append(datetime.datetime.now().timestamp(),f'Change stoploss of {self.ticker} | {self.optionName} from {self.stopLoss} to {newsl}')
                if verbose:
                  print(f'Change stoploss of {self.ticker} | {self.optionName} from {self.stopLoss} to {newsl}')
                self.stopLoss = newsl
            return self.stopLoss
        elif cp.lower() == 'put':
            client = tda.auth.client_from_token_file(f'Xavier/tokens/kiran.json',config.TD_CLIENT_ID)
            price = client.get_quote(self.td_option_name).json()[self.td_option_name]['mark']
            newsl = self.stopLoss
            for x,y in self.sldict.items():
                if self.minutecount>=x: newsl = self.callBoughtPrice*(1-y)
            if newsl>self.stopLoss and newsl>self.putBoughtPrice and round(price/self.putBoughtPrice,2)/.05>0:
                logs.append(datetime.datetime.now().timestamp(),f'Change stoploss of {self.ticker} | {self.optionName} from {self.stopLoss} to {newsl}')
                if verbose:
                  print(f'Change stoploss of {self.ticker} | {self.optionName} from {self.stopLoss} to {newsl}')
                self.stopLoss = newsl
            return self.stopLoss
        else:
            return 'cp must be either "call" or "put"'
    def buy_call(self,accountslist:list,logs=[],verbose=False):
        #TODO
        client = tda.auth.client_from_token_file(f'Xavier/tokens/kiran.json',config.TD_CLIENT_ID)
        isFriThur = datetime.datetime.today().weekday() == 4 or datetime.datetime.today().weekday() == 3
        expiration = 1 if isFriThur else 0
        calldict = pd.DataFrame(pd.DataFrame(client.get_option_chain(self.ticker,strike_count=4).json()).iloc[expiration]['callExpDateMap']).iloc[0].iloc[-1]
        self.expiration = expiration
        currentprice = apcarest.get_latest_quote(self.ticker)._raw['ap']
        client = tda.auth.client_from_token_file(f'Xavier/tokens/kiran.json',config.TD_CLIENT_ID)
        td_option_name, strike, price, expdate = pd.Series(calldict)[['symbol','strikePrice','mark','expirationDate']].values
        option_name = self.ticker+datetime.datetime.fromtimestamp(expdate/1000).strftime('%y%m%d')+"%08.0f"%(price*1000)
        self.td_option_name = td_option_name
        self.callStrike = strike
        self.optionName = option_name
        for x in accountslist:
            try:
              print(self.td_option_name,price)
              price,numcontracts = x.buy_option(self.td_option_name,price=price,verbose = True)
              self.callBoughtPrice = price
              self.stopLoss = price*self.SLpct
              self.takeProfit = price*self.TPpct
              if verbose:
                  print(option_name,f"price:{price}", f"strike: {strike}", datetime.datetime.now())
            except:
              continue
        if verbose:
            print(f'Stop loss: {self.stopLoss} | Take profit: {self.takeProfit}')
        logs.append(datetime.datetime.now().timestamp(),f'{self.ticker} price: {currentprice} bought call: {option_name} for strike of {strike} and price of {price}. Stop loss: {self.stopLoss} | Take profit: {self.takeProfit}')
        return option_name

    def buy_put(self,accountslist:list,logs=[],verbose=False):
        #TODO
        client = tda.auth.client_from_token_file(f'Xavier/tokens/kiran.json',config.TD_CLIENT_ID)
        isFriThur = datetime.datetime.today().weekday() == 4 or datetime.datetime.today().weekday() == 3
        expiration = 1 if isFriThur else 0
        putdict = pd.DataFrame(pd.DataFrame(client.get_option_chain(self.ticker,strike_count=4).json()).iloc[expiration]['callExpDateMap']).iloc[0].iloc[0]
        self.expiration = expiration
        currentprice = apcarest.get_latest_quote(self.ticker)._raw['ap']
        client = tda.auth.client_from_token_file(f'Xavier/tokens/kiran.json',config.TD_CLIENT_ID)
        td_option_name, strike, price, expdate = pd.Series(putdict)[['symbol','strikePrice','mark','expirationDate']].values
        option_name = self.ticker+datetime.datetime.fromtimestamp(expdate/1000).strftime('%y%m%d')+"%08.0f"%(price*1000)
        self.td_option_name = td_option_name
        self.putStrike = strike
        self.optionName = option_name
        for x in accountslist:
            try:
              print(self.td_option_name,price)
              price,numcontracts = x.buy_option(self.td_option_name,price=price,verbose = True)
              self.putBoughtPrice = price
              self.stopLoss = price*self.SLpct
              self.takeProfit = price*self.TPpct
              if verbose:
                  print(option_name,f"price:{price}", f"strike: {strike}", datetime.datetime.now())
            except:
              continue
        if verbose:
            print(f'Stop loss: {self.stopLoss} | Take profit: {self.takeProfit}')
        logs.append(datetime.datetime.now().timestamp(),f'{self.ticker} price: {currentprice} bought put: {option_name} for strike of {strike} and price of {price}. Stop loss: {self.stopLoss} | Take profit: {self.takeProfit}')
        return option_name

    def check_sell_call(self,accountslist:list,logs=[],verbose=False):
        expiration = self.expiration
        client = tda.auth.client_from_token_file(f'Xavier/tokens/kiran.json',config.TD_CLIENT_ID)
        price = client.get_quote(self.td_option_name).json()[self.td_option_name]['mark']
        if price >= self.takeProfit or price<= self.stopLoss:
          for x in accountslist:
              try:
                print(self.td_option_name,price)
                x.sell_option(self.td_option_name,verbose = True,price = price,boughtprice = self.callBoughtPrice)
                self.minutecount=0
                self.soldCallPrice = price
              except:
                continue
          profit = price-self.callBoughtPrice
          percent_profit = profit/self.callBoughtPrice
          if verbose: print(f'Sell {self.optionName} at {datetime.datetime.now()} for a profit of {profit}')
          logs.append(datetime.datetime.now().timestamp(),f'Sell {self.optionName} at {datetime.datetime.now()} for {price}. Profit: {profit} | {percent_profit}%')
          return price
        else: return False

    def check_sell_put(self,accountslist:list,logs=[],verbose=False):
        expiration = self.expiration
        client = tda.auth.client_from_token_file(f'Xavier/tokens/kiran.json',config.TD_CLIENT_ID)
        price = client.get_quote(self.td_option_name).json()[self.td_option_name]['mark']
        if price >= self.takeProfit or price<= self.stopLoss:
          for x in accountslist:
              try:
                print(self.td_option_name,price)
                x.sell_option(self.td_option_name,verbose = True,price = price, boughtprice = self.putBoughtPrice)
                self.minutecount=0
                self.soldPutPrice = price
              except:
                continue
          profit = price-self.putBoughtPrice
          percent_profit = profit/self.putBoughtPrice
          if verbose: print(f'Sell {self.optionName} at {datetime.datetime.now()} for a profit of {profit}')
          logs.append(datetime.datetime.now().timestamp(),f'Sell {self.optionName} at {datetime.datetime.now()} for {price}. Profit: {profit} | {percent_profit}%')
          return price
        else: return False
    
class log:
    log = pd.DataFrame(columns=["Time","Message"])
    def __init__(self):
      self.log = pd.DataFrame(columns=["Time","Message"])
    def append(self,time,message):
      self.log.loc[len(self.log.index)] = [time,message]
    def datetime_format(self):
        datetimeCol = [datetime.datetime.fromtimestamp(x) for x in self.log.Time.values]
        return pd.DataFrame({'Time':datetimeCol,'Message':self.log.Message})
