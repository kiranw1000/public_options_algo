import logging, time, config, datetime, accounts, json, watchlist
from x_package import *
from alpaca_trade_api.stream import Stream
from alpaca_trade_api.common import URL
from alpaca_trade_api import REST

def run_connection(conn):
    try:
        conn.run()
    except KeyboardInterrupt:
        print("Interrupted execution by user")
        loop.run_until_complete(conn.stop_ws())
        exit(0)
    except Exception as e:
        print(f'Exception from websocket connection: {e}')
    finally:
        print("Trying to re-establish connection")
        time.sleep(5)
        run_connection(conn)

async def print_quote(q):
    print('quote', q)

removeList = ['FB']

options_dict = {}

logs = log()

tickDict = {}
ticklist = tickers.ticklist
ticklist = set(ticklist)
for x in ticklist:
    y = stock(x,allocation=10,TPpct=.35,SLpct=.15)
    print(x)
    try:
        y.getPastTwoDaysCandles()
        if x not in removeList:
            if y.checkPastTwoDays(logs=logs):
                print(f'{x} is in out')
                tickDict[x] = y
                tickDict[x].reload_checklist()
                if datetime.datetime.utcnow().time() >datetime.time(13,30):
                    tickDict[x].checkOpen(logs)
                print(tickDict[x].checklist)
            else:
                print(f"{x} is not in out")
    except:
        continue

datetime_format = logs.datetime_format()
log_file = datetime.datetime.now().strftime(f'%m-%d-%Y')+'_logs.csv'
important_file = datetime.datetime.now().strftime(f'%m-%d-%Y')+'_important_logs.csv'
important_logs = datetime_format.where(datetime_format.Message.str.contains("for")).dropna()
datetime_format.to_csv(f'Xavier/Logs/{log_file}')
important_logs.to_csv(f'Xavier/Logs/{important_file}')

dtf = logs.datetime_format()
dtf.where(dtf.Message.str.contains("Limit")).dropna()

streamlist = [x.stream for x in list(tickDict.values())]

accounts_list = [paper_trade_account('Test',10000.0,.8),trade_account('kiran',accounts.accounts['kiran'],1)]

last_save = 0

client = tda.auth.client_from_token_file(f'Xavier/tokens/kiran.json',config.TD_CLIENT_ID)
watchlist.make_watchlist(client=client,tickers=list(tickDict.keys()))

last_save = 0
def on_open(ws):
    print("opened")
    auth_data = {
        "action": "auth",
        "key": config.APCA_KEY_ID, 
        "secret": config.APCA_SECRET_KEY
    }

    ws.send(json.dumps(auth_data))

#This is the line that tells what tickers to listen to 
    listen_message = {"action": "subscribe", "bars":[x.ticker for x in tickDict.values()]}

    ws.send(json.dumps(listen_message))
    
def on_message(ws, message):
    print('received')
    global last_save
    decoder = json.decoder.JSONDecoder()
    jsonmessage = (decoder.decode(message))[0]
    try:
        tick = jsonmessage['S']
        open = jsonmessage['o']
        high = jsonmessage['h']
        low = jsonmessage['l']
        close = jsonmessage['c']
        volume = jsonmessage['v']
        startts,endts = datetime.datetime.now().timestamp(), (datetime.datetime.now()-datetime.timedelta(minutes=1)).timestamp()
        candle = pd.DataFrame(data = [[tick,open,high,low,close,volume,startts,endts,datetime.datetime.fromtimestamp(startts/1000),datetime.datetime.fromtimestamp(endts/1000)]],columns= ["Symbol","Open","High","Low","Close","Volume","Start time","End time","Start datetime","End datetime"])
        print('made candle')
        tickDict[tick].addCandle(candle)
        print('added candle')
        if tickDict[tick].callBoughtPrice!=0 and tickDict[tick].soldCallPrice==0 and datetime.datetime.utcnow().time()<datetime.time(19,55) and datetime.datetime.utcnow().time() >datetime.time(13,29):
          tickDict[tick].minutecount+=1
          tickDict[tick].updateStopLoss(cp='call',logs=logs,verbose=True)
          now = datetime.datetime.now()
          if now.hour>18 and now.minute>57:
              tickDict[tick].takeProfit = 0
          tickDict[tick].soldCallPrice = tickDict[tick].check_sell_call(accountslist = accounts_list,logs=logs, verbose=True)
          print(f'{tick} check sell call: {tickDict[tick].soldCallPrice}')
        if tickDict[tick].putBoughtPrice!=0 and tickDict[tick].soldPutPrice==0 and datetime.datetime.utcnow().time()<datetime.time(19,55) and datetime.datetime.utcnow().time() >datetime.time(13,29):
          tickDict[tick].minutecount+=1
          tickDict[tick].updateStopLoss(cp='put',logs=logs,verbose=True)
          now = datetime.datetime.now()
          if now.hour>18 and now.minute>57:
              tickDict[tick].takeProfit = 0
          tickDict[tick].soldPutPrice = tickDict[tick].check_sell_put(accountslist = accounts_list,logs=logs, verbose=True)
          print(f'{tick} check sell put: {tickDict[tick].soldPutPrice}')
        else:
            logs.append(datetime.datetime.now().timestamp(),f"No change to {tick}")
        if datetime.datetime.utcnow().time() >datetime.time(13,29):
            if tickDict[tick].updateChecklist(ropen=open,rhigh=high,rlow=low,rclose=close,logs=logs):
                print('entered loop')
                logs.append(datetime.datetime.now().timestamp(),[tick,tickDict[tick].checklist])
                if tickDict[tick].get_oacot(logs=logs) and tickDict[tick].get_oib(logs=logs) and tickDict[tick].callBoughtPrice==0 and tickDict[tick].soldCallPrice==0 and datetime.datetime.utcnow().time() >datetime.time(13,29):
                    print('buy call')
                    tickDict[tick].optionName = tickDict[tick].buy_call(logs=logs,accountslist = accounts_list,verbose = True)
                    options_dict[tickDict[tick].optionName] = [datetime.datetime.utcnow()-datetime.timedelta(hours=4),tickDict[tick].callBoughtPrice]
                    print(tickDict[tick].optionName)
                if tickDict[tick].get_oacub(logs=logs) and tickDict[tick].get_oib(logs=logs) and tickDict[tick].putBoughtPrice==0 and tickDict[tick].soldPutPrice==0 and datetime.datetime.utcnow().time() >datetime.time(13,29):
                    print('buy put')
                    tickDict[tick].optionName = tickDict[tick].buy_put(logs=logs,accountslist = accounts_list,verbose = True)
                    options_dict[tickDict[tick].optionName] = [datetime.datetime.utcnow()-datetime.timedelta(hours=4),tickDict[tick].putBoughtPrice]
                    print(tickDict[tick].optionName)
        if datetime.datetime.now().timestamp()-last_save>=300000:
            datetime_format = logs.datetime_format()
            log_file = datetime.datetime.now().strftime(f'%m-%d-%Y')+'_logs.csv'
            important_file = datetime.datetime.now().strftime(f'%m-%d-%Y')+'_important_logs.csv'
            important_logs = datetime_format.where(datetime_format.Message.str.contains("for")).dropna()
            datetime_format.to_csv(f'Xavier/Logs/{log_file}')
            important_logs.to_csv(f'Xavier/Logs/{important_file}')
            for x in accounts_list:
                df = pd.read_csv(f'Xavier/{x.name}.csv')
                df[datetime.datetime.now().__str__()] = x.get_account_value()
                df.to_csv(f'Xavier/{x.name}.csv')
            last_save = 0
        if datetime.datetime.utcnow().time() >datetime.time(19,55):
            for x in tickDict.values():
                if x.callBoughtPrice > 0 and x.soldCallPrice == 0:
                    client = tda.auth.client_from_token_file(f'Xavier/tokens/kiran.json',config.TD_CLIENT_ID)
                    price = client.get_quote(x.td_option_name).json()[x.td_option_name]['mark']
                    profit = price-x.callBoughtPrice
                    percent_profit = profit/x.callBoughtPrice
                    logs.append(datetime.datetime.now().timestamp(),f'Sell {x.optionName} at {datetime.datetime.now()} for {price}. Profit: {profit} | {percent_profit}%')
                    x.soldCallPrice = price
                    for y in accounts_list:
                        y.sell_option(x.td_option_name,verbose=True, price=price, boughtprice=x.callBoughtPrice)
                if x.putBoughtPrice > 0 and x.soldPutPrice == 0:
                    client = tda.auth.client_from_token_file(f'Xavier/tokens/kiran.json',config.TD_CLIENT_ID)
                    price = client.get_quote(x.td_option_name).json()[x.td_option_name]['mark']
                    profit = price-x.putBoughtPrice
                    percent_profit = profit/x.putBoughtPrice
                    logs.append(datetime.datetime.now().timestamp(),f'Sell {x.optionName} at {datetime.datetime.now()} for {price}. Profit: {profit} | {percent_profit}%')
                    x.soldPutPrice = price
                    for y in accounts_list:
                        y.sell_option(x.td_option_name,verbose=True,price=price, boughtprice=x.putBoughtPrice)
    except Exception as e:
        a,b,c = sys.exc_info()
        print(e,message,a,b)


def on_close(ws, close_status_code, close_msg):
    print("on_close args:")
    if close_status_code or close_msg:
        print("close status code: " + str(close_status_code))
        print("close message: " + str(close_msg))

def on_error(ws, error):
    print('error:'+str(error))

ws = websocket.WebSocketApp("wss://stream.data.alpaca.markets/v2/sip", on_open=on_open, on_message=on_message, on_close=on_close,on_error=on_error)

def run_ws():
    try:
        ws.run_forever()
    except BrokenPipeError:
        run_ws()

run_ws()
