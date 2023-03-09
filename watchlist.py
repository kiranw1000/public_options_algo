import tda, config, accounts,datetime
client = tda.auth.client_from_token_file('Xavier/tokens/kiran.json',api_key=config.TD_CLIENT_ID)
def make_watchlist(client,tickers):
    items = []
    for x in tickers:
        items += [{"instrument":{"symbol":x,"assetType":"equity"}}]
    spec = {"name":datetime.datetime.today().strftime('%m-%d-%Y'),"watchlistItems":items}
    client.create_watchlist(accounts.accounts['kiran'],watchlist_spec=spec).json()

