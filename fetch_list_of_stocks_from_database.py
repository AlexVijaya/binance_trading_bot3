import sqlite3
import pprint
def fetch_list_of_stocks_from_database(database_name='tickers_companies_prices.db'):
    connection=sqlite3.connect(database_name)
    connection.row_factory=sqlite3.Row
    cur=connection.cursor()
    cur.execute('''select symbol, name from stocks;''')
    rows=cur.fetchall()
    list_of_tickers=[row['symbol'] for row in rows]
    connection.close()
    #pprint.pprint(list_of_tickers)
    return list_of_tickers

fetch_list_of_stocks_from_database()