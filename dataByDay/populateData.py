#!/usr/bin/env python 

# populate tables, tables are inserted only. No delete.

import sqlite3 as sql 
import sys, urllib, urllib2, datetime, socket, time, csv

def insert_stocks(cursor):
    print "input tickers"

    while 1:
        line = sys.stdin.readline();
        if line == '':
            # eof reached 
            break

        elif  line.isspace():
            continue

        else:
            ticker = line.strip()
            try:
                cursor.execute('insert into stocks (ticker) values (?)', (ticker,) )

            except sql.IntegrityError as e :
                print >> sys.stderr, "%-15s %s" % ( ticker, e.message )
                continue 
             


def _request(url, symbol):
    # try 3 times , if fail return none 
    html = None
    for  attemp in range(3):
        try:
            html =  urllib2.urlopen( url, timeout= 2 )
            break
        
        except  (urllib2.URLError, urllib2.HTTPError ), e:
            print >> sys.stderr, "%s trial %d " % (symbol,  attemp)

        except socket.timeout:
            print >> sys.stderr, "%s trial %d " % (symbol,  attemp) 
            time.sleep(10)

    if attemp == 3:
        return None 
    else:
        return html


def update_single_stock(cursor, stock_id , ticker):

    count = cursor.execute('select count(stock_id) from stock_price where stock_id = ?', ( stock_id, ) ).next()[0] 
    no_price =  False if count >0 else  True

    if no_price:
        params = urllib.urlencode({
            's': ticker,
            'ignore': '.csv',
        })
        
    else:
        
        exdate = cursor.execute('''
        select max(date(date)) from stock_price 
        where stock_id = ?;''', ( stock_id,) ).next()[0] 

        exdate = datetime.datetime.strptime(  exdate, '%Y-%m-%d' )
        
        curdate = datetime.datetime.today()

        params = urllib.urlencode({
            's': symbol,
            'a': exdate.month - 1,
            'b': exdate.day,
            'c': exdate.year,
            'd': curdate.month - 1,
            'e': curdate.day,
            'f': curdate.year,
            'g': 'd',
            'ignore': '.csv',
        })

    base_url = "http://ichart.finance.yahoo.com/table.csv?%s" % params

    cvsfile= _request(base_url, ticker  )

    if cvsfile == None:
        print >> sys.stderr, "Add %s fail " % ticker
        return 
    
    all_data = [ i for i in  csv.reader( cvsfile ) ]

    for data in all_data[:0:-1]:
        # all_data[0] ignored as it is table header 
        # reverse insert so to insert oldest data first 

        cursor.execute('''
        insert into stock_price (stock_id, date, open, high, low, close, volume, adjclose) values (?,?,?,?,?,?,?,?);
        ''' , ( stock_id, data[0], data[1], data[2], data[3], data[4], data[5], data[6]  ) )
        



def insert_price( cursor):
    stocks = [ (i[0], str(i[1]) ) for i in cursor.execute('''
    select id, ticker from stocks;
    ''')] 
    
    for stock_id, ticker in stocks:
        update_single_stock(cursor, stock_id, ticker )  
        print "updated " + ticker 


if __name__ == "__main__":
    
    if len(sys.argv) < 2 :
        print >> sys.stderr, "Usage! prog.py tablename : choose from stocks|stock_price"
        sys.exit(1)

    db = sql.connect('stockData.db')
    cursor = db.cursor()
    cursor.execute('pragma foreign_keys = on')

    if sys.argv[1] == "stocks":
        insert_stocks(cursor)

    elif sys.argv[1] == "stock_price":
        insert_price(cursor)

    else:
        print >> sys.stderr, "unknown table name"

    db.commit()
    db.close()
