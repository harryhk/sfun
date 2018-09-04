#!/usr/bin/env python 
import sqlite3 as sql 

#haha
def createStocks(cursor):
    
    cursor.execute('drop table if exists stocks;')
    cursor.execute( 
    '''
    create table stocks (  
        id integer primary key autoincrement,
        ticker text not null unique,
        description text 
    );
    ''')



def createPrices(cursor):

    cursor.execute('drop table if exists stock_price;' )
    cursor.execute( 
    '''
    create table stock_price (  
        stock_id int  not null,
        date     date not null,
        open     real,  
        high     real,
        low      real,
        close    real,
        volume   int, 
        adjclose real,
        foreign key (stock_id) references stocks(id)
    );
    ''')

    cursor.execute(
    '''
    create index stock_index on stock_price ( stock_id, date ) ;
    ''')
    
    

if __name__ == "__main__":
    
    db = sql.connect('stockData.db')
    cursor = db.cursor()

    createStocks(cursor)
    createPrices(cursor)

    db.commit()
    db.close()





