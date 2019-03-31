# -*- coding: utf-8 -*-

"""
Created on Fri Dec 7 16:37:23 2018
@author: Pritam Ghosh

This module to fetch stocks from http://finance.yahoo.com using pandas dataReader
then save data into Database

"""

from datetime import datetime
import sqlalchemy
from sqlalchemy import create_engine

import pandas as pd
from pandas_datareader import data, wb

import mysql.connector
from mysql.connector import MySQLConnection, Error


def connect_database():
	
	mydb = mysql.connector.connect(
	  host="localhost",
	  user="root",
	  passwd="root",
	  database="tradingstrategy"
	)

	return mydb


def insert_OHLC_data(symbol, records_df, mycursor, con_mydb):
	""" inserting the closing price of a day for a perticular stock """

	try:
		""" iterate over the dataframe to access individual stock """
		for row in records_df.itertuples():
			# print(row)
			query = ("INSERT INTO stock (date_stamp, symbol, close, percentage) VALUES (%s, %s, %s, %s)")

			date = row.Index.date()
			close_price = round(row.Close, 5)
			values = (date, symbol, close_price, 0.0)

			mycursor.execute( query, values )

		con_mydb.commit()

	except Error as e:
	    print('Error:', e)
	    pass

	finally:
		print("Stock for ", symbol, "inserted")
		# mycursor.close()
		# con_mydb.close()


def update_percentage_data(symbol, mycursor, con_mydb):
	""" updating stock table with daily % change """

	try:
		""" fetch stocks by symbol """
		print(symbol)

		query = ("SELECT * FROM stock WHERE symbol = %s")
		values = (symbol,)
		mycursor.execute(query, values)
		rows = mycursor.fetchall()
		print(mycursor.rowcount)

		count = 1
		for row in rows:
		# while row is not None:
			if count is 1:
				yetarday_closing_price = 0
				today_closing_price = row[2] 	
			else:
				yetarday_closing_price = today_closing_price
				today_closing_price = row[2]

				""" percentage calculation """
				daily_per_change = ((today_closing_price - yetarday_closing_price)
				/(yetarday_closing_price * 100))

				print(daily_per_change)

				""" update percentage column """
				query = ("UPDATE stock SET percentage = %s WHERE symbol = %s AND date_stamp = %s")

				values = (daily_per_change, symbol, row[0])

				mycursor_for_update = con_mydb.cursor()
				mycursor_for_update.execute( query, values )
				con_mydb.commit()
				mycursor_for_update.close()

			# row = mycursor.fetchone()
			count += 1

	except Error as e:
	    print(e)

	finally:
	    mycursor.close()
	    con_mydb.close()


def cache_session():
	expire_after = datetime.timedelta(days=3)
	session = requests_cache.CachedSession(cache_name='cache', backend='mysql.connector', 
		expire_after=expire_after)


if __name__ == "__main__":

	symbol = "AKAM"

	# records_df = data.DataReader(symbol,  "yahoo", datetime(2003,12,10), datetime(2018,12,10))
	# print(records_df)

	""" connect to MySQL database """
	con_mydb = connect_database()

	""" create database """
# 	mycursor = con_mydb.cursor()
# 	mycursor.execute("CREATE DATABASE tradingstrategy")

	""" create table """
	# mycursor = con_mydb.cursor()
	# mycursor.execute( 
 # 		"CREATE TABLE stock ( date_stamp Date , symbol Varchar(100) , close Float(65,10) , percentage Float(65,10) )" 
 # 		)

	""" alter table """
	# mycursor = con_mydb.cursor()
	# mycursor.execute("ALTER TABLE stock DROP COLUMN updated")

	""" insert data into database """
# 	mycursor = con_mydb.cursor()
# 	insert_OHLC_data(symbol, records_df, mycursor, con_mydb)

	""" update data into database """
	mycursor = con_mydb.cursor(buffered=True)
	update_percentage_data(symbol, mycursor, con_mydb)
