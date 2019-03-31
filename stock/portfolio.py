# -*- coding: utf-8 -*-

"""
Created on Fri Dec 21 16:37:23 2018
@author: Pritam Ghosh

This module for portfolio

"""

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

def insert(end_date, ten_per_highest_beta, ten_per_lowest_beta, 
				portfolio_value, return_percentage_change, current_bucket_list, mycursor, con_mydb):

	query = ("INSERT INTO temp_portfolio (date_stamp,buy_list,sell_list,portfolio_value,return_percentage,bucket) VALUES (%s,%s,%s,%s,%s,%s)")
	values = (end_date, ten_per_highest_beta, ten_per_lowest_beta,
				portfolio_value, return_percentage_change, current_bucket_list)
	mycursor.execute( query, values )
	last_row_id = mycursor.lastrowid
	print(last_row_id)
	con_mydb.commit()
	return last_row_id

