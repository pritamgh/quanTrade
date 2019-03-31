# -*- coding: utf-8 -*-

"""
Created on Fri Dec 22 15:37:23 2018
@author: Pritam Ghosh
@company: Wift Cap solutions pvt. LTD

This module contains Graphical represantation of 
Portfolio (return %) & s&p (return %) according to date

Overview:
-
-
-

"""
import pandas as pd

# import mysql.connector
# from mysql.connector import MySQLConnection, Error

import sqlalchemy
from sqlalchemy import create_engine

import matplotlib.pyplot as plt
# from bokeh.palettes import Spectral11
# from bokeh.plotting import figure, output_file, show


def connect_database():
	
	mydb = mysql.connector.connect(
	  host="localhost",
	  user="root",
	  passwd="root",
	  database="tradingstrategy"
	)
	
	return mydb


if __name__ == "__main__":


	# """ connect to MySQL database """
	con_mydb = connect_database()
	mycursor = con_mydb.cursor()

	""" Graph Ploting """

	query = (" SELECT `sd`.date_stamp, `sd`.percentage, `pf`.return_percentage FROM spdata as `sd` INNER JOIN portfolio as `pf` WHERE `sd`.date_stamp=`pf`.date_stamp ")
	mycursor.execute(query)
	records = mycursor.fetchall()

	date_range = []
	snp_percentage = []
	portfolio_return_percentage = []
	
	for record in records:
		date_range.append(record[0])
		snp_percentage.append(record[1])
		portfolio_return_percentage.append(record[2])

	df=pd.DataFrame({'x': date_range, 'y1': snp_percentage, 'y2': portfolio_return_percentage })

	plt.plot( 'x', 'y1', data=df, marker='', color='blue', linewidth=1, label='S&P daily % cahnge' )
	plt.plot( 'x', 'y2', data=df, marker='', color='orange', linewidth=1, label='portfolio return %' )

	plt.title("Graph Plot")
	plt.xlabel('Date')
	plt.ylabel('S&P daily % change and Portfolio return %')

	plt.legend()
	plt.show()
