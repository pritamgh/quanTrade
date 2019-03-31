# -*- coding: utf-8 -*-

"""
Created on Fri Dec 7 16:37:23 2018
@author: Pritam Ghosh

This module contains the beta calculation for individual stocks
"""


import calendar
import datetime
import csv

import numpy as np
import pandas as pd

import  stock_data_pandas


def calculate_days(year, month):
	""" days count """
	days = calendar.monthrange(year, month)
	return days


def calculate_month(year, month):
	""" previous month """
	prev_month = (lambda: (year, month-1), lambda: (year-1, 12))[month == 1]()
	return prev_month


def calculate_date(input_date, input_y):

	""" input_date is the ending date and we have 
	calculate starting date according to input_y """

	end_year = int(input_date[0:4])
	end_month = int(input_date[5:7])
	end_day = int(input_date[8:])

	while input_y:

		days = calculate_days(end_year, end_month)

		prev_month = calculate_month(end_year, end_month)
		# print(prev_month[0], prev_month[1])

		''' from end day to previous month (days - end_day)'''
		one_month = end_day + (days[1] - end_day)
		# print(one_month)

		end_year = prev_month[0]
		end_month = prev_month[1]

		input_y -= 1

	# print(end_year, end_month, end_day)
	start_year = str(end_year)
	start_month = str(end_month)
	start_day = str(end_day)
	start_date = start_year+'-'+start_month+'-'+start_day

	return start_date


def fetch_data(start_date, input_date, mycursor, con_mydb):
	""" fetch datas for stock & snp between start and end date """

	stock_query = ("SELECT * FROM stock WHERE date_stamp BETWEEN %s AND %s")
	values = (start_date, input_date)
	mycursor.execute(stock_query, values)
	stock_record = mycursor.fetchall()
	
	snp_query = ("SELECT * FROM spdata WHERE date_stamp BETWEEN %s AND %s")
	values = (start_date, input_date)
	mycursor.execute(snp_query, values)
	snp_record = mycursor.fetchall()
	return stock_record, snp_record


def calculate_beta(stock_record, snp_record):
	""" storing stock and snp % into individual list """
	stock_percentage_list = []
	snp_percentage_list = []

	for stock in stock_record:
		stock_percentage_list.append(stock[3])

	for snp in snp_record:
		snp_percentage_list.append(snp[2])

	# print(stock_percentage_list, snp_percentage_list)

	covariance = np.cov(stock_percentage_list, snp_percentage_list)[0][1]
	variance = np.var(snp_percentage_list)
	beta_value = covariance/variance
	return beta_value


if __name__ == "__main__":

	print("Taking User Input \n \
		Enter Date, X(to identify black swan), Y(month to calculate beta): \n")
	params = [x for x in input().split()]
	input_date = params[0]
	input_x = params[1]
	input_y = params[2]

	con_mydb = stock_data_pandas.connect_database()
	mycursor = con_mydb.cursor()

	""" filtering the spdata table by input_date """
	query = ("SELECT * FROM spdata WHERE date_stamp = %s")
	values = (input_date,)
	mycursor.execute(query, values)
	record = mycursor.fetchall()

	""" beta calculation """
	snp_percentage = record[0][2]
	print(snp_percentage)

	if snp_percentage > float(input_x) and snp_percentage >= 0:
		print("yesss in")
		start_date = calculate_date(input_date, int(input_y))
		print("Start Date : ", start_date, "\n End Date : ", input_date)

		stock_record, snp_record = fetch_data(start_date, input_date, mycursor, con_mydb)

		print("\n************ Beta ****************\n")
		beta_value = calculate_beta(stock_record, snp_record)
		print(beta_value)

		""" store the beta in csv """
		# with open('timestamp.csv', 'wb') as csvfile:
		#     filewriter = csv.writer(csvfile, delimiter=',',
		#                             quotechar='|', quoting=csv.QUOTE_MINIMAL)
		#     filewriter.writerow(['start-date', 'end-date', 'beta'])

		""" 5% highest beta for will be queued for buy """

		""" 5% lowest beta for will be queued for sell """

	elif snp_percentage > (-float(input_x)) and snp_percentage <= 0:
		print("yesss in elif")
		
		start_date = calculate_date(input_date, int(input_y))
		print("Start Date : ", start_date, "\n End Date : ", input_date)

		stock_record, snp_record = fetch_data(start_date, input_date, mycursor, con_mydb)

		print("\n************ Beta ****************\n")
		beta_value = calculate_beta(stock_record, snp_record)
		print(beta_value)


	else:
		print("No Black Swan identified")
