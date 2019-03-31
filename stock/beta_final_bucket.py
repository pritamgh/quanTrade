# -*- coding: utf-8 -*-

"""
Created on Fri Dec 28 16:37:23 2018
@author: Pritam Ghosh

"""

import math
import itertools
import calendar
import csv
import datetime
from datetime import date, timedelta
from collections import OrderedDict

import numpy as np
import pandas as pd

import stock_data_pandas
import portfolio


def calculate_days(year, month):
    """ days count """
    days = calendar.monthrange(year, month)
    return days


def calculate_month(year, month):
    """ previous month """
    prev_month = (lambda: (year, month - 1),
                  lambda: (year - 1, 12))[month == 1]()
    return prev_month


def calculate_date(end_date, input_y):
    """ input_date is the ending date and we have 
    calculate starting date according to input_y """

    end_year = int(end_date[0:4])
    end_month = int(end_date[5:7])
    end_day = int(end_date[8:])

    if end_year == 2004 and end_month < input_y + 1:
        start_date = '2004-01-02'
    elif end_year == 2004 and end_month == input_y + 1 and end_day == 1:
        start_date = '2004-01-02'
    else:
        while input_y:
            days = calculate_days(end_year, end_month)
            prev_month = calculate_month(end_year, end_month)

            """ from end day to previous month (days - end_day) """
            one_month = end_day + (days[1] - end_day)

            end_year = prev_month[0]
            end_month = prev_month[1]

            input_y -= 1

        start_year = str(end_year)
        start_month = str(end_month)
        start_day = str(end_day)
        start_date = start_year + '-' + start_month + '-' + start_day

    return start_date


def fetch_data(symbol, start_date, input_date, mycursor, con_mydb):
    """ fetch datas for stock & snp between start and end date """

    stock_query = (
        "SELECT * FROM stock WHERE date_stamp BETWEEN %s AND %s AND symbol=%s")
    values = (start_date, input_date, symbol)
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

    covariance = np.cov(stock_percentage_list, snp_percentage_list)[0][1]
    variance = np.var(snp_percentage_list)
    beta_value = covariance / variance
    return beta_value


if __name__ == "__main__":

    print("Taking User Input \n \
		Start-date, End-date, X(to identify black swan), Y(month to calculate beta): \n")
    params = [x for x in input().split()]
    input_start_date = params[0]
    input_end_date = params[1]
    input_x = params[2]
    input_y = params[3]

    con_mydb = stock_data_pandas.connect_database()
    mycursor = con_mydb.cursor()

    """ filtering the spdata table by input date range """
    date_initial = input_start_date
    date_terminate = input_end_date
    query = ("SELECT * FROM spdata WHERE date_stamp BETWEEN %s AND %s")
    values = (date_initial, date_terminate)
    mycursor.execute(query, values)
    rows = mycursor.fetchall()

    """ truncate table portfolio for every new insertion when this script will run """
    query = ("TRUNCATE TABLE temp_portfolio")
    mycursor.execute(query)

    count = 1
    current_date_year = None
    prev_date_year = None
    for row in rows:
        """ loop for date range """
        current_date_year = str(row[0]).split("-")[0]

        """ insert a offset in table with the previous date of the starting date """
        if prev_date_year is None:
            s = date_initial
            s = s.replace('-', '')
            s = datetime.datetime.strptime(s, "%Y%m%d").date()
            static_date = s - timedelta(1)

            """ tickers for year """
            query = ("SELECT `stock_list` FROM temp_SNP_list WHERE year=%s")
            values = (current_date_year,)
            mycursor.execute(query, values)
            tickers = mycursor.fetchall()
            ticker_string = tickers[0][0]
            ticker_list = ticker_string.split(',')

            insert = portfolio.insert(static_date, None, None,
                                      None, None, ticker_string, mycursor, con_mydb)
            last_inserted_id = insert

            prev_date_year = current_date_year

        elif current_date_year != prev_date_year:
            """ if year changed then insert 1 jan static value """

            first_jan = current_date_year + "-01" + "-01"
            static_date = first_jan.replace('-', '')
            static_date = datetime.datetime.strptime(
                static_date, "%Y%m%d").date()

            """ tickers for year """
            query = ("SELECT `stock_list` FROM temp_SNP_list WHERE year=%s")
            values = (current_date_year,)
            mycursor.execute(query, values)
            tickers = mycursor.fetchall()
            ticker_string = tickers[0][0]
            ticker_list = ticker_string.split(',')

            insert = portfolio.insert(static_date, None, None,
                                      None, None, ticker_string, mycursor, con_mydb)
            last_inserted_id = insert

            prev_date_year = current_date_year
            # print("spdate", row[0], "Prev Year:", prev_date_year, "Current Year:", current_date_year, "static date:", static_date)

        """ beta calculation """
        snp_percentage = row[2]
        end_date = row[0]
        start_date = calculate_date(str(end_date), int(input_y))
        print("Start Date : ", start_date, "\n End Date : ", end_date)

        """ list to store beta for stocks """
        beta_list = []
        """ list to store closing price for stocks of this date """
        closing_list = []

        # """ IF condition """
        if snp_percentage > float(input_x) and snp_percentage >= 0:

            if last_inserted_id is not None:
                query = ("SELECT * FROM temp_portfolio WHERE seq_no=%s")
                values = (last_inserted_id,)
                mycursor.execute(query, values)
                prev_rec = mycursor.fetchone()
                prev_bucket_list = prev_rec[6].split(',')
                print(prev_bucket_list)
                ticker_list = list(OrderedDict.fromkeys(prev_bucket_list))
                print(ticker_list)

            for ticker in ticker_list:

                """ closing price of this symbol for this date """
                query = (
                    "SELECT * FROM stock WHERE date_stamp = %s AND symbol = %s")
                values = (end_date, ticker)
                mycursor.execute(query, values)
                rec = mycursor.fetchone()
                closing_list.append(rec[2])

                stock_record, snp_record = fetch_data(
                    ticker, start_date, end_date, mycursor, con_mydb)
                # print(stock_record, snp_record)
                beta_value = calculate_beta(stock_record, snp_record)
                # print(beta_value)
                beta_list.append(beta_value)

            """ Now make a dict where ticker as key and closing,beta as value """
            tickers_beta = dict(zip(ticker_list, zip(closing_list, beta_list)))
            """ sort the dict by beta """
            sorted_tickers_beta = sorted(
                tickers_beta, key=lambda x: tickers_beta[x][1], reverse=True)
            # print(sorted_tickers_beta)

            bar = (len(sorted_tickers_beta) * 10) / 100
            bar = round(bar)

            # list 10% with lowest beta whcih will be queued for buy
            ten_per_lowest_beta = []
            ten_per_lowest_beta = sorted_tickers_beta[-bar:]
            ten_per_lowest_beta_string = ','.join(ten_per_lowest_beta)

            # list 10% with highest beta which will be queued for sell
            ten_per_highest_beta = []
            ten_per_highest_beta = sorted_tickers_beta[:bar]
            ten_per_highest_beta_string = ','.join(ten_per_highest_beta)

            print(ten_per_highest_beta, ten_per_lowest_beta)

            """ Insert data into portfolio table """

            """ filter out the bucket list (buy+ and sell-) """
            current_bucket_list = prev_bucket_list
            d = [current_bucket_list.remove(
                key) for key in ten_per_lowest_beta if key in current_bucket_list]
            print(current_bucket_list)
            a = [current_bucket_list.append(key)
                 for key in ten_per_highest_beta]
            print(current_bucket_list)
            current_bucket_list_string = ','.join(current_bucket_list)

            """ sumation of the closing price of current bucket list """
            current_closing_list = [tickers_beta[key][0]
                                    for key in current_bucket_list if key in tickers_beta]
            current_closing_list_sum = sum(current_closing_list)
            print(current_closing_list_sum)

            """ if last inserted is static then have to update the portfolio with 
				current date closing price sum of previous bucket list """
            if prev_rec[4] is None:
                prev_closing_list = []
                for ticker in prev_bucket_list:
                    query = (
                        "SELECT * FROM stock WHERE date_stamp = %s AND symbol = %s")
                    values = (end_date, ticker)
                    mycursor.execute(query, values)
                    rec = mycursor.fetchone()
                    prev_closing_list.append(rec[2])

                """ update """
                if last_inserted_id is 1:
                    prev_portfolio_value = sum(prev_closing_list)
                    query = (
                        "UPDATE temp_portfolio SET portfolio_value=%s WHERE seq_no=%s")
                    values = (prev_portfolio_value, prev_rec[0])
                    mycursor.execute(query, values)
                    con_mydb.commit()
                else:
                    query = ("SELECT * FROM temp_portfolio WHERE seq_no = %s")
                    seq_n = last_inserted_id - 1
                    values = (seq_n,)
                    mycursor.execute(query, values)
                    r = mycursor.fetchone()
                    prev_prev_portfolio_value = r[4]

                    prev_portfolio_value = sum(prev_closing_list)
                    return_percentage_change = (
                        (prev_portfolio_value - prev_prev_portfolio_value) / prev_prev_portfolio_value) * 100
                    query = (
                        "UPDATE temp_portfolio SET portfolio_value=%s, return_percentage=%s WHERE seq_no=%s")
                    values = (prev_portfolio_value,
                              return_percentage_change, prev_rec[0])
                    mycursor.execute(query, values)
                    con_mydb.commit()
            else:
                prev_portfolio_value = prev_rec[4]

            portfolio_value = current_closing_list_sum
            return_percentage_change = (
                (portfolio_value - prev_portfolio_value) / prev_portfolio_value) * 100

            insert = portfolio.insert(end_date, ten_per_highest_beta_string, ten_per_lowest_beta_string,
                                      portfolio_value, return_percentage_change, current_bucket_list_string, mycursor, con_mydb)

        elif snp_percentage > (-float(input_x)) and snp_percentage <= 0:
            # print("yesss in elif")

            if last_inserted_id is not None:
                query = ("SELECT * FROM temp_portfolio WHERE seq_no=%s")
                values = (last_inserted_id,)
                mycursor.execute(query, values)
                prev_rec = mycursor.fetchone()
                prev_bucket_list = prev_rec[6].split(',')
                print("prev bucket: ", prev_bucket_list)
                ticker_list = list(OrderedDict.fromkeys(prev_bucket_list))
                print("unique bucket: ", ticker_list)

            for ticker in ticker_list:

                """ closing price of this symbol for this date """
                query = (
                    "SELECT * FROM stock WHERE date_stamp = %s AND symbol = %s")
                values = (end_date, ticker)
                mycursor.execute(query, values)
                rec = mycursor.fetchone()
                closing_list.append(rec[2])

                stock_record, snp_record = fetch_data(
                    ticker, start_date, end_date, mycursor, con_mydb)
                # print(stock_record, snp_record)
                beta_value = calculate_beta(stock_record, snp_record)
                # print(beta_value)
                beta_list.append(beta_value)

            """ Now make a dict where ticker as key and closing,beta as value """
            tickers_beta = dict(zip(ticker_list, zip(closing_list, beta_list)))
            """ sort the dict by beta """
            sorted_tickers_beta = sorted(
                tickers_beta, key=lambda x: tickers_beta[x][1], reverse=True)
            # print(sorted_tickers_beta)

            bar = (len(prev_bucket_list) * 10) / 100
            bar = round(bar)

            # list 10% with lowest beta which will be queued for sell
            ten_per_lowest_beta = []
            ten_per_lowest_beta = sorted_tickers_beta[-bar:]
            ten_per_lowest_beta_string = ','.join(ten_per_lowest_beta)

            # list 10% with highest beta which will be queued for buy
            ten_per_highest_beta = []
            ten_per_highest_beta = sorted_tickers_beta[:bar]
            ten_per_highest_beta_string = ','.join(ten_per_highest_beta)

            # print(ten_per_highest_beta, ten_per_lowest_beta)

            """ Insert data into portfolio table """

            """ filter out the bucket list (buy+ and sell-) """
            current_bucket_list = prev_bucket_list
            d = [current_bucket_list.remove(
                key) for key in ten_per_lowest_beta if key in current_bucket_list]
            # print(current_bucket_list)
            a = [current_bucket_list.append(key)
                 for key in ten_per_highest_beta]
            # print(current_bucket_list)
            current_bucket_list_string = ','.join(current_bucket_list)

            """ sumation of the closing price of current bucket list """
            current_closing_list = [tickers_beta[key][0]
                                    for key in current_bucket_list if key in tickers_beta]
            current_closing_list_sum = sum(current_closing_list)
            # print(current_closing_list_sum)

            """ if last inserted is static then have to update the portfolio with 
				current date closing price sum of previous bucket list """
            if prev_rec[4] is None:
                prev_closing_list = []
                for ticker in prev_bucket_list:
                    query = (
                        "SELECT * FROM stock WHERE date_stamp = %s AND symbol = %s")
                    values = (end_date, ticker)
                    mycursor.execute(query, values)
                    rec = mycursor.fetchone()
                    prev_closing_list.append(rec[2])

                """ update """
                """ 1st static insertion """
                if last_inserted_id is 1:
                    prev_portfolio_value = sum(prev_closing_list)
                    query = (
                        "UPDATE temp_portfolio SET portfolio_value=%s WHERE seq_no=%s")
                    values = (prev_portfolio_value, prev_rec[0])
                    mycursor.execute(query, values)
                    con_mydb.commit()
                else:
                    query = ("SELECT * FROM temp_portfolio WHERE seq_no = %s")
                    seq_n = last_inserted_id - 1
                    values = (seq_n,)
                    mycursor.execute(query, values)
                    r = mycursor.fetchone()
                    prev_prev_portfolio_value = r[4]

                    prev_portfolio_value = sum(prev_closing_list)
                    return_percentage_change = (
                        (prev_portfolio_value - prev_prev_portfolio_value) / prev_prev_portfolio_value) * 100
                    query = (
                        "UPDATE temp_portfolio SET portfolio_value=%s, return_percentage=%s WHERE seq_no=%s")
                    values = (prev_portfolio_value,
                              return_percentage_change, prev_rec[0])
                    mycursor.execute(query, values)
                    con_mydb.commit()

            else:
                prev_portfolio_value = prev_rec[4]

            portfolio_value = current_closing_list_sum
            return_percentage_change = (
                (portfolio_value - prev_portfolio_value) / prev_portfolio_value) * 100

            insert = portfolio.insert(end_date, ten_per_highest_beta_string, ten_per_lowest_beta_string,
                                      portfolio_value, return_percentage_change, current_bucket_list_string, mycursor, con_mydb)

        else:
            # print("No Black Swan identified")

            if last_inserted_id is not None:
                query = ("SELECT * FROM temp_portfolio WHERE seq_no=%s")
                values = (last_inserted_id,)
                mycursor.execute(query, values)
                prev_rec = mycursor.fetchone()
                current_bucket_list = prev_rec[6]
                prev_bucket_list = prev_rec[6].split(',')

            current_closing_list = []
            for ticker in prev_bucket_list:
                query = (
                    "SELECT * FROM stock WHERE date_stamp = %s AND symbol = %s")
                values = (end_date, ticker)
                mycursor.execute(query, values)
                rec = mycursor.fetchone()
                current_closing_list.append(rec[2])

            current_closing_list_sum = sum(current_closing_list)

            if prev_rec[4] is None:
                prev_portfolio_value = current_closing_list_sum
                query = (
                    "UPDATE temp_portfolio SET portfolio_value=%s WHERE seq_no=%s")
                values = (current_closing_list_sum, prev_rec[0])
                mycursor.execute(query, values)
                con_mydb.commit()
            else:
                prev_portfolio_value = prev_rec[4]

            """ sumation of the closing price of current bucket list """

            portfolio_value = current_closing_list_sum
            return_percentage_change = (
                (portfolio_value - prev_portfolio_value) / prev_portfolio_value) * 100

            insert = portfolio.insert(end_date, None, None,
                                      portfolio_value, return_percentage_change, current_bucket_list, mycursor, con_mydb)

        count += 1
        last_inserted_id = insert
        print("\n************ portfolio updated ***********\n")
