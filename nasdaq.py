# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 13:40:07 2019

@author: santh
"""


import struct

from datetime import timedelta
from time import time
import pickle
import xlsxwriter
from itertools import groupby
from statistics import mean
import sys
import os


if len(sys.argv)!=2:
    print("missing input or output path----using worrking dir path")
    dataset_path,output_path=os.getcwd(),os.getcwd()
    
else:
    dataset_path = sys.argv[1]
    output_path=sys.argv[2]


tick_count = 0
executed_order_count = 0
trade_message_count = 0
cross_trade_message_count = 0
object_list = {}
stock_map = {}
executing_order_map = {}
tick_count = 0

stk_list = {}
stock_map = {}
exe_orders = {}
messages,message_fields = {},[]
message_count = 0
trade_message_count=0





def add_order_message(message,msg_type):
    global stk_list
    if msg_type=='A':
        result=struct.unpack('>HH6sQsI8sI',message)
    if msg_type=='F':
        result=struct.unpack('>HH6sQsI8sI4s',message)
    #print(result)
    if result[4]== 'B':#if buy order
        order_ref_no = result[3]
        stock_name = result[6].strip()
        stock_price = result[7] / 10000.00 
        #adding order to dictionary for tracking stock name in executed orders
        stk_list[order_ref_no] = (stock_name, stock_price)
    return

def broken_trade_message(message):
    global stock_map
    global stk_list
    global exe_orders
    result=struct.unpack('>HH6sQ',message)
    match_number = result[3]
    try:
        (msg_type, order_ref_no, stock_name) = exe_orders.pop(match_number)
        if stock_name in stock_map:
            stock_list = stock_map[stock_name]
            for index, item in enumerate(stock_list):
                if item[1] == order_ref_no and msg_type == item[0]:
                    del stock_list[index]
                    break
            stock_map[stock_name] = stock_list
    except KeyError as e:
        return	

def cross_trade_message(message):
    global cross_trade_message_count
    global stock_map
    global stk_list
    global exe_orders

    msg_type = 'Q'
    result= struct.unpack('>HH6sQ8sIQs',message)
    stock_price=result[5]/10000.00
    timestamp=result[2]
    t = int.from_bytes(timestamp, byteorder='big')
    x='{0}'.format(timedelta(seconds=t * 1e-9))
    hr=int(x.split(':')[0])
     
    share_volume = result[3]
    match_number = result[6]
    stock_name = result[4].strip()
    
    
    if share_volume == 0:
        return	
    elif stock_name not in stock_map:
        stock_map[stock_name] = [(msg_type,hr, match_number, stock_price, share_volume)]
    else:
        stock_list = stock_map[stock_name]		
        stock_list.append((msg_type,hr, match_number, stock_price, share_volume))
        stock_map[stock_name] = stock_list
    #add order to executed order map
    exe_orders[match_number] = (msg_type,hr, match_number, stock_name)
    cross_trade_message_count+=1
    
def delete_order_message(message):
    global stk_list
    result=struct.unpack('>HH6sQ',message)
    #print(result)
    order_ref_no = result[3]
    try:
        stk_list.pop(order_ref_no)
    except KeyError as e:
        return	
    
def replace_order_message(message):
    global stk_list
    result=struct.unpack('>HH6sQQII',message)
    old_order_ref_number = result[3]
    new_order_ref_number = result[4]
    try:
        (stock_name, stock_price) = stk_list.pop(old_order_ref_number)
        stk_list[new_order_ref_number] = (stock_name, stock_price)
    except KeyError as e:
        return
    return

def trade_message(message):
    global trade_message_count
    global stock_map
    global stk_list
    global exe_orders
    msg_type = 'P'
    trade_message_count+=1
    result= struct.unpack('>HH6sQsI8sIQ',message)
    stock_price=result[7]/10000.00
    timestamp=result[2]
    #print(timestamp)
    t = int.from_bytes(timestamp, byteorder='big')
    x='{0}'.format(timedelta(seconds=t * 1e-9))
    hr=int(x.split(':')[0])
    
     
    share_volume = result[5]
    match_number = result[8]
    stock_name = result[6].strip()
    #print(stock_name)
    if stock_name not in stock_map:
        stock_map[stock_name] = [(msg_type,hr, match_number, stock_price, share_volume)]
    else:
        stock_list = stock_map[stock_name]		
        stock_list.append((msg_type,hr, match_number, stock_price, share_volume))
        stock_map[stock_name] = stock_list
	#add order to executed order map
    exe_orders[match_number] = (msg_type,hr, match_number, stock_name)
    
def executed_price_order_message(message):
    global executed_order_count
    global stock_map
    global stk_list
    global exe_orders
    msg_type = 'C'
    result=struct.unpack('>HH6sQIQsI',message)
    if result[6] == 'Y':
        order_ref_no = result[3]
        stock_price = (result[7]) / 10000.00
        share_volume = result[4]
        match_number = result[5]
        timestamp = result[2]
        t = int.from_bytes(timestamp, byteorder='big')
        x='{0}'.format(timedelta(seconds=t * 1e-9))
        hr=int(x.split(':')[0])
        try:
            (stock_name, stock_price_old) = stk_list[order_ref_no]
            if stock_name not in stock_map:
                stock_map[stock_name] = [(msg_type,hr, order_ref_no, stock_price, share_volume)]
            else:
                stock_list = stock_map[stock_name]
                stock_list.append((msg_type,hr,order_ref_no, stock_price, share_volume))
                stock_map[stock_name] = stock_list
            exe_orders[match_number] = (msg_type,hr, order_ref_no, stock_name)
            executed_order_count+=1
        except KeyError as e:
            return

def executed_order_message(message):
    global executed_order_count
    global stock_map
    global stk_list
    global exe_orders
    msg_type = 'E'
    result=struct.unpack('>HH6sQIQ',message)
    order_ref_no = result[3]
    stock_price = 0
    share_volume = result[4]
    match_number = result[5]
    timestamp = result[2]
    t = int.from_bytes(timestamp, byteorder='big')
    x='{0}'.format(timedelta(seconds=t * 1e-9))
    hr=int(x.split(':')[0])
    
    try:
        (stock_name, stock_price) = stk_list[order_ref_no]
        if stock_name not in stock_map:
            stock_map[stock_name] = [(msg_type,hr, order_ref_no, stock_price, share_volume)]
        else:
            stock_list = stock_map[stock_name]
            stock_list.append((msg_type,hr, order_ref_no, stock_price, share_volume))
            stock_map[stock_name] = stock_list
            #add order to executed order map
        exe_orders[match_number] = (msg_type,hr, order_ref_no, stock_name)
        executed_order_count+=1
    except KeyError as e:
        return	


def split_message(message, msg_type):
    if msg_type == 'P':
        
        trade_message(message)
    elif msg_type == 'C':		
        executed_price_order_message(message)
    elif msg_type == 'E':		
        executed_order_message(message)
    elif msg_type == 'A' or msg_type == 'F':		
        add_order_message(message,msg_type) 
    elif msg_type == 'D':		
        delete_order_message(message)
    elif msg_type == 'Q':
        cross_trade_message(message)
    elif msg_type == 'B':		
        broken_trade_message(message)
    elif msg_type == 'U':		
        replace_order_message(message)
    else:
        return
    

start = time()

f = open(dataset_path+"/01302019.NASDAQ_ITCH50",'rb');#path to unzipped bindata
for _ in range(20000000):#restricting ,because of null char as EOF is failing
    message_size = int.from_bytes(f.read(2), byteorder='big', signed=False)
    if not message_size:
        break
    tick_count+= 1	
    message_type = f.read(1).decode('ascii')
    record = f.read(message_size - 1)
    print(message_type)
    print(tick_count)
    if message_type=='S':
        #record = f.read(message_size - 1)
        #print(len(record))
        result=struct.unpack('>HH6ss',record)
        print(result[3].decode())
        if result[3]=='M':
            break
        
    


    # read & store message
    
    split_message(record, message_type)
    




pickle.dump(stock_map, open("stock_dictionary.d", "wb"))
print(timedelta(seconds=time() - start))
del exe_orders
del stk_list

#print(stock_map)
s1={}
import operator
for k,v in stock_map.items():
    x=v
    myfunc = lambda tu : [(k, sum(v2[4] for v2 in v)) for k, v in groupby(tu, lambda x: x[1])]
    myfunc1 = lambda tu : [(k, mean(v2[3] for v2 in v)) for k, v in groupby(tu, lambda x: x[1])]
    p=myfunc(x)
    q=myfunc1(x)
    id = operator.itemgetter(0)

    idinfo = {id(rec): rec[1:] for rec in q}  # Dict for fast look-ups.

    merged = [info + idinfo[id(info)] for info in p if id(info) in idinfo]
    s1[k]=merged

workbook = xlsxwriter.Workbook(output_path+"/result.xlsx")
for key, value in s1.items():
    #workbook = xlsxwriter.Workbook(os.path.join(os.path.dirname(os.path.abspath('__file__')),key.decode() + ".xlsx"))
    
    #print(workbook)
    sheet = workbook.add_worksheet(key.decode())
    cumulative_volume = 0
    cumulative_volume_price = 0
    sheet.write('A1', "Hour")
    sheet.write('B1', "Price")
    sheet.write('C1', "Volume")
    sheet.write('D1', "Cumulative Volume")
    sheet.write('E1', "Cumulative Volume * Price")
    sheet.write('F1', "VWAP")
    sheet.write('G1',"Stock_code")
    for index, item in enumerate(value):
        
        sheet.write("A"+str(index+2), item[0])
        sheet.write("B"+str(index+2), item[2])
        sheet.write("C"+str(index+2), item[1])
        cumulative_volume+=item[1]
        #print(cumulative_volume)
        cumulative_volume_price+= item[1] * item[2]
        sheet.write("D"+str(index+2), cumulative_volume)
        sheet.write("E"+str(index+2), cumulative_volume_price)
        sheet.write("F"+str(index+2), cumulative_volume_price / (cumulative_volume * 1.00))
        sheet.write("G"+str(index+2), key.decode())
workbook.close()