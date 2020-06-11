#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""
FLASK ENDPOINT FOR POST REQUEST HANDLING
"""
from flask import Flask, request, jsonify
from clickhouse_driver import Client
from datetime import datetime 
import re

client = Client(host='34.70.65.12', user = "", password = "", database = "")

app = Flask(__name__)
@app.route('/', methods = ['POST'])
def json_example():
    content = request.get_json()
 
    slot_id = str(content['slotId'])
    site = str(content['site'])
    auction_id = str(content['auctionId'])
    winning_bidder =  str(content['wbid']['bidder'])
    winning_bid =  str(content['wbid']['cpm'])
    width = str(content['wbid']['width'])
    height = str(content['wbid']['height'])
    
    bidders_list = []
    json_length = len(content['bids'])
    bidder_pre = content['bids']
    for i in range (json_length):
        bidders_list.append(str(bidder_pre[i]['bidder']))
    
    bids_list = []
    for i in range ( json_length):
        bids_list.append(str(bidder_pre[i]['cpm']))

    timestamp = datetime.now()
    
    client.execute('INSERT INTO Gonenc(slot_id,site,auction_id,bidders,bids,width,height,winning_bidder,winning_bid,time) VALUES',[(slot_id,site,auction_id,bidders_list,bids_list,width,height,winning_bidder,winning_bid,timestamp)],types_check = True)
    return 'yarak'
app.run(host='0.0.0.0', port= 8090)

