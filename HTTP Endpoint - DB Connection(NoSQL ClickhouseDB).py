#!/usr/bin/env python
# coding: utf-8

# In[1]:


from clickhouse_driver import Client
from datetime import datetime 
from flatten_json import flatten
import re 
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
import json 

client = Client(host='Foobar', user = "Foobar", password = "Foobar", database = "Foobar")

false = False


# In[ ]:


class S(BaseHTTPRequestHandler):
    
    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        logging.info("GET request,\nPath: %s\nHeaders:\n%s\n", str(self.path), str(self.headers))
        self._set_response()
        self.wfile.write("GET request for {}".format(self.path).encode('utf-8'))

    def do_POST(self):
        
        content_length = int(self.headers['Content-Length']) 
        post_data = self.rfile.read(content_length)
        s = json.dumps(post_data.decode())
        parsed = json.loads(s)
        parsed_dict = eval(post_data)
        flattened = flatten(parsed_dict)
        
        
        bids_not_stripped = re.findall('\"price\":.+',parsed)
        
        bids = [x.strip('"price":,') for x in bids_not_stripped]
        


        bid_ids_not_stripped = re.findall('\"id\": \"\S+\"',parsed)
        
        
        bidders = [x.strip('"id":"') for x in bid_ids_not_stripped[2:]]
        

        auction_id = str(flattened['Auction_id'])
        
        publishers = str(flattened['Auction_publisher'])
        
        page = str(flattened['Auction_site_page'])
        
        impression_id = str(flattened['Auction_impressions_0_id'])
        
        height = str(flattened['Auction_impressions_0_acceptedSizes_0_h'])
        
        width = str(flattened['Auction_impressions_0_acceptedSizes_0_w'])
             
        
        country = str(flattened['Auction_device_geo_country'])
        
    
        timestamp = flattened['Statistics_lastAuctionEndTimestamp']
        


        query_1 = "INSERT INTO Gonenc(auction_id,publisher,page,impid,h,w,bidders,bids,country,time) VALUES "
        results = str((auction_id,
                       publishers,page,
                       impression_id,height,
                       width,bidders,bids,
                       country,timestamp))

        query = query_1 + results 
        
        print(query)
        
        client.execute(query)
        
        self._set_response()
        self.wfile.write("POST request for {}".format(self.path).encode('utf-8'))
        
        
def run(server_class=HTTPServer, handler_class=S, port=8085):
    logging.basicConfig(level=logging.INFO)
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()

if __name__ == '__main__':
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
        
        


# In[ ]:





# In[ ]:





# In[ ]:




