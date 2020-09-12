#!/usr/bin/env python
# coding: utf-8

# In[1]:


import v3io.dataplane
import json
import queue
import threading


# In[2]:


class igz_stream_converge():
    
    def __init__(self):
        class_config = json.loads(os.getenv('CLASS_CONFIG'))
        self.container = class_config['container']
        self.table_path = class_config['table_path']
        self.v3io_client = v3io.dataplane.Client(max_connections=1)
        self._tbl_init()
        self.call_counter=self._counter_init()
        
        self.add_queue = queue.Queue()
        self.add_thread_pool = self._create_add_thread_pool()
        
        self.del_queue = queue.Queue()
        self.del_thread_pool = self._create_del_thread_pool()
        
        return
    
    def _counter_init(self):
        items_cursor = self.v3io_client.kv.new_cursor(container=self.container,
                                         table_path=self.table_path)
        
        #print(items_cursor.all())
        items = items_cursor.all()
        _counter_load={}
        for item in items:
            if 'PartitionKey' in item:
                _counter_load[item['PartitionKey']]  = item['count']

        return _counter_load
    
    
    def _tbl_init(self):
        self.v3io_client.create_schema(container=self.container,
                              path=os.path.join(self.table_path),
                              key='PartitionKey',
                              fields=[
                                  {'name': 'PartitionKey', 'type': 'string', 'nullable': False},
                                  {'name': 'count', 'type': 'long', 'nullable': False}
                          ])
        
    def _create_add_thread_pool(self):
        thread_pool = []
        for thread_idx in range(5):
            thread = threading.Thread(target=self._thread_entry_add, args=())
            thread.start()
            thread_pool.append(thread)
       
    def _create_del_thread_pool(self):
        thread_pool = []
        for thread_idx in range(5):
            thread = threading.Thread(target=self._thread_entry_del, args=())
            thread.start()
            thread_pool.append(thread)
        
    def _thread_entry_add(self):
        # Add entry to KV tables
        while True:
            event = self.add_queue.get()
            try:
                self._put_item(event)
            except:
                print("FAILED to write to KV")
    
    def _thread_entry_del(self):
        # Add entry to KV tables
        while True:
            event = self.del_queue.get()
            #try:
            if True:
                self._delete_item(event)
            #except:
            #    print("FAILED to delete from KV")
                
    def _put_item(self,event):
        self.v3io_client.kv.put(container=self.container,
                         table_path=self.table_path,
                         key = event['PartitionKey'],
                         attributes={
                             'PartitionKey': event['PartitionKey'],
                             'count': event['count'],
                         }) 
    def _delete_item(self,event):
        self.v3io_client.kv.delete(container=self.container,
                         table_path=self.table_path,
                         key=event['PartitionKey'])
    
    def merge_rule_partition_key(self,context,message):
        PartitionKey = message['PartitionKey']
        if PartitionKey in self.call_counter:
            self.call_counter[PartitionKey] += 1
        else:
            self.call_counter[PartitionKey] = 1
        #print("MESSAGE COUNT",self.call_counter,context.worker_id,message['shard'])
        
        if self.call_counter[PartitionKey] == 2:
            url = "http://v3io-webapi:8081/%s/%s.csv"% (os.getenv('BATCH_RESULTS_FOLDER'),os.getenv('STEP_NAME'))
            headers = {
                    "Content-Type": "application-octet-stream",
                    "X-v3io-session-key": os.getenv('V3IO_ACCESS_KEY'),
                    "Range": "-1"
                  }
            payload = "%s\n"%(PartitionKey)
            try:
                response = requests.put(url, data=payload, headers=headers)
            except:
                print("RESP",payload)
                print("RESP",headers)
                print("RESP",url)
            self.del_queue.put({'PartitionKey' : PartitionKey, 'count' : self.call_counter[PartitionKey] })
            self.call_counter.pop(PartitionKey)
        else:
            self.add_queue.put({'PartitionKey' : PartitionKey, 'count' : self.call_counter[PartitionKey] })
        return message
    
    def processing(self,context,message):
        return_message = self.merge_rule_partition_key(context,message)
        return return_message
    
    


# In[5]:


"""
import os
import ast
os.environ['CLASS_CONFIG'] = '{"container" : "bigdata", "table_path" : "stream_processing/stream_converge"}'

new = igz_stream_converge()

context = ''
new.merge_rule_partition_key(context,{'PartitionKey':'rwgerettthr', 'count':1})
"""


# In[ ]:




