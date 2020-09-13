#!/usr/bin/env python
# coding: utf~8

# In[1]:


import v3io.dataplane
import json
import queue
import threading
import os
import base64


# In[2]:


class igz_stream_converge():
    
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self,key,value)
        self.v3io_client = v3io.dataplane.Client(max_connections=1)
        self._tbl_init()
        self.call_counter=self._counter_init()
        self.messages_expected = 3
        
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
                _PartitionKey, _ = item['PartitionKey'].split('~')
                if _PartitionKey not in _counter_load:
                    _counter_load[_PartitionKey]  = 1
                else:
                    _counter_load[_PartitionKey]  += 1

        return _counter_load
    
    
    def _tbl_init(self):
        self.v3io_client.create_schema(container=self.container,
                              path=os.path.join(self.table_path),
                              key='PartitionKey',
                              fields=[
                                  {'name': 'PartitionKey', 'type': 'string', 'nullable': False},
                                  {'name': 'count', 'type': 'long', 'nullable': False},
                                  {'name' : 'messages', 'type' : 'blob', 'nullable' : True }
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
            #try:
            if True:
                print(self._put_item(event))
            #except:
            #    print("FAILED to write to KV")
    
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
        print("MESSAGE ",event['message'])
        _key = event['PartitionKey'] + "~" + str(event['count'])
        self.v3io_client.kv.put(container=self.container,
                         table_path=self.table_path,
                         key = _key,
                         attributes={
                             'PartitionKey': _key,
                             'count': event['count'],
                             'message' : str(event['message'])
                         })
        
    def _delete_item(self,event):
        _msg_count = 1
        while _msg_count <= self.messages_expected:
            _key = event['PartitionKey'] + "~" + str(_msg_count)
            self.v3io_client.kv.delete(container=self.container,
                         table_path=self.table_path,
                         key=_key)
            _msg_count +=1
    
    def merge_rule_partition_key(self,context,message):
        PartitionKey = message['PartitionKey']
        if PartitionKey in self.call_counter:
            self.call_counter[PartitionKey] += 1
        else:
            self.call_counter[PartitionKey] = 1
        #print("MESSAGE COUNT",self.call_counter,context.worker_id,message['shard'])
        
        if self.call_counter[PartitionKey] == self.messages_expected:
            payload = "%s\n"%(PartitionKey)
            try:
                self.v3io_client.object.put(self.container,self.results_file,body=payload,append=True)
            except:
                print("RESP",payload)
            self.del_queue.put({'PartitionKey' : PartitionKey, 'count' : self.call_counter[PartitionKey], 'message' : message })
            self.call_counter.pop(PartitionKey)
        else:
            self.add_queue.put({'PartitionKey' : PartitionKey, 'count' : self.call_counter[PartitionKey], 'message' : message })
        return message
    
    def processing(self,context,message):
        return_message = self.merge_rule_partition_key(context,message)
        return return_message
    


# In[3]:


# import os
# import ast
# os.environ['CLASS_CONFIG'] = '{"container" : "bigdata", "table_path" : "stream_processing/stream_converge", "results_file" : "batch_results/manual.csv"}'
# os.environ['BATCH_RESULTS_FOLDER'] = 'bigdata/batch_results'
# os.environ['STEP_NAME'] = 'manual'
# new = igz_stream_converge()


# In[4]:


# context = ''
# new.merge_rule_partition_key(context,{'PartitionKey':'rwgethr', 'count':1})


# In[ ]:




