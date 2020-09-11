import os
import base64
import json
import requests
import json

from additional_modules import ExternalClass

def igz_post_to_stream(context,message):
    Records=[]
    
    # message debe ser un Python dictionary
    messageb64 = base64.b64encode(json.dumps(message).encode('utf-8'))

    Records.append({
            "Data":  messageb64.decode('utf-8'),
            "PartitionKey": message['PartitionKey'],

            })
    payload = {"Records": Records}
    #print("PAYLOAD",payload)
    #print("URL",context.user_data.url)
    #print("HEADERS",context.user_data.headers)
    r = requests.post(context.user_data.url, headers=context.user_data.headers,json=payload, verify=False)
    #print("RESPONSE",r.text)
    
    
def igz_stream_init(context):
    setattr(context.user_data,'url', "http://%s/%s/%s/"% ('v3io-webapi:8081',os.getenv('OUTPUT_STREAM_CONTAINER'),os.getenv('OUTPUT_STREAM')))
    setattr(context.user_data,'headers', {
            "Content-Type": "application/json",
            "X-v3io-function": "PutRecords",
            "X-v3io-session-key" : os.getenv("V3IO_ACCESS_KEY")
          })
    
def step_watcher(step,message):
    url = "http://v3io-webapi:8081/%s/%s_%s.csv"% (os.getenv('BATCH_RESULTS_FOLDER'),step)
    headers = {
            "Content-Type": "application-octet-stream",
            "X-v3io-session-key": os.getenv('V3IO_ACCESS_KEY'),
            "Range": "-1"
          }
    payload = "%s\n"%(message)
    try:
        response = requests.put(url, data=payload, headers=headers)
    except:
        print("RESP",payload)
        print("RESP",headers)
        print("RESP",url)
    pass

class igz_model():
    ## Fake placeholder model
    def __init__(self):
        self.model = os.getenv('MODEL_PATH')
        return
        
    def processing(self,context,message):
        print("MODEL PATH",self.model)
        message['prediction'] = "H0"
        return message
    
    def last_step(self,context,message):
        print(message)
        open("/tmp/%s.json"%message['PartitionKey'],'w').write(json.dumps(message))
        return
    
class igz_collector():
    ## Fake placeholder model
    
    def __init__(self):
        self.call_counter={}
        return
    
    def processing(self,context,message):
        PartitionKey = message['PartitionKey']
        if PartitionKey in self.call_counter:
            self.call_counter[PartitionKey] += 1
        else:
            self.call_counter[PartitionKey] = 1
        if self.call_counter[PartitionKey] >= 2:
            print("MESSAGE COUNT",PartitionKey,self.call_counter[PartitionKey])
            print("Running final step")
        return message
    
class igz_stream_merge():
    ## Fake placeholder model
    
    def __init__(self):
        self.call_counter={}
        return
    
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
            
            self.call_counter.pop(PartitionKey)
        return message
    
    def processing(self,context,message):
        return_message = self.merge_rule_partition_key(context,message)
        return return_message
    
    
def append_to_file(context,message):
    ec = ExternalClass()
    ec.invoke_ec(context,message)
    url = "http://v3io-webapi:8081/%s/%s.json"% (os.getenv('BATCH_RESULTS_FOLDER'),os.getenv('RESULTS_FILE'))
    headers = {
            "Content-Type": "application-octet-stream",
            "X-v3io-session-key": os.getenv('V3IO_ACCESS_KEY'),
            "Range": "-1"
          }
    payload = "%s\n"%(message)
    try:
        response = requests.put(url, data=payload, headers=headers)
    except:
        print("RESP",payload)
        print("RESP",headers)
        print("RESP",url)
    pass