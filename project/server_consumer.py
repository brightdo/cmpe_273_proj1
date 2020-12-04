import zmq
import sys
from  multiprocessing import Process
import consul
import time


c = consul.Consul()

portDict={}
a = c.agent
context = zmq.Context()
consumer = context.socket(zmq.REP)
def server(port):

    consumer.connect(f"tcp://127.0.0.1:{port}")
    
    print(f"Starting a server at:{port}...")
    mine = dict()
    while True:
        raw = consumer.recv_json()

        # FIXME: if you receive request of Get_one use consumerGet to return result
        if(raw['op'] == "PUT"):
            key, value = raw['key'], raw['value']
            mine[key] = value
            data = { 'status': 'success'}
            #print(f"Server_port={port}:key={key},value={mine[key]}")
            consumer.send_json(data)
        elif(raw['op']== "GET_ONE"):
            key = raw['key']
            data = {"key":key, "value": mine[key]}
            consumer.send_json(data)
        elif(raw['op']== "GET_ALL"):
            data = {"Collection": mine}
            consumer.send_json(data)
        elif(raw['op'] == "REMOVE"):
            # print(raw['key'])
            del mine[raw['key']]
            data = {'status': 'success', 'count':mine} 
            consumer.send_json(data)
        #for demo only
        elif(raw['op'] == "REMOVE_ALL"):
            mine = dict()
            data = {'status': 'success', 'count':mine} 
            consumer.send_json(data)
        # elif(raw['op'] == "KILL"):
        #     data = {'status': 'success'} 
        #     print( "terminating process")
        #     consumer.send_json(data)



    
        
if __name__ == "__main__":
    num_server = len(a.services())

    DictOfServer = a.services()
    
    listOfKeys=[]
    # print(type(DictOfServer))
    # print(DictOfServer)
    for dict in a.services():
        listOfKeys.append(dict)
    print("list of nodes = ",listOfKeys)


    for key in a.services().keys():
        server_port = float(a.services()[key]['Port'])
        thisProcess = Process(target=server, args=(server_port,))
        thisProcess.start()
        portDict[key] = thisProcess


    while True:
        time.sleep(1)
        refresh_num_server = len(a.services())
        
        # ADDED node
        if refresh_num_server > num_server:
            for key in a.services().keys():
                if key not in listOfKeys:
                    server_port = float(a.services()[key]['Port'])
                    Process(target=server, args=(server_port,)).start()
                    listOfKeys.append(key)
            num_server = refresh_num_server
        #removed a node
        elif refresh_num_server < num_server:
            print("removed server")
            for key in listOfKeys:
                if key not in a.services().keys():
                    print(key)
                    portDict[key].terminate()
            num_server = refresh_num_server 
                    





    