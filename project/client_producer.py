import zmq
import time
import sys
import consul
import hashlib 


from itertools import cycle

c = consul.Consul()

producers = {}


def create_clients(servers):

    context = zmq.Context()
    for server in servers:
        #print(f"Creating a server connection to {server}...")
        producer_conn = context.socket(zmq.REQ)
        producer_conn.bind(server)
        producers[server] = producer_conn


    return producers
  
    

def generate_data_round_robin(servers):
    print("Starting round robin")
    global producers
    if (len( producers) ==0):
        producers = create_clients(servers)
    pool = cycle(producers.values())
    for num in range(10):
        data = {'op':"PUT",'key': f'key-{num}', 'value': f'value-{num}' }
        server = next(pool)
        server.send_json(data)
        # print(f"Sending data:{data}" , " to ", servers)
        server.recv_json()
        time.sleep(1)
    #print("Done")

def myhash(x):
    result = ""
    for i in x:
        result += str(ord(i))
    result = (int(result)*947+29)%2168
    return result

def weight(x,y):
    weight= (((251*myhash(x)+163)%2168) ** myhash(y)) %2168
    weight = (weight*1549 + 521) %2168
    return weight

def generate_data_consistent_hashing(servers):
    print("Starting consistent hashing")
    global producers
    if (len(producers) ==0):
        producers = create_clients(servers)
    #print(producers)
    #print()
    for num in range(10):
        found = False
        data = {'op':'PUT', 'key': f'key-{num}', 'value': f'value-{num}' }
        otherData = {'op':'GET_ONE', 'key': f'key-{num}'}
        hash_key = myhash(f'key-{num}')
        #print("key = ", f'key-{num}', " hashkey = " , hash_key)
        #print(hash_key)
        while not found:
            if hash_key in bin:
                found = True
                index = bin.index(hash_key)
                sendTo = producers.get(servers[index])
                sendTo.send_json(data)
                sendTo.recv_json()
                sendTo.send_json(otherData)
                sendTo.recv_json()
                #print("found bin ", hash_key, "server= ", servers[index])
            hash_key = hash_key+1   
            if hash_key > 2168:
                hash_key=0 
        time.sleep(1)
    #print("Done")
    
def generate_data_hrw_hashing(servers):
    print("Starting hrw hashing")
    global producers
    if (len(producers) ==0):
        producers = create_clients(servers)
    for num in range(10):
        highest =9
        index=0
        data = {'op':"PUT",'key': f'key-{num}', 'value': f'value-{num}' }
        for i in range(len(servers)):
            weight_cal = weight(data["key"],servers[i])
            if weight_cal> highest:
                highest = weight_cal
                index = i
        #print()
        #print("sending data to ", servers[index])
        producers.get(servers[index]).send_json(data)
        producers.get(servers[index]).recv_json()
        time.sleep(1)
    #print("Done")
    
def addNode(num):
    print("adding node number " , num)
    global producers
    if (len(producers) ==0):
        producers = create_clients(servers)
    #send add signal to consul
    a.service.register(
    "node" + str(num),
    address = "127.0.0.1",
    port = 8000 + num)
    data = {'op':"GET_ALL"}

    #make a socket and connect
    context = zmq.Context()
    producer_conn = context.socket(zmq.REQ)
    server_port = 8000+num
    producer_conn.bind(f'tcp://127.0.0.1:{server_port}')
    producers["127.0.0.1:" + str(8000+num)] = producer_conn
    serversForNode=[]
    serversForNode.append("127.0.0.1:" + str(8000+num))
    #find bin for the new node
    findBin = myhash((f'tcp://127.0.0.1:{server_port}'))

    #find node I need to rebalance 
    rebalance = 9999999999
    rebalanceAddress =""
    for key in a.services().keys():
        server_port = str(a.services()[key]['Port'])
        #print("key = " , key)
        #print("serverPort = ", server_port)
        addressPort = 'tcp://127.0.0.1:' + str(server_port)
        serversForNode.append(addressPort)
        findHashNode =  myhash((f'tcp://127.0.0.1:{server_port}'))
        #print(findHashNode)
        if findHashNode > findBin and findHashNode< rebalance:
            rebalanceAddress = (f'tcp://127.0.0.1:{server_port}')
            rebalance = findHashNode
    #print("rebalance = " ,rebalanceAddress)
    #print(producers.get(rebalanceAddress))
    getServer = producers.get(rebalanceAddress)
    #print("get server = ", getServer)
    #print(" addressPort = ", addressPort)
    data = {'op':"GET_ALL"}
    #print("getServer = ", getServer)
    getServer.send_json(data)
    
    # get all data assigned to the bin that needs rebalancing
    allData = getServer.recv_json()
    print(allData)
    for key,value in allData["Collection"].items():
        hash_key = myhash(key)
        data = {'op':"PUT",'key': key, 'value': value }
        if( hash_key <findBin):
            # rebalanceNode = producers.get("127.0.0.1:" + str(8000+num))
            producer_conn.send_json(data)
            #print(producer_conn.recv_json())
            # send json to remove data to rebalancing node(getServer)
            data = {'op':"REMOVE",'key': key}
            getServer.send_json(data)
            receiveRemoved = (getServer.recv_json())
    data = {'op':"GET_ALL"}
    producer_conn.send_json(data)
    print(producer_conn.recv_json())
    getServer.send_json(data)
    print(getServer.recv_json())

    


def removeNode(num):
    print("removing node number ", num)
    global producers
    if (len(producers) ==0):
        producers = create_clients(servers)
    # producers = create_clients(servers)
    addressPort = 'tcp://127.0.0.1:' + str(8000+num)
    getServer = producers.get(addressPort)
    server_port = 8000 + num
    findBin = myhash((f'tcp://127.0.0.1:{server_port}'))
    data = {'op':"GET_ALL"}
    getServer.send_json(data)
    allData = (getServer.recv_json())

    rebalance = 9999999999
    rebalanceAddress =""
    #find the node to send all data to
    for key in a.services().keys():
        server_port = str(a.services()[key]['Port'])
        addressPort = 'tcp://127.0.0.1:' + str(server_port)
        findHashNode =  myhash((f'tcp://127.0.0.1:{server_port}'))
        if findHashNode > findBin and findHashNode< rebalance:
            rebalanceAddress = (f'tcp://127.0.0.1:{server_port}')
    #print("rebalanced to ", rebalanceAddress)

    getServer = producers.get(rebalanceAddress)

    for key,value in allData["Collection"].items():
        data = {'op':"PUT",'key': key, 'value': value }
        getServer.send_json(data)
        getServer.recv_json()
        data = {'op':"GET_ALL"}
        getServer.send_json(data)
        newDataList = getServer.recv_json()
    a.service.deregister("node" + str(num))


    #for demo
def getStat():
    data = {'op':"GET_ALL"}
    for key in a.services().keys():
        server_port = a.services()[key]
        getKey = 'tcp://127.0.0.1:' + str(server_port['Port'])
        # print(producers)
        print("get key = " + getKey)

        connection = producers.get(getKey)
        connection.send_json(data)
        print(connection)

        received = connection.recv_json()
        print(received)

    #for demo
def removeAll():
    data = {'op':"REMOVE_ALL"}
    for key in a.services().keys():
        server_port = a.services()[key]
        getKey = 'tcp://127.0.0.1:' + str(server_port['Port'])
        connection = producers.get(getKey)
        connection.send_json(data)
        # print(key)
        connection.recv_json()




    
if __name__ == "__main__":
    producers ={}
    bin = []
    servers = []
    c = consul.Consul()
    a = c.agent
    num_server = len(a.services())

    for key in a.services().keys():
        server_port = str(a.services()[key]['Port'])
        servers.append(f'tcp://127.0.0.1:{server_port}')
        bin.append(myhash('tcp://127.0.0.1:'+server_port))

    #for demo
    # generate_data_round_robin(servers)
    # getStat()
    # removeAll()
    # print()
    # generate_data_hrw_hashing(servers)
    # getStat()
    # removeAll()
    # # print()
    generate_data_consistent_hashing(servers)
    # getStat()    
    addNode(9)
    #getStat()    
    #removeNode(9)
    # getStat()



