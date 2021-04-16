import pika 
from collections import deque
import math 

def PushMessage(path):
    
    global channel 
    global Queue

    message = " ".join([ str(i) for i in path])

    channel.basic_publish(
    exchange='',
    routing_key='task_queue',
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=2,  # make message persistent
    ))

    print("Task Sent to Worker Node:",message)
    Queue += 1 


def isNotVisited(x,path):
    for i in range(len(path)):
        if (path[i] == x):
            return 0             
    return 1
 
def findpaths(g, src, dst, v):
                   
    q = deque()
 
    path = []
    path.append(src)
    q.append(path.copy())
    
    while q:
        path = q.popleft()
        last = path[-1]

        if (last == dst):
            PushMessage(path)
 
        for i in range(len(g[last])):
            if (isNotVisited(g[last][i], path)):
                newpath = path.copy()
                newpath.append(g[last][i])
                q.append(newpath)
 

def callback(ch, method, properties, body):

    global connection 
    global Queue 
    message = body.decode()
    result = message.split("->")

    print(message)
    Queue -= 1 

    if Queue == 0 : 
        connection.close()
    # print("Cost of Path {} is {} ".format(result[0],result[1]))
    ch.basic_ack(delivery_tag=method.delivery_tag)
    


if __name__ == "__main__":
    

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()  
    channel.queue_declare(queue='task_queue', durable=True)

    
    v = 8
    g = [[] for _ in range(8)]
    Queue = 0 

    file = open('fintect.txt','r')
    Lines = file.readlines()
    
    for line in Lines: 
        node1,node2 = map(int,line.split())
        g[node1].append(node2)


    src = 0
    dst = 3
    print("Source {} to Destination {} are".format(src, dst))

    findpaths(g, src, dst, v)
    

    result = connection.channel()

    result.queue_declare(queue='result_queue', durable=True) 


    channel.basic_qos(prefetch_count=1)
    
    channel.basic_consume(queue='result_queue', on_message_callback=callback)

    channel.start_consuming()