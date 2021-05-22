import pika 
from collections import deque
import math 
import matplotlib.pyplot as plt

def PushMessage(message):
    
    global channel 
    global Queue

    
    channel.basic_publish(
    exchange='',
    routing_key='task_queue',
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=2,  # make message persistent
    ))

    print("Task Sent to Worker Node:",message)

    Queue += 1 


def callback(ch, method, properties, body):

    global connection 
    global Queue 
    global Counter
    message = body.decode()
    

    dataValues = message.split(" ")
    x = []
    y = []

    for i in range(1,len(dataValues)): 
        
        dataValues[i] = float(dataValues[i])

        x.append(i)
        y.append(float(dataValues[i]))


    plt.plot(x, y, label = dataValues[0])

    plt.xlabel("Pools")
    plt.ylabel("Swap Cost")
    plt.margins(x = 0)
    PoolMap[Counter] = dataValues[0]
    PoolResult[Counter] = y 

    Counter += 1
    Queue -= 1 

    if Queue == 0 : 
    
        print("All Paths Recieved")

        print("Starting the Path Computation")

        plt.show()
    # print("Cost of Path {} is {} ".format(result[0],result[1]))
    ch.basic_ack(delivery_tag=method.delivery_tag)
    


if __name__ == "__main__":
    

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()  
    channel.queue_declare(queue='task_queue', durable=True)

    PoolMap = {}    
    PoolResult = {}
    file = open('linearDataset.txt','r')
    Lines = file.readlines()

    stepSize  =  1
    MaxValue = 40

    Flow = "1->2"
    Queue = 0 
    
    Counter = 1
    Result = []
    
    # Result[0] = [ 0 for i in range(1,MaxValue + 2,stepSize)]

    for line in Lines: 
        
        pool,asset1,asset2 = line.split(" ")
        
        asset1 = int(asset1)
        asset2 = int(asset2)

        DataFormation = "{} {} {} {} {} {}".format(pool,asset1,asset2,stepSize,MaxValue,Flow)

        PushMessage(DataFormation)
        # Result.append([ 0 for i in range(1,MaxValue + 2,stepSize)])

    result = connection.channel()

    result.queue_declare(queue='result_queue', durable=True) 


    channel.basic_qos(prefetch_count=1)
    
    channel.basic_consume(queue='result_queue', on_message_callback=callback)

    channel.start_consuming()