import pika
import time 
import uuid 

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

workerHash = uuid.uuid4()

def callback(ch, method, properties, body):

    output = body.decode()
    
    Computation(output)
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

def Computation(message):
     
    global connection 
    global workerHash
    result = connection.channel()
    result.queue_declare(queue="result_queue", durable = True)


    pool,asset1,asset2,stepSize,MaxValue,flow = message.split(" ")

    asset1 = int(asset1)

    asset2 = int(asset2)

    LinearData = []

    for i in range(1,int(MaxValue) + 1,int(stepSize)): 
        
        swapIn = .997 * i 

        if flow == "1->2": 
            
            swapOut = asset2 - (asset1 * asset2 ) / ( asset1 + swapIn )  

        else: 
            
            swapOut = asset1 - (asset1 * asset2 ) / ( asset2 + swapIn )
            
        LinearData.append(str(round(swapOut,4)))
        
        # time.sleep(2)
    
    output = pool + " " + " ".join(LinearData)

    print(output)

    result.basic_publish(
    exchange='',
    routing_key='result_queue',
    body=output,
    properties=pika.BasicProperties(
        delivery_mode=2,  # make message persistent
    ))

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)

channel.start_consuming()