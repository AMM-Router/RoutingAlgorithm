import pika
import time 
import uuid 

connection = pika.BlockingConnection(pika.ConnectionParameters(host='172.17.0.2'))

channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

workerHash = uuid.uuid4()

def callback(ch, method, properties, body):

    output = body.decode()
    
    # Add CodeBase for Computation 
    Computation(output)
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

def Computation(message):
     
    global connection 
    global workerHash
    result = connection.channel()
    result.queue_declare(queue="result_queue", durable = True)


    Path = list(map(int,message.split(" ")))
    total = 0 
    for i in Path: 
        total += i

    time.sleep(20)
    output = "Worker: {} ".format(workerHash) + message + " Result->" + str(total)

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