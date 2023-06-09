import json
import pika
from hashlib import sha256

def send_defused_mine(mine_data):
    # establish connection and send over data to defused-mines channel
    new_connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    new_channel = new_connection.channel()
    new_channel.queue_declare(queue='defused-mines')
    new_channel.basic_publish(exchange='', routing_key='defused-mines', body=json.dumps(mine_data))
    new_connection.close()

def disarm_mine(ch, method, properties, body):

    # unravel mine_data
    mine_data = json.loads(body)
    x = mine_data['x']
    y = mine_data['y']
    serial_no = mine_data['serial_no']
    print(f'Received data from demine queue: {mine_data}')
    # note: we increment pin instead of using random to make sure results are reproducible
    # i.e. same time pin is found vs. random time generating random pins
    pin = 0
    success_code = '0' * 4
    mine_key = str(pin) + serial_no
    print('Starting to disarm...')
    while not (hash_ := sha256(f'{mine_key}'.encode()).hexdigest()).startswith(success_code):
        pin += 1
        mine_key = str(pin) + serial_no

    print(f'Found pin: {pin}; Temporary mine key: {hash_}')
    print('Sending over to defused-mines queue')

    # send data over to defuse-mines channel
    mine_data['pin'] = pin
    mine_data['temp_key'] = hash_
    send_defused_mine(mine_data)

if __name__ == '__main__':
    print("Starting deminer service....")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='demine-queue')
    channel.basic_consume(queue='demine-queue', on_message_callback=disarm_mine, auto_ack=True)
    channel.start_consuming()