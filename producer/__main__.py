import sys

import pika

credentials = pika.PlainCredentials("producer_user", "producer_user")
conn_params = pika.ConnectionParameters("localhost", credentials=credentials)
conn_broker = pika.BlockingConnection(conn_params)

channel = conn_broker.channel()

channel.exchange_declare(exchange="test_exchange", exchange_type="direct", auto_delete=True)

msg = sys.argv[1]
msg_props = pika.BasicProperties()
msg_props.content_type = "text/plain"

channel.basic_publish(exchange="test_exchange", routing_key="test", body=msg, properties=msg_props)
