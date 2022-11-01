import pika

credentials = pika.PlainCredentials("consumer_user", "consumer_user")
conn_params = pika.ConnectionParameters("localhost", credentials=credentials)
conn_broker = pika.BlockingConnection(conn_params)

channel = conn_broker.channel()

channel.exchange_declare(
    exchange="test_exchange",
    exchange_type="direct",
    auto_delete=True
)

channel.queue_declare(
    queue="consumer_user_queue",
    auto_delete=True
)

channel.queue_bind(
    queue="consumer_user_queue",
    exchange="test_exchange",
    routing_key="test"
)


def msg_consumer(channel, method, header, body):
    channel.basic_ack(delivery_tag=method.delivery_tag)

    if body.decode() == "quit":
        channel.basic_cancel(consumer_tag="hello-consumer")
        channel.stop_consuming()

    else:
        print(body)

    return


channel.basic_consume(
    queue="consumer_user_queue",
    on_message_callback=msg_consumer,
    consumer_tag="hello-consumer"
)

channel.start_consuming()
