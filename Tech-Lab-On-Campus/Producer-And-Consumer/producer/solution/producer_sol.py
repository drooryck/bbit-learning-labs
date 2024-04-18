from producer_interface import mqProducerInterface
import pika
import os

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.connection = None
        self.setupRMQConnection()
    
    def setupRMQConnection(self) -> None:
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

    def publishOrder(self, message: str) -> None:
        if self.connection is None:
            self.setupRMQConnection()
        
        channel = self.connection.channel()
        channel.exchange_declare(exchange=self.exchange_name)

        channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=message,
        )

        channel.close()
        self.connection.close()

