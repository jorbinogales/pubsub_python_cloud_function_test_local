from google.cloud import pubsub_v1
import logging

logger = logging.getLogger(__name__)


class PubsubConnector:
    def __init__(
        self,
        environment,
        project_id,
        topic_name,
        subscription_id,
        publisher,
        subscriber_client
    ):
        self.environment = environment
        self.project_id = project_id
        self.topic_name = topic_name
        self.subscription_id = subscription_id
        self.publisher = publisher
        self.subscriber_client = subscriber_client

    def connect_subscriber(self):
         try:
          self.subscriber_client = pubsub_v1.SubscriberClient(
                  client_options={"api_endpoint": "localhost:8085"}
              )
         except Exception as e:
            logging.error(
                "Error al crear el subscriber de pubsub:")
            logging.error(f'{str(e)}')

    def get_subscription_path(self):
        return self.subscriber_client.subscription_path(self.project_id, self.subscription_id)


    def subscribe(self, callback):
        self.connect_subscriber()
        self.start_messages_listening(callback) 
  
    def start_messages_listening(self, callback):
        try:
            def callback(message: pubsub_v1.subscriber.message.Message) -> None:
                logging.info('Received message:')
                logging.info(message)
                message.ack()
            self.subscriber_client.subscribe(
                self.get_subscription_path(), callback=callback
            )
        except Exception as e:
            logging.error(
                "Error al crear publisher de pubsub:")
            logging.error(f'{str(e)}')
     