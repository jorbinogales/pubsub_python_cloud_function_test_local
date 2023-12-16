import os
import logging
import json
from src.gcp.pub_sub import PubsubConnector

def received_message():
    """Función para suscribirse a un topico y obtener la lista de ordenes de compra para actualizar la BD Orders."""
    try:
        project_id = os.getenv(
                    "PROJECT_ID")
        topic_name = os.getenv(
                    "TOPIC_NAME")
        subscription_name = os.getenv(
                    "SUBSCRIPTION_NAME")
        enviroment = os.getenv(
                    "ENVIRONMENT")
        pubsub_client = PubsubConnector(enviroment, project_id, topic_name, subscription_name, None, None)
        # Suscribirse a un tópico
        pubsub_client.subscribe(process_message)

    except Exception as e:
        logging.error("Error al suscribirse al tópico: %s", str(e))
        raise Exception(f'[ERROR]: {str(e)}')
    finally:
        logging.info("Saliendo de la función update_status_tracking()")

def process_message(message):
    logging.info(f"Mensaje leido: {message.data}")
    try:
        logging.info(f"Mensaje leido: {message.data}")
    except Exception as e:
        logging.error(f"Error al procesar el mensaje: {e}")
    finally:
        logging.info("Saliendo de la función process_message()")
    return 'Procesado'
