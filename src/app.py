import logging
import functions_framework
import os
from src.common.utils.received_message import received_message

@functions_framework.http
def my_function(request):
      try:
        if (os.getenv('ENVIRONMENT') == "local"):
          os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"
        logging.info('Iniciando la funcion load()')
        received_message()
      except Exception as e:
         logging.error(f'[ERROR]: {str(e)}')
         raise Exception(f'[ERROR]: {str(e)}')
      finally:
         logging.info( "Saliendo de la función load()")
      return "Procesado"