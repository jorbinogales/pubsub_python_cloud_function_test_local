import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from src.app import my_function

if __name__ == '__main__':
    my_function()
    logging.info('Funcion ejecutada correctamente')