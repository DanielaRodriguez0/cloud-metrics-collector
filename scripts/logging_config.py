import logging
import colorlog 

def setup_logger(name: str):
    """
    Configura el logger con formato y colores personalizados.

    Parameters
    ----------
    name : str
        Nombre del logger a configurar.

    Returns
    -------
    logger : logging.Logger
        El logger configurado.
    """
    """"""
    """
    Configura el logger con formato y colores personalizados.
    """
        #'%(asctime)s - %(log_color)s%(levelname)s%(reset)s - %(white)s%(message)s',
        #'%(asctime)s  %(log_color)s%(levelname)-10s%(reset)s %(white)s%(message)s',
        #'%(asctime)s %(filename)-18s %(levelname)-8s: %(message)s',
    formatter = colorlog.ColoredFormatter(
        '%(asctime)s %(log_color)s%(levelname)-8s%(reset)s %(filename)s:%(lineno)-4d %(white)s%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'bold_yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        }
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)
    
    return logger
