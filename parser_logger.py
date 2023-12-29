import logging


class CustomStreamHandler(logging.StreamHandler):
    def emit(self, record):
        if record.levelno == self.level:
            super().emit(record)


class CustomFormatter(logging.Formatter):
    grey = "\x1b[37m"
    green = "\x1b[1;32m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s %(levelname)-8s %(name)s %(message)s"

    FORMATS = {
        logging.DEBUG: green + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


debug_stream_handler = CustomStreamHandler()
debug_stream_handler.setLevel(logging.DEBUG)
debug_stream_handler.setFormatter(CustomFormatter())

info_stream_handler = CustomStreamHandler()
info_stream_handler.setLevel(logging.INFO)
info_stream_handler.setFormatter(CustomFormatter())

warning_stream_handler = CustomStreamHandler()
warning_stream_handler.setLevel(logging.WARNING)
warning_stream_handler.setFormatter(CustomFormatter())

error_stream_handler = CustomStreamHandler()
error_stream_handler.setLevel(logging.ERROR)
error_stream_handler.setFormatter(CustomFormatter())

critical_stream_handler = CustomStreamHandler()
critical_stream_handler.setLevel(logging.CRITICAL)
critical_stream_handler.setFormatter(CustomFormatter())

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler('app.log'),
        debug_stream_handler,
        info_stream_handler,
        warning_stream_handler,
        error_stream_handler,
        critical_stream_handler,
    ],
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
)

logging.getLogger().handlers[0].setLevel(logging.DEBUG)


def set_logger(name):
    return logging.getLogger(name)


def overwrite_setLevel(level):
    logging.getLogger().setLevel(level)


def remove_debug_stream_handler():
    logging.getLogger().removeHandler(debug_stream_handler)
