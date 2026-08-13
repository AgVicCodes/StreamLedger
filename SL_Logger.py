import logging
from logging.handlers import RotatingFileHandler

DEFAULT_LOG_FILE = "stream_log.log"


def streamLog(
    log_file: str = DEFAULT_LOG_FILE,
    level: int = logging.DEBUG,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    console: bool = False,
):
    """Configure and return a named logger (`streamledger`).

    - Uses a RotatingFileHandler so logs don't grow unlimited.
    - Calling this multiple times is safe (handlers added only once).
    """
    logger = logging.getLogger("streamledger")
    logger.setLevel(level)

    if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        fh = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
        fh.setFormatter(fmt)
        fh.setLevel(level)
        logger.addHandler(fh)

        if console:
            ch = logging.StreamHandler()
            ch.setFormatter(fmt)
            ch.setLevel(level)
            logger.addHandler(ch)

    return logger


def get_logger():
    return logging.getLogger("streamledger")


# Convenience aliases that resolve the named logger at call time.
def log(msg, *args, **kwargs):
    get_logger().info(msg, *args, **kwargs)


def warn(msg, *args, **kwargs):
    get_logger().warning(msg, *args, **kwargs)


def debug(msg, *args, **kwargs):
    get_logger().debug(msg, *args, **kwargs)


def error(msg, *args, **kwargs):
    get_logger().error(msg, *args, **kwargs)


if __name__ == "__main__":
    # Configure logger (writes to file and also prints to console)
    streamLog(console = True)
    msg = "Hello, testing logger"
    log(msg)
    warn(msg)
    debug(msg)
    error(msg)