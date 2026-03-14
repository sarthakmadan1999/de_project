import logging
from databricks.sdk.runtime import *

def get_logger(name, log_path):
    """
    Logger that writes to ADLS using dbutils.
    Each run overwrites the log file.
    """

    class ADLSHandler(logging.Handler):
        def __init__(self, log_path):
            super().__init__()
            self.log_path = log_path
            self.logs = []

        def emit(self, record):
            log_entry = self.format(record)
            self.logs.append(log_entry)
            dbutils.fs.put(
                self.log_path,
                "\n".join(self.logs),
                overwrite=True
            )

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    handler = ADLSHandler(log_path)
    handler.setFormatter(logging.Formatter(
        '[%(asctime)s] %(levelname)s - %(message)s'
    ))

    logger.addHandler(handler)
    return logger