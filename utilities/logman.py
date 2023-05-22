"""kafka"""

import logging
import os
import socket
import sys
from datetime import datetime
from logging import handlers
from typing import Dict

from utilities.kafka_logger import KafkaHandler

# loglevel state, if true DEBUG else INFO
LOGLEVEL = logging.DEBUG if os.environ.get("DEBUG") else logging.INFO
KAFKA_ADDRESS = os.environ.get("KAFKA_ADDRESS")
if KAFKA_ADDRESS:
    KAFKA_ADDRESS = KAFKA_ADDRESS.split(",")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")


print(f"KT: {KAFKA_TOPIC}\nKA: {KAFKA_ADDRESS}")


class CustomHandler(handlers.RotatingFileHandler):
    """handles file rollover"""

    def doRollover(self):
        """
        Do a rollover, as described in __init__().
        Application.yyyy-MM-dd.%i.log
        """
        todaytime: str = datetime.now().strftime("%d-%m-%Y")
        if self.stream:
            self.stream.close()
            self.stream = None
        if self.backupCount > 0:
            self.baseFilename = self.baseFilename.split(".")[0]
            for i in range(self.backupCount - 1, 0, -1):
                sfn = self.rotation_filename("%s.%s.%d.log" % (self.baseFilename, todaytime, i))
                dfn = self.rotation_filename("%s.%s.%d.log" % (self.baseFilename, todaytime, i + 1))
                if os.path.exists(sfn):
                    if os.path.exists(dfn):
                        os.remove(dfn)
                    os.rename(sfn, dfn)
            dfn = self.rotation_filename("%s.%s.%s.log" % (self.baseFilename, todaytime, "1"))
            if os.path.exists(dfn):
                os.remove(dfn)
            self.baseFilename = self.baseFilename + ".log"
            self.rotate(self.baseFilename, dfn)
        if not self.delay:
            self.stream = self._open()


def logman(logname="", loglevel=LOGLEVEL):
    """Create a logger with the name and loglevel given as parameters
    Also adds a rotating file handler and a json kafka-handler.

    Params
    logname: string -> accepts a string to return the named logger.
                       If no name, root logger is returned.
                       __name__ returns the default named logger.

    loglevel: int -> returns a logger with the loglevel specified.
                     Also can be logging.INFO, logging.DEBUG etc.

    """
    json_log_format = """{
        "timestamp": "%(asctime)s",
        "severity": "%(levelname)s",
        "service": "%(name)s",
        "trace": "%(trace_id)s",
        "span": "%(span_id)s",
        "parent": "%(parent_span_id)s",
        "exportable": "true",
        "pid": "%(process)d",
        "thread": "%(thread)d",
        "class": "",
        "Exception": "%(stack_info)s",
        "LogMessage": "%(message)s",
        "serverName":"%(serverName)s"
    }
    """

    # Suppress verbose logs from other libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    # create the named logger if no name is provided, root logger is returned
    logger = logging.getLogger(name=f"tbasourcematcher.{logname}")
    logger.setLevel(loglevel)

    # ----------------ADD ROTATING FILE HANDLER-------------------------------
    # check if there is a log folder to write to
    if not os.path.isdir(os.path.join(os.getcwd(), "logs")):
        os.mkdir(os.path.join(os.getcwd(), "logs"))
    name_of_logfile = "Sourcematch.log"
    path_of_logfile = os.path.join(os.getcwd(), "logs", name_of_logfile)
    # create and add the rHandler
    rfh = CustomHandler(filename=path_of_logfile, mode="a+", maxBytes=1024 * 1024 * 50, backupCount=10)
    rfh.setLevel(loglevel)
    rfh.setFormatter(logging.Formatter(json_log_format))
    logger.addHandler(rfh)

    # ---------------------------------KafkaHandler---------------------------
    # Json by default passes all the items in extra param of log, the default
    # log_record attributes will not be included by default, hence add them
    # here and make changes in CustomJsonFormatter class above.
    if KAFKA_ADDRESS and KAFKA_TOPIC:
        print(f"Kafka added to tbasourcematcher.{logname}")
        kafka_handler = KafkaHandler(host=KAFKA_ADDRESS, topic=KAFKA_TOPIC)
        kafka_handler.setFormatter(logging.Formatter(json_log_format))
        logger.addHandler(kafka_handler)
    else:
        print(f"KAFKA NOT ADDED TO LOGGER\nKAFKA ADDRESS:{KAFKA_ADDRESS}\nKAFKA PORT:{KAFKA_TOPIC}")

    # ------------------------------StreamHandler-----------------------------
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(loglevel)
    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
