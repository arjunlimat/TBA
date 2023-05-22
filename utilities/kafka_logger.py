# -*- coding: utf-8 -*-
"""Module to provide kafka handlers for internal logging facility."""

"""
Dependencies:

PROJECT_NAME: Should be an import from settings.py
host : name of kafka Server


"""

import json
import logging
import socket

from kafka import KafkaProducer

PROJECT_NAME = "TBASourceMatcher"


class KafkaHandler(logging.Handler):
    """init a kafka logger."""

    def __init__(self, host, topic):
        logging.Handler.__init__(self)
        self.producer = KafkaProducer(
            bootstrap_servers=host,
            client_id=PROJECT_NAME,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.topic = topic

    # except Exception as e:
    #     print(f"Kafka not working. Error:{e}")

    def emit(self, record):
        """
        All custom handles must have a emit method to send the file.
        """
        try:
            if "kafka." in record.name:
                return None

            ipaddr = socket.gethostbyname(socket.gethostname())
            try:
                to_send_dict = {
                    "timestamp": f"{record.asctime}",
                    "severity": f"{record.levelname}",
                    'service': 'TBASourceMatcher',
                    "trace": f"{record.trace_id}",
                    "span": f"{record.span_id}",
                    "parent": f"{record.parent_span_id}",
                    "exportable": "true",
                    "pid": f"{record.process}",
                    "thread": f"{record.thread}",
                    "class": f"{record.name}",
                    "Exception": f"{record.stack_info}",
                    "LogMessage": f"{record.message}",
                    "serverName": f"{ipaddr}",
                }
            except (AttributeError):
                pass

            # Async by default
            self.producer.send(topic=self.topic, value=to_send_dict)

            # block until all async messages are sent
            self.flush()

        except Exception as err:
            print(f"KafkaError: {repr(err)}")
            # logging.Handler.handleError(self, record)
