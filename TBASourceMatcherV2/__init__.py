import os
import sys

import py_eureka_client.eureka_client as eureka_client
from django.core.management.commands.runserver import Command as runserver

from TBASourceMatcherV2.settings import EUREKA_URL, APPLICATION_NAME, APPLICATION_PORT

if sys.platform != "win32":

    class EurekaRegister:
        def __init__(self, eureka_url, app_name, instance_port, strategy):
            self.eureka_url = eureka_url
            self.app_name = APPLICATION_NAME
            self.instance_port = instance_port
            self.strategy = strategy
            print(self.eureka_url)

        def register(self):
            eureka_client.init(
                eureka_server=self.eureka_url,
                app_name=self.app_name,
                instance_port=self.instance_port,
                ha_strategy=self.strategy,
            )

    print('Registering with the Eureka')

    try:
        print("Registering with the Eureka")
        runserver.default_port = int(os.getenv('TBA_PORT', APPLICATION_PORT))
        eurekaclient = EurekaRegister(
            eureka_url=EUREKA_URL,
            app_name=APPLICATION_NAME,
            instance_port=int(runserver.default_port),
            strategy=eureka_client.HA_STRATEGY_STICK,
        )

        eurekaclient.register()
    except AttributeError as aerr:
        print(f"No Eureka networks found {repr(aerr)}")
    except Exception as err:
        print(f"Eureka Registration Failed with {repr(err)}")
