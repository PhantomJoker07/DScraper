from Nodes.interface import IntfzNode
from Nodes.logger import *
import nest_asyncio
nest_asyncio.apply()
import Nodes.utils
import asyncio
import logging
import getopt
import aiomas
import sys
import os


class IntfzClient():
    def __init__(self, ip_address = 'localhost', port = 7700, interface = 'eth0'): 
        self.ip_address = Nodes.utils.get_ip(interface)
        self.port = port
        self.node_addr = ("tcp://"+self.ip_address+":" + str(port) + "/0")
        self.bootaddr = ("tcp://"+self.ip_address+":" + str(port-3) + "/0")
        self.container = aiomas.Container
        self.loop = None
        self.node = None
        self.timeout = 30
    

    def run(self):
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.async_run())
        self.loop.run_until_complete(self.node.init_routine())
        self.loop.run_forever()
        self.loop.close()


    def close(self):
        self.container.shutdown()
        self.loop.close()


    async def async_run (self):

        self.container = self.container.create((self.ip_address, self.port))
        debug_log("Interface::ip_address: ", str(self.ip_address))
        self.node = IntfzNode(self.container,self.node_addr, self.bootaddr)

        await self.node.check_conection()


def run_fz_client():

    fzClient = IntfzClient(port = 7703)
    debug_log(f'{fzClient.ip_address}:7701',"Interface launched succesfully")
    fzClient.run()


if __name__ == '__main__':
    clear_log()
    run_fz_client()
