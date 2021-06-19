from Nodes.bd import BDNode
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


class BDClient():
	def __init__(self, ip_address = 'localhost', port = 7700, interface = 'eth0'): 
		self.ip_address = Nodes.utils.get_ip(interface)
		self.port = port
		self.node_addr = ("tcp://"+ip_address+":" + str(port) + "/0")
		self.container = aiomas.Container
		self.node = None
		self.loop = None
		self.timeout = 30
	

	def run(self):
		self.loop = asyncio.get_event_loop()
		self.loop.run_until_complete(self.async_run())
		self.loop.run_forever()
		self.loop.close()


	def close(self):
		self.container.shutdown()
		self.loop.close()


	async def async_run (self):

		self.container = self.container.create((self.ip_address, self.port))
		debug_log("BD::ip_address: ", str(self.ip_address))
		self.node = BDNode(self.container,self.node_addr, self.port-1)
		self.node.create_table()


def run_bd_client():

	bdClient = BDClient(port = 7701)
	debug_log(f'{bdClient.ip_address}:7701',"Data Base launched succesfully")
	bdClient.run()


if __name__ == '__main__':
	clear_log()
	run_bd_client()