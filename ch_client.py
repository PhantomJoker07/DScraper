from Nodes.scrapper import ScraperNode
from argparse import ArgumentParser
from Nodes.chord import ChordNode
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


class ChordClient():
	def __init__(self,
				ip_address = 'localhost',
				port = 7700,
				api_port = 4423,
				bootaddr = None,
				interface='eth0',):
		self.ip_address = Nodes.utils.get_ip(interface)
		self.port = port
		self.api_port = api_port
		self.node_addr = ("tcp://"+self.ip_address+":" + str(self.port) + "/0")
		self.bootstrap_addr = f'tcp://{bootaddr}/0' if bootaddr is not None else None
		self.container = aiomas.Container
		self.agent = None
		self.nodeChord = None
		self.nodeBd = None
		self.nodeScrapper = None
		self.api_server = None
		self.timeout = 30
	

	def run(self):
		self.loop = asyncio.get_event_loop()
		self.loop.run_until_complete(self.chord_node_run())
		self.loop.run_forever()
		self.loop.close()


	def close(self):
		self.container.shutdown()
		self.loop.close()


	async def chord_node_run(self):

		self.container = self.container.create((self.ip_address, self.port))
		debug_log("bootstrap_address: ", str(self.bootstrap_addr))
		self.node = ChordNode(self.container,self.node_addr,bootstrap_address=self.bootstrap_addr)

		result = await self.join()
		if (result == -1):
			self.close()

		self.agent = ChordAgent(self.container, self.node, self.timeout)


	async def join(self):
		try:
			res = await asyncio.wait_for(self.node.join(),timeout = self.timeout)
		except Exception as e:
			error_log(str(e))
			res = -1
		return res


###########################################################
#+Agent for receive external petitions and return answers+#
###########################################################

class ChordAgent(aiomas.Agent):  

	def __init__(self, container, node, timeout): 
		super().__init__(container)
		self.node = node
		self.timeout = timeout


	@aiomas.expose
	async def request_handler(self, addr, request_type, data):
		
		ans = None
		if (request_type == "leave"):
			await self.leave()
		
		elif(request_type == "put_data"):
			try:
				a,b = data
			except:
				debug_log("error","invalid data format")
				return ans
			key, dat = data
			ans = await self.put_data(key,dat)

		elif(request_type == "lookup_for"):
			ans = await self.lookup_for(data)

		else:
			debug_log("error","invalid request type")

		return -1 if ans is None else ans


	async def join(self):
		try:
			res = await asyncio.wait_for(self.node.join(),timeout = self.timeout)
		except Exception as e:
			error_log(str(e))
			res = -1
		return res


	async def leave(self, term = True):
		try:
			res = await asyncio.wait_for(self.node.leave(terminate = term),timeout = self.timeout)
		except Exception as e:
			error_log(str(e))
			res = -1
		return res


	async def lookup_for(self, key):
		try:
			res = await asyncio.wait_for(self.node.lookup_for(key),timeout = self.timeout)
		except Exception as e:
			error_log(str(e))
			res = -1
		return res


	async def put_data(self, key,data):
		try:
			res = await asyncio.wait_for(self.node.put_data(key,data),timeout = self.timeout)
		except Exception as e:
			error_log(str(e))
			res = -1
		return res


def run_chord_client(bootstrap_addr):

    chordClient = ChordClient(bootaddr=bootstrap_addr)
    debug_log(f'{chordClient.ip_address}:7700',"Chord Node launched succesfully")
    chordClient.run()


if __name__ == '__main__':

	clear_log()

	argsParser = ArgumentParser()
	argsParser.add_argument('--address', default=None, help='address of some node of a ring,\ndefault: None (Create a new ring)')

	args = argsParser.parse_args()

	run_chord_client(args.address)