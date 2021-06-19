from bs4 import BeautifulSoup
from Nodes.logger import *
import requests
import asyncio
import aiomas


class ScraperNode(aiomas.Agent):

	def __init__(self, container, node_address): 
		super().__init__(container)
		self.node_address =  str(node_address)            
		self.soup = None


	async def get_html(self, url):
		debug_log(self.node_address, f'Resolving {url}')
		page = requests.get(url, timeout = 15)
		self.soup = BeautifulSoup(page.content, 'html.parser')
		debug_log(self.node_address, f'{url} resolved')
		return str(self.soup)


	def exit(self):
		debug_log(self.node_address, f'shuting down scrapper node')
		self.container.shutdown()
		asyncio.get_event_loop().stop()


	@aiomas.expose
	async def request_handler(self, request_type, data=None):

		if (request_type == "scrap"):
			return await self.get_html(data)

		elif request_type == 'leave':
			self.exit()
		
		else:
			debug_log("error","invalid request type")
			return None