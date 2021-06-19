from Nodes.logger import *
import asyncio
import aiomas


class IntfzNode(aiomas.Agent):

	def __init__(self, container, node_address, dest_address): 
		super().__init__(container)
		self.node_address =  str(node_address)
		self.dest_address =  str(dest_address)
		self.chord_agent = f'{self.dest_address[0:len(self.dest_address)-1]}1'
		self.timeout = 30


	async def check_conection(self):
		remote_peer = await asyncio.wait_for(self.container.connect(self.chord_agent), timeout=self.timeout)
		debug_log(self.node_address, f'Interface conected to Chord Node')


	async def init_routine(self):

		print('Type "exit" to finish the program')

		while True:
			key = input('[URL]:>>> ')

			remote_peer = await asyncio.wait_for(self.container.connect(self.chord_agent), timeout=self.timeout)
			if key == 'exit':
				remote_peer.request_handler(self.chord_agent, 'leave', key)
				break

			print(f'Trying to resolve {key}')
			ret = await asyncio.wait_for(remote_peer.request_handler(self.chord_agent, 'lookup_for', str(key)), timeout=self.timeout)

			if ret == -1:
				print('Page not found or server is down temporally')
				continue

			print('HTML receibed succesfully')

			option = input('Press 1 to see de HTML code, 2 to skip: ')
			if option == '1':
				print(ret)

		
		print('Finnishing program...')
		self.container.shutdown()
		asyncio.get_event_loop().stop()


	@aiomas.expose
	def ACK(self):
		return 'ACK'