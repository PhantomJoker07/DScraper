from Nodes.logger import *
import threading
import asyncio
import random
import aiomas
import math
import sys
import hashlib


class ChordNode(aiomas.Agent):

	def __init__(self, container, node_address, nodeID = None, bootstrap_address = None, nBits = 5, timeout = 30):
		super().__init__(container)
		self.node_address =  str(node_address)              # Node address
		self.bootstrap_address = bootstrap_address          # Bootstrap address from the Chord Network
		self.nBits   = nBits                                # Num of bits for the ID space 
		self.MAXPROC = pow(2, nBits)                        # Maximum num of processes
		self.timeout = timeout                              # Timeout for waiting a reply
		self.ping_timeout = 1                               # Timeout for waiting a ping response
		self.nodeID  = nodeID 
		self.FT      = [None for i in range(self.nBits+1)]  # FT[0] is predecessor
		self.nodeSet = []                                   # Nodes discovered so far
		self.reachableNodes = {}                            # dict containing reachable nodes, key: nodeId and value: node_address
		self.data = {}                                      # dummy for future data holder reference
		self.lock = threading.Lock()
		self.pending_updates = []
		self.replicas = 1                                   #Max number of replicas of a given info stored
		self.NET_CHK_TIME = 6

		ip, port = node_address.split(':')[1][2:], int(node_address.split(':')[2].split('/')[0])
		self.bd_address = f'tcp://{ip}:{port+1}/0'
		self.scrapper_address = f'tcp://{ip}:{port+2}/0'
		self.intfz_address = f'tcp://{ip}:{port+3}/0'


		#########################################
		### Asynchronous coroutines of Chord  ###
		#########################################

	#@asyncio.coroutine
	async def join(self):

		if (self.bootstrap_address is None):   # Create a new Chord Network
			self.nodeID = self.nodeID or self.generate_key(self.node_address)    
			debug_log(self.nodeID, "join request without bootstrap address, creating new chord ring")
			self.addNode(self.nodeID,self.node_address)
			self.recomputeFingerTable()
			await self.load_from_bd()
			return

		else: 
			#Connect to the bootstrap node
			debug_log(self.node_address, "join request to bootstrap address: " + str(self.bootstrap_address))
			
			try:
				remote_peer = await asyncio.wait_for(self.container.connect(self.bootstrap_address), timeout=self.timeout)
				ret = await asyncio.wait_for(remote_peer.recv_join_request(self.node_address), timeout=self.timeout)
			except:
				await self.terminate()
				raise JoinChordError(self.bootstrap_address)

			self.nodeID, reachableNodes, bootstrapId = ret

			#Update finger table and node set
			for key,value in reachableNodes.items():
				self.addNode(int(key),value)
			self.addNode(self.nodeID,self.node_address)
			debug_log("reachableNodes: ", str(self.reachableNodes))                  
			self.recomputeFingerTable()

			debug_log(self.node_address, "Success joining ring, new nodeID: " + str(self.nodeID))

			await self.load_from_bd()
			await self.transfer_data(replication=True)

			#Tell all known nodes to update their information
			try:
				await self.network_update()
				await asyncio.wait_for(self.send_update_join(), timeout = self.timeout)
			except Exception as e:
				error_log(f'line 81: {e}')
				raise Timeout_Error(self.timeout)


	#@asyncio.coroutine
	async def leave(self, terminate = True):
		await self.network_update()
		self.delNode(self.nodeID,self.node_address)
		self.FT = [self.FT[0] for i in range(self.nBits+1)]
		await self.send_update_leave()
		if self.FT[0] != self.nodeID: await self.transfer_data(self.FT[0])		
		if terminate:
			await self.terminate()


	#@asyncio.coroutine
	async def send_update_join(self):
		for key in self.reachableNodes.keys():
			if key != self.nodeID and key in self.nodeSet:
				dest_addr = self.reachableNodes[key]
				try:
					remote_peer = await asyncio.wait_for(self.container.connect(dest_addr), timeout=self.timeout)  
					debug_log(self.nodeID,"sending update after join to all reachable nodes, self addr: " + str(self.node_address))
					ans = await asyncio.wait_for(remote_peer.join_update(self.nodeID,self.node_address), timeout=self.timeout)  
				except Exception as e:
					error_log(f'line 107: {e}')
					raise ChordException()
			
			elif(key != self.nodeID):
				self.pending_updates.append(key)


	#@asyncio.coroutine
	async def send_update_leave(self):
		for key in self.reachableNodes.keys():
			if key != self.nodeID and key in self.nodeSet:
				dest_addr = self.reachableNodes[key] 
				try:
					remote_peer = await asyncio.wait_for(self.container.connect(dest_addr), timeout=self.timeout)  
					debug_log(self.nodeID,"sending update after leave to all reachable nodes, self addr: " + str(self.node_address))
					ans = await asyncio.wait_for(remote_peer.leave_update(self.nodeID,self.node_address), timeout=self.timeout) 
				except Exception as e:
					error_log(f'line 124: {e}')
					raise ChordException()


	#@asyncio.coroutine
	async def lookup_for(self,key):
		await self.network_update()
		debug_log(self.nodeID, "looking up for data with key: " + str(key))
		target = self.localSuccNode(key)
		if  (target == self.nodeID):
			if (key in self.data.keys()):
				debug_log(self.nodeID,"data owner, returning data requested with key: " + str(key))
				return self.data[key]
			else:
				debug_log(self.nodeID, "requested data doesnt exist, key: " + str(key))
				dat = await self.scrap(key)
				if not (dat is None):
					self.data[key] = dat
					await self.sav_to_bd(key)
				return dat
		else: 
			if (target not in self.reachableNodes.keys()):
				return None
			dest_addr = self.reachableNodes[target]
			debug_log(self.nodeID,"sending looking up requested with key: " + str(key) + " to node with ID " + str(target))
			try:
				remote_peer = await asyncio.wait_for(self.container.connect(dest_addr), timeout=self.timeout)  
				ans = await asyncio.wait_for(remote_peer.lookup_routine(key), timeout=self.timeout) 
			except Exception as e:
				error_log(f'line 153: {e}')
				raise ChordException()
			return ans


	#@asyncio.coroutine
	async def put_data(self, key, data, replication = True):
		await self.network_update()
		debug_log(self.nodeID, "putting for data with key: " + str(key))
		target = self.localSuccNode(key)
		if (target == self.nodeID):
			debug_log(self.nodeID,"data destination reached, saving data with key: " + str(key))
			self.data[key] = data
			await self.sav_to_bd(key)

			#Replicate data received
			if (replication and self.replicas >= 1):
				await self.replicate(key,data, self.replicas)
				return
			return
			
		if (target not in self.reachableNodes.keys()):  #should be Impossible
			raise ChordException()

		dest_addr = self.reachableNodes[target]
		debug_log(self.nodeID,"sending put data request with key: " + str(key) + " to node with ID " + str(target))
		try:
			remote_peer = await asyncio.wait_for(self.container.connect(dest_addr), timeout=self.ping_timeout)  
			await remote_peer.put_data_routine(key, data, replication)
		except Exception as e:
			error_log(f'line 183: {e}')
			raise ChordException()


	#@asyncio.coroutine
	async def transfer_data(self, dest=None, transfer_all = True, replication = False):
		if (dest is None):
			for key,data in self.data.items():
				debug_log(self.nodeID , "tem: " + str(key))
				target = self.localSuccNode(key)
				debug_log(self.nodeID, "target: " + str(target))
				if (target != self.nodeID):
					await self.put_data(key, data, replication)

		elif(not transfer_all):
			for key,data in self.data.items():
				target = self.localSuccNode(key)
				if (dest != self.nodeID and dest == taget):
					await self.put_data(key, data, replication)

		elif (dest in self.reachableNodes.keys() and dest in self.nodeSet):
			dest_addr = self.reachableNodes[dest]

			try:
				remote_peer = await asyncio.wait_for(self.container.connect(dest_addr), timeout=self.ping_timeout)  
			except Exception as e:
				error_log(f'line 209: {e}')
				raise Timeout_Error(self.timeout)

			for key,data in self.data.items():
				debug_log(self.nodeID,"sending transfer data with key: " + str(key) + " to node with ID: " + str(dest))
				try:
					await remote_peer.transfer_data_routine(key,data,replication)
				except Exception as e:
					error_log(f'line 217: {e}')
					raise TransferDataError(dest)
		else:
			debug_log(self.nodeID, "destination not found, predecessor ID: " + str(dest))


	#@asyncio.coroutine
	async def sav_to_bd(self, key):
		debug_log(self.nodeID,"saving data to bd for the url: " + str(key))
		ret = None
		dest_addr = self.bd_address
		try:
			remote_peer = await asyncio.wait_for(self.container.connect(dest_addr), timeout=2)
		except Exception as e:
			error_log(f'line 231: {e}')
			raise Timeout_Error(self.timeout)

		try:
			ret = await asyncio.wait_for(remote_peer.request_handler("add", key = str(key), data = str(self.data[key])), timeout=self.timeout*6)
		except Exception as e:
			error_log(f'line 237: {e}')
			raise ChordException() 
		
		return ret


	#@asyncio.coroutine
	async def load_from_bd(self):
		debug_log(self.nodeID,"loading all data from the bd with addr: " + str(self.bd_address))
		ret = None
		dest_addr = self.bd_address
		try:
			remote_peer = await asyncio.wait_for(self.container.connect(dest_addr), timeout=2)
			ret = await asyncio.wait_for(remote_peer.request_handler("get_all"), timeout=15)
		except Exception as e:
			error_log(f'line 252: {e}')
			await self.leave()

		if (ret is not None):
			self.data = ret


	#@asyncio.coroutine
	async def scrap(self,url):
		debug_log(self.nodeID,"calling scraper for the url: " + str(url))
		html = None
		dest_addr = self.scrapper_address
		try:
			remote_peer = await asyncio.wait_for(self.container.connect(dest_addr), timeout=2)
		except Exception as e:
			error_log(f'line 267: {e}')
			await self.leave()
		try:
			html = await asyncio.wait_for(remote_peer.request_handler("scrap",url), timeout=17)
		except Exception as e:
			error_log(f'line 272: {e}')
			return None
		
		return html


	#@asyncio.coroutine
	async def replicate(self, key, data, rep):
		if (rep != self.replicas):
			self.data[key] = data
			await self.sav_to_bd(key)
		
		if(rep == 0): return
		
		if (self.FT[0] == self.nodeID):
			return
		dest_addr = self.reachableNodes[self.FT[0]]
		try:
			remote_peer = await asyncio.wait_for(self.container.connect(dest_addr), timeout=self.ping_timeout)  
		except Exception as e:
			error_log(f'line 292: {e}')
			raise Timeout_Error(self.timeout)
		try:
			await remote_peer.replication_routine(key, data, rep-1)
		except Exception as e:
			error_log(f'line 297: {e}')
			raise TransferDataError(self.FT[0])


	#@asyncio.coroutine
	async def ping_all(self):
		new_nodes = []
		for key in self.reachableNodes.keys():
			if (key == self.nodeID): continue
			dest_addr = self.reachableNodes[key] 
			try:
				remote_peer = await asyncio.wait_for(self.container.connect(dest_addr), timeout=2)  
				await asyncio.wait_for(remote_peer.ping(), timeout=2)  
			except Exception as e:
				error_log(f'line 311: {e}')
				if (key in self.nodeSet):
					self.unreachableNode(key, dest_addr)
				continue
			
			if(key not in self.nodeSet):
				self.addNode(key,self.reachableNodes[key])
				new_nodes.append(key)
		return new_nodes


	#@asyncio.coroutine
	async def retry_updates(self):
		for i in self.pending_updates:
			if i in self.nodeSet:
				dest_addr = self.reachableNodes[i]
				try:
					remote_peer = await asyncio.wait_for(self.container.connect(dest_addr), timeout=self.timeout)  
					debug_log(self.nodeID,"sending update after join to a previously unreachable node: " + str(i))
					ans = await asyncio.wait_for(remote_peer.join_update(self.nodeID,self.node_address), timeout=self.timeout)  
				except Exception as e:
					error_log(f'line 332: {e}')
					raise ChordException()
				self.pending_updates.remove(i)


	#@asyncio.coroutine
	async def check_backups(self):
		for key in self.data.keys():
			if self.FT[0] not in self.nodeSet:
				self.recomputeFingerTable()
				dest_addr = self.reachableNodes[self.FT[0]]
				try:
					remote_peer = await asyncio.wait_for(self.container.connect(dest_addr), timeout=self.timeout)  
					debug_log(self.nodeID,"sending data backup after founding an unreachable node, backup to: " + str(self.FT[0]))
					ans = await asyncio.wait_for(remote_peer.transfer_data_routine(key,self.data[key]), timeout=self.timeout)  
				except Exception as e:
					error_log(f'line 348: {e}')
					raise ChordException()


	#@asyncio.coroutine
	async def check_succ(self):
		for key, data in self.data.items():
			if (self.localSuccNode(key) not in self.nodeSet):
				await self.replicate(key, data, self.replicas)


	#@asyncio.coroutine
	async def network_update(self):
		new_nodes = await self.ping_all()
		await self.retry_updates()
		await self.check_backups()
		await self.check_succ()
		self.recomputeFingerTable()
		for i in new_nodes:
			self.transfer_data(i, transfer_all=False, replication=True)


	###############################################
	### Functions for maintaining Chord's logic ###
	###############################################

	def inbetween(self, key, lwb, upb):
		if lwb <= upb:                                                            
			return lwb <= key and key < upb                                        
		else:                                                                     
			return (lwb <= key and key < upb + self.MAXPROC) or (lwb <= key + self.MAXPROC and key < upb)                        


	def addNode(self, nodeID, node_address):  
		self.lock.acquire()                                                
		self.nodeSet.append(int(nodeID))                                          
		self.nodeSet = list(set(self.nodeSet))                                    
		self.nodeSet.sort()
		self.reachableNodes[nodeID] = node_address       
		self.lock.release()                               


	def delNode(self, nodeID, node_address):
		self.lock.acquire()                                           
		assert nodeID in self.nodeSet, ''                                         
		del self.nodeSet[self.nodeSet.index(nodeID)]                              
		self.nodeSet.sort()         
		self.reachableNodes.pop(nodeID)                                              
		self.lock.release()


	def unreachableNode(self, nodeID, node_address):       
		self.lock.acquire()                                           
		assert nodeID in self.nodeSet, ''                                         
		del self.nodeSet[self.nodeSet.index(nodeID)]                              
		self.nodeSet.sort()                                                    
		self.lock.release()


	def finger(self, i):
		succ = (self.nodeID + pow(2, i-1)) % self.MAXPROC    # succ(p+2^(i-1))
		lwbi = self.nodeSet.index(self.nodeID)               # own index in nodeset
		upbi = (lwbi + 1) % len(self.nodeSet)                # index next neighbor
		for k in range(len(self.nodeSet)):                   # go through all segments
			if self.inbetween(succ, self.nodeSet[lwbi]+1, self.nodeSet[upbi]+1):
				return self.nodeSet[upbi]                        # found successor
			(lwbi,upbi) = (upbi, (upbi+1) % len(self.nodeSet)) # go to next segment
		return None                                                                


	def recomputeFingerTable(self):
		self.lock.acquire()
		self.FT[0]  = self.nodeSet[self.nodeSet.index(self.nodeID)-1] # Predecessor
		self.FT[1:] = [self.finger(i) for i in range(1,self.nBits+1)] # Successors
		if (len(self.nodeSet)==1):
			self.FT = [self.nodeID for i in range(self.nBits+1)]
		self.lock.release()


	def localSuccNode(self, url_key): 
		key = self.get_key_from_url(url_key)
		if (len(self.nodeSet)==1): return self.FT[0] 
		if self.inbetween(key, self.FT[0]+1, self.nodeID+1): # key in (FT[0],self]
			return self.nodeID                                 # node is responsible
		elif self.inbetween(key, self.nodeID+1, self.FT[1]): # key in (self,FT[1]]
			return self.FT[1]                                  # successor responsible
		for i in range(1, self.nBits+1):                     # go through rest of FT
			if self.inbetween(key, self.FT[i], self.FT[(i+1) % self.nBits]):
				return self.FT[i]                                # key in [FT[i],FT[i+1]) 
		return key   #rare case
	

	def get_key_from_url(self, url):
		return int(hashlib.sha256(url.encode()).hexdigest(), 16) % self.MAXPROC
		

	def delOldData(self):  #Delete data that doesnt belong to this node or his predecessor anymore
		for key in self.data.keys():
			successor = localSuccNode(key)
			if(successor != self.nodeID and successor != self.FT[0]):
				self.data.pop(key)


	async def terminate(self):

		remote_peer = await asyncio.wait_for(self.container.connect(self.bd_address), timeout=self.timeout)
		remote_peer.request_handler('leave')

		remote_peer = await asyncio.wait_for(self.container.connect(self.scrapper_address), timeout=self.timeout)
		remote_peer.request_handler('leave')

		debug_log(self.node_address,"shuting down chord node")
		self.container.shutdown()
		asyncio.get_event_loop().stop()


	#####################################################
	### RPC functions for maintaining Chord's network ###
	#####################################################

	@aiomas.expose
	def recv_join_request(self, addr):
		null_return = None,None,None
		new_key = self.generate_key(addr)
		if (new_key is None): return null_return     
		return new_key, self.reachableNodes, self.nodeID

	@aiomas.expose
	async def join_update(self,nodeID,addr):
		self.addNode(nodeID,addr)
		self.recomputeFingerTable()
		debug_log(self.nodeID,'updating join for: NodeID>>' + str(nodeID) + " addr: "+ str(addr))
		debug_log("nodeSet",str(self.nodeSet))
		debug_log("FT",str(self.FT))
		await self.transfer_data()

	@aiomas.expose
	async def leave_update(self,nodeID,addr):
		if (nodeID in self.nodeSet):
			self.delNode(nodeID,addr)
			self.recomputeFingerTable()
			debug_log(self.nodeID,'updating leave for: NodeID>>' + str(nodeID) + " addr: "+ str(addr))
		else:
			debug_log(self.nodeID, "the node that requested to leave was already removed: " + str(nodeID))
		self.recomputeFingerTable()
		debug_log("nodeSet",str(self.nodeSet))
		await self.transfer_data()

	@aiomas.expose
	def lookup_routine(self,key):
		return self.lookup_for(key)

	@aiomas.expose
	def put_data_routine(self,key,data,rep):
		return self.put_data(key,data,rep)

	@aiomas.expose
	def replication_routine(self,key,data,rep):
		return self.replicate(key,data,rep)

	@aiomas.expose
	async def transfer_data_routine(self,key,data,rep=None):
		if key not in self.data.keys():
			self.data[key] = data
			await self.sav_to_bd(key)

	@aiomas.expose
	def ping(self):
		return "ACK"


		######################
		### Helper methods ###
		######################

	def generate_key(self,address):
		if (len(self.reachableNodes.keys())>=self.MAXPROC):
			return None
		id = 0
		while (id in self.reachableNodes.keys() or id == self.nodeID):
			id = random.choice(list(set([i for i in range(self.MAXPROC)]))) # + 1
		return id
	

		########################
		### Chord Exceptions ###
		########################

class ChordException(Exception):
	def __init__(self,msg = 'UNKNOWN Exception'):
		self.msg = msg
		error_log(str(msg))
		super().__init__(msg)

class JoinChordError(ChordException):
	def __init__(self, addr = 'unknown', message="error trying to join an existing chord network at: "):
		self.message = message + str(addr)
		super().__init__(self.message)

class TransferDataError(ChordException):
	def __init__(self, nID = 'unknown', message="error trying to transfer helded data to the node with ID: "):
		self.message = message + str(nID)
		super().__init__(self.message)

class Timeout_Error(ChordException):
	def __init__(self, timeout = 0, message="timeout after waiting for reply for "):
		self.message = message + str(timeout) + " seconds"
		super().__init__(self.message)