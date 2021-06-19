from Nodes.logger import *
import sqlite3 as sl
import asyncio
import aiomas


class BDNode(aiomas.Agent):

	def __init__(self, container, node_address, chord_node_port):
		super().__init__(container)
		self.node_address =  str(node_address)            
		self.bd_route = 'chord_' + str(chord_node_port) + '.db'
		self.connection = None


	def create_table(self, load = False):              #Drop Table if the load option is disabled
		self.connection = sl.connect(self.bd_route)
		cursor = self.connection.cursor()
		if (not load):
			cursor.execute("DROP TABLE IF EXISTS SCRAPS")
			cursor.execute('CREATE TABLE SCRAPS (URL TEXT RIMARY KEY NOT NULL,  HTML  TEXT  NOT NULL);')
		self.connection.close()


	def add(self, key, data):                           #Overwrittes if the URL already exists
		self.connection = sl.connect(self.bd_route)
		cursor = self.connection.cursor()
		sel = cursor.execute('SELECT URL FROM SCRAPS where URL = (?)', (key,))
		count = 0
		for i in sel:
			count += 1
		if(count==0):
			try:
				cursor.execute("INSERT INTO SCRAPS VALUES (?,?)", (key, data))
			except Exception as e:
				print(e)
		else:
			cursor.execute('UPDATE SCRAPS set HTML = ? where URL = ?', (data,key))
		self.connection.commit()
		self.connection.close()
		return data


	def delete(self, key):
		self.connection = sl.connect(self.bd_route)
		cursor = self.connection.cursor()
		cursor.execute('DELETE FROM SCRAPS where URL = ?',(key,))
		self.connection.commit()
		self.connection.close()


	def get(self, key):                         #Get the HTML under the URL key
		self.connection = sl.connect(self.bd_route)
		cursor = self.connection.cursor()
		answer = cursor.execute('SELECT * FROM SCRAPS where URL = ?', (key,))
		
		count = 0  #THIS COUNT SHOULD NEVER BE MORE THAN 1
		for row in answer:
			count+=1
			answer = row[1]

		self.connection.commit()
		self.connection.close()
		if count == 0:
			return None
		return answer


	def get_all(self):                          #Returns all DB entries in a dictionary format with key:URL, data: HTMl
		self.connection = sl.connect(self.bd_route)
		cursor = self.connection.cursor()
		answer = cursor.execute('SELECT * FROM SCRAPS;')
		dict = {}
		for row in answer:
			k = row[0]
			d = row[1]
			dict[k] = d
		self.connection.commit()
		self.connection.close()
		return dict


	def clear(self):                            #Erase all entries from the table
		self.connection = sl.connect(self.bd_route)
		cursor = self.connection.cursor()
		cursor.execute('DELETE FROM SCRAPS;')
		self.connection.commit()
		self.connection.close()


	def exit(self):
		debug_log(self.node_address, f'shuting down Data Base node')
		self.container.shutdown()
		asyncio.get_event_loop().stop()


	@aiomas.expose
	def request_handler(self, request_type, addr=None, key=None, data=None):
		
		result = None

		if(request_type == "add"):
			result =  self.add(key,data)

		elif(request_type == "get"):
			result = self.get(key)

		elif(request_type == "get_all"):
			result =  self.get_all()

		elif(request_type == "delete"):
			result =  self.delete(key)

		elif(request_type == "clear"):
			result =  self.clear()

		elif request_type == 'leave':
			self.exit()

		else:
		   #debug_log("error","invalid request type")
		   print ("ERROR")

		return result