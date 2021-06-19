def get_ip(interface):
	from netifaces import interfaces, ifaddresses, AF_INET
	return ifaddresses(interface)[AF_INET][0]['addr']