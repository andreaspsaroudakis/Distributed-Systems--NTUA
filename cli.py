import click
import functools
import time
from hashlib import sha1
from help_scripts import helper
from client import Client

@click.group()
def main():
	pass

@main.command()
@click.option('--ip', default = '127.0.0.1', help = "Server's ip address")
@click.option('--port', '-p', type = int, required = True, help = "Server's port")
@click.option('--consistency', '-c', type = click.Choice(['linearizability', 'eventual']), required = True, help = "Type of consistency")
@click.argument('key',  type = str, required = True, nargs = -1)
@click.argument('value',  type = int, required = True)
def insert(ip, port, key, value, consistency):                                               # Einai etoimh
	'''
	Inserts a new (key,value) pair
	'''
	key = " ".join(key)															# Tragoudia me perissoteres apo mia lexeis
	if key.isnumeric():
	    click.echo("Error: Invalid value for 'KEY': {} is not a valid string".format(key))
	else:
		client = Client(ip,port)
		if consistency == 'linearizability':
			client.send_message('insert_linearizability:{}:{}'.format(key,str(value)))
		else:
			client.send_message('insert_eventual:{}:{}'.format(key,str(value)))
		print(client.receive_message().decode())
		client.close_socket()

@main.command()																	# Einai etoimh
@click.option('--ip', default = '127.0.0.1', help = "Server's ip address")
@click.option('--port', '-p', type = int, required = True, help = "Server's port")
@click.option('--consistency', '-c', type = click.Choice(['linearizability', 'eventual']), default = 'linearizability', help = "Type of consistency")
@click.argument('key', type = str, nargs = -1)
def query(ip, port, key, consistency):
	'''
	Searches for the specified key
	'''
	server_id = ip + ":" + str(port)
	client = Client(ip,port)
	key = " ".join(key)
	if key == '*':
		starter_hash = sha1(server_id.encode('utf-8')).hexdigest()
		client.send_message('print_chord_data:{}'.format(starter_hash))
		answer = client.receive_message().decode()
		servers_data = answer.split(":")[2:]
		chord_data = [":".join(servers_data[i:i+3]) for i in range(0,len(servers_data),3)]
		for i in chord_data:
			temp = i.split(":")
			res = ":".join(temp[:2]),":".join(temp[2:])
			print(res[0] + ": " + res[1])
	else:
		if consistency == 'linearizability':
			client.send_message('query_linearizability:{}:None'.format(key))
		else:
			client.send_message('query_eventual:{}'.format(key))
		print(client.receive_message().decode())
	client.close_socket()

@main.command()
@click.option('--ip', default = '127.0.0.1', help = "Server's ip address")
@click.option('--port', '-p', type = int, required = True, help = "Server's port")
@click.argument('key',  type = str, required = True, nargs = -1)
def delete(ip, port, key):
	'''
	Deletes the (key,value) pair of the specified key
	'''
	key = " ".join(key)
	if key.isnumeric():
		click.echo("Error: Invalid value for 'KEY': {} is not a valid string".format(key))
	else:
		server_id = ip + ":" + str(port)
		starter_hash = sha1(server_id.encode('utf-8')).hexdigest()
		data = 'delete' + ":" + key + ":" + str(3) + ":" + starter_hash
		client = Client(ip,port)
		client.send_message(data)
		print(client.receive_message().decode())
		client.close_socket()

@main.command()																	# Einai etoimh
@click.option('--ip', default = '127.0.0.1', help = "Server's ip address")
@click.option('--port', '-p', type = int, required = True, help = "Server's port")
def overlay(ip,port):
	'''
	Displays the network topology
	'''
	server_id = ip + ":" + str(port)
	starter_hash = sha1(server_id.encode('utf-8')).hexdigest()
	client = Client(ip,port)
	client.send_message('overlay:{}:{}'.format(starter_hash,server_id))
	answer = client.receive_message().decode()
	client.close_socket()
	server_ids = answer.split(":")[2:]
	overlay_network = [":".join(server_ids[i:i+2]) for i in range(0,len(server_ids),2)]
	for i in overlay_network[:-1]:
		print("{} --> ".format(i), end=" ")
	print(overlay_network[-1])

@main.command()
@click.option('--ip', default = '127.0.0.1', help = "Server's ip address")
@click.option('--port', '-p', type = int, required = True, help = "Server's port")
def depart(ip, port):															# Einai etoimh
	'''
	Removes a node from the Chord ring
	'''
	client = Client(ip,port)
	client.send_message('depart')
	print(client.receive_message().decode())
	client.close_socket()

@main.command()
def help():
	'''
	Displays a description of the available commands
	'''
	helper()

if __name__ == '__main__':
	main()
