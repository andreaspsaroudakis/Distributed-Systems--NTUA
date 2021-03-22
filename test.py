import time
import random
import sys
from client import Client

binded_ip_ports = [('192.168.0.4',5001),('192.168.0.4',5002),('192.168.0.1',5003),('192.168.0.1',5004),('192.168.0.2',5005),('192.168.0.2',5006),('192.168.0.3',5007),('192.168.0.3',5008),('192.168.0.5',5009),('192.168.0.5',5010)]

def insert(ip, port, key, value, consistency):
	client = Client(ip,port)
	if consistency == 'linearizability':
		client.send_message('insert_linearizability:{}:{}'.format(key,str(value)))
	else:
		client.send_message('insert_eventual:{}:{}'.format(key,str(value)))
	print(client.receive_message().decode())
	client.close_socket()

def query(ip, port, key, consistency):
	server_id = ip + ":" + str(port)
	client = Client(ip,port)
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

def write_throughput(consistency, file_name = 'insert.txt'):
    file = open(file_name, "r")
    lines = file.readlines()
    cnt = 0
    start_time = time.time()
    for line in lines:
        key, value = line.rstrip("\n").split(", ")
        random_ip_port = random.choice(binded_ip_ports)
        insert(random_ip_port[0],random_ip_port[1],key,value,consistency)
        cnt += 1
    write_time = time.time() - start_time
    file.close()
    print("Write throughput is {}".format((write_time / cnt)))
    return

def read_throughput(consistency, file_name = 'query.txt'):
    file = open(file_name, "r")
    lines = file.readlines()
    cnt = 0
    start_time = time.time()
    for line in lines:
        key = line.rstrip("\n")
        random_ip_port = random.choice(binded_ip_ports)
        query(random_ip_port[0],random_ip_port[1],key,consistency)
        cnt += 1
    read_time = time.time() - start_time
    file.close()
    print("Read throughput is {}".format((read_time / cnt)))
    return

def execute_requests(consistency,file_name = 'requests.txt'):
    file = open(file_name, "r")
    lines = file.readlines()
    for line in lines:
        request = line.rstrip("\n").split(", ")
        random_ip_port = random.choice(binded_ip_ports)
        if request[0] == 'insert':
            insert(random_ip_port[0],random_ip_port[1],request[1],request[2],consistency)
        elif request[0] == 'query':
            query(random_ip_port[0],random_ip_port[1],request[1],consistency)
        else:
            raise TypeError("Request is not valid")
    file.close()
    return

if __name__ == '__main__':
	consistency = sys.argv[1]
	if consistency != 'linearizability' and consistency != 'eventual':
		raise TypeError("Invalid type of consistency!")
	if sys.argv[2] == 'insert.txt':
	    write_throughput(consistency)
	elif sys.argv[2] == 'query.txt':
	    read_throughput(consistency)
	elif sys.argv[2] == 'requests.txt':
	    execute_requests(consistency)
	else:
	    raise TypeError("File does not exist")

# python test.py linearizability insert.txt
# python test.py eventual insert.txt
