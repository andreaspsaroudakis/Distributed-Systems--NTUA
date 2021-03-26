import threading
import sys
from server import Server

def create_bootstrap(port, num_replicas):
    b_server = Server(port, -1, -1, num_replicas)                           # Creates the Bootstrap Server and defines the replication factor
    b_server.connection_thread.start()                                      # Opens a thread to wait for new connections

def join(port, bootstrap_ip, bootstrap_port, num_replicas):
    server = Server(port, bootstrap_ip, bootstrap_port, num_replicas)       # Creates a new Server. Defines its port, bootstrap ip, bootstrap port and replication factor
    server.connection_thread.start()                                        # Opens a thread to wait for new connections
    server.join_chord()                                                     # Join the chord ring

if __name__ == '__main__':
    num_of_arguments = len(sys.argv) - 1
    if num_of_arguments == 2:                                               # 2 arguments: boostrap port, replication factor
        create_bootstrap(sys.argv[1],sys.argv[2])
    else:                                                                   # 4 arguments: server port, bootstrap ip, bootstrap port, replication factor
        join(int(sys.argv[1]),sys.argv[2],int(sys.argv[3]),sys.argv[4])

'''
# Example of chord ring with 2 nodes
python chord.py 5001 3                     # Creation of bootstrap node
python chord.py 5002 192.168.0.4 5001 3    # Creation of second node
'''
