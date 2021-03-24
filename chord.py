import threading
import sys
from server import Server

def create_bootstrap(port, num_replicas):                                       # h porta toy, kai to k
    b_server = Server(port, -1, -1, num_replicas)
    b_server.connection_thread.start()

def join(port, bootstrap_ip, bootstrap_port, num_replicas):                     # h porta toy, h ip toy bootstrap, h port toy bootstrap kai to k
    server = Server(port, bootstrap_ip, bootstrap_port, num_replicas)
    server.connection_thread.start()
    server.join_chord()

if __name__ == '__main__':
    num_of_arguments = len(sys.argv) - 1
    if num_of_arguments == 2:
        create_bootstrap(sys.argv[1],sys.argv[2])
    else:
        join(int(sys.argv[1]),sys.argv[2],int(sys.argv[3]),sys.argv[4])

# python chord.py 5001 3
# python chord.py 5002 192.168.0.4 5001 3
