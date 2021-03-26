import sys
import socket
import threading
from hashlib import sha1
from help_scripts import getIP
from client import Client

class Server(object):
    def __init__(self,port,bootstrap_ip,bootstrap_port,num_replicas):           # server arguments: server port, bootstrap ip, boostrap port, replication factor

        self.my_port = int(port)
        self.my_ip = getIP()

        self.bootstrap_ip = bootstrap_ip
        self.bootstrap_port = int(bootstrap_port)

        self.num_replicas = int(num_replicas)

        self.server_id = self.my_ip + ":" + str(self.my_port)
        self.my_hash = sha1(self.server_id.encode('utf-8')).hexdigest()

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.my_ip,self.my_port))
        self.server_socket.settimeout(1)
        self.server_socket.listen(10)

        self.previous_ip = self.my_ip
        self.previous_port = self.my_port
        self.previous_hash = self.my_hash

        self.next_ip = self.my_ip
        self.next_port = self.my_port
        self.next_hash = self.my_hash

        self.my_data = {}
        self.connection_thread = threading.Thread(target=self.connection)
        self.thread_list = []

        self.stop_connection = False

        # Command dictionary

        self.commands = {
                           'join': self.join_approval,
                           'next': self.update_previous,
                           'previous': self.update_next,
                           'insert_linearizability': self.insert_linearizability,
                           'insert_eventual': self.insert_eventual,
                           'query_linearizability': self.query_linearizability,
                           'query_eventual': self.query_eventual,
                           'depart': self.depart,
                           'delete': self.delete_data,
                           'overlay': self.overlay,
                           'replication': self.add_replicas,
                           'replication_lazy': self.add_replicas_lazy,
                           'give_data': self.give_data,
                           'belongs_to_you': self.belongs_to_you,
                           'has_replica': self.has_replica,
                           'is_last_replica': self.is_last_replica,
                           'get_network_size': self.get_network_size,
                           'delete_excess_replica': self.delete_excess_replica,
                           'print_chord_data': self.print_chord_data
                        }

    def receive_data(self,conn):
        '''
        Receives messages and executes the corresponding commands according to the above dictionary
        '''
        while True:
            try:
                data = conn.recv(1024)
                data = data.decode()
            except socket.error:
                print("Data was not received")
            else:
                if not data:
                    break
                else:
                    if data != 'close socket':
                        close_socket = False
                        function = self.commands.get(data.split(":")[0])
                        answer = function(data)
                    else:
                        close_socket = True
                        answer = "Socket was closed"
                    try:
                        if answer:
                            conn.send(answer.encode('utf-8'))
                        if close_socket:
                            conn.close()
                            break
                    except socket.error:
                        print("Answer failed")

    def connection(self):
        '''
        Waits for new incoming connections to accept
        '''
        print("Server started listening on {}".format(self.server_id))
        while True:
            if self.stop_connection:
                break
            else:
                try:
                    conn, _ = self.server_socket.accept()
                except socket.timeout:
                    pass
                else:
                    self.thread_list.append(threading.Thread(target=self.receive_data, args=(conn,)))
                    self.thread_list[-1].start()

    def belongs_to_me(self,hash):
        '''
        Checks if the hash that is being examined belongs to the node. If so it returns True, else it reutrns False.
        '''
        return (self.previous_hash < hash <= self.my_hash) or (hash <= self.my_hash <= self.previous_hash) or (self.my_hash <= self.previous_hash <= hash)

    def join_approval(self,data):
        '''
        This function is initially being exetuded by Bootstrap node. If Bootstrap is responsible for the new node's hash he informs the node about his next and previous indexes.
        If not, he forwards the "join" request to the next server (that is in front of him) in order to make him check the node's hash. This process goes on until a server
        is found to be responsible for the hash of the new node's id. Then, the latter responds to the node that wants to enter the ring and informs him about his next and
        previous nodes in the Chord.
        '''
        _, hash = data.split(":")
        if self.belongs_to_me(hash):
            answer = ':'.join((self.previous_ip, str(self.previous_port), self.previous_hash, self.my_ip, str(self.my_port), self.my_hash))
        else:
            answer = self.send_next(data)
        return answer

    def join_chord(self):
        '''
        This function is being executed by the node that wants to enter the Chord ring. First, a "join" messsage is being sent to the Bootstrap sever, who checks if the
        new node must be placed behind him. If Boostrap is not responsible for the new node's hash, he forwards the request to the next server in order to make him check
        if new node has to be placed behind him. This process goes on until an existing server is found to be responsible for the new node's hash. Then the latter responds
        to the node that wants to enter the ring and informs him about his next and previous nodes in Chord. Then, the new node, in turn, informs his next and previous nodes
        to update their indexes. Finally, he sends a message to his next node to ask to get data he might be responsible for.
        '''
        client = Client(self.bootstrap_ip,self.bootstrap_port)
        client.send_message('join:{}'.format(self.my_hash))
        self.previous_ip, self.previous_port, self.previous_hash, self.next_ip, self.next_port, self.next_hash = client.receive_message().decode().split(":")
        self.previous_port = int(self.previous_port)
        self.next_port = int(self.next_port)
        client.close_socket()
        self.inform_previous()
        self.inform_next()
        network_size = self.send_next('get_network_size:1:{}'.format(self.my_hash))
        self.send_next('give_data:{}'.format(network_size))

    def get_network_size(self,data):
        '''
        data = get_network_size:num_of_nodes:starter_hash
        This function is being executed by a new node that enters the Chord ring in order him to be informed about the number of nodes that exist in the network.
        The caller sets the number of nodes variable equal to 1 and forwards a message to the next node. The latter checks if his next node is the one that started this
        whole process (new node). If it is so, he returns the network size to the node that joined the ring, else he increases the num_of_nodes by 1 and forwards a new message
        to his next node to repeat the same process.
        '''
        _, num_of_nodes, starter_hash = data.split(":")
        num_of_nodes = str(int(num_of_nodes) + 1)
        if self.next_hash == starter_hash:
            answer = num_of_nodes
        else:
            answer = self.send_next('get_network_size:{}:{}'.format(num_of_nodes,starter_hash))
        return answer

    def inform_previous(self):
        '''
        This function is being executed by a new node that enters the Chord ring in order to inform his previous node to change his next indexes.
        '''
        client = Client(self.previous_ip,self.previous_port)
        client.send_message('next:{}:{}:{}'.format(self.my_ip,self.my_port,self.my_hash))
        answer = client.receive_message().decode()
        client.close_socket()
        return answer

    def inform_next(self):
        '''
        This function is being executed by a new node that enters the Chord ring in order to inform his next node to change his previous indexes.
        '''
        client = Client(self.next_ip,self.next_port)
        client.send_message('previous:{}:{}:{}'.format(self.my_ip,self.my_port,self.my_hash))
        answer = client.receive_message().decode()
        client.close_socket()
        return answer

    def update_previous(self,data):
        '''
        data = next:next_ip:next_port:next_hash
        This function is being executed by the previous of a node that enters the Chord ring in order to update his next indexes.
        '''
        _, self.next_ip, self.next_port, self.next_hash = data.split(":")
        self.next_port = int(self.next_port)
        answer = "Update of previous node was successful"
        return answer

    def update_next(self,data):
        '''
        data = previous:previous_ip:previous_port:previous_hash
        This function is being executed by the next of a node that enters the Chord ring in order to update his previous indexes.
        '''
        _, self.previous_ip, self.previous_port, self.previous_hash = data.split(":")
        self.previous_port = int(self.previous_port)
        answer = "Update of next node was successful"
        return answer

    def send_next(self,data):
        '''
        This function is being executed by a node that wants to forward a message to his next node.
        '''
        client = Client(self.next_ip,self.next_port)
        client.send_message(data)
        answer = client.receive_message().decode()
        client.close_socket()
        return answer

    def send_previous(self,data):
        '''
        This function is being executed by a node that wants to forward a message to his previous node.
        '''
        client = Client(self.previous_ip,self.previous_port)
        client.send_message(data)
        answer = client.receive_message().decode()
        client.close_socket()
        return answer

    def add_replicas(self,data):
        '''
        data = replication:key:value:num_replicas:starter_hash
        This function is being executed by the next k-1 nodes of the successor of a key (if there are at least k nodes in the network), in order them to add a replica
        of the key. It is initially called by the next of the primary node, after the insertion of the key to the latter. It is used for linearizability consistency.
        '''
        temp = data.split(":")[1:]
        keys, value, num_replicas, starter_hash = temp[:-3], temp[-3], temp[-2], temp[-1]
        if len(keys) == 1:
            key = keys[0]
        else:
            key = ":".join(keys)
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        key_value = self.my_data.get(key_hash)
        if key_value == None or key_value[1] != value:                                  # If the node doesn't have the key or has it with an old value
            self.my_data[key_hash] = (key,value)                                            # insert a replica of the (key,value) pair
            num_replicas = str(int(num_replicas)-1)                                         # decrease the replication factor by 1
            print("Node {} added a replica of key {}".format(self.server_id,key))
        if num_replicas == '0' or self.next_hash == starter_hash:                       # If there are no more replicas to add or next node is the primary node
            answer = "Key-value pair ({},{}) was inserted successfully".format(key,value)   # (key,value) pair has been placed successfuly in the network
        else:                                                                           # else (we have to add more replicas)
            answer = self.send_next('replication:{}:{}:{}:{}'.format(key,value,num_replicas,starter_hash)) # forward a message to next node to add a replica
        return answer

    def add_replicas_lazy(self,data):
        '''
        data = replication_lazy:key:value:num_replicas:starter_hash
        This function is being executed by the next k-1 nodes of the successor of a key (if there are at least k nodes in the network), in order them to add a replica
        of the key. It is initially called by the next of the primary node, after the insertion of the key to the latter. It is used for eventual consistency. The only
        difference from the previous implementation (add_replicas), is that we have to open a new thread each time we forward a message to the next node to add a replica.
        This allows the nodes to be able to handle requests before all copies have been added to the network
        '''
        temp = data.split(":")[1:]
        keys, value, num_replicas, starter_hash = temp[:-3], temp[-3], temp[-2], temp[-1]
        if len(keys) == 1:
            key = keys[0]
        else:
            key = ":".join(keys)
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        key_value = self.my_data.get(key_hash)
        if key_value == None or key_value[1] != value:
            self.my_data[key_hash] = (key,value)
            num_replicas = str(int(num_replicas)-1)
            print("Node {} added a replica of key {}".format(self.server_id,key))
        if num_replicas == '0' or self.next_hash == starter_hash:
            answer = "Key-value pair ({},{}) was inserted successfully".format(key,value)
        else:
            data = 'replication_lazy:{}:{}:{}:{}'.format(key,value,num_replicas,starter_hash)
            self.thread_list.append(threading.Thread(target=self.send_next, args=(data,)))
            self.thread_list[-1].start()
            answer = "Sent data to next"
        return answer

    def belongs_to_you(self,data):
        '''
        data = belongs_to_you:key_hash
        This function is very similar to "belongs_to_me". Checks if the hash that is being examined belongs to the node. If yes it returns True, else it returns False
        '''
        _, key_hash = data.split(":")
        if self.belongs_to_me(key_hash):
            answer = 'True'
        else:
            answer = 'False'
        return answer

    def has_replica(self,data):
        '''
        data = has_replica:key_hash
        Checks if the node has a replica of the key, whose hash is being examined. If yes it returns True, else it returns No
        '''
        _, key_hash = data.split(":")
        if self.my_data.get(key_hash) != None:
            answer = 'True'
        else:
            answer = 'False'
        return answer

    def is_last_replica(self,data):
        '''
        data = is_last_replica:key
        Checks if the node has the last replica of the key, whose hash is being examined
        '''
        _, key = data.split(":")
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        next_has_replica = self.send_next('has_replica:{}'.format(key_hash))                     # Check if the next node has a replica of the key
        belongs_to_next = self.send_next('belongs_to_you:{}'.format(key_hash))                   # Check if the next node is the successor of the key
        if next_has_replica == 'True' and belongs_to_next == 'False':                            # If the next node has a replica of the key but is not its successor
            answer = self.send_next(data)                                                            # node doesn't have the last replica of the (key,value) pair
        else:                                                                                    # else (node has the last replica in the chain)
            del self.my_data[key_hash]                                                               # so delete it since it is excess
            print("Last chain replica of key {} was deleted from node {}".format(key,self.server_id))# answer that the excess replica is deleted
            answer = "Last replica was deleted"
        return answer

    def delete_excess_replica(self,data):
        '''
        data = delete_excess_replica:key:remaining_replicas
        This function is being called by the next of a new node that joins the Chord ring. When a new node inserts the Chord ring, the next node checks if he has data
        that need to be sent back to the new node to be added. If this happens, the network gets an excess replica that needs to be deleted.
        '''
        _, key, remaining_replicas = data.split(":")
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        if remaining_replicas != '0':
            answer = self.send_next('delete_excess_replica:{}:{}'.format(key,int(remaining_replicas)-1))
        else:
            del self.my_data[key_hash]
            print("Last chain replica of key {} was deleted from node {}".format(key,self.server_id))
            answer = "Excess replica deleted"
        return answer

    def give_data(self,data):
        '''
        data = give_data:network_size
        This function is being executed by the next of a new node that enters the Chord ring. Checks if the new node has to take some of the (key,value) pairs and calls
        the "delete_excess_replica" function to delete the excess replica if needed.
        '''
        _, network_size = data.split(":")
        network_size = int(network_size)
        if network_size <= self.num_replicas:                                                                          # If number_of_nodes <= replication factor
            if bool(self.my_data):                                                                                       # If node has data
                for key, value in self.my_data.values():                                                                     # for each (key,value) pair he has
                    self.send_previous('replication:{}:{}:1:{}'.format(key,value,self.my_hash))                                  # inform previous (new node) to add a replica of the (key,value) pair
                print("Node {} gave all his (key,value) pairs to his previous node".format(self.server_id))                  # all data were given to the new node (each node in the network has the same data)
                answer = "Gave all (key,value) pairs"
            else:                                                                                                        # else (if the node doesn't have data)
                print("Node {} has no data to give to his previous node".format(self.server_id))                             # there is no data to give to the new node
                answer = "No data to give"
        else:                                                                                                          # else (number_of_nodes > replication factor)
            if bool(self.my_data):                                                                                       # If node has data
                for key_value in list(self.my_data.values()):                                                               # for each (key,value) pair he has
                    key_hash = sha1(key_value[0].encode('utf-8')).hexdigest()                                                   # calculate the hash of the key
                    if self.belongs_to_me(key_hash):                                                                            # if the key belongs to the node (primary node)
                        print("Key {} still belongs to node {}".format(key_value[0],self.server_id))                                # don't send data to the new node
                        answer = "Key still belongs to me"                                                                          # check the next (key,value) pair
                        continue
                    else:                                                                                                       # else (if the key doesn't belong to the node, so it's just a RM)
                        self.send_previous('replication:{}:{}:1:{}'.format(key_value[0],key_value[1],self.my_hash))                 # inform previous (new node) to add a replica of the (key,value) pair
                        belongs_to_previous = self.send_previous('belongs_to_you:{}'.format(key_hash))                              # check if the previous (new node) is the new primary node
                        if belongs_to_previous == 'True':                                                                           # if previous (new node) is the primary node
                            if self.num_replicas == 1:                                                                                  # if there is no replication (replication factor = 1)
                                del self.my_data[key_hash]                                                                                  # delete (key,value) pair
                                print("Node {} deleted key {}".format(self.server_id,key_value[0]))                                         # (key, value) pair was trasnffered to new node
                                answer = "(Key,value) pair was deleted"
                            else:                                                                                                       # else (replication factor > 1)
                                remaining_replicas = self.num_replicas - 2                                                                  # decrease replication factor by 2 and forward a "delete_excess_replica" request to new node
                                answer = self.send_next('delete_excess_replica:{}:{}'.format(key_value[0],remaining_replicas))
                        else:                                                                                                       # else (previous (new) is not primary, so he is somewhere in the replica chain)
                            next_has_replica = self.send_next('has_replica:{}'.format(key_hash))                                        # check if next node has a replica of the key
                            belongs_to_next = self.send_next('belongs_to_you:{}'.format(key_hash))                                      # check if next node is the primary of the key
                            if next_has_replica == 'True' and belongs_to_next == 'False':                                               # if next node has a replica of the key and is not the primary node
                                answer = self.send_next('is_last_replica:{}'.format(key_value[0]))                                          # find the last replica in the chain and delete it
                            else:                                                                                                       # else (node has the last replica in the chain)
                                del self.my_data[key_hash]                                                                                  # delete the (key,value) pair
                                print("Last chain replica of key {} was deleted from node {}".format(key_value[0],self.server_id))          # last (excess) replica was deleted
                                answer = "Last replica was deleted"
            else:
                print("Node {} has no data to give to his previous node".format(self.server_id))
                answer = "No data to give"
        return answer

    def insert_linearizability(self,data):
        '''
        This function is being initially executed by a random node that receives the "insert" request. It is used for linearizability consistency.
        '''
        temp = data.split(":")[1:]
        keys, value = temp[:-1], temp[-1]
        if len(keys) == 1:
            key = keys[0]
        else:
            key = ":".join(keys)
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        key_value = self.my_data.get(key_hash)
        if key_value != None and key_value[1] == value:                                           # If the node has the key with the inserted value
            answer = "Key-value pair already exists"                                                # don't insert it because it already exists with the correct value in all replicas (linearizability consistency)
        elif self.belongs_to_me(key_hash):                                                        # else (The node doesn't have the key or has it with an old value) if the node is the successor of the key (primary node)
            print("Successor node for key {} is {}".format(key,self.server_id))
            self.my_data[key_hash] = (key,value)                                                    # Insert (key,value) pair to the node
            remaining_replicas = self.num_replicas - 1                                              # Decrease the replication factor by 1
            if remaining_replicas != 0:                                                             # If there is replication (replication factor > 1)
                answer = self.send_next('replication:{}:{}:{}:{}'.format(key,value,remaining_replicas,self.my_hash)) # forward a message to next node to add replica
            else:                                                                                   # Else (if there is no replication)
                answer = "Key-value pair ({},{}) was inserted successfully".format(key,value)           # just insert the (key,value pair) and answer
        else:                                                                                     # else (the node is not the successor of the key) forward the "insert" request to the next node
            answer = self.send_next(data)
        return answer

    def insert_eventual(self,data):
        '''
        This function is being initially executed by a random node that receives the "insert" request. It is used for linearizability consistency. It is used for eventual consistency. The only
        difference from the previous implementation (add_replicas), is that we have to open a new thread each time we forward a message to the next node to add a replica.
        This allows the nodes to be able to handle requests before all copies have been added to the network
        '''
        temp = data.split(":")[1:]
        keys, value = temp[:-1], temp[-1]
        if len(keys) == 1:
            key = keys[0]
        else:
            key = ":".join(keys)
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        key_value = self.my_data.get(key_hash)
        if key_value != None and key_value[1] == value:
            answer = "Key-value pair already exists"
        elif self.belongs_to_me(key_hash):
            print("Successor node for key {} is {}".format(key,self.server_id))
            self.my_data[key_hash] = (key,value)
            remaining_replicas = self.num_replicas - 1
            if remaining_replicas != 0:
                data = 'replication_lazy:{}:{}:{}:{}'.format(key,value,remaining_replicas,self.my_hash)
                self.thread_list.append(threading.Thread(target=self.send_next, args=(data,)))
                self.thread_list[-1].start()
            answer = "Key-value pair ({},{}) was inserted successfully".format(key,value)
        else:
            answer = self.send_next(data)
        return answer

    def delete_data(self,data):
        '''
        data = delete:key:remaining_replicas:starter_hash
        This function is being initially executed by a random node that gets the "delete" request. Deletes a certain key from its primary node and all replica managers (if k > 1).
        '''
        _, key, remaining_replicas, starter_hash = data.split(":")
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        key_value = self.my_data.get(key_hash)
        if key_value != None:                                                                      # If the node has the key
            del self.my_data[key_hash]                                                                  # delete the key
            print("Key {} was deleted from node {}".format(key,self.server_id))
            remaining_replicas = str(int(remaining_replicas)-1)                                         # decrease the number of replicas that have to be deleted
            if remaining_replicas != '0' and self.next_hash != starter_hash:                            # if there are still replicas to be deleted and next node isn't the one that started this whole process
                answer = self.send_next('delete:{}:{}:{}'.format(key,remaining_replicas,starter_hash))      # forward a message to next node in order him to delete his replica
            else:                                                                                       # else
                answer = "Data was deleted"                                                                 # answer
        else:
            if self.next_hash != starter_hash:
                answer = self.send_next(data)
            else:
                answer = "Key does not exist"
        return answer

    def query_linearizability(self,data):
        '''
        data = query_linearizability:key:num_replicas
        This function is being initially executed by a random node that receives the "query" request. It is used for linearizability consistency.
        '''
        _, key, num_replicas = data.split(":")
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        if num_replicas != 'None':                                                # If the node is a replica manager (has a copy of the key-value pair) but is not the primary node
            next_has_replica = self.send_next('has_replica:{}'.format(key_hash))    # check if next node has a replica
            belongs_to_next = self.send_next('belongs_to_you:{}'.format(key_hash))  # check if next node is the primary node of the key
            if next_has_replica == 'True' and belongs_to_next == 'False':           # If next node has a replica and is not the primary node of the key
                print("Node {} has replica of key {}".format(self.server_id,key))
                answer = self.send_next(data)                                           # node should not return the value because he is not the last in the chain
            else:                                                                   # else (the node is last one in the chain)
                key_value = self.my_data.get(key_hash)                                  # return the (key,value) pair
                print("Node {} has last replica of key {}".format(self.server_id,key))
                answer = "Node with id {} has key {} with value {}".format(self.server_id,key,key_value[1])
        elif self.belongs_to_me(key_hash):                                        # elif the node is the primary node
            key_value = self.my_data.get(key_hash)                                      # check if primary has the key
            if key_value != None:                                                       # if primary has the key
                print("Key {} belongs to node {}".format(key,self.server_id))
                if self.num_replicas > 1:                                                   # If replication factor > 1 (there are copies of every key)
                    remaining_replicas = self.num_replicas - 1                              # decrease replication factor by 1 and forward a message to next node to repeat the process
                    answer = self.send_next('query_linearizability:{}:{}'.format(key,remaining_replicas))
                else:                                                                       # else (there is no replication)
                    answer = "Node with id {} has key {} with value {}".format(self.server_id,key,key_value[1]) # return the (key,value) pair
            else:                                                                       # else (the primary doesn't have the key)
                answer = "Key {} does not exist".format(key)                                # the key doesn't exist in the network
        else:                                                                      # else (the node is nor the primary neither a RM)
            answer = self.send_next(data)                                               # forward a "query" request to the next node to repeat this process
        return answer

    def query_eventual(self,data):
        '''
        data = query_eventual:key
        This function is being initially executed by a random node that receives the "query" request. It is used for eventual consistency.
        '''
        _, key = data.split(":")
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        key_value = self.my_data.get(key_hash)
        if key_value != None:                                                   # If node has the key
            answer = "Node with id {} has key {} with value {}".format(self.server_id,key,key_value[1]) # return the (key,value) pair
        else:                                                                   # else (the node doesn't have the key)
            if self.belongs_to_me(key_hash):                                        # If node is the primary node (successor of the key)
                answer = "Key {} does not exist".format(key)                            # key doesn't exist in the network
            else:                                                                   # else (node isn't the primary node)
                answer = self.send_next(data)                                           # forward a "query" request to the next node to repeat this process
        return answer

    def depart(self,data):
        '''
        data = depart
        This function is being executed by a node that receives a "depart" request
        '''
        if self.my_hash != self.next_hash:                                                                       # if node isn't alone in the network
            for data in self.my_data.values():                                                                      # for each (key,value) pair he has stored
                self.send_next('replication:{}:{}:1:{}'.format(data[0],data[1],self.my_hash))                           # forward a message to next node to add a replica
            self.send_next('previous:{}:{}:{}'.format(self.previous_ip,self.previous_port,self.previous_hash))      # inform next node to change previous indexes
            self.send_previous('next:{}:{}:{}'.format(self.next_ip,self.next_port,self.next_hash))                  # inform previous node to change next indexes
        self.stop_connection = True                                                                              # Stop connection
        self.connection_thread.join()
        answer = "Server {} departed".format(self.server_id)
        return answer

    def print_chord_data(self, data):
        '''
        Prints the data of each node that exist in the Chord ring through a cyclical messaging process. It is used when the user wants to execute a 'query *' command.
        '''
        starter_hash = data.split(":")[1]
        server_data = []
        for key_value in self.my_data.values():
            server_data.append(key_value)
        data = data + ":" + self.server_id + ":" + str(server_data)
        if self.my_hash == self.next_hash or self.next_hash == starter_hash:
            answer = data
        else:
            answer = self.send_next(data)
        return answer

    def overlay(self,data):
        '''
        data = overlay:starter_hash:list_of_server_ids
        Prints the overlay network through a cyclical messaging process.
        '''
        starter_hash = data.split(":")[1]
        if self.my_hash == starter_hash:
            if self.my_hash != self.next_hash:
                answer = self.send_next(data)
            else:
                answer = "Only node with id {} exists in overlay network".format(self.server_id)
        elif self.next_hash != starter_hash:
            data = data + ":" + self.server_id
            answer = self.send_next(data)
        else:
            answer = data + ":" + self.server_id
        return answer

    def get_port(self):
        '''
        Returns the port of the node
        '''
        return self.my_port

    def get_hash(self):
        '''
        Returns the hash of the node
        '''
        return self.my_hash
