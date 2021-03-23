import socket
from rich.console import Console
from rich.table import Table

def getIP():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.connect(('192.168.0.0', 0))
    return s.getsockname()[0]

def helper():

    table = Table()

    table.add_column("Command", justify="center", width=50, style="cyan", no_wrap=True)
    table.add_column("Syntax", justify="center", width=80, style="magenta")
    table.add_column("Description", justify="center", width=120, style="green")

    table.add_row("insert", "python cli.py insert --ip <ip> -p <port> -c <linearizability>/<eventual> <KEY> <space> <VALUE> ", "Inserts a new (key,value) pair")
    table.add_row("delete", "python cli.py delete --ip <ip> -p <port> <KEY>", "Deletes the (key,value) pair of a certain key if it exists in the network")
    table.add_row("query", "python cli.py query --ip <ip> -p <port> -c <linearizability>/<eventual> <KEY>", "Searches for a certain key and returns its corresponding value")
    table.add_row("query", "python cli.py query --ip <ip> -p <port> '*'", "Displays all stored (key,value) pairs")
    table.add_row("depart", "python cli.py depart --ip <ip> -p <port>", "Removes a node from the Chord ring")
    table.add_row("overlay", "python cli.py overlay --ip <ip> -p <port>", "Displays the network topology")
    table.add_row("help", "python cli.py help", "Displays a description of the available commands")

    console = Console()
    console.print(table)
