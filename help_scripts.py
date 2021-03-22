import socket
from rich.console import Console
from rich.table import Table

def getIP():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.connect(('<broadcast>', 0))
    return s.getsockname()[0]

def helper():

    table = Table()

    table.add_column("Command", justify="center", width=50, style="cyan", no_wrap=True)
    table.add_column("Syntax", justify="center", width=80, style="magenta")
    table.add_column("Description", justify="center", width=120, style="green")

    table.add_row("insert", "insert <space> key <space> value", "Inserts a new (key,value) pair")
    table.add_row("delete", "delete <space> key", "Deletes the (key,value) pair of a certain key if it exists in the network")
    table.add_row("query", "query <space> key", "Searches for a certain key and returns its corresponding value")
    table.add_row("query", "query <space> *", "Displays all stored (key,value) pairs")
    table.add_row("depart", "depart <space> nodeID", "Removes a node from the Chord ring")
    table.add_row("overlay", "overlay", "Displays the network topology")

    console = Console()
    console.print(table)
