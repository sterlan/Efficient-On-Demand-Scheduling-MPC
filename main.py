from distutils.command.config import config
from clients import Clients
from server import Server
from utilities import DataItems
import timeit
import random
from utilities import BenchmarkUtilities 
from config import * 
# # Data Items
# TOTAL_DATA_ITEMS = 10
# THETA = 0.8
# MIN_DATA_SIZE = 10 #KiB
# MAX_DATA_SIZE = 30 #KiB
# DATA_SEED = 100

# # Clients
# CLIENTS = 100
# MIN_DATA_ITEMS = 10
# MAX_DATA_ITEMS = 30
# CLIENT_SEED = 10
# CLIENT_SLEEP_INTERVAL = 1 

# # Server
# TIME_SLOT = random.randint(1, 3)
# BANDWIDTH = 24 #KiB/s
# DELTA = 10 # Must allow at least one full request to be downloaded   

DOWN_STREAM = []

# Spawn data items, clients and a server
def init():
  # Create a list of all available data items used for data selection based on their probabilities
  data_items = DataItems(TOTAL_DATA_ITEMS, THETA, MIN_DATA_SIZE, MAX_DATA_SIZE, DATA_SEED)

  # Create list of requests of data items for clients 
  clients = Clients(CLIENTS, data_items, DOWN_STREAM, MIN_DATA_ITEMS, MAX_DATA_ITEMS, CLIENT_SEED, CLIENT_SLEEP_INTERVAL)
  
  # Clients are connected to the server
  server = Server(clients, data_items, DOWN_STREAM, BANDWIDTH, TIME_SLOT, DELTA)

  # Benchmark information
  benchmark_info = BenchmarkUtilities(clients, server)

  return clients, server, benchmark_info


if __name__ == '__main__':

  # Initialize Clients and Server
  clients, server, benchmark_info = init()

  # Start of execution time
  start = timeit.default_timer()
  
  # Send requests to server
  clients.send_requests()
  
  # Server responds to clients
  server.send_response()

  # End of execution time
  stop = timeit.default_timer()



  print('AAL: ', benchmark_info.get_total_AAL())
  print('Total Time of Execution: ', stop - start)  
