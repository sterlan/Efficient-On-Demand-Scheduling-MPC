import random

# Data Items
TOTAL_DATA_ITEMS = 10
THETA = 0.8
MIN_DATA_SIZE = 10 #KiB
MAX_DATA_SIZE = 30 #KiB
DATA_SEED = 100

# Clients
CLIENTS = 100
MIN_DATA_ITEMS = 10
MAX_DATA_ITEMS = 30
CLIENT_SEED = 10
CLIENT_SLEEP_INTERVAL = 1 

# Server
TIME_SLOT = random.randint(1, 3)
BANDWIDTH = 1024 #KiB/s
DELTA = 4 # Must allow at least one full request to be downloaded 


# Display options
DEBUG = False
BENCHMARK = False