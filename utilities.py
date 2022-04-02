import random
from enum import Enum
from pulp import *
import numpy as np
import time
from config import DEBUG, BENCHMARK


class DataItems:
  def __init__(self, item_count=1000, theta=0.8, minimum_size=10, maximum_size=30, seed=100):
    # Zipf distribution related information
    self.item_count = item_count
    self.theta = theta

    # Data item size range [minimum_size, minimum_size + max(size_divergence_range)] in KiB
    self.minimum_size = minimum_size * 1024  # Value in KiB
    self.maximum_size = maximum_size * 1024  # Value in KiB

    # Seed to generate random size for each data item
    self.seed = seed

    # List of data items to be initialized
    self.data_items = []

    # Initialize the data items list
    self.__init_data_items()


  # Return all available data items
  def get_data_items(self):
    return self.data_items


  # Return data item located at index position
  def get_data_item(self, index):
    if index < len(self.data_items):
      return self.data_items[index]
    
    return None


  # Get requests for linear programming to solve the optimization 
  # problem based on the number of requests given by the server
  def optimization_get_requests(self, total_requests):
    x = []
    for i in range(1, total_requests + 1):
      x.append(str(i))

    # PuLP Variables for linear programming and optimizing solution
    decision_variables = LpVariable.matrix("X", x, cat="Continuous", lowBound=0, upBound=1) # 0 <= x <= 1
    decision_variables_allocation = np.array(decision_variables)

    return decision_variables_allocation


  # Get data items for linear programming to solve the optimization 
  # problem based on the number of requests given by the server
  def optimization_get_data_items(self, total_requests):
    y = []
    for i in range(1, total_requests + 1):
      for j in range(1, self.item_count + 1):
        y.append(str(i) + '_' + str(j))

    # PuLP Variables for linear programming and optimizing solution
    decision_variables = LpVariable.matrix("Y", y, cat="Continuous", lowBound=0, upBound=1) # 0 <= y <= 1
    decision_variables_allocation = np.array(decision_variables).reshape(total_requests, self.item_count)

    return decision_variables_allocation


  # Initialize data items list with random data items
  def __init_data_items(self):
    
    # Seed to generate pseudo-random sizes
    random.seed(self.seed)
    
    for i in range(self.item_count):
      size = random.randint(self.minimum_size, self.maximum_size)
      probability = self.__zipf(self.item_count, self.theta, i)
      data_item = self.DataItem(i, size, probability)

      # Add a data item to the list of data items
      self.data_items.append(data_item)


  # Method that calculates the probability of a data item
  # being selected based on the Zipf distribution
  def __zipf(self, items, theta, position):
    nominator = (1 / (position + 1)) ** theta
    
    denominator = 0
    for i in range(0, items):
      denominator += (1 / (i + 1)) ** theta
  
    return nominator / denominator


  # Inner class that is used to store data item information  
  class DataItem:
    def __init__(self, id, size, probability):
      self.index = id
      self.id = "{}{}".format('d', id)
      self.size = size
      self.probability = probability
      self.submitted_request_time = -1

    # String on how the object was created (for debuging)
    def __repr__(self):
      return "DataItem({}, {}, {}, {})".format(self.id, self.size, self.probability, self.submitted_request_time)


    # String containing the data of the object (for user)
    def __str__(self):
      return "ID: {}, Size: {}, Probability: {}, Submitted: {}".format(self.id, self.size, self.probability, self.submitted_request_time)


    # Return item's id
    def get_index(self):
      return self.index


    # Return item's id
    def get_id(self):
      return self.id


    # Return item's size
    def get_size(self):
      return self.size


    # Return the probability that this data item might be selected
    def get_probability(self):
      return self.probability


    # Return the time that the data item was submitted
    def get_submitted_time(self):
      return self.submitted_request_time


    # Set time that the request is send (used with shallow copy)
    def set_submitted_time(self):
      self.submitted_request_time = time.time()


# Helper Enum to check the status of the request
class RequestStatus(Enum):
  WAITING = 0
  SENT = 1
  FINISHED = 2


# Class used for benchmarking operations
# Feature must be added to handle .xlsx files
class BenchmarkUtilities:
  def __init__(self, clients, server):
    self.clients = clients

  def get_total_AAL(self):
    AAL = 0
    for client in self.clients.get_total_clients():
      AAL += client.get_latency()
      
    AAL /= len(self.clients.get_total_clients())
    
    return AAL
