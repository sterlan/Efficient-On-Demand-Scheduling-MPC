import threading
import random
from time import sleep
from utilities import RequestStatus
import timeit
import copy


class Clients:
  def __init__(self, client_count, data_items, DOWN_STREAM, minimum_data_items=1, maximum_data_items=4, seed=100, maximum_interval=2):
    # Client information 
    self.client_count = client_count
    # Sort data items based on their selection probability 
    self.data_items = sorted(data_items.get_data_items(), key = lambda data_item: data_item.get_probability(), reverse=True)
    
    # Seed to select a random data item based on its probability
    self.seed = seed
    
    # Data items range used in each request
    self.minimum_data_items = minimum_data_items
    self.maximum_data_items = maximum_data_items

    # Interval to wait before sending a request
    self.maximum_interval = maximum_interval

    self.clients = []
    self.threads = []

    # Use at the MTRS optimization problem
    self.maximum_data_item_index = 0

    self.__init_clients(DOWN_STREAM)
  

  # Initialize a list of clients based on the Zipf's distribution
  def __init_clients(self, DOWN_STREAM):
    # Seed to select pseudo-random data items
    random.seed(self.seed)

    # Source on how to select data items based on probabilities: 
    # https://stackoverflow.com/questions/33888612/how-to-make-selection-random-based-on-percentage
    
    # For each client
    for client_index in range(self.client_count):
      request = []

      # For a random number of data items
      for _ in range(random.randint(self.minimum_data_items, self.maximum_data_items)):
        # Probability used to select an item from the items list
        item_index_probability = random.uniform(0, 1)

        # Variable to accumulate probabilities of data items
        accumulated_probability = 0
        
        # For each item append to request based on its probability
        for item in self.data_items:
          # Calculate the next probability for the data item to be selected 
          accumulated_probability += item.get_probability()

          # Data item with the corresponding probability was found 
          if item_index_probability < accumulated_probability:
            data_item = copy.copy(item) # Create a shallow copy to set submission time
            data_item.set_submitted_time()
            request.append(data_item)


            # Find the final item's index which is used by the 
            # server to calculate the optimal solution on the MTRS 
            if self.maximum_data_item_index < item.get_index():
              self.maximum_data_item_index = item.get_index()

            break
      
      # Create Client with requests
      client = self.Client(client_index, request, DOWN_STREAM, self.maximum_interval)
      self.clients.append(client)

  
  # Get final data item's position
  def max_data_item(self):
    return self.maximum_data_item_index

  # Returns all available clients
  # Used for benchmark reasons
  def get_total_clients(self):
    return self.clients


  # Return all available clients
  def get_clients(self):
    active_clients = []
    for client in self.clients:
      if client.status != RequestStatus.FINISHED:
        active_clients.append(client)
    return active_clients


  # Return a client based on his id and status code
  def get_client_by_id(self, id, status=RequestStatus.SENT):
    for client in self.clients:
      if client.get_status() != status or client.get_id() != id:
        continue
      return client
    return None


  def send_requests(self):  
    # Start a Thread for each client
    for client in self.clients:
      thread = threading.Thread(target=client.send_request)
      thread.start()

      # Append to the list of active threads
      self.threads.append(thread)


  # Inner class that is used to store client information  
  class Client:
    def __init__(self, id, request, DOWN_STREAM, maximum_interval):
      self.id = id
      self.request = request
      self.submitted_request_time = -1
      self.downstream = DOWN_STREAM
      self.latency = 0

      # The request was not sent yet. This helps us activate requests at
      # random intervals since all requests do not reach the server at the
      # same exact time. The server checks the status of each request before processing
      self.status = RequestStatus.WAITING

      # Flag to specify if request was received by server.
      # This is used to avoid duplicate requests on the server side
      self.received = False

      # Interval to wait before sending the request
      self.maximum_interval = maximum_interval

      # Semaphore used to block when request is sent till the respose has arrived
      self.semaphore = threading.Semaphore(0)


    # Send request to server and block till a
    # data item arrives. Update remaining data items and continue
    def send_request(self):
      # Wait before sending the request
      sleep(random.randint(0, self.maximum_interval))
      
      self.submitted_request_time = timeit.default_timer()
      
      # Set the flag that the request was send
      self.status = RequestStatus.SENT
      
      while self.request:
        # Block till a data item arrives
        self.semaphore.acquire()

        # Server did not send anything
        if not self.downstream:
          continue        

        for response in self.downstream[:]:
          for request in self.request[:]: 
            if request.get_id() != response.get_id():
              continue
            
            # Request received and is not needed anymore
            self.request.remove(request)
            
      # Calculate AAL after the response was received and the request is finished
      self.latency = timeit.default_timer() - self.submitted_request_time

      # Mark request as finished
      self.status = RequestStatus.FINISHED
              

     # String on how the object was created (for debuging)
    def __repr__(self):
      return "Client({}, {}, {})".format(self.id, self.request, self.submitted_request_time)


    # String containing the data of the object (for user)
    def __str__(self):
      return "ID: {}, Request: {}, Time: {}".format(self.id, self.request, self.submitted_request_time)


    # Return client's id
    def get_id(self):
      return self.id


    # Return client's request
    def get_request(self):
      return self.request


    # Returns latency to calculate AAL
    def get_latency(self):
      return self.latency


    # Return client's request at position index
    def get_indexed_data_item(self, index):
      for item in self.request:
        if item.get_index() == index:
          return item
      return None


    # Return client's request status
    def get_status(self):
      return self.status


    # Return whether request received by server or not
    def request_received(self):
      return self.received