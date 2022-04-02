from utilities import BENCHMARK, DEBUG, RequestStatus
from pulp import *
import numpy as np
import math
from colorama import Fore, Style
import sys
import time
from utilities import DEBUG, BENCHMARK # Settings to control stdout


class Server:
  def __init__(self, clients, data_items, DOWN_STREAM, bandwidth=10, time_slot=1, delta=4):
    # Connection with Clients
    self.clients = clients
    self.data_items = data_items

    # Server related information for throughput computation
    self.bandwidth = bandwidth * 1024
    self.timeslots = time_slot

    # Scheduling related information
    self.delta = delta
    self.pending = []   # L
    self.completed = [] # C
    self.broadcast = [] # V
    self.downstream = DOWN_STREAM
    

  @property
  def pending_requests_count(self):
    return len(self.pending)


  # Functions that handles sending responses with the help of the scheduler
  def send_response(self):
    while True:
      clients = self.clients.get_clients()
      if not clients:
        return

      #Populate pending list (put Q into L)
      self.__receive_requests(clients)
      
      for request in self.pending[:]:
        if not request.request:
          self.pending.remove(request)
          self.completed.append(request)
      
      if len(self.completed) != self.clients.client_count:
        if not self.pending:
          continue
      else:
        break

      # Run MTRS, Least Lost Heuristic and MLRO
      self.__scheduler()

      download_timeslots = 0
      # Calculate time to actually download items from client side
      for data in self.broadcast:
        download_timeslots = download_timeslots + (data.get_size() / (self.bandwidth * self.timeslots)) # 
      
      # Write to downstream
      self.downstream.extend(self.broadcast[:])
      
      # Wait for a certain amount of timeslots to begin sending 
      download_timeslots = math.ceil(download_timeslots)
      
      time.sleep(download_timeslots)

      # Up clients semaphores to enable receiving
      for client in self.clients.get_clients():
        if client.request_received():
          client.semaphore.release()
      
      # Wait for clients to receive data
      for client in self.clients.get_clients():
        if client.request_received(): 
          while client.semaphore._value != 0 and client.get_status() != RequestStatus.FINISHED: # Can be improved to avoid active waiting
            continue
      
      # Dequeue items from list (completely delete it)
      self.broadcast.clear()
      

  # Perform MTRS, Pruning and MLRO to populate the self.broadcast channel    
  def __scheduler(self):
    if not self.broadcast:
      # Apply the MTRS to get a maximum throughput request set Q
      Q = self.__mtrs()

      # Print Q for debuging purposes
      if DEBUG:
        print(json.dumps(Q, indent=2))

      # Calculate T(Q)
      time_to_send = self.__time_to_send_requests(Q)
 
      # Perform a pruning algorithm
      if time_to_send > self.delta:
        self.__least_lost_heuristic(Q)

        # Print Q for debuging purposes
        if DEBUG:
          print(json.dumps(Q, indent=2))

      # Apply the MLRO to optimize for latency
      S = self.__mlro(Q)
      
      # Print Q for debuging purposes
      if DEBUG:
        print(json.dumps(S, indent=2))

      # Create broadcast channel populated with data items
      # Put items into V
      for request in S:
        for data in request["data"]:
          self.broadcast.append(self.pending[data["request_index"]].get_indexed_data_item(data["data_index"]))
              

  # Calculate time to send request
  def __time_to_send_requests(self, Q):
    # Q is empty
    if not Q:
      return 0
    
     # Calculate time to send Q
    time = 0
    for request in Q:
      for data in request["data"]:
        time += data["time"]
    
    return time


  # Perform the MLRO to optimize for latency
  def __mlro(self, Q):
    if not BENCHMARK:
      print(Fore.GREEN + "Performing the MLRO Algorithm" + Style.RESET_ALL)
    
    # W <- D(Q)
    W = self.__populate_remainder_data_items(Q)

    # n = |D(Q)|
    n = len(W)
    
    # Optimal schedule  
    if n <= len(Q):
      # Equations (3) and (4)
      if not BENCHMARK:
        print("Appling Equations 3 and 4")
      return self.__data_optimal_schedule(Q, W)

    else:
      # Equations (5) and (6)
      if not BENCHMARK:
        print("Appling Equations 5 and 6")
      return self.__request_optimal_schedule(Q)


  # MLRO helper equations (3, 4) and (5, 6)
  def __data_optimal_schedule(self, Q, W):
    current_time = time.time()
    
    # Data order
    order = []
    
    # Find data item that minimized AAL
    while True:
      # Equation 3
      AAL = 0
      lowest_latency_data_item = None
      for data in W:
        latency = self.__data_average_access_latency(current_time, data["data_index"])

        if AAL < latency:
          AAL = latency
          lowest_latency_data_item = data
    
      # No data item was found
      if not AAL:
        break
      
      # Equation 4
      order.append(lowest_latency_data_item)
      W.remove(lowest_latency_data_item)

    # Rearrange data items in requests based on the order
    for request in Q:
      for data in request["data"]:
        # Calculate a weight for each data item to rearrange based on the weight
        self.__calculate_weight(data, order)
      request["data"].sort(key=lambda item: item["weight"], reverse=True)

    return Q
    
  # Helper for MLOR to calculate weights of each request
  def __calculate_weight(self, data_item, order):
    for i in range(len(order)):
      if order[i]["data_index"] != data_item["data_index"]:
        continue
      data_item["weight"] = i
  

  # Calculate access latency for each data item
  def __data_average_access_latency(self, current_time, data_index):
    AAL = 0

    for request in self.pending:
      for data in request.request:
        if data.get_index() != data_index:
          continue
        
        AAL += current_time - data.get_submitted_time()
    
    return AAL


  # Optimal MLRO schedule with bottom-up approach
  def __request_optimal_schedule(self, Q):
    current_time = time.time()
    
    S = []
    # Find request that minimized AAL
    while True:
      # Equation 5
      AAL = 0
      lowest_latency_request = None
      for request in Q:
        latency = self.__request_average_access_latency(current_time, request["request"]["request_index"])

        if AAL < latency:
          AAL = latency
          lowest_latency_request = request
      
      # No request was found
      if not AAL:
        break
      
      # Equation 6
      S.append(lowest_latency_request)
      Q.remove(lowest_latency_request)

    return S


  # Calculate the AAL of a request Q
  def __request_average_access_latency(self, current_time, request_index):
    AAL = current_time - self.pending[request_index].submitted_request_time

    # Check for identical data items sets
    for i in range(len(self.pending)):
      if i == request_index:
        continue
      
      # Identical data items
      if self.pending[i] == self.pending[request_index].request:
        AAL += current_time - self.pending[i].submitted_request_time
    
    return AAL


  # Least Lost Heuristic Algorithm that returns an updated requests list Q
  def __least_lost_heuristic(self, Q):
    if not BENCHMARK:
      print(Fore.RED + "Performing Least Loss Heuristic Algorithm" + Style.RESET_ALL)
    

    # W <- D(Q)
    W = self.__populate_remainder_data_items(Q)

    # Find item such that |Q| / t(d) is minimized
    while self.__time_to_send_requests(Q) > self.delta:
      minimized = sys.maxsize
      minimized_data_item = None
      for data in W:        
        total_requests_containing_data_item = self.__total_requests_containing_data_item(Q, data['data_index'])
        if minimized > total_requests_containing_data_item:
          minimized = total_requests_containing_data_item
          minimized_data_item = data
        
      
      # This is NOT mentioned by the paper but helps to avoid starvation on large requests
      if len(Q) < 2:
        break
      
      # Delete d from W and delete Q that includes d
      W.remove(minimized_data_item)
      self.__remove_requests_conaining_data_item(Q, minimized_data_item)
      
    return Q


  # Delete requests that contain a certain data item
  def __remove_requests_conaining_data_item(self, Q, data_item):
    for request in Q[:]:
      for data in request["data"]:
        if data != data_item:
          continue
        Q.remove(request)


  # Get request based on its index
  def __get_request_by_index(self, Q, index):
    for request in Q:
      if request["request"]["request_index"] != index:
        continue
        
      return request
    return None


  # Least lost heuristic: |{Q | Q ∈ Q, d ∈ D(Q)}|
  def __total_requests_containing_data_item(self, Q, index):
    total_requests = 0

    for request in Q:
      for data in request["data"]:
        if data["data_index"] != index:
          continue
        total_requests += 1

    return total_requests
    

  # Helper function to populate remainder data items W
  def __populate_remainder_data_items(self, Q):
    W = []
    for request in Q:
      for data in request["data"]:
        W.append(data)
    
    return W

 
  # Perform the MTRS algorithm to optimize for throughput
  def __mtrs(self):
    if not BENCHMARK:
      print(Fore.GREEN + "Performing the MTRS Algorithm" + Style.RESET_ALL)

    model, status = self.__optimization_model()
    
    x, y = self.__preprocess_model_results(model)
   
    # n <- max(x_i)
    n = -1
    for request in x:
      try:
        if request["value"] > n:
          n = request["value"]
      except:
          print("[ERROR] MTRS")

    if n < 0:
      return None

    Q = []
    # x_i = (x_i < n) ? 0 : 1
    for request in x:
      try:
        if request["value"] < n:
          request["value"] = 0
        else:
          request["value"] = 1
          
          D_Q = []
          for data in y:
            if data["request_index"] != request["request_index"]:
              continue
            
            # Append only data items that are actually included in the result
            data["value"] = 1
            D_Q.append(data)

          Q.append({"request": request, "data": D_Q})
      except:
          print("[ERROR] MTRS")


    # y_j = (yj belongs to D_Q) ? 1 : 0
    for data in y:
      if self.__is_data_in_Q(Q, data["data_index"]):
        data["value"] = 1
      else:
        data["value"] = 0
      
    return Q


  # Check if data item is included in the request
  def __is_data_in_Q(self, Q, index):
    for request in Q:
      for data in request["data"]:
        if data["data_index"] != index:
          continue
        return True
    
    return False
    

  # Preprocess data to feed to PuLP for linear programming solution
  def __preprocess_model_results(self, model):
    # Get 
    x = []
    y = []
    for variable in model.variables():
      request_index = int(variable.name.split('_')[1]) - 1
      try:
        if variable.name[0] == 'X':
          item = {"name": variable.name, "value": variable.value(), "request_index": request_index}
          x.append(item)
        
        elif variable.name[0] == 'Y':
          data_index = int(variable.name.split('_')[2]) - 1

          # Append only data items that are actually requested
          data_item = self.pending[request_index].get_indexed_data_item(data_index)
          if data_item != None:
            item = {
              "name": variable.name, 
              "value": variable.value(), 
              "request_index": request_index, 
              "data_index": data_index, 
              "time":  self.__calculate_time(data_item.get_size()),
              "weight": 0
            }

            y.append(item)
      
      except:
        print("[ERROR] MTRS")
    
    return x, y


  # PuLP model for MTRS
  def __optimization_model(self):
    model = LpProblem("MTRS", LpMaximize)

    # Acquire decision variables for x and y 
    data_decision_variables_x, data_decision_variables_y = self.__decision_data_definition()
    
    # Build the objective function
    objective_function = lpSum(data_decision_variables_x)
    model += objective_function


    # Add constraints
    self.__constraints(model, data_decision_variables_x, data_decision_variables_y)
    
    # Solve the model and hide log message
    model.solve(PULP_CBC_CMD(msg=False))

    # Get model status
    status =  LpStatus[model.status]

    return model, status


  # Calculate time slots based on the size of the data item and throughput of the server
  def __calculate_time(self, size):
    return math.ceil(size / (self.bandwidth * self.timeslots))


  # Add constraints to the model
  def __constraints(self, model, data_decision_variables_x, data_decision_variables_y):
    # Source for solving optimization problems using linear programming 
    # with PuLP: https://towardsdatascience.com/linear-programming-using-python-priyansh-22b5ee888fe0

    # List that pending requests are stored
    # each row is a request with data items
    pending_request_list = []
    for client in self.pending:
      
      # Each data item for a request, zero if data item
      # is not included on the pending request, else t(di) 
      pending_data_list = []
      
      for i in range(self.data_items.item_count):
        if not client.get_indexed_data_item(i):
          pending_data_list.append(0)
        else:
          try:
            # Calculate send data time for each item in the request 
            time =  self.__calculate_time(client.get_indexed_data_item(i).get_size())
            pending_data_list.append(time)
          except:
            pending_data_list.append(0)

      # Append data to the request
      pending_request_list.append(pending_data_list)

    # Weights for each data item  
    pending_request_matrix_y = np.array(pending_request_list)
    
    # Iterate over the pending requests to set: sum for all d_j of (t(d_j) * y_j) <= 1
    for i in range(self.pending_requests_count):
      model += lpSum(data_decision_variables_y[i] * pending_request_matrix_y[i]) <= 1
      
      # Iterate over the data items to set: x_i <= y_i with d_j belongs to D(Q_i)
      for j in range(self.data_items.item_count):
        model += lpSum(data_decision_variables_x[i] - data_decision_variables_y[i][j]) <= 0


  # Create decision variables for x,y to use in PuLP
  def __decision_data_definition(self):
    data_decision_variables_x = self.data_items.optimization_get_requests(self.pending_requests_count)
    data_decision_variables_y = self.data_items.optimization_get_data_items(self.pending_requests_count)
    
    return data_decision_variables_x, data_decision_variables_y


  # A function that receives requests from connected clients
  def __receive_requests(self, clients):
    
    for i in range(len(clients)):
      if clients[i].get_status() == RequestStatus.SENT and not clients[i].request_received():
        self.pending.append(clients[i])
        
        clients[i].received = True

        # Sort data items of the pending request based on their id.
        # This is used later for latency computation.        
        self.pending[-1].request.sort(key=lambda item: item.id)

      elif clients[i].get_status() == RequestStatus.FINISHED:
        self.clients.threads[i].join()