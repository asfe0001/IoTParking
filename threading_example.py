import threading
import time

exitFlag = 0

class garageThread (threading.Thread):
   def __init__(self, threadID, name, counter):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
      self.counter = counter
   def run(self):
      print ("Starting " + self.name)
      subGarageState(self.name, 5, self.counter)
      print ("Exiting " + self.name)



class carThread (threading.Thread):
   def __init__(self, threadID, name, counter):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
      self.counter = counter
   def run(self):
      print ("Starting " + self.name)
      subCar(self.name, 5, self.counter)
      print ("Exiting " + self.name)
      
      
class AssignmentThread (threading.Thread):
   def __init__(self, threadID, name, counter):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
      self.counter = counter
   def run(self):
      print ("Starting " + self.name)
      pubCar(self.name, 5, self.counter)
      print ("Exiting " + self.name)

def subGarageState(threadName, counter, delay):
   while counter:
      if exitFlag:
         threadName.exit()
      time.sleep(delay)
      print ("%s: %s" % (threadName, time.ctime(time.time())))
      counter -= 1
      
      
def subCar(threadName, counter, delay):
   while counter:
      if exitFlag:
         threadName.exit()
      time.sleep(delay)
      print ("%s: %s" % (threadName, time.ctime(time.time())))
      counter -= 1 
      
      
def pubCar(threadName, counter, delay):
   while counter:
      if exitFlag:
         threadName.exit()
      time.sleep(delay)
      print ("%s: %s" % (threadName, time.ctime(time.time())))
      counter -= 1     
      

# Create new threads
thread1 = garageThread(1, "Thread-1", 1)
thread2 = carThread(2, "Thread-2", 2)
thread3 = AssignmentThread(3, "Thread-3", 3)

# Start new Threads
thread1.start()
thread2.start()
thread3.start()

print ("Exiting Main Thread")
