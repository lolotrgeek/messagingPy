from queue import Queue as Q
from multiprocessing.managers import SyncManager
from multiprocessing import Process, Pipe
from time import time, sleep

#SOURCE: https://stackoverflow.com/questions/53613583/queue-function-for-multiprocess-priority-queue-in-python-with-syncmanager-class/53623278#53623278

class Queue(Q):
    def get_attribute(self, name):
        return getattr(self, name)

class QueueManager(SyncManager):
    pass

class Channel():
    def __init__(self):
        pass
        
    def create(self):
        QueueManager.register("Queue", Queue)
        m = QueueManager()
        m.start()
        return m.Queue()


class Publisher():
    def __init__(self, channel):
        self.queue = channel

    def publish(self, message):
        q = self.queue.get_attribute("queue")
        for i in q:
            self.queue.get()
        if (self.queue.empty()):
            self.queue.put(message)
            

class Subscriber():
    def __init__(self, channel):
        self.queue = channel
        self.calledback = False

    def subscribe(self, callback=lambda x: x):
        try:
            while True:
                if self.queue.empty():
                    self.calledback = False
                    continue
                elif self.calledback == True:
                    continue
                else:
                    try:
                        q = self.queue.get_attribute("queue")
                        callback(q[0])
                        self.calledback = True
                    except IndexError:
                        continue
        except KeyboardInterrupt:
            return False


def publish(queue):
    try:
        p = Publisher(queue)
        while True:
            p.publish("Hello")
    except KeyboardInterrupt:
        return


def sub(queue, pipe):
    messages = 0

    def received(message):
        nonlocal messages
        messages += 1
        print(messages, end="\r")


    try:
        s = Subscriber(queue)
        s.subscribe(received)
    except KeyboardInterrupt:
        return messages


if __name__ == '__main__':

    channel = Channel().create()

    parent_conn, child_conn = Pipe()
    

    publisher = Process(target=publish, args=(channel,))
    subscriber = Process(target=sub, args=(channel, child_conn))

    publisher.start()
    subscriber.start()

    message = 0
    
    count = 0
    max_count = 1

    try:
        start = time()
        end = None
        while True:
            # print(time()-start)
            if(time()-start >= max_count):
                end = time()
                break
            # print(count)
            # message = parent_conn.recv()
            # sleep(.1)
        
        publisher.terminate()
        subscriber.terminate()
        
        # print(message, "messages received in", end-start, "seconds")
        exit(0)
    except KeyboardInterrupt:
        print("attempting to close processes..." )
        publisher.join()
        subscriber.join()
        print("processes successfully closed")

    finally:
        exit(0)