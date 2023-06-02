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


class Pusher():
    def __init__(self, channel):
        self.queue = channel
        self.highwatermark = 3 # how many messages to keep in queue, reduce the throughput but decreases a pull returning None
        self.lowwatermark = 1

    def clear(self, q):
        for i in range(self.lowwatermark):
            self.queue.get()

    def push(self, message):
        try:
            q = self.queue.get_attribute("queue")
            if len(q) >= self.highwatermark:
                self.clear(q)
            self.queue.put(message)
            return message
        except Exception as e:
            return None

class Puller():
    def __init__(self, channel):
        self.queue = channel

    def pull(self):
        try:
            if self.queue.empty():
                return None
            else:
                q = self.queue.get_attribute("queue")
                return q[-1]
        except Exception as e:
            print(e)
            return None


def push(queue):
    try:
        p = Pusher(queue)
        nones = 0
        messages = 0
        start = time()
        max_count = 10
        while True:
            if(time()-start >= max_count):
                end = time()
                break
            msg = p.push("Hello")
            if msg == None:
                nones += 1
            elif msg:
                messages += 1
    
    except KeyboardInterrupt:
        return
    finally:
        print(messages / max_count, "messages sent per second and ", nones, "nones")

def pull(queue):
    try:
        s = Puller(queue)
        sleep(.5)
        nones = 0
        messages = 0
        start = time()
        max_count = 10
        while True:
            if(time()-start >= max_count):
                end = time()
                break
            msg = s.pull()
            if msg == None:
                nones += 1
            elif msg:
                messages += 1

    except KeyboardInterrupt:
        return messages
    
    finally:
        print(messages / max_count, "messages received per second and ", nones, "nones")



if __name__ == '__main__':

    channel = Channel().create()

    messages = 0

    pusher = Process(target=push, args=(channel,))
    puller = Process(target=pull, args=(channel, ))

    pusher.start()
    puller.start()

    count = 0
    max_count = 1

    try:
        start = time()
        end = None
        while True:
            sleep(.1)

    except KeyboardInterrupt:
        print("attempting to close processes..." )
        pusher.join()
        puller.join()
        print("processes successfully closed")

    finally:
        exit(0)