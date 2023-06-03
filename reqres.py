from queue import Queue as Q
from multiprocessing.managers import SyncManager
from multiprocessing import Process, Pipe
from time import time, sleep
from uuid import uuid4

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


class Responder():
    def __init__(self, req, res):
        self.req = req
        self.res = res
        self.highwatermark = 500

    def respond(self, topic, response):
        try:
            while True:
                if self.res.qsize() > self.highwatermark:
                    self.res.get() # clears oldest response
                message = self.req.get()
                nones = 0
                messages = 0
                if type(message) is not dict:
                    continue
                elif message.get("id") is None:
                    continue
                elif message.get("message") != topic:
                    continue
                else:
                    id = message.get("id")
                    message.get("message")
                    self.res.put({"id": id, "response": response})
                    continue
                                    
        except Exception as e:
            print(e)
            pass
        finally:
            print(messages, "messages responded per second and ", nones, "nones")

class Requester():
    def __init__(self, req, res):
        self.req = req
        self.res = res
        self.max_listens = 1000

    def request(self, topic):
        try:
            message_id = uuid4()
            self.req.put({"message": topic, "id": message_id})
            listening = 0
            while True:
                if listening >= self.max_listens:
                    print("max listens reached")
                    return None
                listening += 1
                responses = self.res.get_attribute("queue")
                response = next((response for response in responses if response['id'] == message_id), None)
                if type(response) is dict:
                    message_id = None
                    return response
        except Exception as e:
            # print(e)
            pass

def respond(req, res):
    try:
        p = Responder(req, res)
        p.respond("Hello", "World")
    except KeyboardInterrupt:
        return


def request(req, res):
    try:
        s = Requester(req, res)
        sleep(.5)
        nones = 0
        messages = 0
        start = time()
        max_count = 10
        while True:
            if(time()-start >= max_count):
                end = time()
                break
            msg = s.request("Hello")
            if msg == None:
                nones += 1
            elif msg:
                messages += 1

    except KeyboardInterrupt:
        return messages
    
    finally:
        print(messages / max_count, "messages requested per second and ", nones, "nones")

if __name__ == '__main__':

    req = Channel().create()
    res = Channel().create()

    messages = 0

    responder = Process(target=respond, args=(req, res))
    requester = Process(target=request, args=(req, res))

    responder.start()
    requester.start()

    count = 0
    max_count = 1

    try:
        start = time()
        end = None
        while True:
            sleep(.1)

    except KeyboardInterrupt:
        print("attempting to close processes..." )
        responder.join()
        requester.join()
        print("processes successfully closed")

    finally:
        exit(0)