from multiprocessing import Process, Pipe
from time import time, sleep
import zmq


class Requester():
    def __init__(self):
        pass

    def request(self, message):
        try:
            context = zmq.Context()
            socket = context.socket(zmq.REQ)
            socket.connect('tcp://127.0.0.1:5555')
            socket.send(message)
            return socket.recv()
        except Exception as e:
            print(e)
            return None

class Responder():
    def __init__(self):
        pass

    def respond(self, topic, response):
        try:
            context = zmq.Context()
            socket = context.socket(zmq.REP)
            socket.bind('tcp://127.0.0.1:5555')
            while True:
                msg = socket.recv()
                if msg == topic:
                    socket.send(response)
                else:
                    socket.send(None)
        except Exception as e:
            print(e)
            return None


def request():
    try:
        sleep(.5)
        r = Requester()
        nones = 0
        messages = 0
        start = time()
        max_count = 10
        while True:
            if(time()-start >= max_count):
                end = time()
                break
            msg = r.request(b"hello")
            if msg == None:
                nones += 1
            elif msg:
                messages += 1        
    except Exception as e:
        print(e)
        return None
    finally:
        print(messages / max_count, "messages received per second and ", nones, "nones")
        exit(0)
    

def respond():
    try:
        s = Responder()
        s.respond(b"hello", b"world")
    except Exception as e:
        print(e)
        return None

if __name__ == '__main__':

    messages = 0

    responder = Process(target=respond)
    requester = Process(target=request)

    responder.start()
    requester.start()

    count = 0
    max_count = 1

    try:
        while True:
            sleep(.1)

    except KeyboardInterrupt:
        print("attempting to close processes..." )
        responder.join()
        requester.join()
        print("processes successfully closed")

    finally:
        exit(0)