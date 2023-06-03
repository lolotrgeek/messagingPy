from multiprocessing import Process, Pipe
from time import time, sleep
import zmq

#SOURCE: https://stackoverflow.com/questions/53613583/-function-for-multiprocess-priority--in-python-with-syncmanager-class/53623278#53623278


class Pusher():
    def __init__(self):
        self.highwatermark = 3 # how many messages to keep in , reduce the throughput but decreases a pull returning None
        self.lowwatermark = 1


    def push(self, message):
        try:
            context = zmq.Context()
            zmq_socket = context.socket(zmq.PUSH)
            zmq_socket.bind("tcp://127.0.0.1:5557")
            zmq_socket.send_json(message)
        except Exception as e:
            print(e)
            return None


class Puller():
    def __init__(self):
        pass

    def pull(self):
        try:
            context = zmq.Context()
            results_receiver = context.socket(zmq.PULL)
            results_receiver.connect("tcp://127.0.0.1:5557")
            return results_receiver.recv_json()
        except Exception as e:
            print(e)
            return None


def push():
    try:
        p = Pusher()
        nones = 0
        messages = 0
        start = time()
        max_count = 10
        while True:
            if(time()-start >= max_count):
                end = time()
                break
            msg = p.push({"Hello": "World"})
            if msg == None:
                nones += 1
            elif msg:
                messages += 1
    
    except KeyboardInterrupt:
        return
    finally:
        print(messages / max_count, "messages sent per second and ", nones, "nones")

def pull():
    try:
        s = Puller()
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

    messages = 0

    pusher = Process(target=push)
    puller = Process(target=pull)

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