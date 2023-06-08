from multiprocessing import Process, Pipe
from time import time, sleep
import zmq

#SOURCE: https://stackoverflow.com/questions/53613583/-function-for-multiprocess-priority--in-python-with-syncmanager-class/53623278#53623278


class Pusher():
    def __init__(self):
        self.highwatermark = 3 # how many messages to keep in , reduce the throughput but decreases a pull returning None
        self.lowwatermark = 1
        self.context = zmq.Context()
        self.zmq_socket = self.context.socket(zmq.PUSH)
        self.zmq_socket.connect("tcp://127.0.0.1:5558")
        

    def push(self, message):
        try:
            self.zmq_socket.send_json(message)
            return True
        except Exception as e:
            print(e)
            return None


class Puller():
    def __init__(self):
        self.context = zmq.Context()
        self.results_receiver = self.context.socket(zmq.PULL)
        self.results_receiver.connect("tcp://127.0.0.1:5556")

    def pull(self):
        try:
            msg = self.results_receiver.recv_json()
            return msg
        except Exception as e:
            print(e)
            return None


class Router():
    def __init__(self, producer='5558', consumer='5556'):
        self.context = zmq.Context()
        self.producer_socket = self.context.socket(zmq.PULL)
        self.producer_socket.bind(f"tcp://127.0.0.1:{producer}")
        self.consumer_socket = self.context.socket(zmq.PUSH)
        self.consumer_socket.bind(f"tcp://127.0.0.1:{consumer}")
        self.poller = zmq.Poller()
        self.poller.register(self.producer_socket, zmq.POLLIN)

    def route(self, cb=None):
        last_msg = {}
        while True:
            try:
                evts = dict(self.poller.poll(.5))
                if self.producer_socket in evts:
                    msg = self.producer_socket.recv_json(zmq.DONTWAIT)
                    if msg is not None and msg != last_msg and msg != {}:
                        last_msg = msg
                # print("last", last_msg)
                self.consumer_socket.send_json(last_msg)
            except Exception as e:
                print(e)
                continue  

def push():
    try:
        p = Pusher()
        nones = 0
        messages = 0
        start = time()
        max_count = 10

        while True:
            if(time()-start >= max_count):
                print(messages / max_count, "messages pushed per second and ", nones, "nones")
                break
            msg = p.push({"Hello": "World"})
            if msg == None:
                nones += 1
            elif msg:
                messages += 1
    
    except KeyboardInterrupt:
        return

def pull():
    try:
        s = Puller()
        sleep(.5)
        nones = 0
        messages = 0
        start = time()
        max_count = 11
        while True:
            if(time()-start >= max_count):
                print(messages / max_count, "messages pulled per second and ", nones, "nones")
                break
            msg = s.pull()
            if msg == None:
                nones += 1
            elif msg:
                messages += 1
    
    except Exception as e:
        print(e)
        return None

def route():
    try:
        r = Router()
        r.route()
    except Exception as e:
        print(e)
        return None    


if __name__ == '__main__':
    try:

        pusher = Process(target=push)
        puller = Process(target=pull)
        puller2 = Process(target=pull)
        router = Process(target=route)

        pusher.start()
        puller.start()
        puller2.start()
        router.start()

        while True:
            sleep(.1)

    except KeyboardInterrupt:
        print("attempting to close processes..." )
        pusher.terminate()
        puller.terminate()
        puller2.terminate()
        router.terminate()
        pusher.join()
        puller.join()
        puller2.join()
        router.join()
        print("processes successfully closed")

    finally:
        exit(0)