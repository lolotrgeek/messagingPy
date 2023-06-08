from multiprocessing import Process
from time import time, sleep
import zmq
import traceback

class Requester():
    def __init__(self):
            self.context = zmq.Context()
            self.socket = self.context.socket(zmq.REQ)
            self.socket.connect('tcp://127.0.0.1:5555')

    def request(self, topic, args=None):
        try:
            self.socket.send_json({'topic': topic, 'args': args})
            return self.socket.recv_json()
        except Exception as e:
            print(e)
            return None

class Responder():
    def __init__(self, topics={}):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind('tcp://127.0.0.1:5555')
        self.topics = topics

    def respond(self):
        try:
            msg = self.socket.recv_json()
            if msg["topic"] in self.topics.keys():
                response = self.topics[msg['topic']](msg['args'])
                self.socket.send_json(response)
                return response
            else:
                self.socket.send_json(None)
                return None
        except Exception as e:
            print("Response Error", e)
            traceback.print_exc()
            return None

def request(topic, message):
    try:
        r = Requester()
        nones = 0
        messages = 0
        start = time()
        max_count = 10
        while True:
            if(time()-start >= max_count):
                print(messages / max_count, "messages received per second and ", nones, "nones")
                break
            msg = r.request(topic, message)
            if msg == None:
                nones += 1
            elif msg == "world":
                messages += 1
            elif msg == "cruel_world":
                messages += 1
    except Exception as e:
        print(e)
        return None

def respond():
    try:
        topics = {
            "hello": lambda x: "world" if x == "world" else None,
            "goodbye": lambda x: "cruel_world" if x == "cruel_world" else None,
            }
        s = Responder(topics)
        nones = 0
        messages = 0
        start = time()
        max_count = 11        
        while True:
            if(time()-start >= max_count):
                print(messages / max_count, "messages responded per second and ", nones, "nones")
                break
            response = s.respond()
            if response == None:
                nones += 1
            elif response == "world":
                messages += 1
            elif response == "cruel_world":
                messages += 1    
                    
    except Exception as e:
        print(e)
        return None
        

if __name__ == '__main__':

    responder = Process(target=respond)
    requester = Process(target=request, args=("hello", "world"))
    requester_bye = Process(target=request, args=("goodbye", "cruel_world"))

    responder.start()
    requester.start()
    requester_bye.start()
    try:
        while True:
            sleep(.1)

    except KeyboardInterrupt:
        print("attempting to close processes..." )
        responder.terminate()
        requester.terminate()
        requester_bye.terminate()
        responder.join()
        requester.join()
        requester_bye.join()
        print("processes successfully closed")

    finally:
        exit(0)