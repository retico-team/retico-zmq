"""
ZeroMQ Module
=============

This module defines two incremental modules ZeroMQReader and ZeroMQWriter that act as a
a bridge between ZeroMQ and retico. For this, a ZeroMQIU is defined that contains the
information revceived over the ZeroMQ bridge.
"""

# retico
import retico_core
from retico_core.abstract import *

# zeromq & supporting libraries
import zmq, json
import threading
import datetime
import time
from collections import deque

zmq_delimiter = '_!_'

class ReaderSingleton(retico_core.AbstractModule):

    """A ZeroMQ Reader Module

    Attributes:

    """

    @staticmethod
    def name():
        return "ZeroMQ Reader Module"

    @staticmethod
    def description():
        return "A Module providing reading from a ZeroMQ bus"

    @staticmethod
    def input_ius():
        return []

    @staticmethod
    def output_iu():
        return IncrementalUnit

    def __init__(self, ip, port, **kwargs):
        """Initializes the ZeroMQReader.

        Args: topic(str): the topic/scope where the information will be read.

        """
        super().__init__(**kwargs)
        self.queue = deque()
        self.target_iu_types = {}
        self.socket = zmq.Context().socket(zmq.SUB)
        self.socket.connect("tcp://{}:{}".format(ip, port))    

    def process_update(self, input_iu):
        """
        This assumes that the message is json formatted, then packages it as payload into an IU
        """
        return None

    def add(self, topic, target_iu_type):
        self.socket.subscribe(topic)
        self.target_iu_types[topic] = target_iu_type

    def run_process(self):

        while True:
            time.sleep(0.2)
            if len(self.queue) > 0:
                # print("ZMQ Reader process update")
                topic,message = self.queue.popleft()
                if topic not in self.target_iu_types:
                    print(topic, "is not a recognized topic")
                    continue
                j = json.loads(message)
                    # print(self.topic, topic.decode())
                
                output_iu = self.target_iu_types[topic](
                            creator=self,
                            iuid=f"{hash(self)}:{self.iu_counter}",
                            previous_iu=self._previous_iu,
                            grounded_in=None,
                        )
                self.iu_counter += 1
                self._previous_iu = output_iu
                
                output_iu.from_zmq(j)

                update_message = retico_core.UpdateMessage()

                if "update_type" not in j:
                    print("Incoming IU has no update_type!")
                if j["update_type"] == "UpdateType.ADD":
                    update_message.add_iu(output_iu, retico_core.UpdateType.ADD)
                elif j["update_type"] == "UpdateType.REVOKE":
                    update_message.add_iu(output_iu, retico_core.UpdateType.REVOKE)
                elif j["update_type"] == "UpdateType.COMMIT":
                    update_message.add_iu(output_iu, retico_core.UpdateType.COMMIT)
                # print('iu created by ', self.topic)
                self.append(update_message)        
    
    def run_reader(self):
        # print(self.topic)
        while True:
            topic,message = self.socket.recv_string().split(zmq_delimiter)
            self.queue.append((topic,message))

    def prepare_run(self):
        t = threading.Thread(target=self.run_reader)
        t.start()
        t = threading.Thread(target=self.run_process)
        t.start()                

class WriterSingleton:
    __instance = None

    @staticmethod
    def getInstance():
        """Static access method."""
        return WriterSingleton.__instance

    def __init__(self, ip, port):
        """Virtually private constructor."""
        if WriterSingleton.__instance == None:
            context = zmq.Context()
            self.queue = deque()
            self.socket = context.socket(zmq.PUB)
            self.socket.bind("tcp://{}:{}".format(ip, port))
            WriterSingleton.__instance = self
            t = threading.Thread(target=self.run_writer)
            t.start()
    
    def send(self, data):
        self.queue.append(data)

    def run_writer(self):
        while True:
            if len(self.queue) == 0:
                time.sleep(0.1) 
                continue
            data = self.queue.popleft()
            self.socket.send_string(data)



# class ZeroMQIU(retico_core.IncrementalUnit):
#     @staticmethod
#     def type():
#         return "ZeroMQ Incremental Unit"

#     def __init__(
#         self,
#         creator=None,
#         iuid=0,
#         previous_iu=None,
#         grounded_in=None,
#         payload=None,
#         **kwargs
#     ):
#         """Initialize the DialogueActIU with act and concepts.

#         Args:
#             act (string): A representation of the act.
#             concepts (dict): A representation of the concepts as a dictionary.
#         """
#         super().__init__(
#             creator=creator,
#             iuid=iuid,
#             previous_iu=previous_iu,
#             grounded_in=grounded_in,
#             payload=payload,
#         )

#     def set_payload(self, payload):
#         self.payload = payload


# class ZeroMQReader(retico_core.AbstractModule):

#     """A ZeroMQ Reader Module

#     Attributes:

#     """

#     @staticmethod
#     def name():
#         return "ZeroMQ Reader Module"

#     @staticmethod
#     def description():
#         return "A Module providing reading from a ZeroMQ bus"

#     @staticmethod
#     def output_iu():
#         return IncrementalUnit

#     def __init__(self, topic, target_iu_type, **kwargs):
#         """Initializes the ZeroMQReader.

#         Args: topic(str): the topic/scope where the information will be read.

#         """
#         super().__init__(**kwargs)
#         self.topic = topic
#         self.reader = None
#         self.queue = deque()
#         self.target_iu_type = target_iu_type

#     def process_update(self, input_iu):
#         """
#         This assumes that the message is json formatted, then packages it as payload into an IU
#         """

#         return None

#     def run_process(self):

#         while True:
#             time.sleep(0.2)
#             if len(self.queue) > 0:
#                 print("ZMQ Reader process update", self.topic)
#                 message = self.queue.popleft()
#                 j = json.loads(message)
#                     # print(self.topic, topic.decode())
                
#                 output_iu = self.target_iu_type(
#                             creator=self,
#                             iuid=f"{hash(self)}:{self.iu_counter}",
#                             previous_iu=self._previous_iu,
#                             grounded_in=None,
#                         )
#                 self.iu_counter += 1
#                 self._previous_iu = output_iu
                
#                 output_iu.from_zmq(j)

#                 update_message = retico_core.UpdateMessage()

#                 if "update_type" not in j:
#                     print("Incoming IU has no update_type!")
#                 if j["update_type"] == "UpdateType.ADD":
#                     update_message.add_iu(output_iu, retico_core.UpdateType.ADD)
#                 elif j["update_type"] == "UpdateType.REVOKE":
#                     update_message.add_iu(output_iu, retico_core.UpdateType.REVOKE)
#                 elif j["update_type"] == "UpdateType.COMMIT":
#                     update_message.add_iu(output_iu, retico_core.UpdateType.COMMIT)
#                 # print('iu created by ', self.topic)
#                 self.append(update_message)        
    
#     def run_reader(self):
#         # print(self.topic)
#         while True:
#             topic,message = self.reader.recv_string().split(zmq_delimiter)
#             # print(self.topic)
#             # I hate these two stupid hacks, but sometimes
#             # the recv_multipart function is missing the topic
#             # if len(data) != 2: return None
#             # print(type(data[0].decode()))
#             # if type(data[0].decode()) != str: return None
            
#             # topic,message = data
#             # print(self.topic, topic, self.topic==topic)
#             if self.topic != topic: # only deal with IUs designated for this topic
#                 continue

#             self.queue.append(message)

            

#     def prepare_run(self):
#         self.reader = ReaderSingleton.getInstance().socket
#         # self.reader.setsockopt(zmq.SUBSCRIBE, self.topic)
#         self.reader.subscribe(self.topic)
#         t = threading.Thread(target=self.run_reader)
#         t.start()
#         t = threading.Thread(target=self.run_process)
#         t.start()        


class ZeroMQWriter(retico_core.AbstractModule):

    """A ZeroMQ Writer Module

    Note: If you are using this to pass IU payloads to PSI, make sure you're passing JSON-formatable stuff (i.e., dicts not tuples)

    Attributes:
    topic (str): topic/scope that this writes to
    """

    @staticmethod
    def name():
        return "ZeroMQ Writer Module"

    @staticmethod
    def description():
        return "A Module providing writing onto a ZeroMQ bus"

    @staticmethod
    def output_iu():
        return None

    @staticmethod
    def input_ius():
        return [retico_core.IncrementalUnit]

    def __init__(self, topic, **kwargs):
        """Initializes the ZeroMQReader.

        Args: topic(str): the topic/scope where the information will be read.

        """
        super().__init__(**kwargs)
        self.topic = topic
        self.queue = deque()  # no maxlen
        self.writer = WriterSingleton.getInstance()

    def process_update(self, update_message):
        """
        This assumes that the message is json formatted, then packages it as payload into an IU
        """
        # print("ZMQ Writer process update", self.topic)        
        for input_iu,um in update_message:
            self.writer.send(
                self.topic + zmq_delimiter + json.dumps(input_iu.to_zmq(um))
            )
            # time.sleep(0.1)

        return None
        # for um in update_message:
        #     self.queue.append(um)
        # for input_iu,um in update_message:
        #     # print('sending', self.topic)
        #     # print(input_iu.payload)
        #     # if isinstance(input_iu, ImageIU) or isinstance(input_iu, DetectedObjectsIU)  or isinstance(input_iu, ObjectFeaturesIU):
        #     #     payload['message'] = json.dumps(input_iu.get_json())
        #     self.writer.send_string(
        #        self.topic + zmq_delimiter + json.dumps(input_iu.to_zmq(um))
        #     )

        # return None

    # def run_writer(self):

    #     while True:
    #         if len(self.queue) == 0:
    #             time.sleep(0.5)
    #             continue
    #         input_iu, ut = self.queue.popleft()
    #         # print('sending', self.topic)
    #         # print(input_iu.payload)
    #         # if isinstance(input_iu, ImageIU) or isinstance(input_iu, DetectedObjectsIU)  or isinstance(input_iu, ObjectFeaturesIU):
    #         #     payload['message'] = json.dumps(input_iu.get_json())
    #         self.writer.send_string(
    #            self.topic + zmq_delimiter + json.dumps(input_iu.to_zmq(ut))
    #         )

    def prepare_run(self):
        pass
        # self.writer = WriterSingleton.getInstance().socket
        # t = threading.Thread(target=self.run_writer)
        # t.start()


# class ZMQtoImage(retico_core.AbstractModule):
#     @staticmethod
#     def name():
#         return "ZMQtoASR"
#     @staticmethod
#     def description():
#         return "Convert ZeroMQIU to SpeechRecognitionIU"
#     @staticmethod
#     def input_ius():
#         return [ZeroMQIU]
#     @staticmethod
#     def output_iu():
#         return ImageIU

#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)

#     def process_update(self,update_message):

#         for iu,um in update_message:
#             # print("getting ZMQ", iu.payload['message'])
#             output_iu = self.create_iu(iu)
#             output_iu.create_from_json(json.loads(iu.payload['message']))
#             # self.current_ius.append(output_iu)
#             new_um = retico_core.UpdateMessage.from_iu(output_iu, um)
            
#         return new_um

#     def setup(self):
#         pass


# class ZMQtoDetectedObjects(retico_core.AbstractModule):
#     @staticmethod
#     def name():
#         return "ZMQtoDetectedObjects"
#     @staticmethod
#     def description():
#         return "Convert ZeroMQIU to DetectedObjectsIU"
#     @staticmethod
#     def input_ius():
#         return [ZeroMQIU]
#     @staticmethod
#     def output_iu():
#         return ObjectFeaturesIU

#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)

#     def process_update(self,update_message):
#         for iu,um in update_message:
#             # print("getting ZMQ", iu.payload['message'])
#             output_iu = self.create_iu(iu)
#             output_iu.create_from_json(json.loads(iu.payload['message']))
#             # self.current_ius.append(output_iu)
#             new_um = retico_core.UpdateMessage.from_iu(output_iu, um)
            
#         return new_um

#     def setup(self):
#         pass
