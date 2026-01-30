"""
ZeroMQ Module
=============

This module defines two incremental modules ZeroMQReader and ZeroMQWriter that act as a
a bridge between ZeroMQ and retico. For this, a ZeroMQIU is defined that contains the
information revceived over the ZeroMQ bridge.
"""

# retico
import retico_core

# zeromq & supporting libraries
import zmq, json
import threading
import time
from collections import deque
import pickle

zmq_delimiter = '_!_'

# Keeping for Backwards Compatibility, the ReaderModule supports ZMQ objects and
# and allows reader->module subscriptions using ZMQ topics to ensure the appropriate reader
# instance receives the message
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
        t = threading.Thread(target=self.run_reader, daemon=True)
        t.start()
        t = threading.Thread(target=self.run_process, daemon=True)
        t.start()


class ZMQReaderModule(retico_core.AbstractModule):

    """A ZeroMQ Reader Module

    Attributes:

    """

    def name(self):
        return f"[ZMQ] {str(self.expected_iu_type)}"

    @staticmethod
    def description():
        return "A Module providing reading from a ZeroMQ bus"

    @staticmethod
    def input_ius():
        return []

    def output_iu(self):
        return self.expected_iu_type

    def __init__(self, ip, port, topic, expected_iu_type, **kwargs):
        """Initializes the ZeroMQReader.

        Args: topic(str): the topic/scope where the information will be read.

        """
        super().__init__(**kwargs)
        self.queue = deque()
        self.target_iu_types = {}
        self.socket = zmq.Context().socket(zmq.SUB)
        self.socket.connect("tcp://{}:{}".format(ip, port))
        self.topic = topic
        self.socket.subscribe(topic)
        self.expected_iu_type = expected_iu_type

    def process_update(self, input_iu):
        """
        As an edge case, process_update is not used for Reader Modules because updates do not come from being subscribed to another module.
        Rather, they come from populating a queue with messages from the ZMQ writer filtered by the appropriate topic
        """
        # the _run function in the Abstract module we inherit expects to be able to call process update. Return None as a no-op
        return None

    def process_message(self):
        """
        Read ZMQ message from reader queue, load input IU from pickle, and pass along
        """
        while True:
            time.sleep(0.02) # prevent tight loop if queue is empty
            if len(self.queue) > 0:
                message = self.queue.popleft()
                input_iu, update_type = pickle.loads(message)
                um = retico_core.UpdateMessage.from_iu(input_iu, update_type) # pass input IU on using its own update type
                self.append(um)

    def run_reader(self):
        """
        Wait for messages from the writer for the defined topic, add to queue to be processed
        ZMQ handles topic management
        """
        while True:
            topic,message = self.socket.recv_multipart()
            self.queue.append(message)

    def prepare_run(self):
        t = threading.Thread(target=self.run_reader, daemon=True)
        t.start()
        t = threading.Thread(target=self.process_message, daemon=True)
        t.start()


class WriterSingleton:
    __instance = None

    @staticmethod
    def getInstance():
        """Static access method."""
        return WriterSingleton.__instance

    def __init__(self, ip, port):
        """Virtually private constructor."""
        if WriterSingleton.__instance is None:
            context = zmq.Context()
            self.queue = deque()
            self.socket = context.socket(zmq.PUB)
            self.socket.bind("tcp://{}:{}".format(ip, port))
            WriterSingleton.__instance = self
            t = threading.Thread(target=self.run_writer)
            t.daemon = True
            t.start()

    def add_to_queue(self, data):
        self.queue.append(data)

    def run_writer(self):
        while True:
            if len(self.queue) == 0:
                time.sleep(0.1)
                continue
            # Once we are sending only pickled IU objects, we can remove the logic checking for json and just unpack
            # topic, message, and update_type from the queue
            # topic, message, update_type = self.queue.popleft()
            data = self.queue.popleft()
            if len(data) == 3:
                topic, message, update_type = data
                self.socket.send_multipart([topic.encode(), pickle.dumps((message, update_type))])
            else:
                self.socket.send_string(data)

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

    def __init__(self, topic, use_json=True, **kwargs):
        """Initializes the ZeroMQReader.

        Args: topic(str): the topic/scope where the information will be read.

        """
        super().__init__(**kwargs)
        self.topic = topic
        self.queue = deque()  # no maxlen
        self.writer = WriterSingleton.getInstance()
        self.use_json = use_json # remove this when fully moved to sending pickled IU objects over ZMQ

    def process_update(self, update_message):
        """
        Adds the IU to the writer queue
        """
        for input_iu,update_type in update_message:
            # Once we are only sending pickled IU objects, we can drop the if logic
            if self.use_json:
                # This assumes that the message is json formatted, then packages it as payload into an IU
                self.writer.add_to_queue(self.topic + zmq_delimiter + json.dumps(input_iu.to_zmq(update_type)))
            else:
                self.writer.add_to_queue([self.topic, input_iu, update_type])
        return None

    def prepare_run(self):
        pass
