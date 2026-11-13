# retico-zmq
ReTiCo module for ZeroMQ

## Installation and Requirements

To use the ZeroMQ modules you first need to install the retico-zmq package:
* Install retico-zmq:
```pip install retico-zmq```

  or, for the latest development version from GitHub:
```pip install git+https://github.com/retico-team/retico-zmq.git```

## Modules

Writers are individual modules, but Readers are all a single module with the option of having many channels. Each channel can be set to output a specific IU type.

### `WriterSingleton`
Not a module: the ZeroMQ publisher that all `ZeroMQWriter`s send through. Create it once, before the writers.

#### Arguments:
* `ip` (str): the IP of the writer, which should be the originating PC's IP
* `port` (str): the port to publish on

### `ZeroMQWriter`
Subscribes to any IU and sends it over ZeroMQ under a topic. It doesn't output any IUs.

#### Arguments:
* `topic` (str): the topic the IUs are sent under
* `use_json` (bool): if `True`, the IU is sent as JSON, to be read by `ReaderSingleton` (or by non-Python programs); if `False`, the IU itself is sent (pickled), to be read by `ZMQReaderModule`; defaults to `True`

### `ReaderSingleton`
Produces IUs from the JSON messages of one or more topics. Add a topic with `add(topic, target_iu_type)`; each message on that topic is turned into an IU of `target_iu_type`.

#### Arguments:
* `ip` (str): the IP of the writer to read from
* `port` (str): the port of the writer

### `ZMQReaderModule`
Produces the IUs sent by a `ZeroMQWriter` with `use_json=False` on one topic, passing each IU on with its original update type.

#### Arguments:
* `ip` (str): the IP of the writer to read from
* `port` (str): the port of the writer
* `topic` (str): the topic to read

## Example

```python
from retico_core.dialogue import GenericDictIU
from retico_vision.vision import ImageIU
from retico_zmq import WriterSingleton, ZeroMQWriter, ReaderSingleton

# the IP of the writer should be the originating PC's IP
WriterSingleton(ip=ip, port='12345')
refer_od = ZeroMQWriter(topic='referod')
refer_dm = ZeroMQWriter(topic='referdm')
refer_wac = ZeroMQWriter(topic='referwac')

# the IP of the reader should be the target PC's IP
reader = ReaderSingleton(ip=ip2, port='12346')
reader.add(topic='camera', target_iu_type=ImageIU)
reader.add(topic='refer', target_iu_type=GenericDictIU)
```
