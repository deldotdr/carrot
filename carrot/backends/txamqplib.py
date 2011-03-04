"""
TODO:
    setup all deferred rpc style amqp methods with a set of standard
    call/err backs in backend, so messaging objects can try not to worry
    about deferreds. maybe with a decorator
"""
import os
from itertools import count
import warnings
import weakref


from txamqp import spec
from txamqp.content import Content
from txamqp.protocol import AMQClient
from txamqp.protocol import AMQChannel
from txamqp.client import TwistedDelegate

from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet import protocol

from twisted.python import log

from carrot.backends.base import BaseMessage, BaseBackend

DEFAULT_PORT = 5672
import carrot
spec_path_def = os.path.join(carrot.__path__[0], 'spec', 'amqp0-8.xml')

class QueueAlreadyExistsWarning(UserWarning):
    """A queue with that name already exists, so a recently changed
    ``routing_key`` or other settings might be ignored unless you
    rename the queue or restart the broker."""

class ChannelWithCallback(AMQChannel):

    def __init__(self, id, outgoing):
        AMQChannel.__init__(self, id, outgoing)
        #self.deliver_callbacks = []
        self.deliver_callback = None

    def register_deliver_callback(self, callback):
        #self.deliver_callbacks.append(callback)
        self.deliver_callback = callback

    def _deliver(self, msg):
        if not self.deliver_callback:
            raise NotImplementedError("No channel callbacks...")
        #for cb in self.deliver_callbacks:
        #    cb(msg)
        return self.deliver_callback(msg)


class Connection(AMQClient):
    """
    @note Adds to and augments functionality of the txAMQP library.
    """
    channelClass = ChannelWithCallback
    next_channel_id = 0

    def channel(self, id=None):
        """Overrides AMQClient. Changes:
            1) no need to return deferred. The channelLock doesn't protect
            against any race conditions; the channel reference is returned,
            so any number of those references could exist already.
            2) auto channel numbering
            3) replace deferred queue for basic_deliver(s) with simple
               buffer(list)
        """
        if id is None:
            self.next_channel_id += 1
            id = self.next_channel_id
        try:
            ch = self.channels[id]
        except KeyError:
            # XXX The real utility is in the exception body; is that good
            # style?
            ch = self.channelFactory(id, self.outgoing)
            # the PacketDelegate defined above requires this buffer
            self.channels[id] = ch
        return ch

    # @defer.inlineCallbacks
    def connectionMade(self):
        """
        Here you can do something when the connection is made.
        """
        AMQClient.connectionMade(self)
        # yield self.authenticate(self.factory.username, self.factory.password)

    def frameLengthExceeded(self):
        """
        """

    def installReceiver(self, target):
        """receiver target is sent the delivery messages for all consumers
        on this connection.
        """

    def commonDelivery(self, msg):
        """delegate sends basic_delivery, basic_get_ok, etc. here
        """

class InterceptionPoint(TwistedDelegate):
    """@todo allow for filters/interceptors to be installed
    """

    @defer.inlineCallbacks
    def basic_deliver(self, ch, msg):
        # self.client.commonDelivery(msg)
        yield defer.maybeDeferred(ch._deliver, msg)

    def Xbasic_get_ok(self, ch, msg):
        ch._deliver(msg)

class ConnectionCreator(object):
    """Create AMQP Client.
    The AMQP Client uses one persistent connection, so a Factory is not
    necessary.

    Client Creator is initialized with AMQP Broker configuration.

    ConnectTCP is called with TCP configuration.
    """

    protocol = Connection

    def __init__(self, reactor, username='guest', password='guest',
                                vhost='/', delegate=None,
                                spec_path=spec_path_def,
                                heartbeat=0):
        self.reactor = reactor
        self.username = username
        self.password = password
        self.vhost = vhost
        self.spec= spec.load(spec_path)
        self.heartbeat = heartbeat
        if delegate is None:
            delegate = TwistedDelegate()
        self.delegate = delegate
        self.connector = None

    def connectTCP(self, host, port, timeout=30, bindAddress=None):
        """Connect to remote Broker host, return a Deferred of resulting protocol
        instance.
        """
        d = defer.Deferred()
        p = self.protocol(self.delegate,
                                    self.vhost,
                                    self.spec,
                                    heartbeat=self.heartbeat)
        p.factory = self
        f = protocol._InstanceFactory(self.reactor, p, d)
        self.connector = self.reactor.connectTCP(host, port, f, timeout=timeout,
                bindAddress=bindAddress)
        def auth_cb(conn):
            d = conn.authenticate(self.username, self.password)
            d.addCallback(lambda _: conn)
            return d
        d.addCallback(auth_cb)
        return d

class QueueAlreadyExistsWarning(UserWarning):
    """A queue with that name already exists, so a recently changed
    ``routing_key`` or other settings might be ignored unless you
    rename the queue or restart the broker."""


class Message(BaseMessage):
    """A message received by the broker.

    Usually you don't insantiate message objects yourself, but receive
    them using a :class:`carrot.messaging.Consumer`.

    :param backend: see :attr:`backend`.
    :param amqp_message: see :attr:`_amqp_message`.


    .. attribute:: body

        The message body.

    .. attribute:: delivery_tag

        The message delivery tag, uniquely identifying this message.

    .. attribute:: backend

        The message backend used.
        A subclass of :class:`carrot.backends.base.BaseBackend`.

    .. attribute:: _amqp_message

        A :class:`txamqp.content.Content` instance.
        This is a private attribute and should not be accessed by
        production code.

    """

    def __init__(self, backend, amqp_message, **kwargs):
        self._amqp_message = amqp_message
        kwargs['body'] = amqp_message.content.body
        kwargs['delivery_tag'] = amqp_message.delivery_tag
        for attr_name in (
                          "content type",
                          "content encoding",
                          "headers",
                          "delivery mode",
                          "priority",
                          "correlation id",
                          "reply to",
                          "expiration",
                          "message id",
                          "timestamp",
                          "type",
                          "user id",
                          "app id",
                          "cluster id",
                          ):
            kwargs[attr_name.replace(' ', '_')] = amqp_message.content.properties.get(attr_name, None)
        # more amqp properties (not defined in original BaseMessage)
        self.reply_to = kwargs['reply_to']
        self.headers = kwargs['headers']

        # super(Message, self).__init__(backend, **kwargs)
        BaseMessage.__init__(self, backend, **kwargs)

    def X__str__(self):
        routing_key = self._amqp_message.routing_key
        exchange = self._amqp_message.exchange

class Backend(BaseBackend):
    """

    :param connection: see :attr:`connection`.


    .. attribute:: connection

    A :class:`carrot.connection.BrokerConnection` instance. An established
    connection to the broker.

    """
    default_port = DEFAULT_PORT

    Message = Message

    def __init__(self, connection, **kwargs):
        self.connection = connection
        self.default_port = kwargs.get("default_port", self.default_port)
        self._channel_ref = None

    @property
    def _channel(self):
        return callable(self._channel_ref) and self._channel_ref()

    @property
    def channel(self):
        """If no channel exists, a new one is requested."""
        if not self._channel:
            self._channel_ref = weakref.ref(self.connection.get_channel())
        return self._channel

    def establish_connection(self, delegate=None):
        """Establish connection to the AMQP broker."""
        conninfo = self.connection
        if not conninfo.port:
            conninfo.port = self.default_port
        if delegate is None:
            delegate = InterceptionPoint()
        conn_creator = ConnectionCreator(reactor, 
                          username=conninfo.userid,
                          password=conninfo.password,
                          vhost=conninfo.virtual_host,
                          delegate=delegate,
                          heartbeat=conninfo.heartbeat,
                          # insist=conninfo.insist,
                          # ssl=conninfo.ssl,
                          # connect_timeout=conninfo.connect_timeout
                          )
        return conn_creator.connectTCP(conninfo.hostname, conninfo.port)

    def close_connection(self, connection):
        """Close the AMQP broker connection by calling connection_close on
        channel 0"""
        chan0 = connection.channel(0)
        return chan0.connection_close()

    def queue_exists(self, queue):
        """Check if a queue has been declared.

        :rtype bool:

        """
        try:
            self.channel.queue_declare(queue=queue, passive=True)
        except Exception, e:
            if e.amqp_reply_code == 404:
                return False
            raise e
        else:
            return True

    def queue_delete(self, queue, if_unused=False, if_empty=False):
        """Delete queue by name."""
        return self.channel.queue_delete(queue=queue, if_unused=if_unused, if_empty=if_empty)

    def queue_purge(self, queue, **kwargs):
        """Discard all messages in the queue. This will delete the messages
        and results in an empty queue."""
        return self.channel.queue_purge(queue=queue)

    def queue_declare(self, queue, durable, exclusive, auto_delete,
            warn_if_exists=False):
        """Declare a named queue."""
        """
        if warn_if_exists and self.queue_exists(queue):
            warnings.warn(QueueAlreadyExistsWarning(
                QueueAlreadyExistsWarning.__doc__))
        """

        return self.channel.queue_declare(queue=queue,
                                          durable=durable,
                                          exclusive=exclusive,
                                          auto_delete=auto_delete)

    def exchange_declare(self, exchange, type, durable, auto_delete):
        """Declare an named exchange."""
        return self.channel.exchange_declare(exchange=exchange,
                                             type=type,
                                             durable=durable,
                                             auto_delete=auto_delete)

    def queue_bind(self, queue, exchange, routing_key, arguments=None):
        """Bind queue to an exchange using a routing key."""
        return self.channel.queue_bind(queue=queue,
                                       exchange=exchange,
                                       routing_key=routing_key,
                                       arguments=arguments)

    def message_to_python(self, raw_message):
        """Convert encoded message body back to a Python value."""
        return self.Message(backend=self, amqp_message=raw_message)

    def get(self, queue, no_ack=False):
        """Receive a message from a declared queue by name.

        :returns: A :class:`Message` object if a message was received,
            ``None`` otherwise. If ``None`` was returned, it probably means
            there was no messages waiting on the queue.

        """
        return self.channel.basic_get(queue=queue, no_ack=no_ack)

    def declare_consumer(self, queue, no_ack, callback, consumer_tag, nowait=False):
        """Declare a consumer.
        XXX here is where delegate comes in, callback
        """
        self.channel.register_deliver_callback(callback)
        return self.channel.basic_consume(queue=queue,
                                          no_ack=no_ack,
                                          # callback=callback,
                                          consumer_tag=consumer_tag,
                                          nowait=nowait)

    def consume(self, limit=None):
        """
        """

    def cancel(self, consumer_tag):
        """Cancel a channel by consumer tag."""
        return self.channel.basic_cancel(consumer_tag=consumer_tag)

    def close(self):
        """Close the channel if open."""
        # if self._channel and self._channel.is_open:
        d = self._channel.channel_close()
        # @todo  does this work before callback is fired?
        self._channel_ref = None
        return d

    def ack(self, delivery_tag):
        """Acknowledge a message by delivery tag."""
        return self.channel.basic_ack(delivery_tag)

    def reject(self, delivery_tag):
        """Reject a message by deliver tag."""
        return self.channel.basic_reject(delivery_tag, requeue=False)

    def requeue(self, delivery_tag):
        """Reject and requeue a message by delivery tag."""
        return self.channel.basic_reject(delivery_tag, requeue=True)

    def prepare_message(self, message_data, delivery_mode, priority=None,
                content_type=None, content_encoding=None, headers={},
                reply_to=None, correlation_id=None, expiration=None,
                message_id=None, timestamp=None, type=None, user_id=None,
                app_id=None, cluster_id=None):
        """Encapsulate data into a AMQP message."""
        properties = {
                  'content type':content_type,
                  'content encoding':content_encoding,
                  'delivery mode':delivery_mode,
                  'priority':priority,
                  'correlation id':correlation_id,
                  'reply to':reply_to,
                  'expiration':expiration,
                  'message id':message_id,
                  'timestamp':timestamp,
                  'type':type,
                  'user id':user_id,
                  'app id':app_id,
                  'cluster id':cluster_id,
                  }
        message = Content(message_data, properties=properties)
        return message

    def publish(self, message, exchange, routing_key, mandatory=None,
            immediate=None, headers=None):
        """Publish a message to a named exchange."""

        """
        if headers:
            message.properties["headers"] = headers
        """

        ret = self.channel.basic_publish(content=message, exchange=exchange,
                                         routing_key=routing_key,
                                         mandatory=mandatory,
                                         immediate=immediate)
        """
        if mandatory or immediate:
            self.close()
        """
        return ret

    def qos(self, prefetch_size, prefetch_count, apply_global=False):
        """Request specific Quality of Service."""
        return self.channel.basic_qos(prefetch_size=prefetch_size,
                prefetch_count=prefetch_count, global_=apply_global)

    def flow(self, active):
        """Enable/disable flow from peer."""
        self.channel.flow(active)
