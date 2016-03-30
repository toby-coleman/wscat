from autobahn.twisted.websocket import WebSocketClientProtocol, WebSocketClientFactory, connectWS
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet import reactor, ssl
import json
import sys
import os
import logging
from logging.handlers import TimedRotatingFileHandler
from signal import signal, SIGPIPE, SIG_DFL

logger = logging.getLogger(__name__)
# Don't create broken pipe errors
signal(SIGPIPE, SIG_DFL)

class MyClientProtocol(WebSocketClientProtocol):

    def onConnect(self, response):
        logger.info('Server connected: {0}'.format(response.peer))

    def onOpen(self):
        logger.info('WebSocket connection open')

        # Send message to subscribe to topic
        if settings.get('connect_message', None):
            logger.info('Sending: {0}'.format(settings['connect_message']))
            self.sendMessage(settings['connect_message'].encode('utf-8'))

    def onMessage(self, payload, isBinary):
        if isBinary:
            logger.info('Binary message received: {0} bytes'.format(len(payload)))
        else:
            msg = payload.decode('utf8').strip()
            logger.info('Text message received: {0}'.format(msg))
            # Print to stdout
            print(' '.join(msg.split()))

    def onPing(self, payload):
        logger.info('Ping received from {}'.format(self.peer))
        self.sendPong(payload)
        logger.info('Pong sent to {}'.format(self.peer))

    def onClose(self, wasClean, code, reason):
        logger.info('WebSocket connection closed: {0}'.format(reason))


class MyClientFactory(WebSocketClientFactory, ReconnectingClientFactory):

    protocol = MyClientProtocol

    def clientConnectionFailed(self, connector, reason):
        logger.error('Client connection failed: {0}'.format(reason))
        logger.info('Retrying connection')
        self.retry(connector)

    def clientConnectionLost(self, connector, reason):
        logger.error('Client connection failed: {0}'.format(reason))
        logger.info('Retrying connection')
        self.retry(connector)


if __name__ == '__main__':
    # Load settings
    settings_path = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(sys.argv[1])))
    settings_file = os.path.join(settings_path, sys.argv[1])
    logger.info('Using settings from: {0}'.format(settings_file))
    try:
        with open(settings_file, 'r') as data_file:
            settings = json.load(data_file)
    except Exception as e:
        logger.error('Unable to load settings file: {0}'.format(e))
        exit()

    log_level = logging.getLevelName(settings.get('log_level', 'ERROR').upper())
    logger.setLevel(log_level)
    if settings.get('log_file', None):
        # Set up logging to file
        log_path, log_file = os.path.split(settings['log_file'])
        if log_path and not os.path.exists(log_path):
            os.makedirs(log_path)
        fh = TimedRotatingFileHandler(settings['log_file'], 'D', 1, 7)
        fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
        fh.setFormatter(fmt)
        fh.setLevel(log_level)
        logger.addHandler(fh)

    try:
        factory = MyClientFactory(settings['url'])
        factory.protocol = MyClientProtocol
        factory.setProtocolOptions(autoPingInterval=settings.get('ping_interval', 30),
                                   autoPingTimeout=settings.get('ping_timeout', 3))
    except Exception as e:
        logger.error(e)
        exit()

    try:
        if factory.isSecure:
            contextFactory = ssl.ClientContextFactory()
        else:
            contextFactory = None
        connectWS(factory, contextFactory)
        reactor.run()
    except Exception as e:
        logger.error(e)
