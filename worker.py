"""
Throttler main worker
"""
from pymongo import MongoClient
from pika import spec
import pika
import json
import logging
import sys
import time

db_auth_log = None #Mongo users DB auth log
db_users = None #Mongo users DB
db_user_ip = None
logger = None

def processUserRegistration(ch, method, properties, body):
    """
    Function gets called whenever we get an item from the queue about a user logging on or off.
    """
    global db_auth_log
    global db_users
    global db_user_ip
    global logger
    
    request = json.loads(body)
    logger.debug("Got user %s on address %s" % (request['username'], request['ip_address']))
    
    #A user auth has come in.  Log it in our historical records table
    db_auth_log.insert( {
        'username': request['username'],
        'authed_time': request['timestamp'],
        'method': request['method'],
        'ip_address': request['ip_address'],
    } )
    
    db_user_ip.update( {
        '_id': request['ip_address'],
        'username': request['username'],
        'authed_time': request['timestamp'],
        'method': request['method'],
    }, upsert=True)

    #Ack the processing of this transaction
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main(settings):
    """
    settings:  The setting dictionary
    """
    logger.debug("Starting main function..")
    global db_auth_log
    global db_users
    global db_user_ip
    global logger
    
    
    #Setup the MongoDB Connection
    mongo_client = MongoClient(settings['mongodb_server'], 27017)
    db = mongo_client[settings['mongodb_database']]
    db.authenticate(settings['mongodb_username'], settings['mongodb_password'])
    db_auth_log = db[settings['mongodb_auth_log_collection']]
    db_auth_log.ensure_index('authed_time', expire_after_seconds=31536000L) #Keep the auth log around for 12 months
    db_user_ip = db.user_ip
    
    exclusive = False
    durable=True

    #Setup the message queue
    amqp_connection = pika.BlockingConnection(pika.ConnectionParameters(settings['amqp_server']))
    amqp_channel = amqp_connection.channel()
    amqp_channel.exchange_declare(exchange=settings['amqp_exchange'] ,type='fanout')
    amqp_channel.queue_declare(queue=settings['amqp_queue'], durable=durable, exclusive=exclusive)
    amqp_channel.queue_bind(exchange=settings['amqp_exchange'], queue=settings['amqp_queue'])
    
    #Setup the basic consume settings so we don't try and process too much at a time
    amqp_channel.basic_qos(prefetch_count=4)
            
    #Bind to the queues and start consuming
    amqp_channel.basic_consume(processUserRegistration, queue=settings['amqp_queue'])
    amqp_channel.start_consuming()
    
if __name__ == "__main__":
    #Load up the settings from disk
    logging.basicConfig()
    
    settings = {}
    for setting in open('settings.txt', 'r').read().split('\n'):
        setting = setting.strip()
        if setting == '' or setting[0] in ['!', '#'] or ':' not in setting:
            continue    
        key, value = setting.split(":")
        settings[key.strip()] = value.strip()
    
    #If we're in debug/testing.. just run and die
    logger = logging.getLogger('worker')
    if 'mode' in settings and settings['mode'] == 'debug':
        logger.setLevel(logging.DEBUG)
    elif 'mode' in settings and setting['mode' == 'test']:
        logger.setLevel(logging.INFO)
        main(settings)
        sys.exit(0)
    
    #If we're in production, print out the exception and try and restart the app
    while 1:
        try:
            main(settings)
        except:
            logging.critical("-- ERROR Has occured in Herbert Throttler.  Sleeping and re-running")
            logging.critical(sys.exc_info()[0])
            time.sleep(5)
