(Herbert) User-auth-writer
-------------------

This project is responsible for logging user-auth messages into the MongoDB data store.
It logs both the current user_ip IP to User mapping as well as a historical table that gets kept for a year


Requirements
-------------------
 * Python 2.7
 * RabbitMQ Library, Pika https://github.com/pika/pika/
 * pymongo

Configuration
-------------------
The Configuration required is as simple as follows.  It will try and open config.txt in the working directory

config.txt

```

amqp_server: RabbitMQ Server
amqp_exchange: RabbitMQ User Reg exchange (probably user_auth)
amqp_queue: RabbitMQ Queue to use for Processing the user-auth registration (probably herbert_auth_log)

mongodb_server: MongoDB Cluster (probably mongo.its.uow.edu.au)
mongodb_database: MongoDB Database (probably herbert)
mongodb_auth_log_collection: MongoDB User Database (probably auth_log)
mongodb_username: MongoDB Username
mongodb_password: MongoDB Password

```