let amqp = require('amqplib/callback_api')

function bail(err) {
    console.error(err);
}

function encode(doc) {
    return new Buffer(JSON.stringify(doc));
}

class rabbitAdapter {
    constructor(
        hostname,
        port,
        username,
        password,
        locale = "en_US",
        frameMax = 0,
        heartbeat = 0,
        vhost = "/",
        protocol = 'amqp'
    ) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.locale = locale;
        this.frameMax = frameMax;
        this.heartbeat = heartbeat;
        this.vhost = vhost;
        this.amqp = amqp;
        this.protocol = protocol

        /**
             * Publish to an Exchange and queue
             * 
             * @function
             *
             * @param {string} exchangeName
             *   Exchange 
             * @param {string} queueName
             *   Queue Name 
             *  @param {object} message
             *   Message as object (json)
             */
        this.publishExchangeQueueMessage = async (exchangeName, queueName, message) => {
            this.amqp.connect(
                {
                    hostname: this.hostname,
                    port: this.port,
                    username: this.username,
                    password: this.password,
                    locale: this.locale,
                    frameMax: this.frameMax,
                    heartbeat: this.heartbeat,
                    vhost: this.vhost,
                    protocol: this.protocol,
                }, (err, conn) => {
                    if (err != null) bail(err);
                    // Create Exchange
                    let cE = async (conn, exchangeName) => {
                        if (!conn || conn == undefined) {
                            return
                        }
                        try {
                            conn.createChannel(on_open);
                        }
                        catch (erro) {
                            console.log(erro)
                        }
                        function on_open(err, ch) {
                            if (err != null) bail(err);
                            let common_options = { durable: true, noAck: true };
                            ch.assertExchange(exchangeName, 'direct', common_options);
                            ch.sendToQueue(queueName, encode(message));
                            ch.close(function () {
                                conn.close();
                            });
                        }
                    }

                    cE(conn, exchangeName)
                })
        }
        /**
            * Listen to exchange and queue
            *
            * @function
            *
            * @param {string} exchangeName
            *   Exchange Name
            * @param {string} queueName
            *   Queue Name
            * @param {Object} onListen
            *   Callback
            */
        this.listenExchange = async (exchangeName, queueName, onListen) => {
            this.amqp.connect(
                {
                    hostname: this.hostname,
                    port: this.port,
                    username: this.username,
                    password: this.password,
                    locale: this.locale,
                    frameMax: this.frameMax,
                    heartbeat: this.heartbeat,
                    vhost: this.vhost,
                    protocol: this.protocol,
                }, (err, conn) => {
                    if (err != null) bail(err);
                    const listen = async (conn, exchangeName, queueName, onListen) => {
                        if (!conn || conn == undefined) {
                            return
                        }
                        conn.createChannel((err, ch) => {
                            if (err != null) bail(err);
                            let common_options = { durable: true, noAck: true };
                            ch.assertExchange(exchangeName, 'direct', common_options);
                            ch.assertQueue(queueName, { exclusive: true }, (err, q) => {
                                console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", q.queue);
                                ch.bindQueue(q.queue, exchangeName, '');

                                ch.consume(q.queue, onListen, { noAck: true });
                            })
                        })
                    }
                    listen(conn, exchangeName, queueName, onListen);
                });
        }
    }
}

module.exports.rabbitAdapter = rabbitAdapter