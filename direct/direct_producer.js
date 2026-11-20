const amqp = require('amqplib');

// A producer creates messages and sends them to RabbitMQ.
async function publishMessages() {
    let connection;
    let channel;
    try {
        // A connection talks to the RabbitMQ server; a channel is a lightweight
        // workspace inside that connection where we do messaging work.
        connection = await amqp.connect('amqp://localhost');
        connection.on('error', (error) => {
            console.error('RabbitMQ connection error:', error.message);
        });

        // A confirm channel lets us know RabbitMQ accepted each published message.
        channel = await connection.createConfirmChannel();

        // A direct exchange sends a message to queues whose binding key exactly
        // matches the message's routing key.
        const exchange = 'mail_exchange';
        const routes = [
            {
                routingKey: 'send_mail',
                queue: 'mail_queue',
                message: {
                    to: 'rahul@gmail.com',
                    from: 'ajay@mail.com',
                    subject: 'Hello IP address',
                    body: 'This is for testing only'
                },
            },
            {
                routingKey: 'send_notification',
                queue: 'notification_queue',
                message: {
                    type: 'notification',
                    recipient: 'rahul',
                    body: 'You have a new notification'
                },
            }
        ];

        // assertExchange / assertQueue make sure these named RabbitMQ objects exist.
        await channel.assertExchange(exchange, 'direct', { durable: false });

        for (const route of routes) {
            await channel.assertQueue(route.queue, { durable: true });
            // A binding is the rule connecting a queue to an exchange.
            await channel.bindQueue(route.queue, exchange, route.routingKey);
        }

        console.log(`Publishing messages to exchange "${exchange}"...`);

        for (const route of routes) {
            // RabbitMQ carries bytes, so convert our JavaScript object to JSON bytes.
            const body = Buffer.from(JSON.stringify(route.message));
            await new Promise((resolve, reject) => {
                // The routing key tells the direct exchange which queue should get it.
                // persistent asks RabbitMQ to keep the message if it restarts.
                channel.publish(exchange, route.routingKey, body, { persistent: true },
                    (error) => {
                        if (error) {
                            reject(error);
                            return;
                        }

                        console.log(
                            `Published to queue "${route.queue}" with routing key "${route.routingKey}":`,
                            route.message
                        );
                        resolve();
                    }
                );
            });
        }
    } catch (error) {
        console.error('Failed to publish messages:', error.message);
    } finally {
        // Close resources when publishing is finished so the process can exit cleanly.
        if (channel) await channel.close().catch((error) => {
            console.error('Failed to close RabbitMQ channel:', error.message);
        });
        if (connection) await connection.close().catch((error) => {
            console.error('Failed to close RabbitMQ connection:', error.message);
        });
    }
}

publishMessages();
