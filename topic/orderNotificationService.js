const amqp = require('amqplib');

// This service listens only for order-related notification events.
async function receiveMessages() {
    let connection;
    let channel;

    try {
        // Connect to RabbitMQ and create a channel for this service's work.
        connection = await amqp.connect('amqp://localhost');
        channel = await connection.createChannel();

        const exchange = 'notification_exchange';
        const queue = 'order_queue';

        // The exchange name and type must match the producer's exchange.
        await channel.assertExchange(exchange, 'topic', { durable: true });
        await channel.assertQueue(queue, { durable: true });
        // order.* matches one word after "order", for example order.created.
        await channel.bindQueue(queue, exchange, 'order.*');

        console.log('[Order Notification] Waiting for messages');
        // RabbitMQ calls this function whenever an order message arrives.
        await channel.consume(queue, (msg) => {
            if (!msg) return;

            try {
                console.log(
                    `[Order Notification] Consumed ${msg.fields.routingKey}: ${msg.content.toString()}`
                );
                // ack confirms successful handling so RabbitMQ can remove the message.
                channel.ack(msg);
            } catch (error) {
                console.error('[Order Notification] Failed to process message:', error.message);
                // Reject malformed/unhandled messages without sending them back to the queue.
                channel.nack(msg, false, false);
            }
        }, { noAck: false });
    } catch (error) {
        console.error('[Order Notification] Failed to start:', error.message);
        if (channel) await channel.close().catch(() => {});
        if (connection) await connection.close().catch(() => {});
        process.exitCode = 1;
    }
}

receiveMessages();
