const amqp = require('amqplib');

// This consumer handles notification messages from the direct-exchange example.
async function getNotifications() {
    let connection;
    let channel;

    try {
        connection = await amqp.connect('amqp://localhost');
        connection.on('error', (error) => {
            console.error('RabbitMQ connection error:', error.message);
        });

        channel = await connection.createChannel();

        const exchange = 'mail_exchange';
        const routingKey = 'send_notification';
        const queue = 'notification_queue';

        // These names and the routing key must match the producer's setup.
        await channel.assertExchange(exchange, 'direct', { durable: false });
        await channel.assertQueue(queue, { durable: true });
        // A direct exchange uses an exact routing-key match.
        await channel.bindQueue(queue, exchange, routingKey);

        // Manual acknowledgements let RabbitMQ retain a message until handled.
        await channel.consume(
            queue,
            (message) => {
                try {
                    // Convert the message's bytes back into the JSON object we sent.
                    const content = JSON.parse(message.content.toString());
                    console.log('Received notification message:', content);
                    // Tell RabbitMQ it can remove this message from the queue.
                    channel.ack(message);
                } catch (error) {
                    console.error('Invalid notification message; rejecting it:', error.message);
                    // Reject a message we cannot read; do not put it back in the queue.
                    channel.nack(message, false, false);
                }
            },
            { noAck: false }
        );
    } catch (error) {
        console.error('Failed to start notification consumer:', error.message);
        if (channel) await channel.close().catch(() => {});
        if (connection) await connection.close().catch(() => {});
        process.exitCode = 1;
    }
}

getNotifications();
