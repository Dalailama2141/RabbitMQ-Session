const amqp = require('amqplib');

// A consumer waits for messages and handles them one at a time.
async function getMail() {
    let connection;
    let channel;
    try {
        connection = await amqp.connect('amqp://localhost');
        connection.on('error', (error) => {
            console.error('RabbitMQ connection error:', error.message);
        });

        // Work on a channel inside our RabbitMQ connection.
        channel = await connection.createChannel();

        const exchange = 'mail_exchange';
        const routingKey = 'send_mail';
        const queue = 'mail_queue';

        // Use the same exchange, queue, and routing key as producer.js.
        await channel.assertExchange(exchange, 'direct', { durable: false });
        await channel.assertQueue(queue, { durable: true });
        // This exact-match binding directs 'send_mail' messages into mail_queue.
        await channel.bindQueue(queue, exchange, routingKey);

        // noAck:false means we must acknowledge a message after handling it.
        await channel.consume(
            queue,
            (message) => {
                try {
                    // The producer sent JSON bytes; turn them back into an object.
                    const content = JSON.parse(message.content.toString());
                    console.log('Received mail message:', content);
                    // ack tells RabbitMQ this message was handled successfully.
                    channel.ack(message);
                } catch (error) {
                    console.error('Invalid mail message; rejecting it:', error.message);
                    // nack with requeue=false rejects it and prevents a bad-message loop.
                    channel.nack(message, false, false);
                }
            },
            { noAck: false }
        );
    } catch (error) {
        console.error('Failed to start mail consumer:', error.message);
        if (channel) await channel.close().catch(() => {});
        if (connection) await connection.close().catch(() => {});
        process.exitCode = 1;
    }
}

getMail();