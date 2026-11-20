const amqp = require('amqplib');

const consumeCommentsAndLikes = async () => {
    let connection;
    let channel;

    try {
        connection = await amqp.connect('amqp://localhost');
        channel = await connection.createChannel();

        const exchange = 'header_exchange';
        await channel.assertExchange(exchange, 'headers', { durable: true });

        const queue = await channel.assertQueue('', { exclusive: true });
        await channel.bindQueue(queue.queue, exchange, '', {
            'x-match': 'any',
            'notification-type-comment': 'comment',
            'notification-type-like': 'like'
        });

        console.log('Waiting for comment and like notifications');
        await channel.consume(queue.queue, (msg) => {
            if (!msg) return;
            console.log('Received comment or like notification:', msg.content.toString());
            channel.ack(msg);
        });
    } catch (error) {
        console.error('Failed to start comments/likes consumer:', error.message);
        if (channel) await channel.close().catch(() => {});
        if (connection) await connection.close().catch(() => {});
        process.exitCode = 1;
    }
};

consumeCommentsAndLikes();
