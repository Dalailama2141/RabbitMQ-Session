const amqp = require('amqplib');

const consumeNewVideoNotifications = async () => {
    let connection;
    let channel;

    try {
        connection = await amqp.connect('amqp://localhost');
        channel = await connection.createChannel();

        const exchange = 'header_exchange';
        await channel.assertExchange(exchange, 'headers', { durable: true });

        const queue = await channel.assertQueue('', { exclusive: true });
        await channel.bindQueue(queue.queue, exchange, '', {
            'x-match': 'all',
            'notification-type': 'new_video',
            'content-type': 'video'
        });

        console.log('Waiting for new video notifications');
        await channel.consume(queue.queue, (msg) => {
            if (!msg) return;
            console.log('Received new video notification:', msg.content.toString());
            channel.ack(msg);
        });
    } catch (error) {
        console.error('Failed to start new video consumer:', error.message);
        if (channel) await channel.close().catch(() => {});
        if (connection) await connection.close().catch(() => {});
        process.exitCode = 1;
    }
};

consumeNewVideoNotifications();
