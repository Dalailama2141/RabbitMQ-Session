const amqp = require('amqplib');

const consumeLiveStreamNotifications = async () => {
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
            'notification-type': 'live_stream',
            'content-type': 'gaming'
        });

        console.log('Waiting for live stream notifications');
        await channel.consume(queue.queue, (msg) => {
            if (!msg) return;
            console.log('Received live stream notification:', msg.content.toString());
            channel.ack(msg);
        });
    } catch (error) {
        console.error('Failed to start live stream consumer:', error.message);
        if (channel) await channel.close().catch(() => {});
        if (connection) await connection.close().catch(() => {});
        process.exitCode = 1;
    }
};

consumeLiveStreamNotifications();
