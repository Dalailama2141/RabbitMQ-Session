const amqp = require('amqplib');

async function pushNotification() {
    let connection;
    let channel;

    try {
        connection = await amqp.connect('amqp://localhost');
        channel = await connection.createChannel();

        const exchange = 'new_product_launch';
        const queue = 'push_notification_queue';

        await channel.assertExchange(exchange, 'fanout', { durable: true });
        await channel.assertQueue(queue, { durable: true }); 
        // Each service has its own queue, so both receive every announcement.
        await channel.bindQueue(queue, exchange, '');

        console.log('[Push Notification] Waiting for product announcements');
        await channel.consume(queue, (msg) => {
            if (!msg) return;

            try {
                const product = JSON.parse(msg.content.toString());
                console.log('[Push Notification] New product:', product.name);
                channel.ack(msg);
            } catch (error) {
                console.error('[Push Notification] Failed to process message:', error.message);
                channel.nack(msg, false, false);
            }
        }, { noAck: false });
    } catch (error) {
        console.error('[Push Notification] Failed to start:', error.message);
        if (channel) await channel.close().catch(() => {});
        if (connection) await connection.close().catch(() => {});
        process.exitCode = 1;
    }
}

pushNotification();
