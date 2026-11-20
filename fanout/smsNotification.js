const amqp = require('amqplib');

async function smsNotification() {
    let connection;
    let channel;

    try {
        connection = await amqp.connect('amqp://localhost');
        channel = await connection.createChannel();

        const exchange = 'new_product_launch';
        const queue = 'sms_notification_queue';

        await channel.assertExchange(exchange, 'fanout', { durable: true });
        await channel.assertQueue(queue, { durable: true });
        // Fanout exchanges ignore the binding key and copy each message to every bound queue.
        await channel.bindQueue(queue, exchange, '');

        console.log('[SMS Notification] Waiting for product announcements');
        await channel.consume(queue, (msg) => {
            if (!msg) return;

            try {
                const product = JSON.parse(msg.content.toString());
                console.log('[SMS Notification] New product:', product.name);
                channel.ack(msg);
            } catch (error) {
                console.error('[SMS Notification] Failed to process message:', error.message);
                channel.nack(msg, false, false);
            }
        }, { noAck: false });
    } catch (error) {
        console.error('[SMS Notification] Failed to start:', error.message);
        if (channel) await channel.close().catch(() => {});
        if (connection) await connection.close().catch(() => {});
        process.exitCode = 1;
    }
}

smsNotification();
