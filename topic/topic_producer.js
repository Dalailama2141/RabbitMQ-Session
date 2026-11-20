 const amqp = require('amqplib');

// Both notification services listen to this topic exchange.
const EXCHANGE = 'notification_exchange';

// Publish one order event and one payment event.
async function publishNotifications() {
    let connection;
    let channel;

    try {
        // Connect to RabbitMQ, then open a confirm channel to publish on.
        connection = await amqp.connect('amqp://localhost');
        channel = await connection.createConfirmChannel();
        // Topic exchanges match routing-key words using patterns such as order.*.
        await channel.assertExchange(EXCHANGE, 'topic', { durable: true });

        // Routing keys describe the event: category.action.
        const messages = [
            {
                routingKey: 'order.created',
                message: { orderId: 'ORD-1001', status: 'created' }
            },
            {
                routingKey: 'payment.completed',
                message: { orderId: 'ORD-1001', amount: 49.99, status: 'completed' }
            }
        ];

        for (const { routingKey, message } of messages) {
            // Convert data to JSON bytes. The services bind to matching key patterns.
            channel.publish(EXCHANGE, routingKey, Buffer.from(JSON.stringify(message)), {
                persistent: true,
                contentType: 'application/json'
            });
            console.log(`Published ${routingKey}:`, message);
        }

        // Wait until RabbitMQ confirms it received all the published messages.
        await channel.waitForConfirms();
    } catch (error) {
        console.error('Failed to publish notifications:', error.message);
        process.exitCode = 1;
    } finally {
        // Always close the channel and connection after publishing.
        if (channel) await channel.close().catch(() => {});
        if (connection) await connection.close().catch(() => {});
    }
}

publishNotifications();
