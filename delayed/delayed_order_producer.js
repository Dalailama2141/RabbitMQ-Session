const amqp = require('amqplib');

const EXCHANGE = 'delayed_order_updates_exchange';
const QUEUE = 'delayed_order_updates_queue';
const ROUTING_KEY = 'order.update';

async function sendOrderUpdates() {
    let connection;
    let channel;

    try {
        connection = await amqp.connect('amqp://localhost:5673');
        channel = await connection.createConfirmChannel();

        // Requires RabbitMQ's rabbitmq_delayed_message_exchange plugin.
        await channel.assertExchange(EXCHANGE, 'x-delayed-message', {
            durable: true,
            arguments: { 'x-delayed-type': 'direct' }
        });
        await channel.assertQueue(QUEUE, { durable: true });
        await channel.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY);

        const updates = [
            { batchId: 'BATCH-1001', delayMs: 5000 },
            { batchId: 'BATCH-1002', delayMs: 10000 },
            { batchId: 'BATCH-1003', delayMs: 15000 }
        ];

        for (const { batchId, delayMs } of updates) {
            const message = JSON.stringify({ batchId });
            channel.publish(EXCHANGE, ROUTING_KEY, Buffer.from(message), {
                persistent: true,
                contentType: 'application/json',
                headers: { 'x-delay': delayMs }
            });
            console.log(`Scheduled order update for batch ${batchId} in ${delayMs} ms`);
        }

        await channel.waitForConfirms();
    } catch (error) {
        console.error('Failed to schedule order updates:', error.message);
        process.exitCode = 1;
    } finally {
        if (channel) await channel.close().catch(() => {});
        if (connection) await connection.close().catch(() => {});
    }
}

sendOrderUpdates();
