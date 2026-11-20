const amqp = require('amqplib');

const QUEUE = 'delayed_order_updates_queue';

async function updateOrderStatuses(batchId) {
    // Replace this log with the application's order status update logic.
    console.log(`Order statuses updated for batch: ${batchId}`);
}

async function processOrderUpdates() {
    let connection;
    let channel;

    try {
        connection = await amqp.connect('amqp://localhost:5673');
        channel = await connection.createChannel();

        await channel.assertQueue(QUEUE, { durable: true });
        console.log(`Waiting for delayed order updates in ${QUEUE}. Press CTRL+C to exit.`);

        await channel.consume(QUEUE, async (msg) => {
            if (msg === null) return;

            try {
                const { batchId } = JSON.parse(msg.content.toString());
                console.log(`Processing order update task for batch: ${batchId}`);
                await updateOrderStatuses(batchId);
                channel.ack(msg);
            } catch (error) {
                console.error('Failed to process order update:', error.message);
                // Reject malformed or failed tasks without creating a requeue loop.
                channel.nack(msg, false, false);
            }
        }, { noAck: false });
    } catch (error) {
        console.error('Error consuming delayed order updates:', error.message);
        if (channel) await channel.close().catch(() => {});
        if (connection) await connection.close().catch(() => {});
        process.exitCode = 1;
    }
}

processOrderUpdates();
