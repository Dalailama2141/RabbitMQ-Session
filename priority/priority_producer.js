const amqp = require('amqplib');

async function sendMessages() {
    let connection;
    let channel;

    try {
        connection = await amqp.connect('amqp://localhost');
        channel = await connection.createConfirmChannel();

        const exchange = 'priority_exchange';
        const queue = 'priority_queue';
        const routingKey = 'priority_key';

        await channel.assertExchange(exchange, 'direct', { durable: true });
        await channel.assertQueue(queue, {
            durable: true,
            arguments: { 'x-max-priority': 10 }
        });
        await channel.bindQueue(queue, exchange, routingKey);

        const messages = [
            { msg: 'Hello low: 1', priority: 1 },
            { msg: 'Hello High: 8', priority: 8 },
            { msg: 'Hello Mid: 2', priority: 2 }
        ];

        for (const item of messages) {
            channel.publish(exchange, routingKey, Buffer.from(item.msg), {
                persistent: true,
                priority: item.priority
            });
            console.log(`Sent: ${item.msg} (priority ${item.priority})`);
        }

        await channel.waitForConfirms();
    } catch (error) {
        console.error('Error publishing priority messages:', error.message);
        process.exitCode = 1;
    } finally {
        if (channel) await channel.close().catch(() => {});
        if (connection) await connection.close().catch(() => {});
    }
}

sendMessages();
