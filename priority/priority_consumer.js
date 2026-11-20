const amqp = require('amqplib');

async function consumeMessages() {
    let connection;
    let channel;

    try {
        connection = await amqp.connect('amqp://localhost');
        channel = await connection.createChannel();

        const queue = 'priority_queue';

        await channel.assertQueue(queue, {
            durable: true,
            arguments: { 'x-max-priority': 10 }
        });

        console.log(`Waiting for messages in ${queue}. To exit press CTRL+C`);

        await channel.consume(queue, (msg) => {
            if (msg !== null) {
                console.log(`Received: ${msg.content.toString()} (priority ${msg.properties.priority ?? 0})`);
                channel.ack(msg);
            }
        });
    } catch (error) {
        console.error('Error consuming messages:', error.message);
        if (channel) await channel.close().catch(() => {});
        if (connection) await connection.close().catch(() => {});
        process.exitCode = 1;
    }
}

consumeMessages();
