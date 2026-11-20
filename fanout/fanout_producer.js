const amqp = require('amqplib');

// Both notification services receive every message from this fanout exchange.
const EXCHANGE = 'new_product_launch';

async function announceNewProduct(product) {
    let connection;
    let channel;

    try {
        connection = await amqp.connect('amqp://localhost');
        channel = await connection.createConfirmChannel();

        await channel.assertExchange(EXCHANGE, 'fanout', { durable: true });

        const message = JSON.stringify(product);
        channel.publish(EXCHANGE, '', Buffer.from(message), {
            persistent: true,
            contentType: 'application/json'
        });
        await channel.waitForConfirms();

        console.log('Published product announcement:', message);
    } catch (error) {
        console.error('Failed to publish product announcement:', error.message);
        process.exitCode = 1;
    } finally {
        if (channel) await channel.close().catch(() => {});
        if (connection) await connection.close().catch(() => {});
    }
}

announceNewProduct({ id: 123, name: 'iPhone 19 Pro Max', price: 200000 });
