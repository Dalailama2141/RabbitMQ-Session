const amqp = require('amqplib');

const EXCHANGE = 'header_exchange';

async function sendNotifications() {
    let connection;
    let channel;

    try {
        connection = await amqp.connect('amqp://localhost');
        channel = await connection.createConfirmChannel();
        await channel.assertExchange(EXCHANGE, 'headers', { durable: true });

        const notifications = [
            {
                headers: {
                    'x-match': 'all',
                    'notification-type': 'new_video',
                    'content-type': 'video'
                },
                message: 'New music video uploaded'
            },
            {
                headers: {
                    'x-match': 'all',
                    'notification-type': 'live_stream',
                    'content-type': 'gaming'
                },
                message: 'Gaming live stream started'
            },
            {
                headers: {
                    'x-match': 'any',
                    'notification-type-comment': 'comment',
                    'notification-type-like': 'like',
                    'content-type': 'vlog'
                },
                message: 'New comment on your vlog'
            },
            {
                headers: {
                    'x-match': 'any',
                    'notification-type-like': 'like',
                    'content-type': 'vlog'
                },
                message: 'Someone liked your vlog'
            }
        ];

        for (const { headers, message } of notifications) {
            channel.publish(EXCHANGE, '', Buffer.from(message), {
                persistent: true,
                headers
            });
            console.log('Published notification:', message, headers);
        }

        await channel.waitForConfirms();
    } catch (error) {
        console.error('Failed to publish notifications:', error.message);
        process.exitCode = 1;
    } finally {
        if (channel) await channel.close().catch(() => {});
        if (connection) await connection.close().catch(() => {});
    }
}

sendNotifications();
