const amqp = require('amqplib');
const mysql = require('mysql2/promise');
const dbConfig = require('./dbConfig'); // Import database configuration

async function receiveFromQueue() {
    try {
        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();
        const queueName = 'qtest1';
        await channel.assertQueue(queueName, { durable: true });

        console.log('Worker service is waiting for messages...');

        channel.consume(queueName, async (message) => {
            if (message !== null) {
                const data = JSON.parse(message.content.toString());
                console.log('Received message:', data);
                await processMessage(data);
                channel.ack(message);
            }
        });
    } catch (error) {
        console.error('Error:', error.message);
    }
}

async function processMessage(data) {
    try {
        const connection = await mysql.createConnection(dbConfig);

        switch (data.command) {
            case 'create':
                await handleCreate(connection, data.data);
                break;
            case 'update':
                await handleUpdate(connection, data.data);
                break;
            case 'delete':
                await handleDelete(connection, data.data);
                break;
            default:
                console.log('Unknown command:', data.command);
        }

        await connection.end(); // Close connection after processing message
    } catch (error) {
        console.error('Error:', error.message);
    }
}

async function handleCreate(connection, data) {
    const [results, fields] = await connection.execute(
        'INSERT INTO Test01 (Nama, Status, Created, Updated) VALUES (?, ?, NOW(), NOW())',
        [data.Nama, data.Status]
    );
    console.log('Data inserted:', data);
}

async function handleUpdate(connection, data) {
    const [results, fields] = await connection.execute(
        'UPDATE Test01 SET Nama = ?, Status = ?, Updated = NOW() WHERE Id = ?',
        [data.Nama, data.Status === null ? null : data.Status, data.Id]
    );
    console.log('Data updated:', data);
}

async function handleDelete(connection, data) {
    const [results, fields] = await connection.execute(
        'DELETE FROM Test01 WHERE Id = ?',
        [data.Id]
    );
    console.log('Data deleted:', data);
}


receiveFromQueue(); // Start listening for messages
