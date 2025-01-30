import * as amqp from 'amqplib';
import { MongoClient } from 'mongodb';

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const MONGO_URL = process.env.MONGO_URL || 'mongodb://localhost:27017';
const DB_NAME = process.env.DB_NAME || 'rabbitmq_messages';
const COLLECTION_NAME = process.env.COLLECTION_NAME || 'messages';

const EXCHANGE_NAME = process.env.EXCHANGE_NAME || 'example_exchange';
const QUEUE_NAME = process.env.QUEUE_NAME || 'example_queue';
const ROUTING_KEY = process.env.ROUTING_KEY || 'example_queue';

async function connectToMongoDB() {
    try {
        const client = new MongoClient(MONGO_URL);
        await client.connect();
        console.log(`✅ Connected to MongoDB at ${MONGO_URL}`);
        return client.db(DB_NAME).collection(COLLECTION_NAME);
    } catch (error) {
        console.error('❌ Failed to connect to MongoDB:', error);
        process.exit(1);
    }
}

async function consumeMessages() {
    try {
        const collection = await connectToMongoDB();

        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();

        await channel.assertExchange(EXCHANGE_NAME, 'direct', { durable: true });
        await channel.assertQueue(QUEUE_NAME, { durable: true });
        await channel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

        console.log('Waiting for messages...');

        channel.consume(QUEUE_NAME, async (msg) => {
            if (msg) {
                const messageContent = msg.content.toString();
                console.log(`Received: ${messageContent}`);

                // Save to MongoDB
                const messageObject = { message: messageContent, receivedAt: new Date() };
                await collection.insertOne(messageObject);
                console.log(await collection.find().toArray());
                console.log('Message saved to MongoDB');

                channel.ack(msg);
            }
        });
    } catch (error) {
        console.error('Error in consumer:', error);
    }
}

consumeMessages();
