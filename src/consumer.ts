import * as amqp from 'amqplib';
import { MongoClient } from 'mongodb';
import express from 'express';
import cors from 'cors';
import { setTimeout } from "timers/promises";

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const MONGO_URL = process.env.MONGO_URL || 'mongodb://localhost:27017';
const DB_NAME = process.env.DB_NAME || 'rabbitmq_messages';
const COLLECTION_NAME = process.env.COLLECTION_NAME || 'messages';
const API_PORT = process.env.API_PORT || 5001;

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

// Express API to serve messages
async function startApiServer(collection: any) {
    const app = express();
    app.use(cors());

    app.get('/messages', async (req, res) => {
        try {
            const messages = await collection.find().sort({ receivedAt: -1 }).toArray();
            res.json(messages);
        } catch (error) {
            res.status(500).json({ error: 'Failed to fetch messages' });
        }
    });

    app.listen(API_PORT, () => {
        console.log(`🚀 API server running on http://localhost:${API_PORT}`);
    });
}

async function consumeMessages() {
    try {
        const collection = await connectToMongoDB();
        await startApiServer(collection);

        await setTimeout(20000);

        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();

       // Assert the queue (create it if not already existing)
        await channel.assertQueue(QUEUE_NAME, { durable: true });

        console.log(`👂 Consumer listening for messages in queue: "${QUEUE_NAME}"`);

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
