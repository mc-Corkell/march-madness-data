const fs = require("fs");
const { Kafka } = require("@confluentinc/kafka-javascript").KafkaJS;
const axios = require("axios");
const express = require('express');
const cors = require("cors");

// Initialize Express app
const app = express();
app.use(cors());

// Store connected SSE clients
let clients = [];

// SSE endpoint
app.get('/events', (req, res) => {
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
    });

    const clientId = Date.now();
    const newClient = {
        id: clientId,
        res
    };
    clients.push(newClient);

    req.on('close', () => {
        clients = clients.filter(client => client.id !== clientId);
    });
});

// Function to send SSE to all connected clients
function sendSSEMessage(data) {
    clients.forEach(client => {
        client.res.write(`data: ${JSON.stringify(data)}\n\n`);
    });
}

function readConfig(fileName) {
    const data = fs.readFileSync(fileName, "utf8").toString().split("\n");
    return data.reduce((config, line) => {
        const [key, value] = line.split("=");
        if (key && value) {
            config[key] = value;
        }
        return config;
    }, {});
}

async function produce(topic, config, freeThrowData, gameId) {
    // create a new producer instance
    const producer = new Kafka().producer(config);

    // connect the producer to the broker
    await producer.connect();

    // Process each free throw entry
    for (const freeThrow of freeThrowData) {
        const key = gameId;
        const value = JSON.stringify({
            time: freeThrow.time,
            homeText: freeThrow.homeText,
            visitorText: freeThrow.visitorText,
        });

        // send a message for each free throw
        const produceRecord = await producer.send({
            topic,
            messages: [{ key, value }],
        });
        // console.log(
        //     `\n\n Produced message to topic ${topic}: key = ${key}, value = ${value}, ${JSON.stringify(
        //         produceRecord,
        //         null,
        //         2
        //     )} \n\n`
        // );
    }

    // disconnect the producer
    await producer.disconnect();
}

async function consume(topic, config) {
    // setup graceful shutdown
    const disconnect = () => {
        consumer.commitOffsets().finally(() => {
            consumer.disconnect();
        });
    };
    process.on("SIGTERM", disconnect);
    process.on("SIGINT", disconnect);

    // Initialize counters
    let madeFreeThrows = 0;
    let missedFreeThrows = 0;

    // set the consumer's group ID, offset and initialize it
    config["group.id"] = "nodejs-group-1"; 
    config["auto.offset.reset"] = "earliest";
    const consumer = new Kafka().consumer(config);

    // connect the consumer to the broker
    await consumer.connect();

    // subscribe to the topic
    await consumer.subscribe({ topics: [topic] });

    // consume messages from the topic
    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const value = JSON.parse(message.value.toString());
            
            // Check if it's a missed free throw
            if (value.homeText.includes("Free Throw MISSED") || value.visitorText.includes("Free Throw MISSED")) {
                missedFreeThrows++;
            } else if (value.homeText.includes("Free Throw  ") || value.visitorText.includes("Free Throw  ")) {
                madeFreeThrows++;
            }
            
            // Calculate percentage
            const totalFreeThrows = madeFreeThrows + missedFreeThrows;
            const madePercentage = totalFreeThrows > 0 
                ? ((madeFreeThrows / totalFreeThrows) * 100).toFixed(2) 
                : 0;

            const messageData = {
                time: message.key.toString(),
                play: value.homeText,
                stats: {
                    made: madeFreeThrows,
                    missed: missedFreeThrows,
                    total: totalFreeThrows,
                    percentage: madePercentage
                }
            };

            // Send to SSE clients
            sendSSEMessage(messageData);

            // Optional: keep console logging for debugging
            const debugMsg = `GameId: ${message.key.toString()}\n` +
                `Time: ${value.time}\n` +
                `Play: ${value.homeText}\n` +
                `Current Stats:\n` +
                `- Made: ${madeFreeThrows}\n` +
                `- Missed: ${missedFreeThrows}\n` +
                `- Total: ${totalFreeThrows}\n` +
                `- Made Percentage: ${madePercentage}%\n` +
                `------------------------`;
            console.log(debugMsg);
        },
    })
}

function isFreeThrow(play) { 
    return play.homeText.includes("Free Throw MISSED") || play.visitorText.includes("Free Throw MISSED") || play.homeText.includes("Free Throw  ") || play.visitorText.includes("Free Throw  ");
}

// Function to filter homeText and visitorText for "Free Throw"
function extractFreeThrows(data) {
    const freeThrows = [];
  
    data.periods.forEach(period => {
      period.playStats.forEach(play => {
        if (isFreeThrow(play)) {
          freeThrows.push({ time: play.time, homeText: play.homeText, visitorText: play.visitorText });
        }
      });
    });
  
    return freeThrows;
  }

async function fetchPlayByPlayData(gameId) {
    try {
        const response = await axios.get(`https://ncaa-api.henrygd.me/game/${gameId}/play-by-play`);
        
        // Verify the response data structure
        if (!response.data || !response.data.periods) {
            throw new Error('Invalid response structure: missing periods data');
        }

        const freeThrowPlays = extractFreeThrows(response.data);
        
        // Truncate to 10 items for debugging
        // const truncatedFreeThrows = freeThrowPlays.slice(0, 10);
        // console.log('Truncated to 10 free throws:', truncatedFreeThrows);
        // return truncatedFreeThrows;
        console.log(" Free throws for game id: " + gameId + " is " + freeThrowPlays.length);
        return freeThrowPlays;
    } catch (error) {
        console.error('Error fetching play-by-play data:', error.message);
        if (error.response) {
            console.error('API Response:', error.response.data);
        }
        throw error;
    }
}

async function main() {
    const config = readConfig("client.properties");
    const topic = "march_madness_data_2025_v2";
    
    try {
        const gameIds = ["6384749"];//, "6384750", "6384751", "6384752"];
        for (const gameId of gameIds) {
            const freeThrowData = await fetchPlayByPlayData(gameId);
            await produce(topic, config, freeThrowData, gameId);
        }
        
        // Start the Express server
        const PORT = process.env.PORT || 3000;
        app.listen(PORT, () => {
            console.log(`SSE Server running on port ${PORT}`);
        });

        await produce(topic, config, freeThrowData, gameId);
        await consume(topic, config);
    } catch (error) {
        console.error('Failed to process game data:', error);
    }
}

main();