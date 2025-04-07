const fs = require("fs");
const { Kafka } = require("@confluentinc/kafka-javascript").KafkaJS;
const axios = require("axios");
const express = require('express');
const cors = require("cors");

// Initialize Express app
const app = express();
app.use(cors({
    origin: '*', // Allow all origins for development
    methods: ['GET'],
    allowedHeaders: ['Content-Type']
}));

// Store connected SSE clients
let clients = [];

// SSE endpoint
app.get('/events', (req, res) => {
    // Set headers for SSE
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*'
    });

    // Send an initial connection message
    res.write('data: {"type": "connected"}\n\n');

    const clientId = Date.now();
    const newClient = {
        id: clientId,
        res
    };
    clients.push(newClient);

    // Handle client disconnect
    req.on('close', () => {
        console.log(`Client ${clientId} disconnected`);
        clients = clients.filter(client => client.id !== clientId);
    });

    // Handle errors
    req.on('error', (error) => {
        console.error(`Error with client ${clientId}:`, error);
        clients = clients.filter(client => client.id !== clientId);
    });
});

// Function to send SSE to all connected clients
async function sendSSEMessage(data) {
    // Add a 1-second delay before sending the message
    // to have my UI be able to visualize the flow of data in a human detectable speed
    // await new Promise(resolve => setTimeout(resolve, 1000));
    clients.forEach(client => {
        try {
            client.res.write(`data: ${JSON.stringify(data)}\n\n`);
        } catch (error) {
            console.error(`Error sending message to client ${client.id}:`, error);
            // Remove the client if there's an error
            clients = clients.filter(c => c.id !== client.id);
        }
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
    console.log("Starting Kafka consumer...");
    console.log("Consumer config:", config);
    
    // setup graceful shutdown
    const disconnect = () => {
        console.log("Disconnecting consumer...");
        consumer.commitOffsets().finally(() => {
            consumer.disconnect();
            console.log("Consumer disconnected");
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

    try {
        // connect the consumer to the broker
        console.log("Connecting consumer to broker...");
        await consumer.connect();
        console.log("Consumer connected successfully");

        // subscribe to the topic
        console.log(`Subscribing to topic: ${topic}`);
        await consumer.subscribe({ topics: [topic] });
        console.log("Successfully subscribed to topic");

        // consume messages from the topic
        console.log("Starting to consume messages...");
        consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                console.log(`Received message from topic ${topic}, partition ${partition}`);
                const value = JSON.parse(message.value.toString());
                
                // Check if it's a missed free throw
                if (isFreeThrowMiss(value)) {
                    missedFreeThrows++;
                } else if (isFreeThrowMade(value)) {
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
                await sendSSEMessage(messageData);

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
        });
    } catch (error) {
        console.error("Error in consumer setup:", error);
        throw error;
    }
}

function isFreeThrow(play) { 
    return isFreeThrowMiss(play) || isFreeThrowMade(play);
}

function isFreeThrowMiss(play) { 
    return play.homeText.includes("Free Throw MISSED") || play.visitorText.includes("Free Throw MISSED");
}

function isFreeThrowMade(play) { 
    return play.homeText.includes("Free Throw  ") || play.visitorText.includes("Free Throw  ");
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

async function processTournamentGames(schedule, tournamentType) {
    let gameIds = [];

    for (const [date, bracketRound] of schedule.entries()) {
        const response = await axios.get(`https://ncaa-api.henrygd.me/scoreboard/basketball-${tournamentType}/d1/${date}`);
   
        const roundGameIds = response.data.games
            .filter(game => game.game.bracketRound.includes(bracketRound))
            .map(game => game.game.url.split('/').pop());
        gameIds = gameIds.concat(roundGameIds);
        //console.log(`Found ${roundGameIds.length} ${tournamentType} ${bracketRound} games for ${date}`);
    }

    return gameIds;
}

async function fetchTournamentGameIds() {
    const womenTournamentSchedule = new Map([
        ["2025/03/19", "First Four"],
        ["2025/03/20", "First Four"],
        ["2025/03/21", "First Round"],
        ["2025/03/22", "First Round"],
        ["2025/03/23", "Second Round"],
        ["2025/03/24", "Second Round"],
        ["2025/03/28", "Sweet 16"],
        ["2025/03/29", "Sweet 16"],
        ["2025/03/30", "Elite Eight"],
        ["2025/03/31", "Elite Eight"],
        ["2025/04/04", "FINAL FOUR"],
        ["2025/04/06", "Championship"]
    ]);

    const menTournamentSchedule = new Map([
        ["2025/03/18", "First Four"],
        ["2025/03/19", "First Four"],
        ["2025/03/20", "First Round"],
        ["2025/03/21", "First Round"],
        ["2025/03/22", "Second Round"],
        ["2025/03/23", "Second Round"],
        ["2025/03/27", "Sweet 16"],
        ["2025/03/28", "Sweet 16"],
        ["2025/03/29", "Elite Eight"],
        ["2025/03/30", "Elite Eight"],
        ["2025/04/05", "FINAL FOUR"],
        ["2025/04/07", "Championship"]
    ]);

    const womenGameIds = await processTournamentGames(womenTournamentSchedule, "women");
    const menGameIds = await processTournamentGames(menTournamentSchedule, "men");
    
    return womenGameIds.concat(menGameIds);
}

async function main() {
    const config = readConfig("client.properties");
    const topic = "march_madness_data_2025_v2";
    
    try {
        // Start the Express server
        const PORT = process.env.PORT || 3001;
        app.listen(PORT, () => {
            console.log(`SSE Server running on port ${PORT}`);
        });

        const tournamentGameIds = await fetchTournamentGameIds();
        console.log(`Found ${tournamentGameIds.length} total games across both tournaments`);
        
        // Start the consumer before processing games
        console.log("Starting Kafka consumer...");
        await consume(topic, config);
        
        for (const gameId of tournamentGameIds) {
            const freeThrowData = await fetchPlayByPlayData(gameId);
            await produce(topic, config, freeThrowData, gameId);
        }
    } catch (error) {
        console.error('Failed to process game data:', error);
    }
}

main();