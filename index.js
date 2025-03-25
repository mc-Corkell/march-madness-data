const fs = require("fs");
const { Kafka } = require("@confluentinc/kafka-javascript").KafkaJS;
const axios = require("axios");

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

async function produce(topic, config, freeThrowData) {
    // create a new producer instance
    const producer = new Kafka().producer(config);

    // connect the producer to the broker
    await producer.connect();

    // Process each free throw entry
    for (const freeThrow of freeThrowData) {
        const key = freeThrow.time;  // Extract time from each free throw entry
        const value = JSON.stringify({
            homeText: freeThrow.homeText,
            visitorText: freeThrow.visitorText
        });

        // send a message for each free throw
        const produceRecord = await producer.send({
            topic,
            messages: [{ key, value }],
        });
        console.log(
            `\n\n Produced message to topic ${topic}: key = ${key}, value = ${value}, ${JSON.stringify(
                produceRecord,
                null,
                2
            )} \n\n`
        );
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

            console.log(
                `Time: ${message.key.toString()}\n` +
                `Play: ${value.homeText}\n` +
                `Current Stats:\n` +
                `- Made: ${madeFreeThrows}\n` +
                `- Missed: ${missedFreeThrows}\n` +
                `- Made Percentage: ${madePercentage}%\n` +
                `------------------------`
            );
        },
    });
}

// Function to filter homeText and visitorText for "Free Throw"
function extractFreeThrows(data) {
    const freeThrows = [];
  
    data.periods.forEach(period => {
      period.playStats.forEach(play => {
        if (play.homeText.includes("Free Throw MISSED") || play.visitorText.includes("Free Throw")) {
          freeThrows.push({ time: play.time, homeText: play.homeText, visitorText: play.visitorText });
        }
      });
    });
  
    return freeThrows;
  }

  // Function to count the occurrences of "Free Throw MISSED" and "Free Throw"
function countFreeThrows(freeThrowPlays) {
    let missedCount = 0;
    let totalCount = 0;
  
    // Iterate over the array to count the occurrences
    freeThrowPlays.forEach(play => {
      if (play.homeText.includes("Free Throw MISSED")) {
        missedCount++;
      }
      if (play.visitorText.includes("Free Throw")) {
        totalCount++;
      }
    });
  
    return {
      "Free Throw MISSED": missedCount,
      "Free Throw Total": totalCount
    };
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
        
        return truncatedFreeThrows;
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
    const topic = "march_madness_data_2025";
    
    try {
        const gameId = "6384794";
        const freeThrowData = await fetchPlayByPlayData(gameId);
        console.log('Free throw data:', freeThrowData);
        
        await produce(topic, config, freeThrowData);
        await consume(topic, config);
    } catch (error) {
        console.error('Failed to process game data:', error);
    }
}

main();