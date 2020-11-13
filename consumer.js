require('dotenv').config();
const PORT = process.env.PORT;
const KAFKA_BROKER = process.env.KAFKA_BROKER;
const API_URL = process.env.API_URL;
const SOCKET_URL = process.env.SOCKET_URL;
const axios = require('axios');
const express = require('express');
const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const client = new kafka.KafkaClient({kafkaHost: KAFKA_BROKER});
const offset = new kafka.Offset(client);
const io_client = require('socket.io-client');
const io = io_client.connect(SOCKET_URL);
const app = express();

app.listen(PORT, () => {
    console.log(`Consumer running at ${PORT}`)
})

io.on('connect', () => {
    console.log('Connected to Socket Server');
})

// Start from latest offset.
offset.fetch(
    [{ topic: 'contentPlayCount', partition: 1, time: -1, maxNum: 1 }], (err, data) => {
        var consumer = new Consumer(
            client,
            [{topic: 'contentPlayCount', partition: 1, offset: data['contentPlayCount'][0][0]}],
            {
                autoCommit: false,
                fromOffset: true
            }
        )

        consumer.on('message', async message => {
            try {
                log_data = JSON.parse(message.value);

                let play_log = [{
                    licenseId: log_data.license_id,
                    contentId: log_data.content_id,
                    logDate: log_data.timestap
                }];

                const logs = await sendLogs(play_log);
                io.emit('CS_content_log', play_log);
                console.log('Logs Sent Successfully:', logs, play_log);
            } catch(err) {
                console.log('Invalid Log Format', message.value, err);
            }
        })
    }
);

const sendLogs = (data) => {
    return new Promise((resolve, reject) => {
        axios.post(`${API_URL}/ContentPlays/Log`, data)
        .then((res) => {
            resolve(res.status)
        }).catch((err) => {
            console.log('Error sending logs to API Server', err.response)
        })
    })
}

app.get('/ping', (req, res) => {
    res.send('Consumer is up and running.')
})
