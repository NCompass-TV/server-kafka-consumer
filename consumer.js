require('dotenv').config();
const axios = require('axios');
const express = require('express');
const kafka = require('kafka-node');
var Consumer = kafka.Consumer;
const client = new kafka.KafkaClient({kafkaHost: process.env.KAFKA});
var offset = new kafka.Offset(client);
const io_client = require('socket.io-client');
const io = io_client.connect(process.env.SOCKET);
const API = process.env.API;
const app = express();
const PORT = process.env.PORT;

app.listen(PORT, function(){
    console.log(`Kafka consumer running at port ${PORT}`)
})

// Start from latest offset.
offset.fetch(
    [{ topic: 'contentPlayCount', partition: 0, time: -1, maxNum: 1 }], (err, data) => {
        let count = 0;
        var consumer = new Consumer(
            client,
            [{topic: 'contentPlayCount', partition: 0, offset: data['contentPlayCount'][0][0]}],
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

                await sendLogs(play_log);
                io.emit('CS_content_log', play_log);
            } catch(err) {
                console.log('Invalid Log Format', message.value, err);
            }
        })
    }
);

const sendLogs = (data) => {
    return new Promise((resolve, reject) => {
        axios.post(`${API}/api/ContentPlays/Log`, data)
        .then((res) => {
            resolve(res.status)
        }).catch((err) => {
            console.log('Error sending logs to API Server', err.response)
			reject(err);
		})
    })
}

app.get('/ping', (req, res) => {
    res.send('Consumer is up')
})