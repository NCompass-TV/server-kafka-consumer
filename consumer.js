const axios = require('axios');
const express = require('express');
const kafka = require('kafka-node');
var Consumer = kafka.Consumer;
const client = new kafka.KafkaClient({kafkaHost: 'staging.n-compass.online:9092'});
var offset = new kafka.Offset(client);
const io_client = require('socket.io-client');
const io = io_client.connect('http://192.168.100.13:3000');
const app = express();

const server = app.listen(3000,function(){
    console.log('Kafka consumer running at 3000')
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

                const send_logs = await sendLogs(play_log);
                io.emit('CS_content_log', play_log);
                console.log('Logs Sent:', log_data);
            } catch(err) {
                console.log('Invalid Log Format', message.value, err);
            }
        })
    }
);

const sendLogs = (data) => {
    return new Promise((resolve, reject) => {
        axios.post(`http://3.212.225.229:72/api/ContentPlays/Log`, data)
        .then((res) => {
            resolve(res.status)
        }).catch((err) => {
            console.log('Error sending logs to API Server', err.response)
        })
    })
}

app.get('/', (req, res) => {
    res.send('Consumer is up')
})