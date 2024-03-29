const axios = require('axios');
const express = require('express');
const kafka = require('kafka-node');
var Consumer = kafka.Consumer;
const client = new kafka.KafkaClient({kafkaHost: 'staging.n-compass.online:9092'});
var offset = new kafka.Offset(client);
const io_client = require('socket.io-client');
const io = io_client.connect('http://staging.n-compass.online:73');
const app = express();

const server = app.listen(5001,function(){
    console.log('Kafka consumer running at 5001')
})

// Start from latest offset.
offset.fetch(
    [{ topic: 'contentPlayCount', partition: 2, time: -1, maxNum: 1 }], (err, data) => {
        let count = 0;
        var consumer = new Consumer(
            client,
            [{topic: 'contentPlayCount', partition: 2, offset: data['contentPlayCount'][0][0]}],
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
        axios.post(`http://testurl:port/api/ContentPlays/Log`, data)
        .then((res) => {
            resolve(res.status)
        }).catch((err) => {
            console.log('Error sending logs to API Server', err.response)
        })
    })
}

app.get('/ping', (req, res) => {
    res.send('Consumer is up')
})
