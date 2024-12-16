
const axios = require('axios');
const fs = require('fs');
const path = require('path');

async function sendDataToThingsBoard(filePath) {
    return new Promise(async (resolve, reject) => {
        const data = fs.readFileSync(filePath, 'utf8');
        const jsonData = JSON.parse(data);

        // Send data to ThingsBoard
        console.log(jsonData);
        axios.post('https://thingsboard.gruenecho.de/api/v1/[deviceId]/telemetry', jsonData, {
            headers: {
                'Content-Type': 'application/json'
            }
        }).then(result => {
            console.log('Data sent to ThingsBoard:', result.data);
            resolve();
        }).catch(error => {
            console.log('Error sending data to ThingsBoard:', error);
            reject();
        })
    });
}

async function main() {
    // read all files in per-device/timestamps
    const directoryPath = path.join(__dirname, 'per-device/timestamps');

    const files = fs.readdirSync(directoryPath);

    for (let i = 0; i < files.length; i++) {
        const file = files[i];
        const filePath = path.join(directoryPath, file);

        // send data to ThingsBoard
        await sendDataToThingsBoard(filePath);
    }
    
}
main();