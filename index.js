const sql = require('mssql')
const fs = require('fs');
const axios = require('axios');

const mapping_json = require('./config/mapping-keys.json')


let channels = [];
let channelsObject = {};
let limit = 2;
let currentRow = 0;
let lastSelectedLength = limit

let testAccessKey = 'caOGT9wWSZcijFQaVRmH';

async function paginate(limit = 10, offset = 0) {
    let recordset = [];
    try{
        console.log(offset, limit)
        const result = await sql.query`select * from dez33_barth.measurements ORDER BY unixtime OFFSET ${offset} ROWS FETCH NEXT ${limit} ROWS ONLY`
        recordset = result.recordset;
    }
    catch (err) {
        console.log('ERROR:', err)
    }
    return recordset;
}
async function getMeasurementsByUnixtime(unixtime) {
    let recordset = [];
    try{
        const result = await sql.query`select * from dez33_barth.measurements where unixtime = ${unixtime}`
        recordset = result.recordset;
    }
    catch (err) {
        console.log('ERROR:', err)
    }
    return recordset;
}
async function saveDistrict(object){
    return fs.writeFileSync('tmp/unixtime.json', JSON.stringify(object));
}
async function createUnixtimeDistrictFile() {
    let timestampObjects = {};
    const unixtimeResult = await sql.query`SELECT DISTINCT unixtime FROM dez33_barth.measurements`

    for (let i = 0; i < unixtimeResult.recordset.length; i++) {
        timestampObjects[unixtimeResult.recordset[i].unixtime] = {
            synced: false,
        };
    }

    // Save distinct unixtime values in file
    saveDistrict(timestampObjects);

    return timestampObjects;
}
async function resetDistrict(data) {
    let uniqueUnixTimestampObject = JSON.parse(fs.readFileSync('tmp/unixtime.json'));
    const entries = Object.entries(uniqueUnixTimestampObject);
    for(let i = 0; i < entries.length; i++) {
        entries[i][1].synced = false;
    }
    saveDistrict(uniqueUnixTimestampObject);
}
async function convertToThingsboardTelemetry(data){
    let locations = {};
    for(let i = 0; i < data.length; i++) {
        const channel = channelsObject[data[i].channelid];
        channel['location']
        if(!locations[channel['location']])
            locations[channel['location']] = {
                "ts": data[i].unixtime,
                "values": {}
            };
        if(locations[channel['location']].values[channel['name']])
            throw new Error(`Duplicate channel name found in data: ${channel['name']} - ${data[i].channelid}`);

        locations[channel['location']].values[channel['name']] = data[i].data; 
    }

    return locations;
}
function getAccessKey(key){
    return mapping_json.find((item) => item.contains.startsWith(key)).key;
}
async function upload(url, value){
   
    return axios.post(url, value)
    .then(function (response) {
        if (response.status != 200) {
            console.log('Error sending data to ThingsBoard:', file);
        }else{
            console.log('Data sent to ThingsBoard:', url);
        }
    })
    .catch(function (error) {
        console.log('ERROR:', error);
    });
}
async function uploadToThingsboard(data){
    // https://thingsboard.io/docs/user-guide/telemetry/

    try{
        const entries = Object.entries(data);
        for(let i = 0; i < entries.length; i++) {
            const key = entries[i][0];
            const value = entries[i][1];

            let accessKey = getAccessKey(key);
            
            if(!accessKey){
                throw new Error(`Access Key not found for key: ${key}`);
            }

            
            // FOR TEST ONLY (Kienhorst_Bestand will be replaced with accessKey)
            console.log('Access Key:', key, accessKey)
            if(accessKey !== '7ywauoJI3R6YSsKmnksd'){
                continue;
            }

            accessKey = testAccessKey;

            const url = `${process.env.THINGSBOARD_PROTOCOL}://${process.env.THINGSBOARD_HOST}:${process.env.THINGSBOARD_PORT}/api/v1/${accessKey}/telemetry`;
            
            try{
                await upload(url, value);
            } catch (err) {
                throw new Error(`Error uploading data to Thingsboard: ${err}`);
            }
            
        }
    } catch (err) {
        console.log('ERROR:', err)
    }

    
    return true;
}

async function main() {

    resetDistrict();
    
    try{
        await sql.connect(`Server=${process.env.MSSQL_HOST};Database=${process.env.MSSQL_DB};User Id=${process.env.MSSQL_USER};Password=${process.env.MSSQL_PASSWORD};Encrypt=false`)

        const channelsResult = await sql.query`select * from dez33_barth.channels`
        channels = channelsResult.recordset;

        // get all unique

        // create channels object by id
        channelsObject = {};
        for(let i = 0; i < channels.length; i++) {
            channelsObject[channels[i].id] = channels[i];
        }
        // Save Channels in file
        fs.writeFileSync('tmp/channels.json', JSON.stringify(channelsObject, null, 2));
        lastSelectedLength = limit = channels.length;
        console.log(limit, currentRow)

        //let uniqueUnixTimestampObject = await createUnixtimeDistrictFile();
        let uniqueUnixTimestampObject = JSON.parse(fs.readFileSync('tmp/unixtime.json'));

        console.log('Unique Unixtime:', Object.keys(uniqueUnixTimestampObject).length);
        return;

        const entries = Object.entries(uniqueUnixTimestampObject);
        for(let i = 0; i < entries.length; i++) {
            const key = entries[i][0];
            const value = entries[i][1];

            const data = await getMeasurementsByUnixtime(key);
            if(data.length > 0) {
                try{
                    const converted = await convertToThingsboardTelemetry(data);
                    const uploaded = await uploadToThingsboard(converted);

                    if(!uploaded) {
                        console.log('Error uploading data to Thingsboard')
                        break;
                    }

                    value.synced = uploaded;
                    //saveDistrict(uniqueUnixTimestampObject);
                    console.log('Uploaded data to Thingsboard: ', key);
                } catch (err) {
                    console.log('ERROR:', err)
                    break;
                }
            }
            
        }

        console.log('DONE')
        await sql.close();
        return true;
    } catch (err) {
        console.log('ERROR:', err)
    }
}


main();
