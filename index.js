const sql = require('mssql')
const fs = require('fs');

const mapping_json = require('./config/mapping-keys.json')

let channels = [];
let limit = 2;
let currentRow = 0;
let lastSelectedLength = limit

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

async function uploadToThingsboard(data){
    console.log('Uploading data to Thingsboard', data)
    throw new Error('Not implemented');
}

async function main() {

    resetDistrict();
    
    try{
        await sql.connect(`Server=${process.env.MSSQL_HOST};Database=${process.env.MSSQL_DB};User Id=${process.env.MSSQL_USER};Password=${process.env.MSSQL_PASSWORD};Encrypt=false`)

        const channelsResult = await sql.query`select * from dez33_barth.channels`
        channels = channelsResult.recordset;
        lastSelectedLength = limit = channels.length;
        console.log(limit, currentRow)

        //let uniqueUnixTimestampObject = await createUnixtimeDistrictFile();
        let uniqueUnixTimestampObject = JSON.parse(fs.readFileSync('tmp/unixtime.json'));

        const entries = Object.entries(uniqueUnixTimestampObject);
        for(let i = 0; i < entries.length; i++) {
            const key = entries[i][0];
            const value = entries[i][1];

            const data = await getMeasurementsByUnixtime(key);
            if(data.length > 0) {
                await uploadToThingsboard(data);

                value.synced = true;
                saveDistrict(uniqueUnixTimestampObject);
            }
            
        }

        console.log('DONE')

    } catch (err) {
        console.log('ERROR:', err)
    }
}


main();
