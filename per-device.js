const sql = require('mssql')
const fs = require('fs');

function toThingsboardArray(timestamp, values, channelsObject) {
    const thingsboardObject = {
        ts: timestamp,
        values: {}
    };
    for (let i = 0; i < values.length; i++) {
        thingsboardObject.values[values[i]['channelName']] = values[i].data;
    }
    return thingsboardObject;
}

async function main(deviceName) {
    try{
        await sql.connect(`Server=${process.env.MSSQL_HOST};Database=${process.env.MSSQL_DB};User Id=${process.env.MSSQL_USER};Password=${process.env.MSSQL_PASSWORD};Encrypt=false`)
        
        const result = await sql.query`SELECT DISTINCT location FROM dez33_barth.channels;`;
        const locations = result.recordset;

        for(let i = 0; i < locations.length; i++) {
            const location = locations[i].location;
            if (location !== deviceName) {
                continue;
            }

            const result = await sql.query`SELECT * FROM dez33_barth.channels WHERE location = ${location};`;
            const channels = result.recordset;
            const channelsObject = {};
            for(let i = 0; i < channels.length; i++) {
                channelsObject[channels[i].id] = channels[i];
            }
            fs.writeFileSync(`per-device/keys/${location}.json`, JSON.stringify(channelsObject, null, 2));
            

            // Array From ids in location
            const ids = channels.map(channel => channel.id);

            // Construct the SQL query with parameters
            const query = `SELECT channelid, unixtime, data FROM dez33_barth.measurements WHERE channelid IN (${ids.map((_, i) => `@id${i}`).join(',')}) ORDER BY unixtime`;

            // Create the parameters object
            const request = new sql.Request();
            ids.forEach((id, i) => {
                request.input(`id${i}`, sql.Int, id);
            });

            // Execute the query with parameters
            const resultTimestamp = await request.query(query);
            const valuesRecordset = resultTimestamp.recordset;
            console.log('Result:', valuesRecordset.length);

            // Save in file
            //fs.writeFileSync(`per-device/values/${location}.json`, JSON.stringify(valuesRecordset, null, 2));

            // SORT per timestamp
            const uniqueUnixTimestampObject = {};
            for (let i = 0; i < resultTimestamp.recordset.length; i++) {
                const row = resultTimestamp.recordset[i];
                row.channelName = channelsObject[row.channelid].name;
                if (!uniqueUnixTimestampObject[row.unixtime]) {
                    uniqueUnixTimestampObject[row.unixtime] = [];
                }
                uniqueUnixTimestampObject[row.unixtime].push(row);
            }

            
            for (const [key, value] of Object.entries(uniqueUnixTimestampObject)) {
                
                thingsboardObject = toThingsboardArray(key, value, channelsObject);
                fs.writeFileSync(`per-device/timestamps/${location}-${key}.json`, JSON.stringify(thingsboardObject, null, 2));
            }

            break;

        }
    } catch (err) {
        console.log('ERROR:', err)
    }
}

main('Kienhorst_FF');