const http = require('http');
require('dotenv').config();
const Redis = require('ioredis');
let redis = new Redis(process.env.REDIS_URL);

let regions = [],
    dataTypes = [],
    values = [];

function pushType(header) {
    let [name, units] = header.split(',');
    dataTypes.push({
        id: dataTypes.length,
        name: name.charAt(0).toUpperCase() + name.slice(1),
        units
    });
    values.push([]);
}

function pushRegion(value) {
    if(regions.findIndex((sub) => sub.name === value) === -1 && value) regions.push({
        id: regions.length,
        name: value
    });
}

function pushValues(data) {
    for (let i = 3; i < data.length; i < 4) {
        var index = dataTypes.findIndex((sub) => sub.id == i - 3);
        values[index].push({
            dateStart: `${data.year}-${data.month}-01`,
            value: data[i]
        });
    }
}

redis.get('initialized').then(function (initialized) {
    if (!initialized) {
        redis.flushall();
        const csv = require('csv-parser')
        const fs = require('fs');
        let index = 0;
        fs.createReadStream('co2.csv')
            .pipe(csv({
                mapHeaders: ({ header, index }) => {
                    if (index >= 3) pushType(header);
                    return header;
                },
                mapValues: ({ index, value }) => {
                    if (index == 2) pushRegion(value);
                }
            }))
            .on('data', (data) => {
                pushRegion(data.region);
                pushValues(data);
                //redis.set(index, JSON.stringify(data));
                //index++;
            })
            .on('end', () => {
                redis.set('regions', JSON.stringify(regions));
                redis.set('dataTypes', JSON.stringify(dataTypes));
                redis.set('initialized', true);
                console.log('Redis has been initialized');
            });
    } else {
        redis.get('regions').then(function(value) {
            regions = value;
        });
        redis.get('dataTypes').then(function(value) {
            dataTypes = value;
        });
    }
});

const HOSTNAME = '0.0.0.0';
const PORT = process.env.PORT || 80;

const server = http.createServer((req, res) => {
    if (req.method === 'GET') {
        switch (req.url) {
            case '/distribution/regions':
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(regions));
                break;

            case '/distribution/dataTypes':
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(dataTypes));
                break;

            case '/distribution/summary':
                res.writeHead(200, { 'Content-Type': 'text/plain' });
                res.end(':(');
                break;

            default:
                res.writeHead(404, { 'Content-Type': 'text/plain' });
                res.end('Invalid route');
        }
    } else {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('Invalid route');
    }
});

server.listen(PORT, HOSTNAME, () => {
    console.log(`Server running at http://${HOSTNAME}:${PORT}/`);
});