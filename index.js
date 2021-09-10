const http = require('http');
/*require('dotenv').config();
const Redis = require('ioredis');
let redis = new Redis(process.env.REDIS_URL);

redis.get('initialized').then(function (initialized) {
    if (!initialized) {
        redis.flushall();
        const csv = require('csv-parser')
        const fs = require('fs');
        let index = 0;
        fs.createReadStream('co2.csv')
            .pipe(csv())
            .on('data', (data) => {
                redis.set(index, JSON.stringify(data));
                index++;
            })
            .on('end', () => {
                redis.set('initialized', true);
                console.log('Redis has been initialized');
            });
    }
});*/

const HOSTNAME = '127.0.0.1';
const PORT = process.env.PORT || 80;

const server = http.createServer((req, res) => {
    /*if (req.method === 'GET') {
        switch (req.url) {
            case '/distribution/regions':
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ message: 'OK' }));
                break;

            case '/distribution/dataTypes':
                break;

            case '/distribution/summary':
                break;

            default:
                res.writeHead(404, { 'Content-Type': 'text/plain' });
                res.end('okk');
        }
    } else */{
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('okkk');
    }
});

server.listen(PORT, HOSTNAME, () => {
    console.log(`Server running at http://${HOSTNAME}:${PORT}/`);
});