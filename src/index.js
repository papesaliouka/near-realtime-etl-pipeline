const fillRedis = require('./fill-redis');
const changeStream = require('./change-streams');
const mongoConnect = require('./mongo');
const startWatcher = require('./watcher');
const startServer = async () => {
    await mongoConnect();
    await fillRedis();
    await changeStream();
    await startWatcher();
};


startServer();
