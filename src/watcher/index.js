const chokidar = require('chokidar');
const {spawnSync} = require('child_process');
async function startWatcher() {
    console.log('watcher started.......')
    chokidar.watch(process.env.FTP_DIR, {ignoreInitial: true}).on('add', async (path) => {
        console.log('handling file', path);
        console.log(spawnSync(`python3`, [`${process.env.BATCHER}`, `${path}`], {encoding: 'utf8'}).stdout.replace('\n', ''));
    });
};

module.exports = startWatcher;
