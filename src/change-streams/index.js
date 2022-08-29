const {MongoClient} = require('mongodb');
const fillRedis = require('../fill-redis');
const compteursDB = require('../models/compteurs.mongo');

async function main(){
    console.log('monitoring db changes')
    const uri = process.env.DB_CONNECTION;
    const mongoClient = new MongoClient(uri, {useUnifiedTopology:true});
    try{
        await mongoClient.connect();
        const updatePipeline = [{$match: {'operationType':'update'}}];
        const insertPipeline = [{$match:{'operationType':'insert'}}];
        await monitorUpdates(mongoClient, updatePipeline);
        await monitorInsert(mongoClient, insertPipeline);
    }catch(e){
        console.log(e);
    }
}


async function monitorInsert(client,pipeline){
    const collection = client.db(process.env.DB_NAME).collection('compteurs');
    const changeStream = collection.watch(pipeline);
    changeStream.on('change', async(_)=>{
        await fillRedis()
    })
}


async function monitorUpdates(client,pipeline){
    const collection = client.db(process.env.DB_NAME).collection('compteurs');
    const changeStream = collection.watch(pipeline);
    changeStream.on('insert', async(_)=>{
        await fillRedis()
    })
}




module.exports=main
