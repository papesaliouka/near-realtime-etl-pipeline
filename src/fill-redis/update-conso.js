const redis = require('redis');
const REDIS_PORT = process.env.REDIS_PORT || 6379;
const client = redis.createClient(REDIS_PORT);
const statsDB = require('../models/stats.mongo');
const {ObjectId} = require('mongodb');
const mongoConnect = require('../mongo');

// run this file with a cron job every 5mn 

async function main() {
    try {
        await mongoConnect();
        await client.connect();
        let keys = await client.keys('*');
        let id_compteurs = await getCompteurs(keys);
        for (const compteur of id_compteurs) {
            let data = await getConso(compteur, '2022-05-01', '2022-05-31')
            await client.set(compteur, data);
            console.log(`updated values for compteur_id ${compteur}`)
        }
    } catch (e) {
        console.log('unable to run job', e)
    }
}

async function getCompteurs(keys) {
    let allData = [];
    for (const item of keys) {
        let data = await client.get(item);
        data = JSON.parse(data)
        allData.push(data.id_compteur)
    }
    return allData;
}

async function getConso(id_compteur, startDate, endDate) {
    let query = [
        {
            '$match': {
                'compteur': new ObjectId(id_compteur),
                '$expr': {
                    '$and': [
                        {
                            '$gte': [
                                '$date', startDate,
                            ]
                        }, {
                            '$lte': [
                                '$date', endDate
                            ]
                        }
                    ]
                }
            }
        }, {
            '$group': {
                '_id': null,
                'energie': {
                    '$sum': '$energie'
                }
            }
        }
    ];
    let data = await statsDB.aggregate(query)
    return data.length > 0 ? data[0].energie : 0;
}

main()
