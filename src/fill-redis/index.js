const redis = require('redis');
const REDIS_PORT = process.env.REDIS_PORT || 6379;
const client = redis.createClient(REDIS_PORT);
const siteDB = require('../models/site.mongo');
const tarificationDB = require('../models/tarifications.mongo')

// a ajouter id_compteur id_depart_reel id_site

const query = [
    {
        '$match': {
            'f_actif': true
        }
    }, {
        '$lookup': {
            'from': 'compteurs',
            'let': {
                'idSite': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$$idSite', '$site'
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'controleurs',
                        'let': {
                            'id_controlleur': '$controleur'
                        },
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$eq': [
                                            '$_id', '$$id_controlleur'
                                        ]
                                    }
                                }
                            }
                        ],
                        'as': 'controleur'
                    }
                }, {
                    '$unwind': {
                        'path': '$controleur'
                    }
                }, {
                    '$lookup': {
                        'from': 'departreels',
                        'let': {
                            'id_depart_reel': '$depart_reel'
                        },
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$eq': [
                                            '$$id_depart_reel', '$_id'
                                        ]
                                    }
                                }
                            }
                        ],
                        'as': 'departreels'
                    }
                }, {
                    '$unwind': {
                        'path': '$departreels'
                    }
                }
            ],
            'as': 'compteurs'
        }
    }, {
        '$lookup': {
            'from': 'departreels',
            'localField': 'depart_principal_electricite',
            'foreignField': '_id',
            'as': 'departreel'
        }
    }, {
        '$unwind': {
            'path': '$departreel',
            'preserveNullAndEmptyArrays': true
        }
    }];
function formatData(arr) {
    const fullData = [];
    arr.forEach(el => {
        el.compteurs.forEach(el2 => {
            let {repertoire_local, repertoire_distant} = el2.controleur;
            repertoire_distant = repertoire_distant.replace(/^[\/]|[\/]$/, '');
            repertoire_local = repertoire_local.replace(/^[\/]|[\/]$/, '')
            el2['key'] = `${process.env.FTP_DIR}/${repertoire_local || repertoire_distant}/${el2.repertoire_lecture}`;
            el2['id_site'] = el._id;
            el2['id_depart_reel'] = el2.depart_reel._id;
            el2['facteur_c'] = el2.departreels.facteur_c;
            el2['facteur_p'] = el2.departreels.facteur_p;
            el2['facteur_i'] = el2.departreels.facteur_i;
            el2['id_compteur'] = el2._id;
            el2['puissance_souscrite'] = el.departreel.puissance_souscrite;
            if (el2.departreels.tarification != undefined) {
                el2['tarification'] = el2.departreels.tarification
            } else {
                el2['tarification'] = el.tarification
            }
            el2['designation_site'] = el.designation
            el2['designation_compteur'] = el2.designation
            fullData.push(el2);
        })
    });
    return fullData.map(item => {
        return {
            key: item.key,
            site: item.id_site,
            id_depart_reel: item.id_depart_reel,
            id_compteur: item.id_compteur,
            puissance_souscrite: item.puissance_souscrite,
            facteur_p: item.facteur_p,
            facteur_i: item.facteur_i,
            facteur_c: item.facteur_c,
            tarification: item.tarification,
            designation_site: item.designation_site,
            designation_compteur: item.designation_compteur
        }
    })
}

async function addTarification(formated) {

    let tarifications = formated.filter(el => el.tarification).map(el => ({tarification: el.tarification, designation: el.designation_site}));
    const map = new Map();
    let result = [];
    for (const item of tarifications) {
        if (!map.has(item.designation)) {
            map.set(item.designation, true);
            result.push(item)
        }
    }
    let allData = [];
    try {
        for (const item of result) {
            let data = await tarificationDB.find({_id: item.tarification})
            allData.push(data[0])
        }
    } catch (e) {
        console.log(e)
    }

    let updatedTarifications = [];
    let map2 = new Map()
    allData.forEach(el => {
        tarifications.forEach(el2 => {
            formated.forEach(el3 => {
                if (el2.designation == el3.designation_site && !map2.has(el3.key)) {
                    map2.set(el3.key, true);
                    el3.code = el.code;
                    el3.tarif_1er_tranche = el.tarif_1er_tranche;
                    el3.tarif_2eme_tranche = el.tarif_2eme_tranche;
                    el3.tarif_3eme_tranche = el.tarif_3eme_tranche;
                    el3.tarif_heure_creuse = el.tarif_heure_creuse;
                    el3.tarif_heure_pleine = el.tarif_heure_pleine;
                    el3.max_1er_tranche = el.max_1er_tranche;
                    el3.max_2eme_tranche = el.max_2eme_tranche;
                    updatedTarifications.push(el3);
                }
            })
        })
    })

    return updatedTarifications;
}

async function fillRedis() {
    console.log('filling redis.....')
    const data = await siteDB.aggregate(query);
    const filtered = data.filter(el => el.compteurs.length > 0);
    const formated = formatData(filtered);
    const updated = await addTarification(formated);
    console.log(updated)
    await client.connect()
    updated.forEach(async (el) => await client.set(el.key, JSON.stringify(el)))
}

module.exports = fillRedis;
