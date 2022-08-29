const mongoose = require("mongoose");

const CompteurSchema = mongoose.Schema(
    {
        designation: {
            type: String,
            required: true,
        },
        // TODO: Remettre le type de compteur
        // type: {
        //   type: mongoose.Schema.Types.ObjectId,
        //   ref: "TypeCompteur",
        //   required: true,
        // },
        equipement: {
            type: mongoose.Schema.Types.ObjectId,
            required: false,
        },
        depart: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "Depart",
            required: false,
        },
        depart_reel: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "DepartReel",
            required: false,
        },
        site: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "Site",
            required: false,
        },
        type_compteur: {
            type: String,
            enum: ['prise', 'modulaire'],
            default: 'prise'
        },
        emplacement_compteur: {
            type: String,
        },
        // ide du conpteur (device) dans la bd de tuya
        provider_item_id: {
            type: String,
        },
        puissance_btu: {
            type: Number,
            default: 0,
        },
        puissance: {
            type: Number,
            default: 0,
        },
        marque: String,
        icone: String,
        type_puissance: String,
        modele: String,
        type_modele: String,
        type_protocole: String,
        controleur: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "Controleur",
            required: true,
        },
        num_ip: String,
        nom_adresse: String,
        repertoire_lecture: String,
        num_node: String,
        f_envoi_cloud: {
            type: Boolean,
            default: false,
        },
        f_actif: {
            type: Boolean,
            default: true,
        },
        deleted_at: Date,
        responsable: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "user",
            required: true,
        },
        // // id du compteur (Device) provenant de la BD de tuya
        // provider_data: {
        //   type: Object,
        // },
    },
    {timestamps: {createdAt: "created_at", updatedAt: "updated_at"}}
);

const compteurModel = mongoose.model("Compteur", CompteurSchema);

module.exports = compteurModel;
