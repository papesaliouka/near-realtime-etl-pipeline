const mongoose = require("mongoose");

const SiteSchema = mongoose.Schema(
  {
    designation: {
      type: String,
      required: true,
    },
    est_admin: {
      type: Boolean,
      default: true,
    },
    heure_ouverture: {
      type: String,
      default: "08:00",
    },
    heure_fermeture: {
      type: String,
      default: "17:00",
    },
    puissance_souscrite: {
      type: Number,
      default: 100,
    },

    geo_name: {
      type: String,
    },
    lat: {
      type: Number,
    },
    lon: {
      type: Number,
    },
    home_status: {
      type: Number,
      default: 2,
    },
    adresse: {
      type: String,
    },
    pays: {
      type: String,
    },

    compteurs: [
      {
        type: mongoose.Schema.Types.ObjectId,
        ref: "Compteur",
      },
    ],

    departs: [
      {
        type: mongoose.Schema.Types.ObjectId,
        ref: "Depart",
      },
    ],
    depart_principal_electricite: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "DepartReel",
    },
    membres: [
      {
        type: mongoose.Schema.Types.ObjectId,
        ref: "Member",
      },
    ],
    proprietaire: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "user",
    },
    budget: {
      type: Number,
      default: 0,
    },
    tarifications: [
      {
        type: mongoose.Schema.Types.ObjectId,
        ref: "tarification",
      },
    ],
    tarification: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "tarification",
    },
    type: {
      type: String,
      enum: ["Particulier", "Professionnel"],
      default: "Particulier",
    },
    organisation: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Organisation",
    },
    puissance_transfo: { type: Number },
    type_comptage: { type: String },
    rapport_tc: { type: Number },
    rapport_tp: { type: Number },
    alpha: { type: Number },
    beta: { type: Number },
    gamma: { type: Number },
    delta: { type: Number },
    type_facture: { type: String },
    num_compte_contrat: { type: Number },
    compteur: { type: Number },
    bordereau_rang: { type: String },
    provider_item_id: {
      type: Number,
    },
    f_actif: {
      type: Boolean,
      default: true,
    },
    deleted_at: Date,
    icone: String,
    date_facture: String,
    // donnees du site (HOME) dans la BD de tuya
    // provider_data: {
    //   type: Object,
    // },
  },
  { timestamps: { createdAt: "created_at", updatedAt: "updated_at" } }
);

const siteModel = mongoose.model("Site", SiteSchema);

module.exports = siteModel;
