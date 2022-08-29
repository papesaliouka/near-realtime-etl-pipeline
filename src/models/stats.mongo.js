const mongoose = require("mongoose");

const StatHeureCompteurSchema = new mongoose.Schema(
  {
    date: { type: String },
    heure: { type: String },
    energie: { type: Number, default: 0 },
    energie_reactive: { type: Number, default: 0 },
    montant: { type: Number, default: 0 },
    tension_mini: { type: Number, default: 0 },
    tension_maxi: { type: Number, default: 0 },
    tension_moyenne: { type: Number, default: 0 },
    puissance_mini: { type: Number, default: 0 },
    puissance_maxi: { type: Number, default: 0 },
    puissance_moyenne: { type: Number, default: 0 },
    intensite_mini: { type: Number, default: 0 },
    intensite_maxi: { type: Number, default: 0 },
    intensite_moyenne: { type: Number, default: 0 },
    id_depart: {
      type: String,
    },
    id_depart_reel: {
      type: String,
    },
    id_compteur: {
      type: String,
    },
    id_site: {
      type: String,
    },
    depart: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Depart",
      required: true,
    },
    depart_reel: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "DepartReel",
      required: true,
    },
    compteur: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Compteur",
      required: true,
    },
    site: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Site",
      required: true,
    },
    // id du device dont on a recuperer le conso
    provider_item_id: {
      type: String,
    },
  },

  { timestamps: { createdAt: "created_at", updatedAt: "updated_at" } }
);

const StatHeureCompteur = mongoose.model(
  "stat_heure_compteurs",
  StatHeureCompteurSchema
);
module.exports = StatHeureCompteur;
