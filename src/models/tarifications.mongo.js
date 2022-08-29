const mongoose = require("mongoose");

const TarificationSiteSchema = mongoose.Schema(
  {
    code: {
      type: String,
    },
    categorie: {
      type: String,
    },
    designation: {
      type: String,
    },
    type_tarif: {
      type: String,
      enum: ['Tranche', 'Periode', 'Woyofal', 'Custom'],
    },
    prix_moyen_electricite: {
      type: Number,
    },
    debut_heure_creuse: {
      type: String,
      default: "19:00",
    },
    fin_heure_creuse: {
      type: String,
      default: "00:00",
    },
    tarif_heure_creuse: {
      type: Number,
      default: 0,
    },
    debut_heure_pleine: {
      type: String,
      default: "00:00",
    },
    fin_heure_pleine: {
      type: String,
      default: "19:00",
    },
    tarif_heure_pleine: {
      type: Number,
      default: 0,
    },
    tarif_1er_tranche: {
      type: Number,
      default: 0,
    },
    tarif_2eme_tranche: {
      type: Number,
      default: 0,
    },
    tarif_3eme_tranche: {
      type: Number,
      default: 0,
    },
    max_1er_tranche: {
      type: Number,
      default: 0,
    },
    max_2eme_tranche: {
      type: Number,
      default: 0,
    },
    prime_fixe: {
      type: Number,
      default: 0,
    },
    monnaie: {
      type: String,
      default: "XOF",
    },
    date_debut_validite: {
      type: Date,
    },
    date_limite_validite: {
      type: Date,
    },
    site: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "site",
    },
    periode_creuse: [String],
    periode_pleine: [String],
  },
  { timestamps: { createdAt: "created_at", updatedAt: "updated_at" } }
);

const TarificationSite = mongoose.model("tarification", TarificationSiteSchema);

module.exports = TarificationSite;
