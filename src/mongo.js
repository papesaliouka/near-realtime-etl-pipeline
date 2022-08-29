require("dotenv/config");

var mongoose = require("mongoose");

//module.exports = {
//mongoose,
//connect: async () => {
///await mongoose.connect(process.env.DB_CONNECTION, {
//   useNewUrlParser: true,
// useUnifiedTopology: true,
// });
//},
//};



async function mongoConnect() {
    mongoose.connect(process.env.DB_CONNECTION, {
        useUnifiedTopology: true

    });

};

module.exports = mongoConnect
