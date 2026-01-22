db.applications.deleteMany({})
db.owapplications.deleteMany({})
db.claims.deleteMany({})
db.herds.deleteMany({})

const fs = require('fs');

var raw = fs.readFileSync('/home/cdpshell/ProdNewWorldApplications.json');
var docs = EJSON.parse(raw);

db.applications.insertMany(docs);

raw = fs.readFileSync('/home/cdpshell/ProdOldWorldApplications.json');
docs = EJSON.parse(raw);

db.owapplications.insertMany(docs);

raw = fs.readFileSync('/home/cdpshell/ProdClaims.json');
docs = EJSON.parse(raw);

db.claims.insertMany(docs);

raw = fs.readFileSync('/home/cdpshell/ProdHerds.json');
docs = EJSON.parse(raw);

db.herds.insertMany(docs);

console.log('done')