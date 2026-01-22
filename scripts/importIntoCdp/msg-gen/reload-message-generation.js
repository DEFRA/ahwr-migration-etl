db.messagegeneration.deleteMany({})

const fs = require('fs');

var raw = fs.readFileSync('/home/cdpshell/MessageGenerationEntries.json');
var docs = EJSON.parse(raw);

db.messagegeneration.insertMany(docs);

console.log('done')