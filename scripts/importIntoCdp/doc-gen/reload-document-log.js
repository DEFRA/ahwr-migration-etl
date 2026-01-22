db.documentlog.deleteMany({})

const fs = require('fs');

var raw = fs.readFileSync('/home/cdpshell/DocLogEntries.json');
var docs = EJSON.parse(raw);

db.documentlog.insertMany(docs);

console.log('done')