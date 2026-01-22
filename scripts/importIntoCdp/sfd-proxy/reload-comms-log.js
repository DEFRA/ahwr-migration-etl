db.commsrequests.deleteMany({})

const fs = require('fs');

var raw = fs.readFileSync('/home/cdpshell/CommsLogEntries.json');
var docs = EJSON.parse(raw);

db.commsrequests.insertMany(docs);

console.log('done')