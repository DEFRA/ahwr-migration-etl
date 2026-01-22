db.paymentrequests.deleteMany({})

const fs = require('fs');

var raw = fs.readFileSync('/home/cdpshell/PaymentEntries.json');
var docs = EJSON.parse(raw);

db.paymentrequests.insertMany(docs);

console.log('done')