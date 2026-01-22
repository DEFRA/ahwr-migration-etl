console.log();
console.log("Claims per application");

console.log(db.applications.aggregate([
  {
    $lookup: {
      from: "claims",                    // collection to join
      localField: "reference",          // field in 'application'
      foreignField: "applicationReference", // field in 'claim'
      as: "claims"                      // output array
    }
  },
  {
    $project: {
      _id: 0,
      reference: 1,
      claims_count: { $size: "$claims" }  // count of related claims
    }
  },
  {
    $sort: { claims_count: -1 }          // descending order
  }
]));

console.log();
console.log("Total Herds");
console.log(db.herds.countDocuments())


console.log();
console.log("Herd by application");

console.log(db.herds.aggregate([
  {
    $group: {
      _id: "$applicationReference",
      herd_count: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      reference: '$_id',
      herd_count: 1
    }
  },
  {
    $sort: { herd_count: -1 }
  }
]));

console.log();
console.log("Herd by species");

console.log(db.herds.aggregate([
  {
    $group: {
      _id: "$species",
      herd_count: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      species: '$_id',
      herd_count: 1
    }
  },
  {
    $sort: { herd_count: -1 }
  }
]));

console.log();
console.log("Claims per herd");

console.log(db.claims.aggregate([
  {
    $match: {
      herd: { $exists: true }
    }
  },
  {
    $group: {
      _id: {
        id: "$herd.id",
        version: "$herd.version"
      },
      claims_count: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      id: "$_id.id",
      version: "$_id.version",
      claims_count: 1
    }
  },
  {
    $sort: { claims_count: -1 }
  }
]));
