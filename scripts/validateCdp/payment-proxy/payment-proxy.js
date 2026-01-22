const crypto = require("crypto");


const stats = db.paymentrequests.aggregate([
  {
    $group: {
      _id: null,
      total_rows: { $sum: 1 },
      first_created_doc: { $min: "$createdAt" },
      last_created_doc: { $max: "$createdAt" },
      avg_ts_epoch_created: { $avg: { $toLong: "$createdAt" } },
      first_updated_doc: { $min: "$updatedAt" },
      last_updated_doc: { $max: "$updatedAt" },
      avg_ts_epoch_updated: { $avg: { $toLong: "$updatedAt" } },
      lowest_ref: { $min: "$reference" },
      highest_ref: { $max: "$reference" },
      lowest_frn: { $min: "$frn" },
      highest_frn: { $max: "$frn" },
      payment_check_totalsum: { $sum: "$paymentCheckCount" }
    }
  },
  {
    $project: {
      _id: 0,
      total_rows: 1,
      first_created_doc: 1,
      last_created_doc: 1,
      avg_ts_epoch_created: 1,
      first_updated_doc: 1,
      last_updated_doc: 1,
      avg_ts_epoch_updated: 1,
      lowest_ref: 1,
      highest_ref: 1,
      lowest_frn: 1,
      highest_frn: 1,
      payment_check_totalsum: 1
    }
  }
]).toArray()[0];


const bigString = db.paymentrequests.aggregate([
  {
    // Ensure legacyId exists
    $match: {
      "legacyData.legacyId": { $exists: true, $ne: null }
    }
  },
  {
    // Deterministic order
    $sort: {
      "legacyData.legacyId": 1
    }
  },
  {
    // Collect legacyIds in order
    $group: {
      _id: null,
      legacyIds: { $push: "$legacyData.legacyId" }
    }
  },
  {
    // Join with '|'
    $project: {
      _id: 0,
      concatenatedLegacyIds: {
        $reduce: {
          input: "$legacyIds",
          initialValue: "",
          in: {
            $cond: [
              { $eq: ["$$value", ""] },
              "$$this",
              { $concat: ["$$value", "|", "$$this"] }
            ]
          }
        }
      }
    }
  }
]).toArray()[0]?.concatenatedLegacyIds ?? '';

const hash = crypto
      .createHash("md5")
      .update(bigString, "utf8")
      .digest("hex");


const counts_by_status = db.paymentrequests.aggregate([
  {
    // Optional: ignore missing status
    $match: {
      status: { $exists: true, $ne: null }
    }
  },
  {
    // Count per templateId
    $group: {
      _id: "$status",
      count: { $sum: 1 }
    }
  },
  {
    $sort: {
      _id: 1
    }
  },
  {
    // Convert to key/value format
    $project: {
      _id: 0,
      k: "$_id",
      v: "$count"
    }
  },
  {
    // Build array of key/value pairs in sorted order
    $group: {
      _id: null,
      countsArray: { $push: { k: "$k", v: "$v" } }
    }
  },
  {
    // Convert array → object
    $project: {
      _id: 0,
      counts_by_status: {
        $arrayToObject: "$countsArray"
      }
    }
  }
]).toArray()[0].counts_by_status;




console.log(JSON.stringify(
  {
      total_rows: stats.total_rows,
      total_unknown_status: stats.total_unknown_status,
      total_requested_status: stats.total_requested_status,
      first_created_doc: stats.first_created_doc,
      last_created_doc: stats.last_created_doc,
      avg_ts_epoch_created: stats.avg_ts_epoch_created,
      first_updated_doc: stats.first_updated_doc,
      last_updated_doc: stats.last_updated_doc,
      avg_ts_epoch_updated: stats.avg_ts_epoch_updated,
      lowest_ref: stats.lowest_ref,
      highest_ref: stats.highest_ref,
      lowest_frn: stats.lowest_frn,
      highest_frn: stats.highest_frn,
      all_legacy_field_md5: hash,
      counts_by_status,
      payment_check_totalsum: stats.payment_check_totalsum
    }, null, 2));
