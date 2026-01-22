const crypto = require("crypto");


const dates = db.commsrequests.aggregate([
  {
    $group: {
      _id: null,
      first_created_doc: { $min: "$createdAt" },
      last_created_doc: { $max: "$createdAt" },
      avg_ts_epoch_created: { $avg: { $toLong: "$createdAt" } },
      first_updated_doc: { $min: "$updatedAt" },
      last_updated_doc: { $max: "$updatedAt" },
      avg_ts_epoch_updated: { $avg: { $toLong: "$updatedAt" } },
      lowest_claim_ref: { $min: "$claimReference" },
      highest_claim_ref: { $max: "$claimReference" },
      lowest_app_ref: { $min: "$agreementReference" },
      highest_app_ref: { $max: "$agreementReference" }
    }
  },
  {
    $project: {
      _id: 0,
      first_created_doc: 1,
      last_created_doc: 1,
      avg_ts_epoch_created: 1,
      first_updated_doc: 1,
      last_updated_doc: 1,
      avg_ts_epoch_updated: 1,
      lowest_claim_ref: 1,
      highest_claim_ref: 1,
      lowest_app_ref: 1,
      highest_app_ref: 1
    }
  }
]).toArray()[0];


const stats = db.commsrequests.aggregate([
  {
    $group: {
      _id: null,
      // Total row count
      total_rows: { $sum: 1 },
      
      total_unknown_status: {
        $sum: { $cond: [{ $eq: ["$status", "UNKNOWN"] }, 1, 0] }
      },

      total_requested_status: {
        $sum: { $cond: [{ $eq: ["$status", "REQUESTED"] }, 1, 0] }
      }
    }
  }
])
.toArray()[0];

const bigString = db.commsrequests.aggregate([
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


const counts_by_template = db.commsrequests.aggregate([
  {
    // Optional: ignore missing templateId
    $match: {
      templateId: { $exists: true, $ne: null }
    }
  },
  {
    // Count per templateId
    $group: {
      _id: "$templateId",
      count: { $sum: 1 }
    }
  },
  {
    // Sort by templateId (UUID string order)
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
      counts_by_template: {
        $arrayToObject: "$countsArray"
      }
    }
  }
]).toArray()[0].counts_by_template;




console.log(JSON.stringify(
  {
      total_rows: stats.total_rows,
      total_unknown_status: stats.total_unknown_status,
      total_requested_status: stats.total_requested_status,
      first_created_doc: dates.first_created_doc,
      last_created_cdoc: dates.last_created_doc,
      avg_ts_epoch_created: dates.avg_ts_epoch_created,
      first_updated_doc: dates.first_updated_doc,
      last_updated_doc: dates.last_updated_doc,
      avg_ts_epoch_updated: dates.avg_ts_epoch_updated,
      lowest_claim_ref: dates.lowest_claim_ref,
      highest_claim_ref: dates.highest_claim_ref,
      lowest_app_ref: dates.lowest_app_ref,
      highest_app_ref: dates.highest_app_ref,
      all_legacy_field_md5: hash,
      counts_by_template
    }, null, 2));
