const crypto = require("crypto");


const dates = db.documentlog.aggregate([
  {
    $group: {
      _id: null,
      first_created_doc: { $min: "$createdAt" },
      last_created_doc: { $max: "$createdAt" },
      avg_ts_epoch_created: { $avg: { $toLong: "$createdAt" } },
      first_updated_doc: { $min: "$updatedAt" },
      last_updated_doc: { $max: "$updatedAt" },
      avg_ts_epoch_updated: { $avg: { $toLong: "$updatedAt" } },
      lowest_sbi: { $min: "$inputData.sbi" },
      highest_sbi: { $max: "$inputData.sbi" }
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
      lowest_sbi: 1,
      highest_sbi: 1
    }
  }
]).toArray()[0];


const stats = db.documentlog.aggregate([
  {
    $group: {
      _id: null,
      // Total row count
      total_rows: { $sum: 1 },
      
      total_created: {
        $sum: { $cond: [{ $eq: ["$status", "document-created"] }, 1, 0] }
      },

      total_failed: {
        $sum: { $cond: [{ $eq: ["$status", "send-failed"] }, 1, 0] }
      },

      total_legacy_email_status: {
        $sum: {
          $cond: [
            {
              $regexMatch: {
                input: "$status",
                regex: "^email",
                options: "i"
              }
            },
            1,
            0
          ]
        }
      },

      count_legacy_completed: {
        $sum: {
          $cond: [
            {
              $and: [
                { $ne: ["$legacyData.completed", null] },
                { $ne: [{ $type: "$legacyData.completed" }, "missing"] }
              ]
            },
            1,
            0
          ]
        }
      }
    }
  }
])
.toArray()[0];

const bigString = db.documentlog.aggregate([
  {
    // Only ensure legacyId exists so ordering is deterministic
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
    // Build "legacyId|emailReference"
    // emailReference becomes empty string if missing
    $project: {
      combined: {
        $concat: [
          "$legacyData.legacyId",
          "|",
          { $ifNull: ["$legacyData.emailReference", ""] }
        ]
      }
    }
  },
  {
    // Collect in sorted order
    $group: {
      _id: null,
      values: { $push: "$combined" }
    }
  },
  {
    // Join all entries with '|'
    $project: {
      _id: 0,
      concatenatedValues: {
        $reduce: {
          input: "$values",
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
]).toArray()[0]?.concatenatedValues ?? '';

const hash = crypto
      .createHash("md5")
      .update(bigString, "utf8")
      .digest("hex");



console.log(JSON.stringify(
  {
      total_rows: stats.total_rows,
      total_created: stats.total_created,
      total_failed: stats.total_failed,
      total_legacy_email_status: stats.total_legacy_email_status,
      first_created_doc: dates.first_created_doc,
      last_created_cdoc: dates.last_created_doc,
      avg_ts_epoch_created: dates.avg_ts_epoch_created,
      first_updated_doc: dates.first_updated_doc,
      last_updated_doc: dates.last_updated_doc,
      avg_ts_epoch_updated: dates.avg_ts_epoch_updated,
      lowest_sbi: dates.lowest_sbi,
      highest_sbi: dates.highest_sbi,
      count_legacy_completed: stats.count_legacy_completed,
      all_legacy_field_md5: hash
    }, null, 2));
