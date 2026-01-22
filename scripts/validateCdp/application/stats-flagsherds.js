const crypto = require("crypto");


const herdStats = db.herds.aggregate([
  {
    $group: {
      _id: null,
      missing_app_ref: {
        $sum: {
          $cond: [
            {
              $or: [
                { $eq: ["$applicationReference", null] }, // null
                { $eq: ["$applicationReference", ""] },   // empty string
                { $eq: [{ $type: "$applicationReference" }, "missing"] } // field doesn't exist
              ]
            },
            1,
            0
          ]
        }
      },
      missing_name: {
        $sum: {
          $cond: [
            {
              $or: [
                { $eq: ["$name", null] },
                { $eq: ["$name", ""] },
                { $eq: [{ $type: "$name" }, "missing"] }
              ]
            },
            1,
            0
          ]
        }
      },
      missing_cph: {
        $sum: {
          $cond: [
            {
              $or: [
                { $eq: ["$cph", null] },
                { $eq: ["$cph", ""] },
                { $eq: [{ $type: "$cph" }, "missing"] }
              ]
            },
            1,
            0
          ]
        }
      },
      missing_species: {
        $sum: {
          $cond: [
            {
              $or: [
                { $eq: ["$species", null] },
                { $eq: ["$species", ""] },
                { $eq: [{ $type: "$species" }, "missing"] }
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

const { orphaned_herd_count } = db.herds.aggregate([
  {
    $lookup: {
      from: "applications",
      localField: "applicationReference",
      foreignField: "reference",
      as: "matchedApplication"
    }
  },
  {
    $facet: {
      orphaned: [
        { $match: { matchedApplication: { $size: 0 } } },
        { $count: "count" }
      ]
    }
  },
  {
    $project: {
      orphaned_herd_count: {
        $ifNull: [{ $arrayElemAt: ["$orphaned.count", 0] }, 0]
      }
    }
  }
]).toArray()[0];


console.log(JSON.stringify(
{
      missing_applicationReference: herdStats.missing_app_ref,
      missing_herdName: herdStats.missing_name,
      missing_cph: herdStats.missing_cph,
      missing_species: herdStats.missing_species,
      orphaned_count: orphaned_herd_count
}, null, 2));



const bigString = db.applications.aggregate([
  // 1️⃣ Keep flags array
  { $project: { flags: { $ifNull: ["$flags", []] } } },
  { $unwind: "$flags" },

  // 2️⃣ Union with owapplications
  {
    $unionWith: {
      coll: "owapplications",
      pipeline: [
        { $project: { flags: { $ifNull: ["$flags", []] } } },
        { $unwind: "$flags" }
      ]
    }
  },

  // 3️⃣ Keep only id, note, deletedNote
  {
    $project: {
      id: "$flags.id",
      notes: [
        { $ifNull: ["$flags.note", null] },
        { $ifNull: ["$flags.deletedNote", null] }
      ]
    }
  },

  // 4️⃣ Filter out null or empty strings
  {
    $project: {
      id: 1,
      notes: {
        $filter: {
          input: "$notes",
          as: "n",
          cond: { $and: [{ $ne: ["$$n", null] }, { $ne: ["$$n", ""] }] }
        }
      }
    }
  },

  // 5️⃣ Group by id to sort the flags
  {
    $group: {
      _id: "$id",
      notes: { $push: "$notes" }
    }
  },

  // 6️⃣ Flatten nested arrays for each flag
  {
    $project: {
      notes: {
        $reduce: {
          input: "$notes",
          initialValue: [],
          in: { $concatArrays: ["$$value", "$$this"] }
        }
      }
    }
  },

  // 7️⃣ Sort by flag id
  { $sort: { _id: 1 } },

  // 8️⃣ Combine all notes into one array
  {
    $group: {
      _id: null,
      all_notes: { $push: "$notes" }
    }
  },

  // 9️⃣ Flatten final array
  {
    $project: {
      all_notes_flat: {
        $reduce: {
          input: "$all_notes",
          initialValue: [],
          in: { $concatArrays: ["$$value", "$$this"] }
        }
      }
    }
  },

  // 🔟 Concatenate into single string with '|'
  {
    $project: {
      _id: 0,
      all_notes_concat: {
        $reduce: {
          input: "$all_notes_flat",
          initialValue: "",
          in: {
            $concat: [
              "$$value",
              { $cond: [{ $eq: ["$$value", ""] }, "", "|"] },
              "$$this"
            ]
          }
        }
      }
    }
  }
]).toArray()[0].all_notes_concat

const hash = crypto
      .createHash("md5")
      .update(bigString, "utf8")
      .digest("hex");

const flagDates = db.applications.aggregate([
  // --- Extract flags from applications
  {
    $project: {
      flags: { $ifNull: ["$flags", []] }
    }
  },
  { $unwind: "$flags" },

  // --- Union with owapplications
  {
    $unionWith: {
      coll: "owapplications",
      pipeline: [
        {
          $project: {
            flags: { $ifNull: ["$flags", []] }
          }
        },
        { $unwind: "$flags" }
      ]
    }
  },

  // --- Keep only the date fields we care about
  {
    $project: {
      createdAt: "$flags.createdAt",
      deletedAt: "$flags.deletedAt"
    }
  },

  // --- Aggregate date statistics
  {
    $group: {
      _id: null,

      earliest_createdAt: { $min: "$createdAt" },
      latest_createdAt:   { $max: "$createdAt" },
      avg_createdAt:      { $avg: { $toLong: "$createdAt" } },

      earliest_deletedAt: { $min: "$deletedAt" },
      latest_deletedAt:   { $max: "$deletedAt" },
      avg_deletedAt:      { $avg: { $toLong:"$deletedAt" } }
    }
  },

  // --- Final shape
  {
    $project: {
      _id: 0,
      earliest_createdAt: 1,
      latest_createdAt: 1,
      avg_createdAt: 1,
      earliest_deletedAt: 1,
      latest_deletedAt: 1,
      avg_deletedAt: 1
    }
  }
]).toArray()[0];;


const flagStats = db.applications.aggregate([
  {
    $project: {
      flags: { $ifNull: ["$flags", []] }
    }
  },
  { $unwind: "$flags" },

  {
    $unionWith: {
      coll: "owapplications",
      pipeline: [
        {
          $project: {
            flags: { $ifNull: ["$flags", []] }
          }
        },
        { $unwind: "$flags" }
      ]
    }
  },

  {
    $group: {
      _id: null,
      total_flags: { $sum: 1 },

      deleted_flags: {
        $sum: {
          $cond: [{ $eq: ["$flags.deleted", true] }, 1, 0]
        }
      },

      active_flags: {
        $sum: {
          $cond: [
            { $ne: ["$flags.deleted", true] },
            1,
            0
          ]
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      total_flags: 1,
      deleted_flags: 1,
      active_flags: 1
    }
  }
]).toArray()[0];


console.log(JSON.stringify(
  {
      total_rows: flagStats.total_flags,
      total_active: flagStats.active_flags,
      total_deleted: flagStats.deleted_flags,
      first_created_flag: flagDates.earliest_createdAt,
      last_created_flag: flagDates.latest_createdAt,
      avg_ts_epoch_created: flagDates.avg_createdAt,
      first_deleted_flag: flagDates.earliest_deletedAt,
      last_deleted_flag: flagDates.latest_deletedAt,
      avg_ts_epoch_deleted: flagDates.avg_deletedAt,
      all_notes_md5: hash
    }, null, 2));
