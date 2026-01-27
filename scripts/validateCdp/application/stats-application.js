const nwDuplicates = db.applications.aggregate([
  {
    $group: {
      _id: "$reference",
      count: { $sum: 1 }
    }
  },
  {
    $match: {
      count: { $gt: 1 }
    }
  }
]).toArray().length;

const owDuplicates = db.owapplications.aggregate([
  {
    $group: {
      _id: "$reference",
      count: { $sum: 1 }
    }
  },
  {
    $match: {
      count: { $gt: 1 }
    }
  }
]).toArray().length;

const nwDates = db.applications.aggregate([
  {
    $group: {
      _id: null,
      first_created_application: { $min: "$createdAt" },
      last_created_application: { $max: "$createdAt" },
      avg_ts_epoch_created: { $avg: { $toLong: "$createdAt" } },
      first_updated_application: { $min: "$updatedAt" },
      last_updated_application: { $max: "$updatedAt" },
      avg_ts_epoch_updated: { $avg: { $toLong: "$updatedAt" } },
      min_len: { $min: { $strLenCP: "$reference" } },
      max_len: { $max: { $strLenCP: "$reference" } },
      avg_len: { $avg: { $strLenCP: "$reference" } }
    }
  },
  {
    $project: {
      _id: 0,
      first_created_application: 1,
      last_created_application: 1,
      avg_ts_epoch_created: 1,
      first_updated_application: 1,
      last_updated_application: 1,
      avg_ts_epoch_updated: 1,
      min_len: 1,
      max_len: 1,
      avg_len: 1
    }
  }
]).toArray()[0];

const owDates = db.owapplications.aggregate([
  {
    $group: {
      _id: null,
      first_created_application: { $min: "$createdAt" },
      last_created_application: { $max: "$createdAt" },
      avg_ts_epoch_created: { $avg: { $toLong: "$createdAt" } },
      first_updated_application: { $min: "$updatedAt" },
      last_updated_application: { $max: "$updatedAt" },
      avg_ts_epoch_updated: { $avg: { $toLong: "$updatedAt" } },
      min_len: { $min: { $strLenCP: "$reference" } },
      max_len: { $max: { $strLenCP:"$reference" } },
      avg_len: { $avg: { $strLenCP: "$reference" } }
    }
  },
  {
    $project: {
      _id: 0,
      first_created_application: 1,
      last_created_application: 1,
      avg_ts_epoch_created: 1,
      first_updated_application: 1,
      last_updated_application: 1,
      avg_ts_epoch_updated: 1,
      min_len: 1,
      max_len: 1,
      avg_len: 1
    }
  }
]).toArray()[0];

const nwInvalidDate = db.applications.find({
  $expr: {
    $and: [
      { $eq: [{ $type: "$updatedAt" }, "date"] },
      { $eq: [{ $type: "$createdAt" }, "date"] },
      { $lt: ["$updatedAt", "$createdAt"] }
    ]
  }
}).toArray().length;

const owInvalidDate = db.owapplications.find({
  $expr: {
    $and: [
      { $eq: [{ $type: "$updatedAt" }, "date"] },
      { $eq: [{ $type: "$createdAt" }, "date"] },
      { $lt: ["$updatedAt", "$createdAt"] }
    ]
  }
}).toArray().length;

const nwStats = db.applications.aggregate([
  {
    $group: {
      _id: null,
      // Total row count
      total_rows: { $sum: 1 },
      // eligiblePiiRedaction = true / false
      no_of_applications_eligible_for_pii_redaction: {
        $sum: { $cond: [{ $eq: ["$eligiblePiiRedaction", true] }, 1, 0] }
      },
      no_of_applications_not_eligible_for_pii_redaction: {
        $sum: { $cond: [{ $eq: ["$eligiblePiiRedaction", false] }, 1, 0] }
      }
    }
  }
]).toArray()[0];


const owStats = db.owapplications.aggregate([
  {
    $group: {
      _id: null,
      // Total row count
      total_rows: { $sum: 1 },
      // eligiblePiiRedaction = true / false
      no_of_applications_eligible_for_pii_redaction: {
        $sum: { $cond: [{ $eq: ["$eligiblePiiRedaction", true] }, 1, 0] }
      },
      no_of_applications_not_eligible_for_pii_redaction: {
        $sum: { $cond: [{ $eq: ["$eligiblePiiRedaction", false] }, 1, 0] }
      },
      no_of_applications_claimed_true: {
        $sum: { $cond: [{ $eq: ["$claimed", true] }, 1, 0] }
      },
      no_of_applications_claimed_false: {
        $sum: { $cond: [{ $eq: ["$claimed", false] }, 1, 0] }
      }
    }
  }
]).toArray()[0];

console.log(JSON.stringify(
[
  {
    type: 'EE',
    app_metrics: {
      total_rows: nwStats.total_rows,
      duplicates: nwDuplicates,
      first_created_application: nwDates.first_created_application,
      last_created_application: nwDates.last_created_application,
      avg_ts_epoch_created: nwDates.avg_ts_epoch_created,
      first_updated_application: nwDates.first_updated_application,
      last_updated_application: nwDates.last_updated_application,
      avg_ts_epoch_updated: nwDates.avg_ts_epoch_updated,
      reference_length: {
        min: nwDates.min_len,
        max: nwDates.max_len,
        avg: nwDates.avg_len
      },
      invalid_update_date: nwInvalidDate,
      claimed: {
        true: 0,
        false: nwStats.total_rows, // Not recorded, so just default to false for all
      },
      eligible_pii_redaction: {
        true: nwStats.no_of_applications_eligible_for_pii_redaction,
        false: nwStats.no_of_applications_not_eligible_for_pii_redaction
      }
    }
  },
  {
    type: 'VV',
    app_metrics: {
      total_rows: owStats.total_rows,
      duplicates: owDuplicates,
      first_created_application: owDates.first_created_application,
      last_created_application: owDates.last_created_application,
      avg_ts_epoch_created: owDates.avg_ts_epoch_created,
      first_updated_application: owDates.first_updated_application,
      last_updated_application: owDates.last_updated_application,
      avg_ts_epoch_updated: owDates.avg_ts_epoch_updated,
      reference_length: {
        min: owDates.min_len,
        max: owDates.max_len,
        avg: owDates.avg_len
      },
      invalid_update_date: owInvalidDate,
      claimed: {
        true: owStats.no_of_applications_claimed_true,
        false: owStats.no_of_applications_claimed_false
      },
      eligible_pii_redaction: {
        true: owStats.no_of_applications_eligible_for_pii_redaction,
        false: owStats.no_of_applications_not_eligible_for_pii_redaction
      }
    }
  }
], null, 2));


const nwDataPresence = db.applications.aggregate([
  {
    $group: {
      _id: null,

      missing: {
        $sum: {
          $cond: [
            {
              $or: [
                { $eq: [ { $type: "$data" }, "missing" ] },
                { $eq: [ "$data", null ] }
              ]
            },
            1,
            0
          ]
        }
      },

      present: {
        $sum: {
          $cond: [
            {
              $and: [
                { $ne: [ { $type: "$data" }, "missing" ] },
                { $ne: [ "$data", null ] }
              ]
            },
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
      missing: 1,
      present: 1
    }
  }
]).toArray()[0];

const owDataPresence = db.owapplications.aggregate([
  {
    $group: {
      _id: null,

      missing: {
        $sum: {
          $cond: [
            {
              $or: [
                { $eq: [ { $type: "$data" }, "missing" ] },
                { $eq: [ "$data", null ] }
              ]
            },
            1,
            0
          ]
        }
      },

      present: {
        $sum: {
          $cond: [
            {
              $and: [
                { $ne: [ { $type: "$data" }, "missing" ] },
                { $ne: [ "$data", null ] }
              ]
            },
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
      missing: 1,
      present: 1
    }
  }
]).toArray()[0];

const dataFields = db.applications.aggregate([
  {
    $project: {
      dataEntries: {
        $cond: [
          { $eq: [{ $type: "$data" }, "object"] },
          { $objectToArray: "$data" },
          []
        ]
      }
    }
  },
  { $unwind: "$dataEntries" },
  {
    $match: {
      "dataEntries.v": { $ne: null }
    }
  },
  {
    $group: {
      _id: "$dataEntries.k",
      present_count: { $sum: 1 },
      types: { $addToSet: { $type: "$dataEntries.v" } }
    }
  },
  {
    $group: {
      _id: null,
      fields: {
        $push: {
          k: "$_id",
          v: {
            present_count: "$present_count",
            types: "$types"
          }
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      data_fields: { $arrayToObject: "$fields" }
    }
  }
]).toArray().flatMap(x =>
      Object.entries(x.data_fields)
        .map(([k, v]) => ({ key: k, type: v.types[0], occurrences: v.present_count })))
      .sort((a, b) => b.occurrences - a.occurrences);

const owDataFields = db.owapplications.aggregate([
  {
    $project: {
      dataEntries: {
        $cond: [
          { $eq: [{ $type: "$data" }, "object"] },
          { $objectToArray: "$data" },
          []
        ]
      }
    }
  },
  { $unwind: "$dataEntries" },
  {
    $match: {
      "dataEntries.v": { $ne: null }
    }
  },
  {
    $group: {
      _id: "$dataEntries.k",
      present_count: { $sum: 1 },
      types: { $addToSet: { $type: "$dataEntries.v" } }
    }
  },
  {
    $group: {
      _id: null,
      fields: {
        $push: {
          k: "$_id",
          v: {
            present_count: "$present_count",
            types: "$types"
          }
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      data_fields: { $arrayToObject: "$fields" }
    }
  }
]).toArray().flatMap(x =>
      Object.entries(x.data_fields)
        .map(([k, v]) => ({ key: k, type: v.types[0], occurrences: v.present_count })))
      .sort((a, b) => b.occurrences - a.occurrences);


const { documents_with_missing_required_fields: nw_missing_fields } = db.applications.aggregate([
  {
    $project: {
      missing_required: {
        $or: [
          // data fields
          {
            $not: {
              $and: [
                { $ne: [{ $ifNull: ["$data.reference", null] }, null] },
                { $ne: [{ $ifNull: ["$data.declaration", null] }, null] },
                { $ne: [{ $ifNull: ["$data.offerStatus", null] }, null] },
                { $ne: [{ $ifNull: ["$data.confirmCheckDetails", null] }, null] }
              ]
            }
          },
          // organisation fields
          {
            $not: {
              $and: [
                { $ne: [{ $ifNull: ["$organisation.crn", null] }, null] },
                { $ne: [{ $ifNull: ["$organisation.email", null] }, null] },
                { $ne: [{ $ifNull: ["$organisation.sbi", null] }, null] },
                { $ne: [{ $ifNull: ["$organisation.name", null] }, null] },
                { $ne: [{ $ifNull: ["$organisation.address", null] }, null] },
                { $ne: [{ $ifNull: ["$organisation.orgEmail", null] }, null] },
                { $ne: [{ $ifNull: ["$organisation.userType", null] }, null] },
                { $ne: [{ $ifNull: ["$organisation.farmerName", null] }, null] }
              ]
            }
          }
        ]
      }
    }
  },
  {
    $group: {
      _id: null,
      documents_with_missing_required_fields: {
        $sum: {
          $cond: ["$missing_required", 1, 0]
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      documents_with_missing_required_fields: 1
    }
  }
]).toArray()[0];

const { documents_with_missing_required_fields: ow_missing_fields } = db.owapplications.aggregate([
  {
    $project: {
      missing_required: {
        $or: [
          // data fields
          {
            $not: {
              $and: [
                { $ne: [{ $ifNull: ["$data.reference", null] }, null] },
                { $ne: [{ $ifNull: ["$data.declaration", null] }, null] },
                { $ne: [{ $ifNull: ["$data.offerStatus", null] }, null] },
                { $ne: [{ $ifNull: ["$data.confirmCheckDetails", null] }, null] }
              ]
            }
          },
          // organisation fields
          {
            $not: {
              $and: [
                { $ne: [{ $ifNull: ["$organisation.crn", null] }, null] },
                { $ne: [{ $ifNull: ["$organisation.email", null] }, null] },
                { $ne: [{ $ifNull: ["$organisation.sbi", null] }, null] },
                { $ne: [{ $ifNull: ["$organisation.name", null] }, null] },
                { $ne: [{ $ifNull: ["$organisation.address", null] }, null] },
                { $ne: [{ $ifNull: ["$organisation.orgEmail", null] }, null] },
                { $ne: [{ $ifNull: ["$organisation.userType", null] }, null] },
                { $ne: [{ $ifNull: ["$organisation.farmerName", null] }, null] }
              ]
            }
          }
        ]
      }
    }
  },
  {
    $group: {
      _id: null,
      documents_with_missing_required_fields: {
        $sum: {
          $cond: ["$missing_required", 1, 0]
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      documents_with_missing_required_fields: 1
    }
  }
]).toArray()[0];


console.log(JSON.stringify(
[
  {
    application_type: 'EE',
    metrics: {
      data_nullability: {
        null_data: nwDataPresence.missing,
        non_null_data: nwDataPresence.present
      },
      json_key_statistics: dataFields,
      missing_payload_fields: nw_missing_fields
    }
  },
  {
    application_type: 'VV',
    metrics: {
      data_nullability: {
        null_data: owDataPresence.missing,
        non_null_data: owDataPresence.present
      },
      json_key_statistics: owDataFields,
      missing_payload_fields: ow_missing_fields
  }
}
], null, 2));
