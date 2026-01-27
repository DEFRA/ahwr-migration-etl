const duplicates = db.claims.aggregate([
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


const dates = db.claims.aggregate([
  {
    $group: {
      _id: null,
      first_created_claim: { $min: "$createdAt" },
      last_created_claim: { $max: "$createdAt" },
      avg_ts_epoch_created: { $avg: { $toLong: "$createdAt" } },
      first_updated_claim: { $min: "$updatedAt" },
      last_updated_claim: { $max: "$updatedAt" },
      avg_ts_epoch_updated: { $avg: { $toLong: "$updatedAt" } },
      min_len: { $min: { $strLenCP: "$reference" } },
      max_len: { $max: { $strLenCP: "$reference" } },
      avg_len: { $avg: { $strLenCP: "$reference" } }
    }
  },
  {
    $project: {
      _id: 0,
      first_created_claim: 1,
      last_created_claim: 1,
      avg_ts_epoch_created: 1,
      first_updated_claim: 1,
      last_updated_claim: 1,
      avg_ts_epoch_updated: 1,
      min_len: 1,
      max_len: 1,
      avg_len: 1
    }
  }
]).toArray()[0];


const invalidDate = db.claims.find({
  $expr: {
    $and: [
      { $eq: [{ $type: "$updatedAt" }, "date"] },
      { $eq: [{ $type: "$createdAt" }, "date"] },
      { $lt: ["$updatedAt", "$createdAt"] }
    ]
  }
}).toArray().length;


const stats = db.claims.aggregate([
  {
    $group: {
      _id: null,
      // Total row count
      total_rows: { $sum: 1 },
      //type of claim
      review_claims: {
        $sum: { $cond: [{ $eq: ["$type", "REVIEW"] }, 1, 0] }
      },
      follow_up_claims: {
        $sum: { $cond: [{ $eq: ["$type", "FOLLOW_UP"] }, 1, 0] }
      },
      in_check: {
        $sum: { $cond: [{ $eq: ["$status", "IN_CHECK"] }, 1, 0] }
      },
      on_hold: {
        $sum: { $cond: [{ $eq: ["$status", "ON_HOLD"] }, 1, 0] }
      },
      rec_pay: {
        $sum: { $cond: [{ $eq: ["$status", "RECOMMENDED_TO_PAY"] }, 1, 0] }
      },
      rec_reject: {
        $sum: { $cond: [{ $eq: ["$status", "RECOMMENDED_TO_REJECT"] }, 1, 0] }
      },
      rejected: {
        $sum: { $cond: [{ $eq: ["$status", "REJECTED"] }, 1, 0] }
      },
      ready_to_pay: {
        $sum: { $cond: [{ $eq: ["$status", "READY_TO_PAY"] }, 1, 0] }
      },
      paid: {
        $sum: { $cond: [{ $eq: ["$status", "PAID"] }, 1, 0] }
      },
      others: {
        $sum: { $cond: [{ $not: { $in: ["$status", ["IN_CHECK","ON_HOLD","RECOMMENDED_TO_PAY","RECOMMENDED_TO_REJECT","REJECTED","READY_TO_PAY","PAID"]] } }, 1, 0] }
      },
      non_numeric_amount: {
        $sum: {
          $cond: [
            {
              $not: {
                $in: [
                  { $type: "$data.amount" },
                  ["int", "long", "double", "decimal"]
                ]
              }
            },
            1,
            0
          ]
        }
      }
    }
  }
]).toArray()[0];


console.log(JSON.stringify(
  {
      duplicates,
      total_rows: stats.total_rows,
      total_reviews: stats.review_claims,
      total_follow_ups: stats.follow_up_claims,
      total_in_check: stats.in_check,
      total_on_hold: stats.on_hold,
      total_recc_pay: stats.rec_pay,
      total_recc_reject: stats.rec_reject,
      total_rejected: stats.rejected,
      total_ready_to_pay: stats.ready_to_pay,
      total_paid: stats.paid,
      total_other_statuses: stats.others,
      first_created_claim: dates.first_created_claim,
      last_created_claim: dates.last_created_claim,
      avg_ts_epoch_created: dates.avg_ts_epoch_created,
      first_updated_claim: dates.first_updated_claim,
      last_updated_claim: dates.last_updated_claim,
      avg_ts_epoch_updated: dates.avg_ts_epoch_updated,
      reference_length: {
        min: dates.min_len,
        max: dates.max_len,
        avg: dates.avg_len
      },
      invalid_update_date: invalidDate,
      invalid_claim_amount: stats.non_numeric_amount
    }, null, 2));


const dataPresence = db.claims.aggregate([
  {
    $group: {
      _id: null,

      review_missing: {
        $sum: {
          $cond: [
            {
              $and: [
                { $eq: ["$type", "REVIEW"] },
                {
                  $or: [
                    { $eq: [{ $type: "$data" }, "missing"] },
                    { $eq: ["$data", null] }
                  ]
                }
              ]
            },
            1,
            0
          ]
        }
      },

      review_present: {
        $sum: {
          $cond: [
            {
              $and: [
                { $eq: ["$type", "REVIEW"] },
                { $ne: [{ $type: "$data" }, "missing"] },
                { $ne: ["$data", null] }
              ]
            },
            1,
            0
          ]
        }
      },

      follow_up_missing: {
        $sum: {
          $cond: [
            {
              $and: [
                { $eq: ["$type", "FOLLOW_UP"] },
                {
                  $or: [
                    { $eq: [{ $type: "$data" }, "missing"] },
                    { $eq: ["$data", null] }
                  ]
                }
              ]
            },
            1,
            0
          ]
        }
      },

      follow_up_present: {
        $sum: {
          $cond: [
            {
              $and: [
                { $eq: ["$type", "FOLLOW_UP"] },
                { $ne: [{ $type: "$data" }, "missing"] },
                { $ne: ["$data", null] }
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
      _id: 0
    }
  }
]).toArray()[0];

const dataFieldsReview = db.claims.aggregate([
  {
    $match: { type: "REVIEW" }
  },
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
  {
    $unwind: "$dataEntries"
  },
  {
    $match: {
      "dataEntries.v": { $ne: null }
    }
  },
  {
    $group: {
      _id: {
        field: "$dataEntries.k",
        type: { $type: "$dataEntries.v" }
      },
      present_count: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      field: "$_id.field",
      type: "$_id.type",
      present_count: 1
    }
  },
  {
    $sort: { field: 1, type: 1 }
  }
]).toArray().map(x => ({ key: x.field, type: x.type, occurrences: x.present_count }))
        .sort((a, b) => b.occurrences - a.occurrences);


const dataFieldsFollowUp = db.claims.aggregate([
  {
    $match: { type: "FOLLOW_UP" }
  },
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
  {
    $unwind: "$dataEntries"
  },
  {
    $match: {
      "dataEntries.v": { $ne: null }
    }
  },
  {
    $group: {
      _id: {
        field: "$dataEntries.k",
        type: { $type: "$dataEntries.v" }
      },
      present_count: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      field: "$_id.field",
      type: "$_id.type",
      present_count: 1
    }
  },
  {
    $sort: { field: 1, type: 1 }
  }
])
.toArray().map(x => ({ key: x.field, type: x.type, occurrences: x.present_count }))
        .sort((a, b) => b.occurrences - a.occurrences);


const missing_required = db.claims.aggregate([
  {
    $project: {
      type: 1,
      missing_required: {
        $not: {
          $and: [
            { $ne: [{ $ifNull: ["$data.amount", null] }, null] },
            { $ne: [{ $ifNull: ["$data.vetsName", null] }, null] },
            { $ne: [{ $ifNull: ["$data.claimType", null] }, null] },
            { $ne: [{ $ifNull: ["$data.dateOfVisit", null] }, null] },
            { $ne: [{ $ifNull: ["$data.testResults", null] }, null] },
            { $ne: [{ $ifNull: ["$data.dateOfTesting", null] }, null] },
            { $ne: [{ $ifNull: ["$data.laboratoryURN", null] }, null] },
            { $ne: [{ $ifNull: ["$data.vetRCVSNumber", null] }, null] },
            { $ne: [{ $ifNull: ["$data.typeOfLivestock", null] }, null] },
            { $ne: [{ $ifNull: ["$data.reviewTestResults", null] }, null] }
          ]
        }
      }
    }
  },
  {
    $group: {
      _id: "$type",
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
      type: "$_id",
      documents_with_missing_required_fields: 1
    }
  }
]).toArray()

const review_missing_fields = missing_required.find(x => x.type === 'REVIEW')?.documents_with_missing_required_fields ?? 0;
const fu_missing_fields = missing_required.find(x => x.type === 'FOLLOW_UP')?.documents_with_missing_required_fields ?? 0;


console.log(JSON.stringify(
[
  {
    claim_type: 'R',
    metrics: {
      data_nullability: {
        null_data: dataPresence.review_missing,
        non_null_data: dataPresence.review_present
      },
      json_key_statistics: dataFieldsReview,
      missing_payload_fields: review_missing_fields
    }
  },
  {
    claim_type: 'E',
    metrics: {
      data_nullability: {
        null_data: dataPresence.follow_up_missing,
        non_null_data: dataPresence.follow_up_present
      },
      json_key_statistics: dataFieldsFollowUp,
      missing_payload_fields: fu_missing_fields
    }
  }
], null, 2));
