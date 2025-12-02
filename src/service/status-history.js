export async function createStatusHistory(partitioned) {

    //find the ones where we had at least one event for a temp ref and a real ref
    const allHistory = [];
    for (const [key, value] of Object.entries(partitioned)) {
        //value is an array of StatusEntry

        value.filter(x => x.eventType !== 'status-updated').forEach((val) => {
          console.log(`Found an entry with different Event Type! ${val.partitionKey} - ${val.eventType}`)
        })
        value.filter(x => x.status !== 'success').forEach((val) => {
          console.log(`Found an entry with different status! ${val.partitionKey} - ${val.status}`)
        })
        value.filter(x => x.payload.note).forEach((val) => {
          console.log(`Found an entry with note! ${val.partitionKey} - ${val.payload.note}`)
        })

        allHistory.push({ reference: key, statusHistory: value.filter(x => x.status === 'success').map(transformToOutput)})
    }

    return allHistory;
}

function transformToOutput(entry) {
    return {
        status: entry.payload.statusId,
        note: entry.payload.note,
        createdBy: entry.changedBy,
        createdAt: { $date: entry.changedOn.toUTC() }
    }
}