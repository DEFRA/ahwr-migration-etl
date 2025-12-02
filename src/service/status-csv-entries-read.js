import {parse} from 'csv';
import fs from "fs";
import _ from "lodash";
import {DateTime} from "luxon";
import {createStatusHistory} from "./status-history.js";

export async function loadAndCombineCsvAndJson(filePath) {

    const items = [];

    if(fs.existsSync(filePath)) {
        const csvContent = fs.readFileSync(filePath).toString();
        await readFileContentIntoArray(
            items,
            mapEntryToObject,
            csvContent,
            filePath,
            'rows'
        );
    }

    const partitioned = _.groupBy(items, 'partitionKey');

    const originalEnvName = process.env.STATUS_ENVIRONMENT ?? 'dev';
    const envName = _.upperFirst(originalEnvName);

    //load CSV history items
    const statusHistory = await createStatusHistory(partitioned);
    const originalLength = statusHistory.length

    //load claims
    const nwClaims = await import(`../../data/${envName}Claims.json`, {
        with: {
            type: "json"
        }
    }) ;
    const nwApps = await import(`../../data/${envName}NewWorldApplications.json`, {
        with: {
            type: "json"
        }
    });
    const owApps = await import(`../../data/${envName}OldWorldApplications.json`, {
        with: {
            type: "json"
        }
    });
    const herds = await import(`../../data/${envName}Herds.json`, {
        with: {
            type: "json"
        }
    });

    //merge claims and status history
    const mergedClaims = nwClaims.default.map((claim) => blendStatusHistory(statusHistory, claim));

    await writeOutputToJsonFile(mergedClaims, `output/${originalEnvName}/${envName}Claims.json`);

    //merge nwApps and status history
    const mergedNWApps = nwApps.default.map((app) => blendStatusHistory(statusHistory, app));

    await writeOutputToJsonFile(mergedNWApps, `output/${originalEnvName}/${envName}NewWorldApplications.json`);

    //merge owApps and status history
    const mergedOWApps = owApps.default.map((app) => blendStatusHistory(statusHistory, app));

    await writeOutputToJsonFile(mergedOWApps, `output/${originalEnvName}/${envName}OldWorldApplications.json`);

    const unJasonedHerds = herds.default.map((h) =>  ({ ...h.jason }));

    await writeOutputToJsonFile(unJasonedHerds, `output/${originalEnvName}/${envName}Herds.json`);

    await reportNoMatches(originalEnvName, envName, statusHistory, originalLength, mergedClaims, mergedNWApps, mergedOWApps);

    return [];
}

async function reportNoMatches(originalEnvName, envName, statusHistory, originalLength, mergedClaims, mergedNWApps, mergedOWApps) {
    //report any entries in status history that did not match any of the above, as these are dead entries
    if(statusHistory.length) {
        console.log(`There are ${statusHistory.length} entries of ${originalLength} in the status history that did not match any claim or application!`);
        await writeOutputToJsonFile(statusHistory, `output/${originalEnvName}/${envName}UnmatchedStatusHistoryEntries.json`);
    }

    const claimsWithNoHistory = mergedClaims.filter((c) => !c.statusHistory.length);

    if(claimsWithNoHistory.length) {
        console.log(`There are ${claimsWithNoHistory.length} of ${mergedClaims.length} claims that did not get any status history!`);
    }

    const nwAppsWithNoHistory = mergedNWApps.filter((c) => !c.statusHistory.length);

    if(nwAppsWithNoHistory.length) {
        console.log(`There are ${nwAppsWithNoHistory.length} of ${mergedNWApps.length} NW apps that did not get any status history!`);
    }
    const owAppsWithNoHistory = mergedOWApps.filter((c) => !c.statusHistory.length);

    if(owAppsWithNoHistory.length) {
        console.log(`There are ${owAppsWithNoHistory.length} of ${mergedOWApps.length} OW apps that did not get any status history!`);
    }
}

function blendStatusHistory(statusHistory, hr) {

    let foundAtIndex = -1;
    const match = statusHistory.find((sh, index) => {

        if(sh.reference === hr.reference) {
            foundAtIndex = index;
            return true;
        }
        return false;
    });
    if (foundAtIndex > -1) { // only splice array when item is found
        statusHistory.splice(foundAtIndex, 1);
    }
    return {
        ...hr,
        statusHistory: match ? match.statusHistory : []
    }
}

async function writeOutputToJsonFile(output, writeTo) {
    fs.writeFileSync(writeTo, JSON.stringify(output), { flag: 'w' });
}

async function readFileContentIntoArray(
    containerArray,
    mappingFunction,
    csvContent,
    file,
    typeOfThing
) {
    let promiseResolve;

    const promise = new Promise((resolve, _reject) => {
        promiseResolve = resolve;
    });

    parse(
        csvContent,
        {
            bom: true,
            columns: true,
            skipRecordsWithError: true,
            skipEmptyLines: true,
        },
        (err, records, info) => {
            console.log(
                `Found ${records.length} ${typeOfThing} entries in file ${file}`
            );
            console.log(`Info about file: ${JSON.stringify(info)}`);
            for (const entry of records) {
                containerArray.push(mappingFunction(entry))
            }

            promiseResolve(`Added  ${typeOfThing}`);
        }
    );

    await promise;
}

function mapEntryToObject(rawEntry) {
    return {
        partitionKey: rawEntry['PartitionKey'],
        rowKey: rawEntry['RowKey'],
        EventId: rawEntry['EventId'],
        eventType: rawEntry['EventType'],
        status: rawEntry['Status'],
        payload: JSON.parse(rawEntry['Payload'],  (key, value) => key === 'statusId' ? lookupStatus(value) : value),
        changedBy: rawEntry['ChangedBy'],
        changedOn: DateTime.fromISO(rawEntry['ChangedOn'])
    }
}

function lookupStatus(statusId) {
    switch (statusId) {
        case 1: return 'AGREED';
        case 2: return 'WITHDRAWN';
        case 3: return 'DATA_INPUTTED';
        case 4: return 'CLAIMED';
        case 5: return 'IN_CHECK';
        case 6: return 'ACCEPTED';
        case 7: return 'NOT_AGREED';
        case 8: return 'PAID';
        case 9: return 'READY_TO_PAY';
        case 10: return 'REJECTED';
        case 11: return 'ON_HOLD';
        case 12: return 'RECOMMENDED_TO_PAY';
        case 13: return 'RECOMMENDED_TO_REJECT';
        case 14: return 'AUTHORISED';
        case 15: return 'SENT_TO_FINANCE';
        case 16: return 'PAYMENT_HELD';
    }
}
