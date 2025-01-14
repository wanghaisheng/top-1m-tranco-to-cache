import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import process from 'node:process';
import { parse, stringify } from 'csv/sync';
import { generateUniqueMessage } from '../util/messages.js';
import { tableFromRecords } from '../util/table.js';

const inputFilePath = process.argv[2];
const domainIdFilePath = 'data/domain_id.csv';
const rankDataFilePath = 'data/rank_data.csv';

if (!inputFilePath) {
  console.error(`Input file path missing. Usage:
  $ pnpm tsx scripts/updateCsvDatabaseRecords.ts data/persisted-to-cache/database.csv
`);
  process.exit(1);
}


// Load existing domain IDs
let domainIdRecords: Map<string, string> = new Map(); // Use Map for O(1) lookup
let maxId = 0;
if (existsSync(domainIdFilePath)) {
  try {
    const records: { id: string; domain: string }[] = parse(
      readFileSync(domainIdFilePath, 'utf-8'),
      {
        columns: true,
      }
    );
    for (const record of records) {
      domainIdRecords.set(record.domain, record.id);
        maxId = Math.max(maxId, parseInt(record.id, 10))
    }
  } catch (err) {
    console.error(`Error reading domain IDs CSV: ${domainIdFilePath}\n`, err);
    process.exit(1);
  }
}

// Load existing rank data (only load the header), defer to append to file instead of storing to memory
let rankHeader = 'id,rank,date\n';
if (existsSync(rankDataFilePath)) {
  try {
    rankHeader = readFileSync(rankDataFilePath, 'utf-8').split('\n')[0] + '\n';
  } catch (err) {
    console.error(`Error reading rank data CSV: ${rankDataFilePath}\n`, err);
    process.exit(1);
  }
}


const newRankDataRecords: string[] = [];
let newDomainCounter = 0;

// Read input data line by line (skip parsing) to be more efficient
try {
    const fileContent = readFileSync(inputFilePath, 'utf-8')
    const lines = fileContent.split('\n');

    // Loop through lines instead of parsing into memory
    for(const line of lines){
        if(!line) {
            continue;
        }
        const [rank, domain] = line.split(',');

        if(!domain || !rank){
            console.warn(`Invalid line: ${line}`)
            continue;
        }

        let id = domainIdRecords.get(domain);

        if (id === undefined) {
          maxId++;
          id = String(maxId);
          domainIdRecords.set(domain, id);
            newDomainCounter++;
        }

        newRankDataRecords.push(`${id},${rank},${new Date().toISOString().split('T')[0]}`);
    }
} catch (err) {
  console.error(`Error reading or parsing input CSV: ${inputFilePath}\n`, err);
  process.exit(1);
}

// Write updated CSV files
try {
  const domainIdRecordsToSave = Array.from(domainIdRecords.entries()).map(([domain, id]) => ({domain, id}));
  writeFileSync(
    domainIdFilePath,
      stringify(domainIdRecordsToSave, { header: true })
  );
} catch (err) {
  console.error(`Error writing to domain ID CSV: ${domainIdFilePath}`, err);
  process.exit(1);
}


// Use append file for new records, to avoid keeping in memory
try {
  writeFileSync(rankDataFilePath, rankHeader, { flag: 'a'}); // Append header to file if non existing.
    writeFileSync(rankDataFilePath, newRankDataRecords.join('\n') + '\n', { flag: 'a'})
} catch (err) {
  console.error(`Error writing to rank data CSV: ${rankDataFilePath}`, err);
  process.exit(1);
}


console.log(`Updated:
- ${newDomainCounter} new domains added to ${domainIdFilePath}
- ${newRankDataRecords.length} new rank records added to ${rankDataFilePath}
`);

// Display the first few records from the Map
console.log("First 100 domainIdRecords:");
console.log(tableFromRecords(Array.from(domainIdRecords.entries()).slice(0, 100).map(([domain, id]) => ({domain, id}))));

// Display the first few records from rank data
console.log("First 100 rankDataRecords");
let first100RankDataRecords: any[] = [];
try {
    const lines = readFileSync(rankDataFilePath, 'utf-8').split('\n').slice(1, 101);
    first100RankDataRecords = lines.map(line => {
        const [id, rank, date] = line.split(',')
        return { id, rank, date };
    })
} catch (e) {
    console.warn("Could not read first 100 of rank data", e);
}
console.log(tableFromRecords(first100RankDataRecords))
