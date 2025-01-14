import { readFileSync, existsSync } from 'node:fs';
import process from 'node:process';
import Database from 'better-sqlite3';
import { parse } from 'csv/sync';

const inputFilePath = process.argv[2];
const dbFilePath = 'data/database.db';
function tableFromRecords(records) {
    const headers = Object.keys(records[0] || {});
    const rows = records.map(record => Object.values(record));
    const table = [headers, ...rows].map(row => row.join('\t')).join('\n');
    return table;
}

if (!inputFilePath) {
  console.error(`Input file path missing. Usage:
  $ pnpm tsx scripts/saveRankData.ts data/persisted-to-cache/database.csv
`);
  process.exit(1);
}

console.log(`Starting saveRankData script with input CSV: ${inputFilePath}`);

const db = new Database(dbFilePath);
db.pragma('journal_mode = WAL');

console.log(`Database opened at ${dbFilePath}`);

// Create tables if not exists
try {
    console.log("Creating tables if not exists...");
    db.prepare(
        `
        CREATE TABLE IF NOT EXISTS domains (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT UNIQUE NOT NULL
        )
        `
    ).run();
        console.log("Domains table created/verified.");

    db.prepare(
        `
            CREATE TABLE IF NOT EXISTS ranks (
                id INTEGER NOT NULL,
                rank INTEGER NOT NULL,
                date TEXT NOT NULL,
                FOREIGN KEY (id) REFERENCES domains(id)
            )
        `
    ).run();
    console.log("Ranks table created/verified.");

} catch (error) {
    console.error(`Error creating tables:`, error);
    process.exit(1)
}


// Load domain IDs from db
const domainIdMap = new Map<string, number>();
let maxId = 0;

try {
    console.log("Loading existing domain IDs from database...")
    const domainIdRecords = db
        .prepare("SELECT id, domain FROM domains")
        .all() as { id: number, domain: string }[];
    for(const {id, domain} of domainIdRecords) {
        domainIdMap.set(domain, id)
    }
    maxId = Math.max(0, ...domainIdRecords.map(record => record.id))
    console.log(`Loaded ${domainIdRecords.length} domain IDs.`)
} catch (error) {
    console.error(`Error loading domain IDs from database`, error);
    process.exit(1)
}


// Insert new domain, rank pairs into database
const insertDomain = db.prepare(
  `
    INSERT OR IGNORE INTO domains (domain)
    VALUES (@domain)
  `
);

const insertRank = db.prepare(
    `
        INSERT INTO ranks (id, rank, date)
        VALUES (@id, @rank, @date)
    `
);

// Parse new domain, rank pairs from input
let newDomainRankPairs: { rank: string; domain: string }[] = [];

try {
    console.log(`Reading input CSV from ${inputFilePath}...`)
    newDomainRankPairs = parse(readFileSync(inputFilePath, 'utf-8'), {
        columns: false,
    }).map((row: string[]) => ({ rank: row[0], domain: row[1] }));
    console.log(`Read ${newDomainRankPairs.length} new domain/rank pairs from CSV.`);
} catch (err) {
    console.error(
        `Error reading or parsing input CSV: ${inputFilePath}\n`,
        err
    );
    process.exit(1);
}

let newDomainsCount = 0;

try {
    console.log(`Starting database transaction...`);
    db.transaction(() => {
        for (const { rank, domain } of newDomainRankPairs) {
            let id = domainIdMap.get(domain);
             if(id === undefined) {
                maxId++;
                id = maxId;
                insertDomain.run({domain});
                domainIdMap.set(domain, id);
                 newDomainsCount++;
                // console.log(`Inserted new domain "${domain}" with id ${id}.`);
            }

            const parsedRank = parseInt(rank)
            if(isNaN(parsedRank)) {
              console.warn(`Skipping rank insert as not a valid integer: ${rank}`)
              continue;
            }
             insertRank.run({id, rank: parsedRank, date: new Date().toISOString().split('T')[0]});
             // console.log(`Inserted rank: ${parsedRank} for domain "${domain}".`);
        }
    })();
     console.log(`Database transaction completed successfully.`);
} catch (error) {
    console.error("Error processing database transaction", error);
    process.exit(1)
}


const newDomains = newDomainRankPairs.filter(({domain}) => !domainIdMap.has(domain))

console.log(`Updated ${dbFilePath}:
- ${newDomainsCount} new domains added
- ${newDomainRankPairs.length} new rank records added
`);

console.log("First 100 domain IDs");
console.log(tableFromRecords([...domainIdMap.entries()].slice(0,100).map(([domain, id]) => ({domain, id}))));
console.log("Finished saveRankData script.");
