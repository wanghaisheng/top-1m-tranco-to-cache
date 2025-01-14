import { readFileSync, existsSync } from 'node:fs';
import process from 'node:process';
import Database from 'better-sqlite3';
import { parse } from 'csv/sync';

const inputFilePath = process.argv[2];
const dbFilePath = 'data/database.db';


if (!inputFilePath) {
  console.error(`Input file path missing. Usage:
  $ pnpm tsx scripts/updateCsvDatabaseRecords.ts data/persisted-to-cache/database.csv
`);
  process.exit(1);
}

const db = new Database(dbFilePath);
db.pragma('journal_mode = WAL');

// Create tables if not exists
db.prepare(
  `
    CREATE TABLE IF NOT EXISTS domains (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        domain TEXT UNIQUE NOT NULL
    )
  `
).run();

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

// Load domain IDs from db
const domainIdMap = new Map<string, number>();
const domainIdRecords = db
    .prepare("SELECT id, domain FROM domains")
    .all() as { id: number, domain: string }[];
for(const {id, domain} of domainIdRecords) {
    domainIdMap.set(domain, id)
}
let maxId = Math.max(0, ...domainIdRecords.map(record => record.id))


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
    newDomainRankPairs = parse(readFileSync(inputFilePath, 'utf-8'), {
        columns: false,
    }).map((row: string[]) => ({ rank: row[0], domain: row[1] }));
} catch (err) {
    console.error(
        `Error reading or parsing input CSV: ${inputFilePath}\n`,
        err
    );
    process.exit(1);
}


const newDomainCounter = 0;

for (const { rank, domain } of newDomainRankPairs) {
    let id = domainIdMap.get(domain);
     if(id === undefined) {
        maxId++;
        id = maxId;
        insertDomain.run({domain});
        domainIdMap.set(domain, id);
    }
    insertRank.run({id, rank: parseInt(rank), date: new Date().toISOString().split('T')[0]});
}

const newDomains = newDomainRankPairs.filter(({domain}) => !domainIdMap.has(domain))

console.log(`Updated ${dbFilePath}:
- ${newDomains.length} new domains added
- ${newDomainRankPairs.length} new rank records added
`);

console.log("First 100 domain IDs");
console.log(tableFromRecords([...domainIdMap.entries()].slice(0,100).map(([domain, id]) => ({domain, id}))));
