import { readFileSync, existsSync } from 'node:fs';
import process from 'node:process';
import Database from 'better-sqlite3';
import { parse } from 'csv/sync';
import path from 'node:path';

const inputFilePaths = process.argv.slice(2);
const dbFilePath = 'data/database.db';

// Flag to clear the database before insert (use an environment variable or command-line argument)
const clearDatabase = process.env.CLEAR_DB === 'true';

function extractDateFromFilename(filename) {
    const baseName = path.basename(filename, '.csv');
    // Assuming the format is "YYYY-MM-DD"
    const dateMatch = baseName.match(/\d{4}-\d{2}-\d{2}/);
    return dateMatch ? dateMatch[0] : null;
}

function tableFromRecords(records) {
    const headers = Object.keys(records[0] || {});
    const rows = records.map(record => Object.values(record));
    const table = [headers, ...rows].map(row => row.join('\t')).join('\n');
    return table;
}

if (inputFilePaths.length === 0) {
    console.error(`Input file paths missing. Usage:
    $ pnpm tsx scripts/saveRankData.ts data/persisted-to-cache/*.csv
    `);
    process.exit(1);
}

const db = new Database(dbFilePath);
db.pragma('journal_mode = WAL');

console.log(`Database opened at ${dbFilePath}`);

if (clearDatabase) {
    console.log("Clearing all data from database...");

    // Clear the domains and ranks tables before inserting new data
    db.prepare("DELETE FROM ranks").run();
    db.prepare("DELETE FROM domains").run();

    console.log("Database cleared.");
}

// Create tables if not exists
try {
    console.log("Creating tables if not exists...");
    db.prepare(
        `CREATE TABLE IF NOT EXISTS domains (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT UNIQUE NOT NULL
        )`
    ).run();
    console.log("Domains table created/verified.");

    db.prepare(
        `CREATE TABLE IF NOT EXISTS ranks (
            id INTEGER NOT NULL,
            rank INTEGER NOT NULL,
            date TEXT NOT NULL,
            FOREIGN KEY (id) REFERENCES domains(id)
        )`
    ).run();
    console.log("Ranks table created/verified.");

} catch (error) {
    console.error("Error creating tables:", error);
    process.exit(1);
}

// Load domain IDs from db
const domainIdMap = new Map<string, number>();
let maxId = 0;

try {
    console.log("Loading existing domain IDs from database...");
    const domainIdRecords = db
        .prepare("SELECT id, domain FROM domains")
        .all() as { id: number, domain: string }[];
    for (const { id, domain } of domainIdRecords) {
        domainIdMap.set(domain, id);
    }
    maxId = Math.max(0, ...domainIdRecords.map(record => record.id));
    console.log(`Loaded ${domainIdRecords.length} domain IDs.`);
} catch (error) {
    console.error("Error loading domain IDs from database", error);
    process.exit(1);
}

// Insert new domain, rank pairs into database
const insertDomain = db.prepare(
    `INSERT OR IGNORE INTO domains (domain) VALUES (@domain)`
);

const insertRank = db.prepare(
    `INSERT INTO ranks (id, rank, date) VALUES (@id, @rank, @date)`
);

// Parse new domain, rank pairs from input
let newDomainRankPairs: { rank: string; domain: string }[] = [];

try {
    console.log(`Reading input CSV from ${inputFilePaths}...`);
    newDomainRankPairs = inputFilePaths.flatMap(inputFilePath =>
        parse(readFileSync(inputFilePath, 'utf-8'), { columns: false }).map((row: string[]) => ({ rank: row[0], domain: row[1] }))
    );
    console.log(`Read ${newDomainRankPairs.length} new domain/rank pairs from CSV.`);
} catch (err) {
    console.error(`Error reading or parsing input CSV: ${inputFilePaths}\n`, err);
    process.exit(1);
}

let newDomainsCount = 0;

try {
    console.log(`Starting database transaction...`);
    db.transaction(() => {
        for (const { rank, domain } of newDomainRankPairs) {
            let id = domainIdMap.get(domain);
            if (id === undefined) {
                maxId++;
                id = maxId;
                insertDomain.run({ domain });
                domainIdMap.set(domain, id);
                newDomainsCount++;
            }

            const parsedRank = parseInt(rank);
            if (isNaN(parsedRank)) {
                console.warn(`Skipping rank insert as not a valid integer: ${rank}`);
                continue;
            }
            insertRank.run({ id, rank: parsedRank, date: extractDateFromFilename(inputFilePaths[0]) });
        }
    })();
    console.log("Database transaction completed successfully.");
} catch (error) {
    console.error("Error processing database transaction", error);
    process.exit(1);
}

console.log(`Updated ${dbFilePath}:
- ${newDomainsCount} new domains added
- ${newDomainRankPairs.length} new rank records added
`);

console.log("First 100 domain IDs");
console.log(tableFromRecords([...domainIdMap.entries()].slice(0, 100).map(([domain, id]) => ({ domain, id }))));
console.log("Finished saveRankData script.");
