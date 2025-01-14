import Database from 'better-sqlite3';
import process from 'node:process';
import { tableFromRecords } from '../util/table.js';

const dbFilePath = 'data/database.db';
const outputFilePath = 'data/top_changing_domains.csv';
const topCount = 10000; // Top 10,000 domains


// Helper function to get date one week ago in YYYY-MM-DD format
function getDateOneWeekAgo(): string {
    const date = new Date();
    date.setDate(date.getDate() - 7);
    return date.toISOString().split('T')[0];
}

const db = new Database(dbFilePath);
const oneWeekAgo = getDateOneWeekAgo();


const rankChanges = db
    .prepare(
        `
            SELECT
                ranks.id,
                ABS(AVG(ranks.rank) - (SELECT rank FROM ranks r2 WHERE r2.id = ranks.id AND r2.date < ranks.date ORDER BY r2.date DESC LIMIT 1) ) AS rank_change
                FROM ranks
                WHERE ranks.date >=  ?
                GROUP BY ranks.id
        `
    )
    .all(oneWeekAgo) as { id: number, rank_change: number}[];


const domainIds = db
    .prepare(`SELECT id, domain from domains`)
    .all() as { id: number, domain: string}[];

const domainIdMap = new Map<number, string>()

for (const { id, domain } of domainIds) {
    domainIdMap.set(id, domain)
}


const sortedRankChanges = rankChanges.sort((a, b) => b.rank_change - a.rank_change).slice(0, topCount)

const topChangingDomains = sortedRankChanges.map(({id, rank_change}) => {
    const domain = domainIdMap.get(id) || "unknown"
  return {
    id,
      domain,
    change: rank_change,
  };
});


import { writeFileSync } from 'node:fs';
import { stringify } from 'csv/sync';
// Write results to CSV
try {
    writeFileSync(
        outputFilePath,
        stringify(topChangingDomains, { header: true })
    );
} catch (err) {
    console.error(`Error writing to output CSV: ${outputFilePath}`, err);
    process.exit(1);
}


console.log(
  `Top ${topCount} domains with most rank change saved to: ${outputFilePath}`
);

console.log(tableFromRecords(topChangingDomains));
