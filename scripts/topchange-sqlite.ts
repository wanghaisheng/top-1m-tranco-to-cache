import Database from 'better-sqlite3';
import process from 'node:process';
import { tableFromRecords } from '../util/table.js';
import { writeFileSync } from 'node:fs';
import { stringify } from 'csv/sync';

const dbFilePath = 'data/database.db';
const outputFilePath = 'data/top_changing_domains.csv';

const analysisType = process.argv[2];

if (!analysisType || (analysisType !== 'daily' && analysisType !== 'weekly')) {
  console.error(`Usage:
    pnpm tsx scripts/calculateRankChanges.ts daily
    pnpm tsx scripts/calculateRankChanges.ts weekly
`);
  process.exit(1);
}


// Helper function to get date in YYYY-MM-DD format
function getDate(daysAgo: number): string {
    const date = new Date();
    date.setDate(date.getDate() - daysAgo);
    return date.toISOString().split('T')[0];
}
const db = new Database(dbFilePath);

let analysisDays = 1;
let topCount = 1000;
let comparisonDate = getDate(analysisDays)

if(analysisType === 'weekly') {
  analysisDays = 7;
    topCount = 10000;
    comparisonDate = getDate(analysisDays);
}


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
    .all(comparisonDate) as { id: number, rank_change: number}[];


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
  `Top ${topCount} domains with most rank change (${analysisType}) saved to: ${outputFilePath}`
);

console.log(tableFromRecords(topChangingDomains));
