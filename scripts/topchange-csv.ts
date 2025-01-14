import { createReadStream, writeFileSync, existsSync } from 'node:fs';
import process from 'node:process';
import { parse, stringify } from 'csv/sync';
import { createInterface } from 'node:readline';
import { tableFromRecords } from '../util/table.js';

const rankDataFilePath = 'data/rank_data.csv';
const domainIdFilePath = 'data/domain_id.csv';
const outputFilePath = 'data/top_changing_domains.csv';
const topCount = 10000; // Top 10,000 domains

// Helper function to get date one week ago in YYYY-MM-DD format
function getDateOneWeekAgo(): string {
  const date = new Date();
  date.setDate(date.getDate() - 7);
  return date.toISOString().split('T')[0];
}

// Helper function to parse a date string and return a comparable number
function parseDate(dateString: string): number {
  const parts = dateString.split('-');
  if (parts.length !== 3) {
    return 0; // Invalid date, assume earliest date
  }
  return new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2])).getTime()
}


// Check if rank data file exists
if (!existsSync(rankDataFilePath)) {
    console.error(`Rank data file does not exist: ${rankDataFilePath}`);
    process.exit(1);
}

// Load domain ID data
let domainIdMap = new Map<string, string>(); // Use Map for O(1) lookup
if (existsSync(domainIdFilePath)) {
  try {
    const records: { domain: string; id: string }[] = parse(
      readFileSync(domainIdFilePath, 'utf-8'),
      { columns: true }
    );
    for (const record of records) {
      domainIdMap.set(record.id, record.domain);
    }
  } catch (err) {
    console.error(`Error reading domain ID CSV: ${domainIdFilePath}\n`, err);
    process.exit(1);
  }
} else {
    console.error(`Domain ID file does not exist: ${domainIdFilePath}`);
    process.exit(1);
}

// Use line reader to read rank data in chunks
const rankChanges = new Map<string, number>();
const lastWeek = getDateOneWeekAgo();
const fileStream = createReadStream(rankDataFilePath, {encoding: 'utf-8'});
const rl = createInterface({
    input: fileStream,
    crlfDelay: Infinity, // To handle different line ending cases
});
let lineNumber = 0;

async function processRankData(){
    for await (const line of rl) {
      lineNumber++;
        if(lineNumber === 1) continue; // Skip header row

        const [id, rank, date] = line.split(',');

        if(!id || !rank || !date) {
            console.warn(`Invalid row, skipping: ${line}`)
            continue;
        }
        const recordDate = parseDate(date);

        if (recordDate < parseDate(lastWeek)) {
          continue; // Skip records not from last week
        }
        // Calculate rank changes
        if (!rankChanges.has(id)) {
            rankChanges.set(id, 0);
        }

         // Calculate rank changes by considering ranks as numbers and only if ranks are valid
         const currentRank = Number(rank);

         const priorRanks: { id: string, rank: string, date: string }[] = [];
         // Use file stream to parse prior records instead of parsing and storing all of the records.
         const priorFileStream = createReadStream(rankDataFilePath, { encoding: 'utf-8' })
         const priorRl = createInterface({
           input: priorFileStream,
           crlfDelay: Infinity, // To handle different line ending cases
         });

         let priorLineNumber = 0;

         for await (const priorLine of priorRl) {
           priorLineNumber++;
           if (priorLineNumber === 1) continue;

            const [priorId, priorRank, priorDate] = priorLine.split(',');

            if (priorId === id && parseDate(priorDate) < recordDate) {
                priorRanks.push({ id: priorId, rank: priorRank, date: priorDate});
            }
         }

         const sortedPriorRanks = priorRanks.sort((a,b) => parseDate(b.date) - parseDate(a.date));
         const priorRank = sortedPriorRanks[0] ? Number(sortedPriorRanks[0].rank) : currentRank;

          if(!isNaN(currentRank) && !isNaN(priorRank)){
              rankChanges.set(id, rankChanges.get(id)! + Math.abs(currentRank - priorRank));
          }
     }

    // Sort by rank change and take the top
    const sortedRankChanges = Array.from(rankChanges.entries()).sort(
      ([, changeA], [, changeB]) => changeB - changeA
    );
    const topChangingIds = sortedRankChanges.slice(0, topCount);

    // Prepare the output data (domain, id, rank change)
    const topChangingDomains = topChangingIds.map(([id, change]) => {
       const domain = domainIdMap.get(id) || "unknown";
      return {
        id,
        domain,
        change,
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
      `Top ${topCount} domains with most rank change saved to: ${outputFilePath}`
    );

  console.log(tableFromRecords(topChangingDomains))
}

processRankData();
