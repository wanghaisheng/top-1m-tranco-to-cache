import axios from 'axios';
import { parseStringPromise } from 'xml2js';
import * as fs from 'fs';
import * as zlib from 'zlib';
import * as dotenv from 'dotenv';
import { promisify } from 'util';

dotenv.config();

const D1_DATABASE_ID = process.env.D1_APP_DATABASE_ID;
const CLOUDFLARE_ACCOUNT_ID = process.env.CLOUDFLARE_ACCOUNT_ID;
const CLOUDFLARE_API_TOKEN = process.env.CLOUDFLARE_API_TOKEN;

const CLOUDFLARE_BASE_URL = `https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/d1/database/${D1_DATABASE_ID}`;

// Set up gunzip for decompressing
const gunzip = promisify(zlib.gunzip);

interface AppData {
    url: string;
    lastmodify: string;
}

async function fetchAndParseSitemap(url: string): Promise<string[]> {
    try {
        console.log(`Fetching sitemap from URL: ${url}`);
        const response = await axios.get(url);
        const sitemapXml = response.data;

        const result = await parseStringPromise(sitemapXml);
        const locTags: string[] = result.urlset.url.map((entry: any) => entry.loc[0]);

        console.log(`Extracted ${locTags.length} <loc> links from sitemap.`);
        return locTags;
    } catch (error) {
        console.error(`Failed to fetch or parse sitemap: ${error} - URL: ${url}`);
        return [];
    }
}

async function fetchAndParseGzip(url: string): Promise<AppData[]> {
    try {
        console.log(`Fetching GZipped sitemap from URL: ${url}`);
        const response = await axios.get(url, { responseType: 'arraybuffer' });
        const decompressed = await gunzip(Buffer.from(response.data));
        const fileContent = decompressed.toString('utf-8');

        const result = await parseStringPromise(fileContent);
        const locTags: string[] = result.urlset.url.map((entry: any) => entry.loc[0]);
        const lastmodTags: string[] = result.urlset.url.map((entry: any) => entry.lastmod[0]);

        const appDataList = locTags.map((loc, index) => ({
            url: loc,
            lastmodify: lastmodTags[index]
        }));

        console.log(`Extracted ${appDataList.length} app data entries from GZipped sitemap.`);
        return appDataList;
    } catch (error) {
        console.error(`Failed to fetch or parse GZipped sitemap: ${error} - URL: ${url}`);
        return [];
    }
}

function saveToCsv(appDataList: AppData[], filename: string): void {
    const csvContent = appDataList.map(appData => `${appData.url},${appData.lastmodify}`).join('\n');

    // Ensure the directory exists
    const directory = 'data/persisted-to-cache';
    if (!fs.existsSync(directory)) {
        fs.mkdirSync(directory, { recursive: true });
    }

    // Save the CSV file to the specified path
    fs.writeFileSync(`${directory}/${filename}`, csvContent);
    console.log(`Data saved to ${directory}/${filename}`);
}

async function processSitemapsAndSaveProfiles(): Promise<void> {
    const sitemapUrl = "https://apps.apple.com/sitemaps_apps_index_app_1.xml";

    const locUrls = await fetchAndParseSitemap(sitemapUrl);

    for (const locUrl of locUrls) {
        console.log(`Processing sitemap: ${locUrl}`);
        const appDataList = await fetchAndParseGzip(locUrl);

        // Save data to the specified path (data/persisted-to-cache/database.csv)
        saveToCsv(appDataList, 'database.csv');
    }
}

processSitemapsAndSaveProfiles();
