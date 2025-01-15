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

// Fetch and parse sitemap, return array of URLs found in <loc> tags
async function fetchAndParseSitemap(url: string): Promise<string[]> {
    try {
        console.log(`Fetching sitemap from URL: ${url}`);
        const response = await axios.get(url);
        const sitemapXml = response.data;

        const result = await parseStringPromise(sitemapXml);
        // Check if the expected <urlset> tag exists
        if (!result.urlset || !result.urlset.url) {
            console.error('Unexpected sitemap structure or missing <urlset> or <url> elements.');
            return [];
        }

        const locTags: string[] = result.urlset.url.map((entry: any) => entry.loc[0]);
        console.log(`Extracted ${locTags.length} <loc> links from sitemap.`);
        return locTags;
    } catch (error) {
        console.error(`Failed to fetch or parse sitemap: ${error} - URL: ${url}`);
        return [];
    }
}

// Fetch and parse GZipped sitemap, return an array of AppData objects
async function fetchAndParseGzip(url: string): Promise<AppData[]> {
    try {
        console.log(`Fetching GZipped sitemap from URL: ${url}`);
        const response = await axios.get(url, { responseType: 'arraybuffer' });
        const decompressed = await gunzip(Buffer.from(response.data));
        const fileContent = decompressed.toString('utf-8');

        const result = await parseStringPromise(fileContent);
        // Ensure the expected structure exists
        if (!result.urlset || !result.urlset.url) {
            console.error('Unexpected sitemap structure or missing <urlset> or <url> elements.');
            return [];
        }

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

// Save data to CSV
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

// Method to handle sub-sitemaps from an index
async function fetchSubSitemapFromIndex(url: string): Promise<string[]> {
    try {
        // Check if the URL contains the "index" keyword
        if (url.includes("index")) {
            console.log(`Fetching sub-sitemap from URL containing "index": ${url}`);
            
            const response = await axios.get(url);
            const sitemapXml = response.data;

            const result = await parseStringPromise(sitemapXml);
            // Ensure the expected <sitemapindex> tag exists
            if (!result.sitemapindex || !result.sitemapindex.sitemap) {
                console.error("The expected <sitemapindex> or <sitemap> elements are missing.");
                return [];
            }

            // Extract all <loc> elements pointing to sub-sitemaps
            const subSitemapUrls: string[] = result.sitemapindex.sitemap.map((entry: any) => entry.loc[0]);
            console.log(`Extracted ${subSitemapUrls.length} sub-sitemap URLs from sitemap index.`);
            return subSitemapUrls;
        } else {
            console.log(`No "index" keyword in the URL: ${url}`);
            return [];
        }
    } catch (error) {
        console.error(`Failed to fetch or parse sitemap index containing "index": ${error} - URL: ${url}`);
        return [];
    }
}

// Main function to process sitemaps and save data
async function processSitemapsAndSaveProfiles(): Promise<void> {
    const sitemapUrl = "https://apps.apple.com/sitemaps_apps_index_app_1.xml";  // Starting point

    const locUrls = await fetchSubSitemapFromIndex(sitemapUrl);

    for (const locUrl of locUrls) {
        console.log(`Processing sitemap: ${locUrl}`);
        
        // Check if the sitemap URL contains "index" and handle it
        const subSitemapUrls = await fetchSubSitemapFromIndex(locUrl);

        if (subSitemapUrls.length > 0) {
            // If sub-sitemaps were found, process them
            for (const subSitemapUrl of subSitemapUrls) {
                console.log(`Processing sub-sitemap: ${subSitemapUrl}`);
                const appDataList = await fetchAndParseGzip(subSitemapUrl);
                saveToCsv(appDataList, 'database.csv');
            }
        } else {
            // If no sub-sitemaps were found, directly process the main sitemap
            const appDataList = await fetchAndParseGzip(locUrl);
            saveToCsv(appDataList, 'database.csv');
        }
    }
}

// Run the process
processSitemapsAndSaveProfiles();
