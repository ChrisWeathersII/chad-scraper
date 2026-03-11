import axios from 'axios';
import * as cheerio from 'cheerio';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
};

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const parseNumber = (text) => {
    if (!text) return null;
    const cleaned = text.replace(/[$,\s]/g, '');
    const num = parseFloat(cleaned);
    if (isNaN(num)) return null;
    if (text.toLowerCase().includes('million') || text.toLowerCase().includes(' m')) {
        return num * 1000000;
    }
    if (text.toLowerCase().includes('k')) {
        return num * 1000;
    }
    return num;
};

const fetchPage = async (url, retries = 3) => {
    for (let i = 0; i < retries; i++) {
        try {
            await sleep(1000 + Math.random() * 2000);
            const response = await axios.get(url, {
                headers: HEADERS,
                timeout: 30000,
                maxRedirects: 5,
            });
            return response.data;
        } catch (err) {
            console.log(`Attempt ${i + 1} failed for ${url}: ${err.message}`);
            if (i < retries - 1) await sleep(3000);
        }
    }
    return null;
};

const extractListing = ($, url) => {
    const company_name =
        $('h1.bfsTitle, h1[class*="title"], h1').first().text().trim() ||
        $('meta[property="og:title"]').attr('content') || '';

    const description =
        $('p.bfsDescription, div[class*="description"] p, div.description')
        .first().text().trim() ||
        $('meta[name="description"]').attr('content') || '';

    const locationText =
        $('span[class*="location"], div[class*="location"], h2[class*="location"]')
        .first().text().trim() || '';
    const locationParts = locationText.split(',').map(s => s.trim());
    const city = locationParts[0] || '';
    const state = locationParts[1] || '';

    const getTableValue = (labels) => {
        for (const label of labels) {
            let value = '';
            $('td').each((i, el) => {
                if ($(el).text().trim().toLowerCase().includes(label.toLowerCase())) {
                    value = $(el).next('td').text().trim();
                }
            });
            if (value) return value;
        }
        return '';
    };

    const askingPriceText = getTableValue(['Asking Price']) ||
        $('span[class*="asking"], div[class*="asking-price"]').first().text().trim() || '';

    const revenueText = getTableValue(['Gross Revenue', 'Revenue']);
    const cashFlowText = getTableValue(['Cash Flow', 'EBITDA', 'Seller Discretionary', 'SDE', 'Discretionary Earnings', 'Net Income']);
    const employeesText = getTableValue(['Employees']);

    const industry =
        $('span[class*="industry"], a[class*="category"], nav[class*="breadcrumb"] a')
        .last().text().trim() || '';

    const broker_name =
        $('span[class*="broker-name"], div[class*="broker"] h3, div[class*="agent"] strong')
        .first().text().trim() || '';

    const broker_firm =
        $('span[class*="broker-firm"], div[class*="brokerage"] span')
        .first().text().trim() || '';

    const broker_email =
        $('a[href^="mailto:"]').first().attr('href')?.replace('mailto:', '') || '';

    const broker_phone =
        $('span[class*="phone"], a[href^="tel:"]').first().text().trim() || '';

    const sourceId = url.split('/').filter(Boolean).pop()?.split('?')[0] || '';

    return {
        source: 'bizbuysell',
        source_url: url,
        source_id: sourceId,
        deal_type: 'brokered',
        company_name: company_name.substring(0, 255),
        description: description.substring(0, 500),
        industry: industry.substring(0, 100),
        city: city.substring(0, 100),
        state: state.substring(0, 50),
        asking_price: parseNumber(askingPriceText),
        revenue: parseNumber(revenueText),
        cash_flow: parseNumber(cashFlowText),
        employees: parseInt(employeesText) || null,
        broker_name: broker_name.substring(0, 255),
        broker_firm: broker_firm.substring(0, 255),
        broker_email: broker_email.substring(0, 255),
        broker_phone: broker_phone.substring(0, 50),
        listing_date: new Date().toISOString().split('T')[0],
        status: 'active',
        scraped_at: new Date().toISOString(),
    };
};

const getListingUrls = async (categoryUrl) => {
    const urls = [];
    let page = 1;
    let hasMore = true;

    while (hasMore && page <= 10) {
        const pageUrl = page === 1 ? categoryUrl : `${categoryUrl}${page}/`;
        console.log(`Fetching category page: ${pageUrl}`);
        const html = await fetchPage(pageUrl);
        if (!html) break;

        const $ = cheerio.load(html);
        let found = 0;

        $('a[href]').each((i, el) => {
            const href = $(el).attr('href');
            if (href && href.match(/\/\d+\/?$/)) {
                const fullUrl = href.startsWith('http')
                    ? href
                    : `https://www.bizbuysell.com${href}`;
                if (!urls.includes(fullUrl)) {
                    urls.push(fullUrl);
                    found++;
                }
            }
        });

        console.log(`Found ${found} listing URLs on page ${page}`);
        hasMore = found > 0;
        page++;
    }

    return urls;
};

const START_URLS = [
    'https://www.bizbuysell.com/building-and-construction-businesses-for-sale/',
    'https://www.bizbuysell.com/hvac-businesses-for-sale/',
    'https://www.bizbuysell.com/electrical-and-mechanical-contracting-businesses-for-sale/',
    'https://www.bizbuysell.com/plumbing-businesses-for-sale/',
    'https://www.bizbuysell.com/roofing-business-for-sale/',
    'https://www.bizbuysell.com/heavy-construction-businesses-for-sale/',
    'https://www.bizbuysell.com/manufacturing-businesses-for-sale/',
    'https://www.bizbuysell.com/machine-shops-and-tool-manufacturers-for-sale/',
    'https://www.bizbuysell.com/metal-product-manufacturers-for-sale/',
    'https://www.bizbuysell.com/service-businesses-for-sale/',
    'https://www.bizbuysell.com/cleaning-businesses-for-sale/',
    'https://www.bizbuysell.com/landscaping-and-yard-service-businesses-for-sale/',
    'https://www.bizbuysell.com/pest-control-businesses-for-sale/',
    'https://www.bizbuysell.com/trucking-companies-for-sale/',
    'https://www.bizbuysell.com/storage-facilities-and-warehouses-for-sale/',
    'https://www.bizbuysell.com/wholesale-and-distribution-businesses-for-sale/',
    'https://www.bizbuysell.com/health-care-and-fitness-businesses-for-sale/',
    'https://www.bizbuysell.com/home-health-care-businesses-for-sale/',
    'https://www.bizbuysell.com/insurance-agencies-for-sale/',
    'https://www.bizbuysell.com/auto-repair-and-service-shops-for-sale/',
];

console.log('Collecting listing URLs...');
const allListingUrls = [];
for (const categoryUrl of START_URLS) {
    const urls = await getListingUrls(categoryUrl);
    allListingUrls.push(...urls);
    console.log(`Total URLs so far: ${allListingUrls.length}`);
}

const uniqueUrls = [...new Set(allListingUrls)];
console.log(`Total unique listing URLs: ${uniqueUrls.length}`);

let saved = 0;
let skipped = 0;

for (const url of uniqueUrls) {
    console.log(`Processing: ${url}`);
    const html = await fetchPage(url);
    if (!html) {
        console.log(`Failed to fetch: ${url}`);
        continue;
    }

    const $ = cheerio.load(html);
    const deal = extractListing($, url);

    if (!deal.company_name || deal.company_name.length <= 3) {
        skipped++;
        continue;
    }

    if (deal.asking_price !== null && deal.asking_price < 500000) {
        skipped++;
        continue;
    }

    const { error } = await supabase
        .from('deals')
        .upsert(deal, { onConflict: 'source_id' });

    if (error) {
        console.log(`Supabase error for ${deal.company_name}: ${error.message}`);
    } else {
        saved++;
        console.log(`Saved (${saved}): ${deal.company_name} | ${deal.city}, ${deal.state} | $${deal.asking_price}`);
    }
}

console.log(`Done. Saved: ${saved}, Skipped: ${skipped}`);
