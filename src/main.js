import { Actor } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { createClient } from '@supabase/supabase-js';

await Actor.init();

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

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

    const askingPriceText =
        $('span[class*="asking"], div[class*="asking-price"], td:contains("Asking Price") + td')
        .first().text().trim() || '';

    const revenueText =
        $('td:contains("Gross Revenue") + td, td:contains("Revenue") + td, span[class*="revenue"]')
        .first().text().trim() || '';

    const cashFlowText =
        $('td:contains("Cash Flow") + td, td:contains("EBITDA") + td, span[class*="cash"]')
        .first().text().trim() || '';

    const employeesText =
        $('td:contains("Employees") + td, span[class*="employee"]')
        .first().text().trim() || '';

    const industry =
        $('span[class*="industry"], a[class*="category"], nav[class*="breadcrumb"] a')
        .last().text().trim() || '';

    const broker_name =
        $('span[class*="broker-name"], div[class*="broker"] h3, div[class*="agent"] strong')
        .first().text().trim() || '';

    const broker_firm =
        $('span[class*="broker-firm"], div[class*="brokerage"] span, div[class*="company-name"]')
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

const crawler = new CheerioCrawler({
    maxRequestsPerCrawl: 500,
    maxConcurrency: 2,
    requestHandlerTimeoutSecs: 30,

    async requestHandler({ $, request, enqueueLinks, log }) {
        const url = request.url;
        log.info(`Processing: ${url}`);

        // Individual listing page - URL contains a numeric ID at the end
        if (url.match(/\/\d+\/?(\?.*)?$/)) {
            const deal = extractListing($, url);
            if (deal.company_name && deal.company_name.length > 3) {
                await Dataset.pushData(deal);

                const { error } = await supabase
                    .from('deals')
                    .upsert(deal, { onConflict: 'source_id' });

                if (error) {
                    log.error(`Supabase error for ${deal.company_name}: ${error.message}`);
                } else {
                    log.info(`Saved to Supabase: ${deal.company_name} in ${deal.city}, ${deal.state}`);
                }
            }
        } else {
            // Category or index page - find individual listing links
            const listingLinks = [];

            $('a[href]').each((i, el) => {
                const href = $(el).attr('href');
                if (href && href.match(/bizbuysell\.com\/.*\/\d+\/?$/) ||
                    href && href.match(/^\/.*\/\d+\/?$/)) {
                    const fullUrl = href.startsWith('http')
                        ? href
                        : `https://www.bizbuysell.com${href}`;
                    if (!listingLinks.includes(fullUrl)) {
                        listingLinks.push(fullUrl);
                    }
                }
            });

            if (listingLinks.length > 0) {
                await enqueueLinks({ urls: listingLinks });
                log.info(`Found ${listingLinks.length} individual listings on ${url}`);
            } else {
                log.info(`No individual listings found on ${url}`);
            }

            // Enqueue next page
            const nextPage = $('a[aria-label="Next"], a.next, a[rel="next"], a[aria-label="Next Page"]').attr('href');
            if (nextPage) {
                const nextUrl = nextPage.startsWith('http')
                    ? nextPage
                    : `https://www.bizbuysell.com${nextPage}`;
                await enqueueLinks({ urls: [nextUrl] });
                log.info(`Queued next page: ${nextUrl}`);
            }
        }
    },

    failedRequestHandler({ request, log }) {
        log.error(`Failed to scrape: ${request.url}`);
    },
});

await crawler.run([
    'https://www.bizbuysell.com/businesses-for-sale/',
    'https://www.bizbuysell.com/hvac-businesses-for-sale/',
    'https://www.bizbuysell.com/manufacturing-businesses-for-sale/',
    'https://www.bizbuysell.com/healthcare-businesses-for-sale/',
    'https://www.bizbuysell.com/service-businesses-for-sale/',
    'https://www.bizbuysell.com/construction-businesses-for-sale/',
    'https://www.bizbuysell.com/transportation-businesses-for-sale/',
    'https://www.bizbuysell.com/food-businesses-for-sale/',
]);

await Actor.exit();
