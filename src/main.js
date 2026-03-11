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
        $('td:contains("Cash Flow") + td, td:contains("EBITDA") + td, td:contains("Seller Discretionary") + td, td:contains("SDE") + td, td:contains("Discretionary Earnings") + td, td:contains("Net Income") + td, span[class*="cash"], span[class*="sde"]')
        .first().text().trim() || '';

    const employeesText =
        $('td:contains("Employees") + td, span[class*="employee"]')
        .first().text().trim() || '';

    const industry =
        $('span[class*="industry"], a[class*="category"], nav[class*="breadcrumb"] a, td:contains("Industry") + td, td:contains("Category") + td, span[class*="type"]')
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
    maxRequestsPerCrawl: 1000,
    maxConcurrency: 3,
    requestHandlerTimeoutSecs: 30,

    async requestHandler({ $, request, enqueueLinks, log }) {
        const url = request.url;
        log.info(`Processing: ${url}`);

        if (url.match(/\/\d+\/?(\?.*)?$/)) {
            const deal = extractListing($, url);

            if (!deal.company_name || deal.company_name.length <= 3) {
                log.info(`Skipping listing with no company name: ${url}`);
                return;
            }

            if (deal.asking_price !== null && deal.asking_price < 500000) {
                log.info(`Skipping deal below $500k: ${deal.company_name} at $${deal.asking_price}`);
                return;
            }

            await Dataset.pushData(deal);

            const { error } = await supabase
                .from('deals')
                .upsert(deal, { onConflict: 'source_id' });

            if (error) {
                log.error(`Supabase error for ${deal.company_name}: ${error.message}`);
            } else {
                log.info(`Saved: ${deal.company_name} | ${deal.city}, ${deal.state} | $${deal.asking_price}`);
            }

        } else {
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
                log.info(`Found ${listingLinks.length} listings on ${url}`);
            }

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
        log.error(`Failed: ${request.url}`);
    },
});

await crawler.run([
    // Building and Construction
    'https://www.bizbuysell.com/building-and-construction-businesses-for-sale/',
    'https://www.bizbuysell.com/hvac-businesses-for-sale/',
    'https://www.bizbuysell.com/electrical-and-mechanical-contracting-businesses-for-sale/',
    'https://www.bizbuysell.com/plumbing-businesses-for-sale/',
    'https://www.bizbuysell.com/roofing-business-for-sale/',
    'https://www.bizbuysell.com/heavy-construction-businesses-for-sale/',
    'https://www.bizbuysell.com/concrete-businesses-for-sale/',
    'https://www.bizbuysell.com/carpet-flooring-businesses-for-sale/',
    'https://www.bizbuysell.com/painting-businesses-for-sale/',
    'https://www.bizbuysell.com/fencing-businesses-for-sale/',

    // Manufacturing
    'https://www.bizbuysell.com/manufacturing-businesses-for-sale/',
    'https://www.bizbuysell.com/industrial-and-commercial-machinery-manufacturers-for-sale/',
    'https://www.bizbuysell.com/metal-product-manufacturers-for-sale/',
    'https://www.bizbuysell.com/machine-shops-and-tool-manufacturers-for-sale/',
    'https://www.bizbuysell.com/packaging-businesses-for-sale/',
    'https://www.bizbuysell.com/electronic-and-electrical-equipment-manufacturers-for-sale/',
    'https://www.bizbuysell.com/rubber-and-plastic-products-manufacturers-for-sale/',
    'https://www.bizbuysell.com/chemical-and-related-product-manufacturers-for-sale/',
    'https://www.bizbuysell.com/lumber-and-wood-products-manufacturers-for-sale/',
    'https://www.bizbuysell.com/paper-manufacturers-and-printing-businesses-for-sale/',

    // Service Businesses
    'https://www.bizbuysell.com/service-businesses-for-sale/',
    'https://www.bizbuysell.com/cleaning-businesses-for-sale/',
    'https://www.bizbuysell.com/landscaping-and-yard-service-businesses-for-sale/',
    'https://www.bizbuysell.com/pest-control-businesses-for-sale/',
    'https://www.bizbuysell.com/staffing-agencies-for-sale/',
    'https://www.bizbuysell.com/security-businesses-for-sale/',
    'https://www.bizbuysell.com/waste-management-and-recycling-businesses-for-sale/',
    'https://www.bizbuysell.com/property-management-businesses-for-sale/',
    'https://www.bizbuysell.com/architecture-and-engineering-firms-for-sale/',
    'https://www.bizbuysell.com/consulting-businesses-for-sale/',
    'https://www.bizbuysell.com/marketing-and-advertising-businesses-for-sale/',
    'https://www.bizbuysell.com/solar-businesses-for-sale/',
    'https://www.bizbuysell.com/funeral-homes-for-sale/',

    // Transportation and Storage
    'https://www.bizbuysell.com/transportation-and-storage-businesses-for-sale/',
    'https://www.bizbuysell.com/trucking-companies-for-sale/',
    'https://www.bizbuysell.com/storage-facilities-and-warehouses-for-sale/',
    'https://www.bizbuysell.com/freight-brokerages-for-sale/',
    'https://www.bizbuysell.com/moving-and-shipping-businesses-for-sale/',

    // Wholesale and Distribution
    'https://www.bizbuysell.com/wholesale-and-distribution-businesses-for-sale/',
    'https://www.bizbuysell.com/durable-goods-wholesalers-and-distributors-for-sale/',
    'https://www.bizbuysell.com/nondurable-goods-wholesalers-and-distributors-for-sale/',

    // Health Care
    'https://www.bizbuysell.com/health-care-and-fitness-businesses-for-sale/',
    'https://www.bizbuysell.com/home-health-care-businesses-for-sale/',
    'https://www.bizbuysell.com/assisted-living-and-nursing-homes-for-sale/',
    'https://www.bizbuysell.com/medical-practices-for-sale/',
    'https://www.bizbuysell.com/dental-practices-for-sale/',
    'https://www.bizbuysell.com/medical-transportation-businesses-for-sale/',

    // Financial Services
    'https://www.bizbuysell.com/financial-services-businesses-for-sale/',
    'https://www.bizbuysell.com/accounting-businesses-and-tax-practices-for-sale/',
    'https://www.bizbuysell.com/insurance-agencies-for-sale/',

    // Automotive
    'https://www.bizbuysell.com/automotive-and-boat-businesses-for-sale/',
    'https://www.bizbuysell.com/auto-repair-and-service-shops-for-sale/',
    'https://www.bizbuysell.com/car-washes-for-sale/',
    'https://www.bizbuysell.com/equipment-rental-and-dealers-for-sale/',

    // Online and Technology
    'https://www.bizbuysell.com/online-and-technology-businesses-for-sale/',
    'https://www.bizbuysell.com/it-and-software-services-businesses-for-sale/',
    'https://www.bizbuysell.com/saas-businesses-for-sale/',

    // Agriculture
    'https://www.bizbuysell.com/agriculture-businesses-for-sale/',

    // Education
    'https://www.bizbuysell.com/education-and-child-related-businesses-for-sale/',
    'https://www.bizbuysell.com/schools-for-sale/',
]);

await Actor.exit();
