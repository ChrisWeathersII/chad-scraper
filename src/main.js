import { Actor } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
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
    if (text.toLowerCase().includes('million')) return num * 1000000;
    if (text.toLowerCase().includes('k')) return num * 1000;
    return num;
};

const extractBizBuySell = (page, url) => {
    return page.evaluate((url) => {
        const getText = (selectors) => {
            for (const sel of selectors) {
                const el = document.querySelector(sel);
                if (el && el.textContent.trim()) return el.textContent.trim();
            }
            return '';
        };

        const getTableValue = (label) => {
            const cells = document.querySelectorAll('td');
            for (let i = 0; i < cells.length; i++) {
                if (cells[i].textContent.trim().toLowerCase().includes(label.toLowerCase())) {
                    return cells[i + 1]?.textContent.trim() || '';
                }
            }
            return '';
        };

        const locationText = getText([
            'span[class*="location"]',
            'div[class*="location"]',
            'h2[class*="location"]'
        ]);
        const locationParts = locationText.split(',').map(s => s.trim());

        const sourceId = url.split('/').filter(Boolean).pop()?.split('?')[0] || '';

        return {
            source: 'bizbuysell',
            source_url: url,
            source_id: `bbs_${sourceId}`,
            deal_type: 'brokered',
            company_name: getText(['h1.bfsTitle', 'h1[class*="title"]', 'h1']).substring(0, 255),
            description: getText(['p.bfsDescription', 'div[class*="description"] p', 'meta[name="description"]']).substring(0, 500),
            industry: getText(['span[class*="industry"]', 'a[class*="category"]', 'td:last-child']).substring(0, 100),
            city: (locationParts[0] || '').substring(0, 100),
            state: (locationParts[1] || '').substring(0, 50),
            asking_price: getTableValue('Asking Price'),
            revenue: getTableValue('Gross Revenue'),
            cash_flow: getTableValue('Cash Flow') || getTableValue('EBITDA') || getTableValue('SDE'),
            employees: getTableValue('Employees'),
            broker_name: getText(['span[class*="broker-name"]', 'div[class*="broker"] h3']).substring(0, 255),
            broker_firm: getText(['span[class*="broker-firm"]', 'div[class*="brokerage"] span']).substring(0, 255),
            broker_email: document.querySelector('a[href^="mailto:"]')?.href?.replace('mailto:', '') || '',
            broker_phone: getText(['span[class*="phone"]', 'a[href^="tel:"]']).substring(0, 50),
        };
    }, url);
};

const extractBizQuest = (page, url) => {
    return page.evaluate((url) => {
        const getText = (selectors) => {
            for (const sel of selectors) {
                const el = document.querySelector(sel);
                if (el && el.textContent.trim()) return el.textContent.trim();
            }
            return '';
        };

        const getTableValue = (label) => {
            const cells = document.querySelectorAll('td, .listing-detail-item');
            for (let i = 0; i < cells.length; i++) {
                if (cells[i].textContent.trim().toLowerCase().includes(label.toLowerCase())) {
                    return cells[i + 1]?.textContent.trim() ||
                           cells[i].nextElementSibling?.textContent.trim() || '';
                }
            }
            return '';
        };

        const locationText = getText([
            '.listing-location',
            'span[class*="location"]',
            'div[class*="location"]',
            '.city-state'
        ]);
        const locationParts = locationText.split(',').map(s => s.trim());

        const pathParts = url.split('/').filter(Boolean);
        const sourceId = pathParts[pathParts.length - 1]?.split('?')[0] || '';

        return {
            source: 'bizquest',
            source_url: url,
            source_id: `bq_${sourceId}`,
            deal_type: 'brokered',
            company_name: getText([
                'h1.listing-title',
                'h1[class*="title"]',
                '.business-name',
                'h1'
            ]).substring(0, 255),
            description: getText([
                '.listing-description',
                'div[class*="description"]',
                '.business-description p'
            ]).substring(0, 500),
            industry: getText([
                '.listing-category',
                'span[class*="category"]',
                '.industry-type'
            ]).substring(0, 100),
            city: (locationParts[0] || '').substring(0, 100),
            state: (locationParts[1] || '').substring(0, 50),
            asking_price: getTableValue('Asking Price') || getTableValue('Price'),
            revenue: getTableValue('Gross Revenue') || getTableValue('Revenue'),
            cash_flow: getTableValue('Cash Flow') || getTableValue('EBITDA') || getTableValue('SDE'),
            employees: getTableValue('Employees'),
            broker_name: getText([
                '.broker-name',
                'span[class*="broker"]',
                '.agent-name'
            ]).substring(0, 255),
            broker_firm: getText([
                '.broker-company',
                '.brokerage-name',
                '.agent-company'
            ]).substring(0, 255),
            broker_email: document.querySelector('a[href^="mailto:"]')?.href?.replace('mailto:', '') || '',
            broker_phone: getText(['.broker-phone', 'a[href^="tel:"]', '.phone-number']).substring(0, 50),
        };
    }, url);
};

const extractBusinessBroker = (page, url) => {
    return page.evaluate((url) => {
        const getText = (selectors) => {
            for (const sel of selectors) {
                const el = document.querySelector(sel);
                if (el && el.textContent.trim()) return el.textContent.trim();
            }
            return '';
        };

        const getTableValue = (label) => {
            const rows = document.querySelectorAll('tr, .detail-row, .listing-info-item');
            for (const row of rows) {
                if (row.textContent.toLowerCase().includes(label.toLowerCase())) {
                    const cells = row.querySelectorAll('td, .value, span');
                    if (cells.length > 1) return cells[cells.length - 1].textContent.trim();
                }
            }
            return '';
        };

        const locationText = getText([
            '.listing-location',
            'span[class*="location"]',
            '.business-location',
            'h2[class*="location"]'
        ]);
        const locationParts = locationText.split(',').map(s => s.trim());

        const pathParts = url.split('/').filter(Boolean);
        const sourceId = pathParts[pathParts.length - 1]?.split('?')[0]?.replace('.aspx', '') || '';

        return {
            source: 'businessbroker',
            source_url: url,
            source_id: `bb_${sourceId}`,
            deal_type: 'brokered',
            company_name: getText([
                'h1.listing-title',
                'h1[class*="title"]',
                '.business-name',
                'h1'
            ]).substring(0, 255),
            description: getText([
                '.listing-description',
                '.business-description',
                'div[class*="description"] p'
            ]).substring(0, 500),
            industry: getText([
                '.listing-category',
                '.business-type',
                'span[class*="category"]'
            ]).substring(0, 100),
            city: (locationParts[0] || '').substring(0, 100),
            state: (locationParts[1] || '').substring(0, 50),
            asking_price: getTableValue('Asking Price') || getTableValue('Price'),
            revenue: getTableValue('Gross Revenue') || getTableValue('Revenue') || getTableValue('Sales'),
            cash_flow: getTableValue('Cash Flow') || getTableValue('EBITDA') || getTableValue('SDE') || getTableValue('Discretionary'),
            employees: getTableValue('Employees'),
            broker_name: getText(['.broker-name', '.agent-name', 'span[class*="broker"]']).substring(0, 255),
            broker_firm: getText(['.broker-company', '.brokerage-name']).substring(0, 255),
            broker_email: document.querySelector('a[href^="mailto:"]')?.href?.replace('mailto:', '') || '',
            broker_phone: getText(['.broker-phone', 'a[href^="tel:"]']).substring(0, 50),
        };
    }, url);
};

const crawler = new PlaywrightCrawler({
    maxRequestsPerCrawl: 500,
    maxConcurrency: 2,
    requestHandlerTimeoutSecs: 60,
    launchContext: {
        launchOptions: {
            headless: true,
        },
    },

    async requestHandler({ page, request, enqueueLinks, log }) {
        const url = request.url;
        log.info(`Processing: ${url}`);

        await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {});

        let deal = null;
        let isListing = false;

        if (url.includes('bizbuysell.com')) {
            isListing = url.match(/\/\d+\/?(\?.*)?$/);
            if (isListing) deal = await extractBizBuySell(page, url);

        } else if (url.includes('bizquest.com')) {
            isListing = url.includes('/BW') || url.match(/\/[A-Z]{2}\d+\/?$/);
            if (isListing) deal = await extractBizQuest(page, url);

        } else if (url.includes('businessbroker.net')) {
            isListing = url.includes('.aspx') && !url.includes('search') && !url.includes('list');
            if (isListing) deal = await extractBusinessBroker(page, url);
        }

        if (deal) {
            if (!deal.company_name || deal.company_name.length <= 3) {
                log.info(`Skipping: no company name at ${url}`);
                return;
            }

            const askingNum = parseNumber(deal.asking_price);
            if (askingNum !== null && askingNum < 500000) {
                log.info(`Skipping below $500k: ${deal.company_name}`);
                return;
            }

            deal.asking_price = askingNum;
            deal.revenue = parseNumber(deal.revenue);
            deal.cash_flow = parseNumber(deal.cash_flow);
            deal.employees = parseInt(deal.employees) || null;
            deal.listing_date = new Date().toISOString().split('T')[0];
            deal.status = 'active';
            deal.scraped_at = new Date().toISOString();

            await Dataset.pushData(deal);

            const { error } = await supabase
                .from('deals')
                .upsert(deal, { onConflict: 'source_id' });

            if (error) {
                log.error(`Supabase error: ${error.message}`);
            } else {
                log.info(`Saved: ${deal.company_name} | ${deal.city}, ${deal.state} | $${deal.asking_price} | ${deal.source}`);
            }

        } else {
            const content = await page.content();
            const $ = (sel) => {
                const links = [];
                const matches = content.matchAll(/href="([^"]+)"/g);
                for (const match of matches) links.push(match[1]);
                return links;
            };

            const allHrefs = [];
            const matches = content.matchAll(/href="([^"]+)"/g);
            for (const match of matches) allHrefs.push(match[1]);

            const listingLinks = allHrefs
                .filter(href => {
                    if (url.includes('bizbuysell.com')) return href.match(/\/\d+\/?$/);
                    if (url.includes('bizquest.com')) return href.includes('/BW') || href.match(/\/[A-Z]{2}\d+\/?$/);
                    if (url.includes('businessbroker.net')) return href.includes('.aspx') && href.includes('/business/') && !href.includes('search');
                    return false;
                })
                .map(href => href.startsWith('http') ? href : `https://${url.split('/')[2]}${href}`)
                .filter((v, i, a) => a.indexOf(v) === i);

            if (listingLinks.length > 0) {
                await enqueueLinks({ urls: listingLinks });
                log.info(`Queued ${listingLinks.length} listings from ${url}`);
            }

            const nextMatch = content.match(/href="([^"]*(?:page|pg|p=)[^"]*\d+[^"]*)"/i);
            if (nextMatch) {
                const nextUrl = nextMatch[1].startsWith('http')
                    ? nextMatch[1]
                    : `https://${url.split('/')[2]}${nextMatch[1]}`;
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
    // BizBuySell - ETA relevant categories
    'https://www.bizbuysell.com/building-and-construction-businesses-for-sale/',
    'https://www.bizbuysell.com/hvac-businesses-for-sale/',
    'https://www.bizbuysell.com/electrical-and-mechanical-contracting-businesses-for-sale/',
    'https://www.bizbuysell.com/plumbing-businesses-for-sale/',
    'https://www.bizbuysell.com/roofing-business-for-sale/',
    'https://www.bizbuysell.com/manufacturing-businesses-for-sale/',
    'https://www.bizbuysell.com/service-businesses-for-sale/',
    'https://www.bizbuysell.com/landscaping-and-yard-service-businesses-for-sale/',
    'https://www.bizbuysell.com/pest-control-businesses-for-sale/',
    'https://www.bizbuysell.com/cleaning-businesses-for-sale/',
    'https://www.bizbuysell.com/trucking-companies-for-sale/',
    'https://www.bizbuysell.com/storage-facilities-and-warehouses-for-sale/',
    'https://www.bizbuysell.com/wholesale-and-distribution-businesses-for-sale/',
    'https://www.bizbuysell.com/health-care-and-fitness-businesses-for-sale/',
    'https://www.bizbuysell.com/financial-services-businesses-for-sale/',
    'https://www.bizbuysell.com/insurance-agencies-for-sale/',
    'https://www.bizbuysell.com/auto-repair-and-service-shops-for-sale/',
    'https://www.bizbuysell.com/it-and-software-services-businesses-for-sale/',

    // BizQuest - top categories
    'https://www.bizquest.com/businesses-for-sale/',
    'https://www.bizquest.com/manufacturing-businesses-for-sale/',
    'https://www.bizquest.com/service-businesses-for-sale/',
    'https://www.bizquest.com/construction-businesses-for-sale/',
    'https://www.bizquest.com/healthcare-businesses-for-sale/',
    'https://www.bizquest.com/transportation-businesses-for-sale/',
    'https://www.bizquest.com/wholesale-distribution-businesses-for-sale/',

    // BusinessBroker.net - top categories
    'https://www.businessbroker.net/businesses-for-sale/',
    'https://www.businessbroker.net/manufacturing-businesses-for-sale/',
    'https://www.businessbroker.net/service-businesses-for-sale/',
    'https://www.businessbroker.net/construction-businesses-for-sale/',
    'https://www.businessbroker.net/healthcare-businesses-for-sale/',
    'https://www.businessbroker.net/distribution-businesses-for-sale/',
]);

await Actor.exit();
