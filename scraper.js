const { chromium } = require('playwright');

const { Worker } = require('worker_threads');

const path = require('path');

const os = require('os');


// Update worker pool management

const NUM_WORKERS = 4;

let workers = [];

let currentWorkerIndex = 0;


// Add this at the top of the file

const processedUrls = new Set();
 // Track all processed URLs globally


function initializeWorkerPool() {
    
// Clean up existing workers
    
cleanupWorkers();
    
    
// Create new workers
    for (let i = 0; i < NUM_WORKERS; i++) {
        const worker = new Worker(path.join(__dirname, 'worker.js'));
        workers.push(worker);
    }
    currentWorkerIndex = 0;
}

function getNextWorker() {
    if (workers.length === 0) {
        initializeWorkerPool();
    }
    const worker = workers[currentWorkerIndex];
    currentWorkerIndex = (currentWorkerIndex + 1) % workers.length;
    return worker;
}

// Function to extract emails from a webpage
async function extractEmailsFromWebsite(page, url) {
    // console.log(`\nStarting email extraction for URL: ${url}`);
    try {
        if (!url || url === 'N/A') {
             //console.log('Invalid URL provided');
            return 'N/A';
        }

        // Clean URL
        url = url.trim().split('?')[0].replace(/\/+$/, '');
        if (!url.startsWith('http')) {
            url = 'https://' + url;
        }
        // console.log(`Cleaned URL: ${url}`);

        try {
            console.log('Navigating to website...');
            await page.goto(url, { 
                timeout: 60000,
                waitUntil: 'domcontentloaded'
            });
            console.log('Successfully loaded website');

            const emails = await extractEmailsFromPage(page);
            // console.log(`Found ${emails.length} potential emails`);  
            
            if (emails.length > 0) {
                console.log(`Emails found: ${emails.join(', ')}`);
                return emails[0];
            }
            
            // console.log('No emails found on main page, checking contact pages...');
            // Check contact page if no emails found
            const contactLinks = await page.$$eval(
                'a[href*="contact"], a[href*="about"], a[href*="Contact"], a[href*="About"]', 
                links => links.map(link => link.href)
            );
            
            // console.log(`Found ${contactLinks.length} contact/about links`);
            
            for (const contactUrl of contactLinks) {
                try {
                    await page.goto(contactUrl, { timeout: 15000 });
                    const contactEmails = await extractEmailsFromPage(page);
                    if (contactEmails.length > 0) {
                        // console.log(`Found emails on contact page: ${contactUrl}`);
                        return contactEmails[0];
                    }
                } catch (error) {
                    console.log(`Error checking contact page: ${contactUrl}`);
                }
            }
        } catch (error) {
            console.log(`Failed to navigate to ${url}: ${error.message}`);
            return 'N/A';
        }
    } catch (error) {
        console.log(`Email extraction error: ${error.message}`);
        return 'N/A';
    }
}

// Helper function to extract emails from a page
async function extractEmailsFromPage(page) {
    const emails = await page.evaluate(() => {
        const results = [];
        
        // Regular expression for email
        const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
        
        // Get text content
        const text = document.body.innerText;
        const textEmails = text.match(emailRegex) || [];
        results.push(...textEmails);
        
        // Get mailto links
        const mailtoLinks = Array.from(document.querySelectorAll('a[href^="mailto:"]'))
            .map(link => link.href.replace('mailto:', '').split('?')[0]);
        results.push(...mailtoLinks);
        
        // Get emails from input fields
        const emailInputs = Array.from(document.querySelectorAll('input[type="email"]'))
            .map(input => input.value)
            .filter(email => email.includes('@'));
        results.push(...emailInputs);
        
        return results;
    });
    
    return emails;
}

// Update the batch size and concurrent operations
const MAX_CONCURRENT_WITH_EMAIL = 4;
const MAX_CONCURRENT_WITHOUT_EMAIL = 8;  // Increased for faster scraping
const MAX_RETRIES = 2;

async function processListingsBatch(listings) {
    const results = [];
    const batchSize = Math.min(extractEmail ? MAX_CONCURRENT_WITH_EMAIL : MAX_CONCURRENT_WITHOUT_EMAIL, listings.length);
    
    for (let i = 0; i < listings.length; i += batchSize) {
        if (isStopped) break;
        
        const batch = listings.slice(i, i + batchSize);
        const batchPromises = batch.map(async (listing) => {
            try {
                const detailsPage = await context.newPage();
                await detailsPage.setDefaultNavigationTimeout(15000);

                const href = await listing.getAttribute('href');
                const name = await listing.getAttribute('aria-label').catch(() => 'N/A');
                
                // Skip duplicates
                const key = `${href}-${name}`;
                if (processedUrls.has(key)) {
                    await detailsPage.close();
                    return null;
                }
                processedUrls.add(key);

                // Fast navigation
                await detailsPage.goto(href, { 
                    waitUntil: 'domcontentloaded',
                    timeout: extractEmail ? 10000 : 5000 
                });

                // Extract basic data first
                const [website, address, rating, reviews, category] = await Promise.all([
                    detailsPage.$eval('a[data-item-id="authority"]', el => el.href).catch(() => 'N/A'),
                    detailsPage.$eval('button[data-item-id="address"] div', el => el.textContent.trim()).catch(() => 'N/A'),
                    detailsPage.$eval('div.F7nice span[aria-hidden="true"]', el => el.textContent.trim()).catch(() => 'N/A'),
                    detailsPage.$eval('div.F7nice span[aria-label*="reviews"]', el => el.getAttribute('aria-label').split(' ')[0]).catch(() => 'N/A'),
                    detailsPage.$eval('button.DkEaL', el => el.textContent.trim()).catch(() => 'N/A')
                ]);

                const business = { name, website, address, rating, reviews, category, phone: 'N/A', countryCode: 'N/A', email: 'N/A' };

                // Extract phone in parallel with other operations
                const phoneElement = await detailsPage.$('button[data-item-id^="phone:tel:"] div');
                if (phoneElement) {
                    business.phone = await phoneElement.textContent();
                    const phoneMatch = business.phone.match(/\+(\d+)/);
                    if (phoneMatch) {
                        business.countryCode = `+${phoneMatch[1]}`;
                        business.phone = business.phone.replace(business.countryCode, '').trim();
                    } else {
                        business.countryCode = '+91';
                        if (business.phone.startsWith('0')) {
                            business.phone = business.phone.substring(1).trim();
                        }
                    }
                }

                // Process address parts
                const addressParts = address.split(',');
                if (addressParts.length >= 3) {
                    business.city = addressParts[addressParts.length - 3].trim();
                    const statePin = addressParts[addressParts.length - 2].trim();
                    const pinMatch = statePin.match(/\d{6}/);
                    business.pincode = pinMatch ? pinMatch[0] : 'N/A';
                    business.state = pinMatch ? statePin.replace(pinMatch[0], '').trim() : statePin;
                }

                // Extract email only if needed
                if (extractEmail && website !== 'N/A') {
                    try {
                        const worker = getNextWorker();
                        if (!worker) {
                            business.email = 'N/A';
                        } else {
                            const requestId = Date.now() + '-' + Math.random();
                            
                            const result = await new Promise((resolve) => {
                                const timeoutId = setTimeout(() => {
                                    try {
                                        worker.removeListener('message', messageHandler);
                                    } catch (err) {}
                                    resolve({ error: false, email: 'N/A' });
                                }, 15000);

                                const messageHandler = (data) => {
                                    if (data.requestId === requestId) {
                                        clearTimeout(timeoutId);
                                        try {
                                            worker.removeListener('message', messageHandler);
                                        } catch (err) {}
                                        resolve(data);
                                    }
                                };

                                try {
                                    worker.on('message', messageHandler);
                                    worker.postMessage({ business, requestId });
                                } catch (err) {
                                    clearTimeout(timeoutId);
                                    resolve({ error: false, email: 'N/A' });
                                }
                            });

                            business.email = result.error ? 'N/A' : (result.email || 'N/A');
                        }
                    } catch (error) {
                        console.error(`Error extracting email for ${business.name}:`, error);
                        business.email = 'N/A';
                    }
                }

                await detailsPage.close();
                return business;
            } catch (error) {
                return null;
            }
        });

        const batchResults = await Promise.all(batchPromises);
        results.push(...batchResults.filter(r => r !== null));
    }
    return results;
}

async function scrapeGoogleMaps(query, total = Infinity, onDataScraped, signal, extractEmail = false) {
    processedUrls.clear(); // Clear processed URLs at start of new scrape
    let browser = null;
    let scrapedData = [];
    let isStopped = false;
    const MAX_CONCURRENT = extractEmail ? 2 : 4; // Adjust concurrent operations
    const MAX_RETRIES = 3; // Maximum number of retries for page load
   

    try {
        // Initialize new worker pool for each scraping session
            initializeWorkerPool();

        browser = await chromium.launch({ 
            headless: true,
            args: ['--disable-dev-shm-usage', '--no-sandbox', '--disable-setuid-sandbox']
        });
        
        const context = await browser.newContext({
            userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport: { width: 1920, height: 1080 }
        });

        const page = await context.newPage();
        await page.setDefaultNavigationTimeout(60000);

        signal?.addEventListener('abort', () => {
            isStopped = true;
            // console.log('Received abort signal');
        });

        //await page.goto('https://www.google.com/maps', { waitUntil: 'networkidle',timeout:60000 });
        //await page.fill('#searchboxinput', query);
       // await page.keyboard.press('Enter');
        //await page.waitForSelector('div[role="feed"]', { timeout: 60000 });

         let retryCount = 0;
        let success = false;
       
         while (retryCount < MAX_RETRIES && !success) {
            try {
                console.log(`Attempt ${retryCount + 1} to load Google Maps...`);
                await page.goto('https://www.google.com/maps', { 
                    waitUntil: 'networkidle',
                    timeout: 60000 
                });

                // Wait for the search box with increased timeout
                await page.waitForSelector('#searchboxinput', { 
                    timeout: 60000,
                    state: 'visible'
                });

                // Clear any existing text and fill the search box
                await page.fill('#searchboxinput', '');
                await page.fill('#searchboxinput', query);
                await page.keyboard.press('Enter');

                // Wait for results to load
                await page.waitForSelector('div[role="feed"]', { 
                    timeout: 60000,
                    state: 'visible'
                });

                success = true;
                console.log('Successfully loaded Google Maps and search results');
            } catch (error) {
                retryCount++;
                console.error(`Attempt ${retryCount} failed:`, error.message);
                
                if (retryCount === MAX_RETRIES) {
                    throw new Error(`Failed to load Google Maps after ${MAX_RETRIES} attempts: ${error.message}`);
                }
                
                // Wait before retrying
                await page.waitForTimeout(5000);
            }
        }


        let scrollAttempts = 0;
        let lastResultsCount = 0;
        let consecutiveNoNewResults = 0;

        // Optimize batch processing
        async function processListingsBatch(listings) {
            const results = [];
            const batchSize = Math.min(MAX_CONCURRENT, listings.length);
            
            for (let i = 0; i < listings.length; i += batchSize) {
                if (isStopped) break;
                
                const batch = listings.slice(i, i + batchSize);
                const batchPromises = batch.map(async (listing) => {
                    try {
                        const detailsPage = await context.newPage();
                        await detailsPage.setDefaultNavigationTimeout(20000);

                        const href = await listing.getAttribute('href');
                        const name = await listing.getAttribute('aria-label').catch(() => 'N/A');
                        
                        // Skip if already processed this URL or business name
                        const key = `${href}-${name}`;
                        if (processedUrls.has(key)) {
                            await detailsPage.close();
                            return null;
                        }
                        processedUrls.add(key);

                        await detailsPage.goto(href, { waitUntil: 'domcontentloaded' });

                        // Extract business data
                        const business = {
                            name: name,
                            website: await detailsPage.$eval('a[data-item-id="authority"]', el => el.href).catch(() => 'N/A'),
                            phone: 'N/A',
                            countryCode: 'N/A',
                            address: await detailsPage.$eval('button[data-item-id="address"] div', el => el.textContent.trim()).catch(() => 'N/A'),
                            rating: await detailsPage.$eval('div.F7nice span[aria-hidden="true"]', el => el.textContent.trim()).catch(() => 'N/A'),
                            reviews: await detailsPage.$eval('div.F7nice span[aria-label*="reviews"]', el => el.getAttribute('aria-label').split(' ')[0]).catch(() => 'N/A'),
                            category: await detailsPage.$eval('button.DkEaL', el => el.textContent.trim()).catch(() => 'N/A'),
                            email: 'N/A'
                        };

                        // Check for duplicate based on multiple fields
                        const isDuplicate = results.some(existingBusiness => 
                            existingBusiness.name === business.name &&
                            existingBusiness.address === business.address &&
                            existingBusiness.phone === business.phone
                        );

                        if (isDuplicate) {
                            await detailsPage.close();
                            return null;
                        }

                        // Get phone number
                        const phoneElement = await detailsPage.$('button[data-item-id^="phone:tel:"] div');
                        if (phoneElement) {
                            business.phone = await phoneElement.textContent().catch(() => 'N/A');
                            const phoneMatch = business.phone.match(/\+(\d+)/);
                            if (phoneMatch) {
                                business.countryCode = `+${phoneMatch[1]}`;
                                business.phone = business.phone.replace(business.countryCode, '').trim();
                            } else if (business.phone.startsWith('0')) {
                                business.countryCode = '+91';
                                business.phone = business.phone.substring(1).trim();
                            } else {
                                business.countryCode = '+91';
                            }
                        }

                        // Process address parts
                        const addressParts = business.address.split(',');
                        if (addressParts.length >= 3) {
                            business.city = addressParts[addressParts.length - 3].trim();
                            const statePin = addressParts[addressParts.length - 2].trim();
                            const pinMatch = statePin.match(/\d{6}/);
                            business.pincode = pinMatch ? pinMatch[0] : 'N/A';
                            business.state = pinMatch ? statePin.replace(pinMatch[0], '').trim() : statePin;
                        }

                        // Extract email if needed
                        if (extractEmail && business.website !== 'N/A') {
                            try {
                                const worker = getNextWorker();
                                if (!worker) {
                                    business.email = 'N/A';
                                } else {
                                    const requestId = Date.now() + '-' + Math.random();
                                    
                                    const result = await new Promise((resolve) => {
                                        const timeoutId = setTimeout(() => {
                                            try {
                                                worker.removeListener('message', messageHandler);
                                            } catch (err) {}
                                            resolve({ error: false, email: 'N/A' });
                                        }, 15000);

                                        const messageHandler = (data) => {
                                            if (data.requestId === requestId) {
                                                clearTimeout(timeoutId);
                                                try {
                                                    worker.removeListener('message', messageHandler);
                                                } catch (err) {}
                                                resolve(data);
                                            }
                                        };

                                        try {
                                            worker.on('message', messageHandler);
                                            worker.postMessage({ business, requestId });
                                        } catch (err) {
                                            clearTimeout(timeoutId);
                                            resolve({ error: false, email: 'N/A' });
                                        }
                                    });

                                    business.email = result.error ? 'N/A' : (result.email || 'N/A');
                                }
                            } catch (error) {
                                console.error(`Error extracting email for ${business.name}:`, error);
                                business.email = 'N/A';
                            }
                        }

                        await detailsPage.close();
                        return business;
                    } catch (error) {
                        console.error(`Error processing business:`, error.message);
                        return null;
                    }
                });

                const batchResults = await Promise.all(batchPromises);
                results.push(...batchResults.filter(r => r !== null));
            }
            return results;
        }

        while (!isStopped && scrollAttempts < 500) {
            try {
                await page.evaluate(() => {
                    const feed = document.querySelector('div[role="feed"]'); 
                    if (feed) {
                        feed.scrollTop = feed.scrollHeight;
                    }
                });
                await page.mouse.wheel(0, 1000);
                await page.waitForTimeout(2000);

                const listings = await page.$$('a[href*="https://www.google.com/maps/place"]');
                console.log(`Found ${listings.length} total listings (${lastResultsCount} previous)`);

                if (listings.length > lastResultsCount) {
                    const newListings = listings.slice(lastResultsCount);
                    const results = await processListingsBatch(newListings);
                    
                    for (const result of results) {
                        scrapedData.push(result);
                        onDataScraped(result);
                        console.log(`✅ Scraped (${scrapedData.length}): ${result.name}`);
                    }

                    lastResultsCount = listings.length; 
                    consecutiveNoNewResults = 0;
                } else {
                    consecutiveNoNewResults++;
                    if (consecutiveNoNewResults >= 4) {
                        // Try clicking "Show more" button
                        await page.click('button[aria-label="Show more"]').catch(() => {});
                        // Break after 4 attempts with no new results
                        if (consecutiveNoNewResults >= 4) {
                            console.log('No more results found after 4 attempts');
                            break;
                        }
                    }
                }

                scrollAttempts++;
            } catch (error) {
                if (error.message.includes('Target closed')) break;
                console.error('Error during scroll:', error.message);
                scrollAttempts++;
            }
        }

        return scrapedData;

    } catch (error) {
        console.error('Scraping error:', error);
        return scrapedData;
    } finally {
        // Clean up resources
        cleanupWorkers();
        if (browser) {
            await browser.close().catch(() => {});
        }
    }
}

// Cleanup function to terminate workers
function cleanupWorkers() {
    workers.forEach(worker => {
        try {
            worker.terminate();
        } catch (error) {
            console.error('Error terminating worker:', error);
        }
    });
    workers.length = 0;
}

// Handle process termination
process.on('SIGTERM', cleanupWorkers);
process.on('exit', cleanupWorkers);

module.exports = { scrapeGoogleMaps };



