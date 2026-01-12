const { parentPort } = require('worker_threads');
const { chromium } = require('playwright');

let browser = null;

async function initBrowser() {
    try {
        // Always create a new browser instance for each request
        if (browser) {
            await browser.close().catch(() => {});
        }

        browser = await chromium.launch({
            headless: true,
            args: [
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox'
            ]
        });

        const context = await browser.newContext({
            userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            viewport: { width: 1280, height: 720 }
        });

        const page = await context.newPage();
        return { page, context };
    } catch (error) {
        console.error('Browser initialization error:', error);
        return null;
    }
}

async function extractEmailsFromPage(page) {
    try {
        await page.waitForLoadState('domcontentloaded', { timeout: 5000 });
        await page.waitForTimeout(500);

        const emails = await page.evaluate(() => {
            const results = new Set();
            const emailRegex = /([a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/gi;
            
            const textContent = document.body.innerText;
            const matches = textContent.match(emailRegex) || [];
            
            if (matches.length === 0) {
                document.querySelectorAll('a[href^="mailto:"]').forEach(link => {
                    const email = link.href.replace('mailto:', '').split('?')[0].toLowerCase();
                    if (email) results.add(email);
                });
            } else {
                for (const email of matches) {
                    if (email.length > 5 && 
                        email.length < 100 && 
                        !email.includes('example') &&
                        !email.includes('test@') &&
                        !email.includes('email@')) {
                        results.add(email.toLowerCase().trim());
                        break;
                    }
                }
            }

            return Array.from(results);
        });

        return emails.length > 0 ? emails[0] : null;
    } catch (error) {
        console.error('Error extracting email:', error);
        return null;
    }
}

async function safeGoTo(page, url) {
    try {
        url = url.trim();
        if (!url.startsWith('http')) {
            url = 'https://' + url;
        }

        await Promise.race([
            page.goto(url, {
                timeout: 5000,
                waitUntil: 'domcontentloaded'
            }),
            new Promise((_, reject) => 
                setTimeout(() => reject(new Error('Timeout')), 5000)
            )
        ]);
        
        return true;
    } catch (error) {
        return false;
    }
}

parentPort.on('message', async (data) => {
    let page = null;
    let context = null;
    
    try {
        const { business, requestId } = data;
        
        if (!business?.website || business.website === 'N/A') {
            parentPort.postMessage({ requestId, error: false, email: 'N/A' });
            return;
        }

        const result = await initBrowser();
        if (!result) {
            parentPort.postMessage({ requestId, error: true, email: 'N/A' });
            return;
        }

        ({ page, context } = result);
        let email = null;

        if (await safeGoTo(page, business.website)) {
            email = await extractEmailsFromPage(page);
        }

        if (!email) {
            const contactPaths = ['contact', 'contact-us', 'about'];
            
            for (const path of contactPaths) {
                if (email) break;
                
                const contactUrl = `${business.website.replace(/\/+$/, '')}/${path}`;
                if (await safeGoTo(page, contactUrl)) {
                    email = await extractEmailsFromPage(page);
                    if (email) break;
                }
            }
        }

        parentPort.postMessage({ requestId, error: false, email: email || 'N/A' });

    } catch (error) {
        parentPort.postMessage({ requestId: data?.requestId, error: true, email: 'N/A' });
    } finally {
        try {
            if (page) await page.close().catch(() => {});
            if (context) await context.close().catch(() => {});
            if (browser) await browser.close().catch(() => {});
            browser = null;
        } catch (error) {
            console.error('Cleanup error:', error);
        }
    }
});

// Clean up browser on exit
process.on('exit', async () => { 
    if (browser) {
        await browser.close().catch(() => {}); 
        browser = null;
    }
});

// Handle worker termination
process.on('SIGTERM', async () => {
    if (browser) {
        await browser.close().catch(() => {});
        browser = null;
    }
});  