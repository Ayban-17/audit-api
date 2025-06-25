import express from 'express';
import axios from 'axios';
import { load } from 'cheerio';
import pLimit from 'p-limit';

const router = express.Router();
const concurrencyLimit = pLimit(5); // 5 concurrent requests max

// URL Categorization Function
function categorizeUrl(url) {
    try {
        const parsedUrl = new URL(url);
        const domain = parsedUrl.hostname;
        
        if (domain !== 'www.adventure-life.com' && domain !== 'adventure-life.com') {
            return 'external-link';
        }

        const path = parsedUrl.pathname;
        const cleanPath = path.replace(/^\/|\/$/g, '');
        const segments = cleanPath.split('/');
        const lastSegment = segments[segments.length - 1];
        
        const contactKeywords = ['contact', 'contact-us', 'get-in-touch'];
        const wrongContactKeywords = ['contactt'];
        
        if (wrongContactKeywords.some(keyword => cleanPath.includes(keyword))) {
            return 'wrong-contact-path';
        }
        
        if (contactKeywords.some(keyword => cleanPath.includes(keyword))) {
            return 'contact-page';
        }
        
        if (/^cruises\/\d+\//.test(cleanPath)) {
            return 'cruise-ship';
        }
        
        if (/\/cruises\/\d+\//.test(cleanPath)) {
            return 'cruise-with-id';
        }
        
        const specialKeywords = ['land-tours', 'ships', 'videos', 'myTrips', 'tours', 'cruises', 'hotels', 'deals', 'info', 'articles', 'stories'];
        if (specialKeywords.includes(lastSegment)) {
            return 'destination-special-page';
        }
        
        if (segments.includes('articles') && segments.length > 1) {
            return 'multi-level/articles/article-name';
        }
        
        if (segments.includes('stories') && segments.length > 1) {
            return 'multi-level/stories/story-name';
        }
        
        if (lastSegment === 'stories' && segments.length === 1) {
            return 'multi-level/stories';
        }
        
        if (cleanPath.startsWith('operators/') && /\/\d+\//.test(cleanPath)) {
            return 'operator-with-id';
        }
        
        if (cleanPath.includes('/tours/')) {
            const toursIndex = segments.indexOf('tours');
            
            if (toursIndex !== -1 && 
                toursIndex < segments.length - 1 && 
                /\d+/.test(segments[toursIndex + 1])) {
                return 'tour-with-id';
            }
            return 'tour-activity';
        }
        
        const nonDestinationKeywords = ['articles', 'stories', 'deals', 'tours', 'cruises', 'operators', 'forms'];
        const isDestination = !segments.some(seg => nonDestinationKeywords.includes(seg));
        
        if (isDestination) {
            return `multi-level/destination-${segments.length}`;
        }
        
        return 'other';
        
    } catch (error) {
        console.error(`URL parsing error: ${url}`, error.message);
        return 'invalid-url';
    }
}

// Section detection with .al-intro support
function detectSectionType($element) {
    // Method 0: Check if link is inside .al-intro first
    const introParent = $element.closest('.al-intro');
    if (introParent.length > 0) {
        return 'intro';
    }
    
    // Method 1: Find closest element with actual section type (not title/content)
    let currentElement = $element;
    let level = 0;
    
    while (currentElement.length > 0 && level < 15) {
        const classes = currentElement.attr('class') || '';
        
        if (classes.includes('al-sec-')) {
            const classList = classes.split(' ');
            
            const sectionClass = classList.find(cls => 
                cls.startsWith('al-sec-') && 
                cls !== 'al-sec-title' && 
                cls !== 'al-sec-content' &&
                cls !== 'al-sec'
            );
            
            if (sectionClass) {
                const sectionType = sectionClass.replace('al-sec-', '');
                return sectionType;
            }
        }
        
        currentElement = currentElement.parent();
        level++;
    }
    
    // Method 2: Direct selector for known section types
    const knownSections = [
        'al-sec-four', 'al-sec-table', 'al-sec-sumtiles', 
        'al-sec-articles', 'al-sec-stories', 'al-sec-video', 
        'al-sec-faqs', 'al-sec-text', 'al-sec-tiles'
    ];
    
    for (const sectionClass of knownSections) {
        const sectionParent = $element.closest(`.${sectionClass}`);
        if (sectionParent.length > 0) {
            const sectionType = sectionClass.replace('al-sec-', '');
            return sectionType;
        }
    }
    
    return 'unknown';
}

// More precise section title detection
function getSectionTitle($element) {
    // Special case for intro section
    const introParent = $element.closest('.al-intro');
    if (introParent.length > 0) {
        return 'Introduction Section';
    }
    
    // Find the actual section parent (not title/content divs)
    let sectionParent = null;
    let currentElement = $element;
    let level = 0;
    
    while (currentElement.length > 0 && level < 15) {
        const classes = currentElement.attr('class') || '';
        
        if (classes.includes('al-sec-')) {
            const classList = classes.split(' ');
            
            const sectionClass = classList.find(cls => 
                cls.startsWith('al-sec-') && 
                cls !== 'al-sec-title' && 
                cls !== 'al-sec-content' &&
                cls !== 'al-sec'
            );
            
            if (sectionClass) {
                sectionParent = currentElement;
                break;
            }
        }
        
        currentElement = currentElement.parent();
        level++;
    }
    
    if (!sectionParent || sectionParent.length === 0) {
        return null;
    }
    
    const titleElement = sectionParent.find('.al-sec-title h2').first();
    
    if (titleElement.length > 0) {
        const title = titleElement.text().trim();
        if (title) {
            return title;
        }
    }
    
    const titleElementH3 = sectionParent.find('.al-sec-title h3').first();
    if (titleElementH3.length > 0) {
        const title = titleElementH3.text().trim();
        if (title) {
            return title;
        }
    }
    
    return null;
}

// Function to check cruise availability
async function checkCruiseAvailability(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 8000
        });

        if (response.status !== 200) {
            return { available: false, error: `HTTP ${response.status}` };
        }

        const $ = load(response.data);
        const amountElement = $('.al-amount').first();
        
        if (!amountElement.length) {
            return { available: false, error: 'No .al-amount element found' };
        }
        
        const amountText = amountElement.text().trim();
        const amountValue = parseFloat(amountText.replace(/[^\d.]/g, ''));
        
        const available = !isNaN(amountValue) && amountValue > 0;
        
        return {
            available,
            price: available ? amountValue : null,
            currency: available ? amountText.replace(/[0-9.,\s]/g, '').trim() || 'USD' : null
        };
    } catch (error) {
        console.error(`Availability check failed for ${url}: ${error.message}`);
        return {
            available: false,
            error: error.message || 'Request failed'
        };
    }
}

// Function to check tour-with-id availability
async function checkTourWithIdAvailability(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 8000
        });

        if (response.status !== 200) {
            return { 
                available: false, 
                error: `HTTP ${response.status}`,
                tourId: null,
                price: null,
                currency: null
            };
        }

        const $ = load(response.data);
        
        let tourId = null;
        const tourIdMatch = url.match(/\/tours\/(\d+)/);
        if (tourIdMatch && tourIdMatch[1]) {
            tourId = tourIdMatch[1];
        }
        
        const priceSelectors = [
            '.al-price-summary .al-amount',
            '.al-price-summary .al-price-min .al-amount',
            '.al-price-summary .al-price .al-amount',
            '.al-price-min .al-amount',
            '.al-price .al-amount',
            '[class*="price"] .al-amount',
            '.al-amount'
        ];
        
        let priceElement = null;
        let priceSelector = '';
        
        for (const selector of priceSelectors) {
            const element = $(selector).first();
            if (element.length) {
                priceElement = element;
                priceSelector = selector;
                break;
            }
        }
        
        if (!priceElement || !priceElement.length) {
            return { 
                available: false, 
                error: 'No price element found',
                tourId: tourId,
                price: null,
                currency: null,
                priceSelector: null
            };
        }
        
        const amountText = priceElement.text().trim();
        const amountValue = parseFloat(amountText.replace(/[^\d.]/g, ''));
        
        const available = !isNaN(amountValue) && amountValue > 0;
        
        const pageTitle = $('h1').first().text().trim() || null;
        const departureInfo = $('.al-tour-departure').text().trim() || null;
        const durationInfo = $('.al-tour-duration').text().trim() || null;
        
        return {
            available,
            tourId: tourId,
            price: available ? amountValue : null,
            currency: available ? amountText.replace(/[0-9.,\s]/g, '').trim() || 'USD' : null,
            priceText: amountText,
            priceSelector: priceSelector,
            pageTitle: pageTitle,
            departureInfo: departureInfo,
            durationInfo: durationInfo,
            error: null
        };
    } catch (error) {
        console.error(`Tour availability check failed for ${url}: ${error.message}`);
        return {
            available: false,
            tourId: null,
            price: null,
            currency: null,
            error: error.message || 'Request failed'
        };
    }
}

// Function to validate destination links
async function validateDestinationLink(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 8000,
            maxRedirects: 0,
            validateStatus: (status) => status >= 200 && status < 400
        });

        return {
            valid: true,
            status: response.status,
            resolvedUrl: response.config.url,
            redirected: false
        };
    } catch (error) {
        if (error.response && [301, 302, 303, 307, 308].includes(error.response.status)) {
            return {
                valid: false,
                status: error.response.status,
                resolvedUrl: error.response.headers.location,
                redirected: true,
                error: `Redirected to: ${error.response.headers.location}`
            };
        }
        
        return {
            valid: false,
            status: error.response?.status || 0,
            resolvedUrl: url,
            redirected: false,
            error: error.message || 'Request failed'
        };
    }
}

// Function to validate tour-activity links
async function validateTourActivityLink(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 8000,
            maxRedirects: 0,
            validateStatus: (status) => status >= 200 && status < 400
        });

        return {
            valid: true,
            status: response.status,
            resolvedUrl: response.config.url,
            redirected: false
        };
    } catch (error) {
        if (error.response && [301, 302, 303, 307, 308].includes(error.response.status)) {
            return {
                valid: false,
                status: error.response.status,
                resolvedUrl: error.response.headers.location,
                redirected: true,
                error: `Redirected to: ${error.response.headers.location}`
            };
        }
        
        return {
            valid: false,
            status: error.response?.status || 0,
            resolvedUrl: url,
            redirected: false,
            error: error.message || 'Request failed'
        };
    }
}

// Function to validate tour-with-id links
async function validateTourWithIdLink(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 8000,
            maxRedirects: 0,
            validateStatus: (status) => status >= 200 && status < 400
        });

        return {
            valid: true,
            status: response.status,
            resolvedUrl: response.config.url,
            redirected: false
        };
    } catch (error) {
        if (error.response && [301, 302, 303, 307, 308].includes(error.response.status)) {
            return {
                valid: false,
                status: error.response.status,
                resolvedUrl: error.response.headers.location,
                redirected: true,
                error: `Redirected to: ${error.response.headers.location}`
            };
        }
        
        return {
            valid: false,
            status: error.response?.status || 0,
            resolvedUrl: url,
            redirected: false,
            error: error.message || 'Request failed'
        };
    }
}

// Function to validate cruise-ship links
async function validateCruiseShipLink(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 8000,
            maxRedirects: 0,
            validateStatus: (status) => status >= 200 && status < 400
        });

        return {
            valid: true,
            status: response.status,
            resolvedUrl: response.config.url,
            redirected: false
        };
    } catch (error) {
        if (error.response && [301, 302, 303, 307, 308].includes(error.response.status)) {
            return {
                valid: false,
                status: error.response.status,
                resolvedUrl: error.response.headers.location,
                redirected: true,
                error: `Redirected to: ${error.response.headers.location}`
            };
        }
        
        return {
            valid: false,
            status: error.response?.status || 0,
            resolvedUrl: url,
            redirected: false,
            error: error.message || 'Request failed'
        };
    }
}

// Function to validate destination-special-page links
async function validateDestinationSpecialPageLink(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 8000,
            maxRedirects: 0,
            validateStatus: (status) => status >= 200 && status < 400
        });

        return {
            valid: true,
            status: response.status,
            resolvedUrl: response.config.url,
            redirected: false
        };
    } catch (error) {
        if (error.response && [301, 302, 303, 307, 308].includes(error.response.status)) {
            return {
                valid: false,
                status: error.response.status,
                resolvedUrl: error.response.headers.location,
                redirected: true,
                error: `Redirected to: ${error.response.headers.location}`
            };
        }
        
        return {
            valid: false,
            status: error.response?.status || 0,
            resolvedUrl: url,
            redirected: false,
            error: error.message || 'Request failed'
        };
    }
}

// Function to validate story links
async function validateStoryLink(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 8000,
            maxRedirects: 0,
            validateStatus: (status) => status >= 200 && status < 400
        });

        return {
            valid: true,
            status: response.status,
            resolvedUrl: response.config.url,
            redirected: false
        };
    } catch (error) {
        if (error.response && [301, 302, 303, 307, 308].includes(error.response.status)) {
            return {
                valid: false,
                status: error.response.status,
                resolvedUrl: error.response.headers.location,
                redirected: true,
                error: `Redirected to: ${error.response.headers.location}`
            };
        }
        
        return {
            valid: false,
            status: error.response?.status || 0,
            resolvedUrl: url,
            redirected: false,
            error: error.message || 'Request failed'
        };
    }
}

// Function to validate contact-page links
async function validateContactPageLink(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 8000,
            maxRedirects: 0,
            validateStatus: (status) => status >= 200 && status < 400
        });

        return {
            valid: true,
            status: response.status,
            resolvedUrl: response.config.url,
            redirected: false
        };
    } catch (error) {
        if (error.response && [301, 302, 303, 307, 308].includes(error.response.status)) {
            return {
                valid: false,
                status: error.response.status,
                resolvedUrl: error.response.headers.location,
                redirected: true,
                error: `Redirected to: ${error.response.headers.location}`
            };
        }
        
        return {
            valid: false,
            status: error.response?.status || 0,
            resolvedUrl: url,
            redirected: false,
            error: error.message || 'Request failed'
        };
    }
}

// Function to validate wrong-contact-path links
async function validateWrongContactPathLink(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 8000,
            maxRedirects: 0,
            validateStatus: (status) => status >= 200 && status < 400
        });

        return {
            valid: false,
            status: response.status,
            resolvedUrl: response.config.url,
            redirected: false,
            error: 'Wrong contact path detected - should probably be /contact instead of /contactt'
        };
    } catch (error) {
        if (error.response && [301, 302, 303, 307, 308].includes(error.response.status)) {
            return {
                valid: false,
                status: error.response.status,
                resolvedUrl: error.response.headers.location,
                redirected: true,
                error: `Wrong contact path + redirected to: ${error.response.headers.location}`
            };
        }
        
        return {
            valid: false,
            status: error.response?.status || 0,
            resolvedUrl: url,
            redirected: false,
            error: `Wrong contact path (/contactt) + ${error.message || 'Request failed'}`
        };
    }
}

// Function to validate external links
async function validateExternalLink(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 10000,
            maxRedirects: 0,
            validateStatus: (status) => status >= 200 && status < 400
        });

        return {
            valid: true,
            status: response.status,
            resolvedUrl: response.config.url,
            redirected: false
        };
    } catch (error) {
        if (error.response && [301, 302, 303, 307, 308].includes(error.response.status)) {
            return {
                valid: false,
                status: error.response.status,
                resolvedUrl: error.response.headers.location,
                redirected: true,
                error: `External link redirected to: ${error.response.headers.location}`
            };
        }
        
        return {
            valid: false,
            status: error.response?.status || 0,
            resolvedUrl: url,
            redirected: false,
            error: error.message || 'External link request failed'
        };
    }
}

// Function to check tour-activity availability with both experience and activity options
async function checkTourActivityAvailability(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 8000
        });

        if (response.status !== 200) {
            return { 
                available: false, 
                error: `HTTP ${response.status}`,
                pageType: 'unknown',
                experienceOptions: [],
                activityOptions: []
            };
        }

        const $ = load(response.data);
        
        const hasIndexList = $('.al-indexlist').length > 0;
        
        if (!hasIndexList) {
            return {
                available: true,
                pageType: 'landing',
                experienceOptions: [],
                activityOptions: [],
                error: null
            };
        }
        
        const experienceOptions = [];
        $('.al-il-fields-experience > ul > li > label').each((i, el) => {
            const text = $(el).text().trim();
            if (text) {
                experienceOptions.push(text);
            }
        });
        
        const activityOptions = [];
        $('.al-il-fields-activity > ul > li > label').each((i, el) => {
            const text = $(el).text().trim();
            if (text) {
                activityOptions.push(text);
            }
        });
        
        const urlPath = new URL(url).pathname;
        const pathSegments = urlPath.split('/');
        const activityPath = pathSegments[pathSegments.length - 1];
        
        function normalizeForComparison(text) {
            return text
                .toLowerCase()
                .replace(/&/g, '')
                .replace(/\+/g, '')
                .replace(/-/g, ' ')
                .replace(/\s+/g, ' ')
                .trim();
        }
        
        const normalizedActivityPath = normalizeForComparison(activityPath);
        
        const isAvailableInExperience = experienceOptions.some(option => {
            const normalizedOption = normalizeForComparison(option);
            return normalizedOption === normalizedActivityPath ||
                   normalizedOption.includes(normalizedActivityPath) ||
                   normalizedActivityPath.includes(normalizedOption);
        });
        
        const isAvailableInActivity = activityOptions.some(option => {
            const normalizedOption = normalizeForComparison(option);
            return normalizedOption === normalizedActivityPath ||
                   normalizedOption.includes(normalizedActivityPath) ||
                   normalizedActivityPath.includes(normalizedOption);
        });
        
        const isAvailable = isAvailableInExperience || isAvailableInActivity;
        
        return {
            available: isAvailable,
            pageType: 'index',
            experienceOptions,
            activityOptions,
            activityPath,
            normalizedActivityPath,
            isAvailableInExperience,
            isAvailableInActivity,
            error: null
        };
        
    } catch (error) {
        console.error(`Tour activity availability check failed for ${url}: ${error.message}`);
        return {
            available: false,
            pageType: 'unknown',
            experienceOptions: [],
            activityOptions: [],
            error: error.message || 'Request failed'
        };
    }
}

// Function to check cruise-ship availability
async function checkCruiseShipAvailability(cruiseShipUrl, baseUrl) {
    try {
        const baseUrlObj = new URL(baseUrl);
        const toursUrl = `${baseUrlObj.origin}${baseUrlObj.pathname}/tours`.replace(/\/+/g, '/').replace(':/', '://');
        
        const response = await axios.get(toursUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 8000
        });

        if (response.status !== 200) {
            return { 
                available: false, 
                error: `HTTP ${response.status} when loading tours page`,
                toursUrl,
                shipOptions: []
            };
        }

        const $ = load(response.data);
        
        const shipOptions = [];
        $('.al-il-fields-ship > ul > li > label').each((i, el) => {
            const text = $(el).text().trim();
            if (text) {
                shipOptions.push(text);
            }
        });
        
        const cruiseUrlPath = new URL(cruiseShipUrl).pathname;
        const cruisePathSegments = cruiseUrlPath.split('/');
        const shipName = cruisePathSegments[cruisePathSegments.length - 1];
        
        function normalizeShipName(text) {
            return text
                .toLowerCase()
                .replace(/^(m\/c|m\.c\.|mc|m\.s\.|ms|m\.v\.|mv)\s*/i, '')
                .replace(/[&+\-_\/\.]/g, ' ')
                .replace(/[^\w\s]/g, '')
                .replace(/\s+/g, ' ')
                .trim();
        }
        
        function shipNamesMatch(urlName, optionName) {
            const normalizedUrl = normalizeShipName(urlName);
            const normalizedOption = normalizeShipName(optionName);
            
            if (normalizedUrl === normalizedOption) return true;
            
            if (normalizedOption.includes(normalizedUrl) || normalizedUrl.includes(normalizedOption)) return true;
            
            const urlWords = normalizedUrl.split(' ').filter(w => w.length > 2);
            const optionWords = normalizedOption.split(' ').filter(w => w.length > 2);
            
            const minWords = Math.min(urlWords.length, optionWords.length);
            if (minWords === 0) return false;
            
            let matchCount = 0;
            urlWords.forEach(urlWord => {
                if (optionWords.some(optionWord => 
                    optionWord.includes(urlWord) || urlWord.includes(optionWord)
                )) {
                    matchCount++;
                }
            });
            
            return matchCount / minWords >= 0.6;
        }
        
        const isAvailable = shipOptions.some(option => shipNamesMatch(shipName, option));
        
        return {
            available: isAvailable,
            shipOptions,
            shipName,
            normalizedShipName: normalizeShipName(shipName),
            normalizedOptions: shipOptions.map(option => ({
                original: option,
                normalized: normalizeShipName(option)
            })),
            toursUrl,
            error: null
        };
        
    } catch (error) {
        console.error(`Cruise ship availability check failed for ${cruiseShipUrl}: ${error.message}`);
        return {
            available: false,
            shipOptions: [],
            toursUrl: null,
            error: error.message || 'Request failed'
        };
    }
}

// Function to check story availability
async function checkStoryAvailability(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 8000
        });

        if (response.status !== 200) {
            return { 
                available: false, 
                error: `HTTP ${response.status}`
            };
        }

        return {
            available: true,
            error: null
        };
        
    } catch (error) {
        console.error(`Story availability check failed for ${url}: ${error.message}`);
        return {
            available: false,
            error: error.message || 'Request failed'
        };
    }
}

// Function to check contact-page availability
async function checkContactPageAvailability(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 8000
        });

        if (response.status !== 200) {
            return { 
                available: false, 
                error: `HTTP ${response.status}`
            };
        }

        return {
            available: true,
            error: null
        };
        
    } catch (error) {
        console.error(`Contact page availability check failed for ${url}: ${error.message}`);
        return {
            available: false,
            error: error.message || 'Request failed'
        };
    }
}

// Function to check external link availability
async function checkExternalLinkAvailability(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 12000
        });

        if (response.status !== 200) {
            return { 
                available: false, 
                error: `HTTP ${response.status}`
            };
        }

        return {
            available: true,
            error: null
        };
        
    } catch (error) {
        console.error(`External link availability check failed for ${url}: ${error.message}`);
        return {
            available: false,
            error: error.message || 'External link request failed'
        };
    }
}

// Enhanced extractLinks function with debugging and section detection
function extractLinks($, containerSelector, baseUrl) {
    const links = [];
    const container = $(containerSelector);
    
    if (container.length === 0) {
        return links;
    }

    container.find('a[href]').each((i, el) => {
        const $el = $(el);
        let href = $el.attr('href')?.trim();
        let text = '';

        if (!href || href === '#' || href.startsWith('javascript:')) {
            return;
        }

        try {
            if (href.startsWith('/')) {
                href = new URL(href, baseUrl).href;
            } else if (!href.startsWith('http')) {
                href = new URL(href, baseUrl).href;
            }
        } catch (e) {
            console.warn(`Invalid URL: ${href}`, e.message);
            return;
        }

        const sectionType = detectSectionType($el);
        const sectionTitle = getSectionTitle($el);

        const customSelectors = [
            '.tour-title', '.card-header', '.item-name', 
            'h1', 'h2', 'h3', 'h4', '.title', '.name', '.label',
            'strong', 'b', 'em'
        ].join(',');
        
        const titleElement = $el.find(customSelectors).first();
        if (titleElement.length) {
            text = titleElement.text().trim();
        }
        else if ($el.find('img').length) {
            text = $el.find('img').attr('alt')?.trim() || '';
        }
        else if ($el.attr('aria-label')) {
            text = $el.attr('aria-label').trim();
        }
        else {
            text = $el.contents().filter((_, node) => 
                node.type === 'text' && 
                !$(node).parent().is('script, style, noscript')
            ).text().trim();
        }

        text = text
            .replace(/\s+/g, ' ')
            .replace(/[\n\t]/g, '')
            .trim();

        function truncate(text, max = 120) {
            if (text.length <= max) return text;
            return text.substring(0, text.lastIndexOf(' ', max)) + '...';
        }
        
        text = truncate(text);

        if (!text && $el.find('img').length) {
            text = 'Image Link';
        } else if (!text) {
            text = '[No text]';
        }

        const category = categorizeUrl(href);
        
        const link = {
            text,
            href,
            position: i + 1,
            element: $el.prop('tagName'),
            classes: $el.attr('class') || '',
            id: $el.attr('id') || '',
            category,
            section: sectionType,
            sectionTitle: sectionTitle
        };
        
        // Add validation placeholders
        if (category === 'cruise-with-id') {
            link.available = null;
            link.price = null;
            link.currency = null;
            link.availabilityError = null;
        }
        else if (category === 'tour-with-id') {
            link.valid = null;
            link.status = null;
            link.redirected = null;
            link.resolvedUrl = null;
            link.validationError = null;
            link.available = null;
            link.tourId = null;
            link.price = null;
            link.currency = null;
            link.priceText = null;
            link.priceSelector = null;
            link.pageTitle = null;
            link.departureInfo = null;
            link.durationInfo = null;
            link.availabilityError = null;
        }
        else if (category.startsWith('multi-level/destination-')) {
            link.valid = null;
            link.status = null;
            link.redirected = null;
            link.resolvedUrl = null;
            link.validationError = null;
        }
        else if (category === 'tour-activity') {
            link.valid = null;
            link.status = null;
            link.redirected = null;
            link.resolvedUrl = null;
            link.validationError = null;
            link.available = null;
            link.pageType = null;
            link.experienceOptions = null;
            link.activityOptions = null;
            link.activityPath = null;
            link.isAvailableInExperience = null;
            link.isAvailableInActivity = null;
            link.availabilityError = null;
        }
        else if (category === 'cruise-ship') {
            link.valid = null;
            link.status = null;
            link.redirected = null;
            link.resolvedUrl = null;
            link.validationError = null;
            link.available = null;
            link.shipOptions = null;
            link.shipName = null;
            link.normalizedShipName = null;
            link.normalizedOptions = null;
            link.toursUrl = null;
            link.availabilityError = null;
        }
        else if (category === 'destination-special-page') {
            link.valid = null;
            link.status = null;
            link.redirected = null;
            link.resolvedUrl = null;
            link.validationError = null;
        }
        else if (category === 'multi-level/stories/story-name') {
            link.valid = null;
            link.status = null;
            link.redirected = null;
            link.resolvedUrl = null;
            link.validationError = null;
            link.available = null;
            link.availabilityError = null;
        }
        else if (category === 'contact-page') {
            link.valid = null;
            link.status = null;
            link.redirected = null;
            link.resolvedUrl = null;
            link.validationError = null;
            link.available = null;
            link.availabilityError = null;
        }
        else if (category === 'wrong-contact-path') {
            link.valid = null;
            link.status = null;
            link.redirected = null;
            link.resolvedUrl = null;
            link.validationError = null;
            link.available = null;
            link.availabilityError = null;
        }
        else if (category === 'external-link') {
            link.valid = null;
            link.status = null;
            link.redirected = null;
            link.resolvedUrl = null;
            link.validationError = null;
            link.available = null;
            link.availabilityError = null;
        }
        
        links.push(link);
    });
    
    return links;
}

// Helper function to process a single URL
async function processSingleUrl(targetUrl) {
    try {
        
        const response = await axios.get(targetUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            timeout: 15000
        });

        if (response.status !== 200) {
            return {
                url: targetUrl,
                success: false,
                error: `Failed to fetch URL. Status: ${response.status}`,
                stats: null,
                linksByCategory: null,
                linksBySection: null,
                detailedResults: null
            };
        }

        const $ = load(response.data);
        
        // Extract links using same logic as existing API
        const introLinks = extractLinks($, '.al-intro', targetUrl);
        const mainLinks = extractLinks($, '#al-main', targetUrl);
        const allLinks = [...introLinks, ...mainLinks];
        
      
        
        // Prepare validation tasks
        const validationTasks = [];
        
        const cruiseLinks = allLinks.filter(link => link.category === 'cruise-with-id');
        const tourWithIdLinks = allLinks.filter(link => link.category === 'tour-with-id');
        const destinationLinks = allLinks.filter(link => 
            link.category.startsWith('multi-level/destination-')
        );
        const tourActivityLinks = allLinks.filter(link => link.category === 'tour-activity');
        const cruiseShipLinks = allLinks.filter(link => link.category === 'cruise-ship');
        const destinationSpecialPageLinks = allLinks.filter(link => link.category === 'destination-special-page');
        const storyLinks = allLinks.filter(link => link.category === 'multi-level/stories/story-name');
        const contactPageLinks = allLinks.filter(link => link.category === 'contact-page');
        const wrongContactPathLinks = allLinks.filter(link => link.category === 'wrong-contact-path');
        const externalLinkLinks = allLinks.filter(link => link.category === 'external-link');
        
        // Add validation tasks
        cruiseLinks.forEach(link => {
            validationTasks.push(
                concurrencyLimit(() => 
                    checkCruiseAvailability(link.href)
                        .then(result => ({
                            href: link.href,
                            type: 'cruise',
                            ...result
                        }))
            ));
        });
        
        tourWithIdLinks.forEach(link => {
            validationTasks.push(
                concurrencyLimit(() => 
                    validateTourWithIdLink(link.href)
                        .then(result => ({
                            href: link.href,
                            type: 'tour-with-id-validation',
                            ...result
                        }))
            ));
        });
        
        destinationLinks.forEach(link => {
            validationTasks.push(
                concurrencyLimit(() => 
                    validateDestinationLink(link.href)
                        .then(result => ({
                            href: link.href,
                            type: 'destination',
                            ...result
                        }))
            ));
        });

        tourActivityLinks.forEach(link => {
            validationTasks.push(
                concurrencyLimit(() => 
                    validateTourActivityLink(link.href)
                        .then(result => ({
                            href: link.href,
                            type: 'tour-activity-validation',
                            ...result
                        }))
            ));
        });

        cruiseShipLinks.forEach(link => {
            validationTasks.push(
                concurrencyLimit(() => 
                    validateCruiseShipLink(link.href)
                        .then(result => ({
                            href: link.href,
                            type: 'cruise-ship-validation',
                            ...result
                        }))
            ));
        });

        destinationSpecialPageLinks.forEach(link => {
            validationTasks.push(
                concurrencyLimit(() => 
                    validateDestinationSpecialPageLink(link.href)
                        .then(result => ({
                            href: link.href,
                            type: 'destination-special-page-validation',
                            ...result
                        }))
            ));
        });

        storyLinks.forEach(link => {
            validationTasks.push(
                concurrencyLimit(() => 
                    validateStoryLink(link.href)
                        .then(result => ({
                            href: link.href,
                            type: 'story-validation',
                            ...result
                        }))
            ));
        });

        contactPageLinks.forEach(link => {
            validationTasks.push(
                concurrencyLimit(() => 
                    validateContactPageLink(link.href)
                        .then(result => ({
                            href: link.href,
                            type: 'contact-page-validation',
                            ...result
                        }))
            ));
        });

        wrongContactPathLinks.forEach(link => {
            validationTasks.push(
                concurrencyLimit(() => 
                    validateWrongContactPathLink(link.href)
                        .then(result => ({
                            href: link.href,
                            type: 'wrong-contact-path-validation',
                            ...result
                        }))
            ));
        });

        externalLinkLinks.forEach(link => {
            validationTasks.push(
                concurrencyLimit(() => 
                    validateExternalLink(link.href)
                        .then(result => ({
                            href: link.href,
                            type: 'external-link-validation',
                            ...result
                        }))
            ));
        });

        // Process validation tasks
        const validationResults = await Promise.allSettled(validationTasks);
        const validationMap = new Map();
        
        validationResults.forEach(result => {
            if (result.status === 'fulfilled') {
                const { href, type, ...data } = result.value;
                validationMap.set(href, { type, ...data });
            }
        });

        // Update links with validation results
        allLinks.forEach(link => {
            const validation = validationMap.get(link.href);
            if (!validation) return;
            
            if (validation.type === 'cruise') {
                link.available = validation.available;
                link.price = validation.price;
                link.currency = validation.currency;
                link.availabilityError = validation.error;
            } 
            else if (validation.type === 'tour-with-id-validation') {
                link.valid = validation.valid;
                link.status = validation.status;
                link.redirected = validation.redirected;
                link.resolvedUrl = validation.resolvedUrl;
                link.validationError = validation.error;
            }
            else if (validation.type === 'destination') {
                link.valid = validation.valid;
                link.status = validation.status;
                link.redirected = validation.redirected;
                link.resolvedUrl = validation.resolvedUrl;
                link.validationError = validation.error;
            }
            else if (validation.type === 'tour-activity-validation') {
                link.valid = validation.valid;
                link.status = validation.status;
                link.redirected = validation.redirected;
                link.resolvedUrl = validation.resolvedUrl;
                link.validationError = validation.error;
            }
            else if (validation.type === 'cruise-ship-validation') {
                link.valid = validation.valid;
                link.status = validation.status;
                link.redirected = validation.redirected;
                link.resolvedUrl = validation.resolvedUrl;
                link.validationError = validation.error;
            }
            else if (validation.type === 'destination-special-page-validation') {
                link.valid = validation.valid;
                link.status = validation.status;
                link.redirected = validation.redirected;
                link.resolvedUrl = validation.resolvedUrl;
                link.validationError = validation.error;
            }
            else if (validation.type === 'story-validation') {
                link.valid = validation.valid;
                link.status = validation.status;
                link.redirected = validation.redirected;
                link.resolvedUrl = validation.resolvedUrl;
                link.validationError = validation.error;
            }
            else if (validation.type === 'contact-page-validation') {
                link.valid = validation.valid;
                link.status = validation.status;
                link.redirected = validation.redirected;
                link.resolvedUrl = validation.resolvedUrl;
                link.validationError = validation.error;
            }
            else if (validation.type === 'wrong-contact-path-validation') {
                link.valid = validation.valid;
                link.status = validation.status;
                link.redirected = validation.redirected;
                link.resolvedUrl = validation.resolvedUrl;
                link.validationError = validation.error;
            }
            else if (validation.type === 'external-link-validation') {
                link.valid = validation.valid;
                link.status = validation.status;
                link.redirected = validation.redirected;
                link.resolvedUrl = validation.resolvedUrl;
                link.validationError = validation.error;
            }
        });

        // Prepare availability tasks
        const availabilityTasks = [];
        
        const validTourWithIdLinks = tourWithIdLinks.filter(link => link.valid === true);
        validTourWithIdLinks.forEach(link => {
            availabilityTasks.push(
                concurrencyLimit(() => 
                    checkTourWithIdAvailability(link.href)
                        .then(result => ({
                            href: link.href,
                            type: 'tour-with-id-availability',
                            ...result
                        }))
            ));
        });
        
        const validTourActivityLinks = tourActivityLinks.filter(link => link.valid === true);
        validTourActivityLinks.forEach(link => {
            availabilityTasks.push(
                concurrencyLimit(() => 
                    checkTourActivityAvailability(link.href)
                        .then(result => ({
                            href: link.href,
                            type: 'tour-activity-availability',
                            ...result
                        }))
            ));
        });

        const validCruiseShipLinks = cruiseShipLinks.filter(link => link.valid === true);
        validCruiseShipLinks.forEach(link => {
            availabilityTasks.push(
                concurrencyLimit(() => 
                    checkCruiseShipAvailability(link.href, targetUrl)
                        .then(result => ({
                            href: link.href,
                            type: 'cruise-ship-availability',
                            ...result
                        }))
            ));
        });

        const validStoryLinks = storyLinks.filter(link => link.valid === true);
        validStoryLinks.forEach(link => {
            availabilityTasks.push(
                concurrencyLimit(() => 
                    checkStoryAvailability(link.href)
                        .then(result => ({
                            href: link.href,
                            type: 'story-availability',
                            ...result
                        }))
            ));
        });

        const validContactPageLinks = contactPageLinks.filter(link => link.valid === true);
        validContactPageLinks.forEach(link => {
            availabilityTasks.push(
                concurrencyLimit(() => 
                    checkContactPageAvailability(link.href)
                        .then(result => ({
                            href: link.href,
                            type: 'contact-page-availability',
                            ...result
                        }))
            ));
        });

        const validExternalLinkLinks = externalLinkLinks.filter(link => link.valid === true);
        validExternalLinkLinks.forEach(link => {
            availabilityTasks.push(
                concurrencyLimit(() => 
                    checkExternalLinkAvailability(link.href)
                        .then(result => ({
                            href: link.href,
                            type: 'external-link-availability',
                            ...result
                        }))
            ));
        });

        // Process availability tasks
        const availabilityResults = await Promise.allSettled(availabilityTasks);
        const availabilityMap = new Map();
        
        availabilityResults.forEach(result => {
            if (result.status === 'fulfilled') {
                const { href, type, ...data } = result.value;
                availabilityMap.set(href, { type, ...data });
            }
        });

        // Update links with availability results
        allLinks.forEach(link => {
            const availability = availabilityMap.get(link.href);
            if (!availability) return;
            
            if (availability.type === 'tour-with-id-availability') {
                link.available = availability.available;
                link.tourId = availability.tourId;
                link.price = availability.price;
                link.currency = availability.currency;
                link.priceText = availability.priceText;
                link.priceSelector = availability.priceSelector;
                link.pageTitle = availability.pageTitle;
                link.departureInfo = availability.departureInfo;
                link.durationInfo = availability.durationInfo;
                link.availabilityError = availability.error;
            }
            else if (availability.type === 'tour-activity-availability') {
                link.available = availability.available;
                link.pageType = availability.pageType;
                link.experienceOptions = availability.experienceOptions;
                link.activityOptions = availability.activityOptions;
                link.activityPath = availability.activityPath;
                link.isAvailableInExperience = availability.isAvailableInExperience;
                link.isAvailableInActivity = availability.isAvailableInActivity;
                link.availabilityError = availability.error;
            }
            else if (availability.type === 'cruise-ship-availability') {
                link.available = availability.available;
                link.shipOptions = availability.shipOptions;
                link.shipName = availability.shipName;
                link.normalizedShipName = availability.normalizedShipName;
                link.normalizedOptions = availability.normalizedOptions;
                link.toursUrl = availability.toursUrl;
                link.availabilityError = availability.error;
            }
            else if (availability.type === 'story-availability') {
                link.available = availability.available;
                link.availabilityError = availability.error;
            }
            else if (availability.type === 'contact-page-availability') {
                link.available = availability.available;
                link.availabilityError = availability.error;
            }
            else if (availability.type === 'external-link-availability') {
                link.available = availability.available;
                link.availabilityError = availability.error;
            }
        });
        
        // Calculate counts
        const categoryCounts = {};
        allLinks.forEach(link => {
            categoryCounts[link.category] = (categoryCounts[link.category] || 0) + 1;
        });
        
        const sectionCounts = {};
        allLinks.forEach(link => {
            sectionCounts[link.section] = (sectionCounts[link.section] || 0) + 1;
        });

        const sectionCategoryMatrix = {};
        allLinks.forEach(link => {
            if (!sectionCategoryMatrix[link.section]) {
                sectionCategoryMatrix[link.section] = {};
            }
            if (!sectionCategoryMatrix[link.section][link.category]) {
                sectionCategoryMatrix[link.section][link.category] = 0;
            }
            sectionCategoryMatrix[link.section][link.category]++;
        });

        const sectionStats = {};
        Object.keys(sectionCounts).forEach(section => {
            const sectionLinks = allLinks.filter(link => link.section === section);
            
            sectionStats[section] = {
                total: sectionLinks.length,
                categories: sectionCategoryMatrix[section] || {},
                available: sectionLinks.filter(link => link.available === true).length,
                unavailable: sectionLinks.filter(link => link.available === false).length,
                valid: sectionLinks.filter(link => link.valid === true).length,
                invalid: sectionLinks.filter(link => link.valid === false).length,
                titles: [...new Set(sectionLinks.map(link => link.sectionTitle).filter(Boolean))]
            };
        });

        // Calculate detailed stats for each link type
        const cruiseStats = {
            total: cruiseLinks.length,
            available: cruiseLinks.filter(link => link.available === true).length,
            unavailable: cruiseLinks.filter(link => link.available === false).length,
            errors: cruiseLinks.filter(link => link.availabilityError).length
        };
        
        const destinationStats = {
            total: destinationLinks.length,
            valid: destinationLinks.filter(link => link.valid === true).length,
            invalid: destinationLinks.filter(link => link.valid === false).length,
            redirected: destinationLinks.filter(link => link.redirected === true).length,
            errors: destinationLinks.filter(link => link.validationError).length
        };

        const tourWithIdStats = {
            total: tourWithIdLinks.length,
            valid: tourWithIdLinks.filter(link => link.valid === true).length,
            invalid: tourWithIdLinks.filter(link => link.valid === false).length,
            redirected: tourWithIdLinks.filter(link => link.redirected === true).length,
            available: tourWithIdLinks.filter(link => link.available === true).length,
            unavailable: tourWithIdLinks.filter(link => link.available === false).length,
            validationErrors: tourWithIdLinks.filter(link => link.validationError).length,
            availabilityErrors: tourWithIdLinks.filter(link => link.availabilityError).length
        };

        const tourActivityStats = {
            total: tourActivityLinks.length,
            valid: tourActivityLinks.filter(link => link.valid === true).length,
            invalid: tourActivityLinks.filter(link => link.valid === false).length,
            redirected: tourActivityLinks.filter(link => link.redirected === true).length,
            available: tourActivityLinks.filter(link => link.available === true).length,
            unavailable: tourActivityLinks.filter(link => link.available === false).length,
            availableInExperienceOnly: tourActivityLinks.filter(link => 
                link.isAvailableInExperience === true && link.isAvailableInActivity === false).length,
            availableInActivityOnly: tourActivityLinks.filter(link => 
                link.isAvailableInExperience === false && link.isAvailableInActivity === true).length,
            availableInBoth: tourActivityLinks.filter(link => 
                link.isAvailableInExperience === true && link.isAvailableInActivity === true).length,
            landingPages: tourActivityLinks.filter(link => link.pageType === 'landing').length,
            indexPages: tourActivityLinks.filter(link => link.pageType === 'index').length,
            validationErrors: tourActivityLinks.filter(link => link.validationError).length,
            availabilityErrors: tourActivityLinks.filter(link => link.availabilityError).length
        };

        const cruiseShipStats = {
            total: cruiseShipLinks.length,
            valid: cruiseShipLinks.filter(link => link.valid === true).length,
            invalid: cruiseShipLinks.filter(link => link.valid === false).length,
            redirected: cruiseShipLinks.filter(link => link.redirected === true).length,
            available: cruiseShipLinks.filter(link => link.available === true).length,
            unavailable: cruiseShipLinks.filter(link => link.available === false).length,
            validationErrors: cruiseShipLinks.filter(link => link.validationError).length,
            availabilityErrors: cruiseShipLinks.filter(link => link.availabilityError).length
        };

        const destinationSpecialPageStats = {
            total: destinationSpecialPageLinks.length,
            valid: destinationSpecialPageLinks.filter(link => link.valid === true).length,
            invalid: destinationSpecialPageLinks.filter(link => link.valid === false).length,
            redirected: destinationSpecialPageLinks.filter(link => link.redirected === true).length,
            validationErrors: destinationSpecialPageLinks.filter(link => link.validationError).length
        };

        const storyStats = {
            total: storyLinks.length,
            valid: storyLinks.filter(link => link.valid === true).length,
            invalid: storyLinks.filter(link => link.valid === false).length,
            redirected: storyLinks.filter(link => link.redirected === true).length,
            available: storyLinks.filter(link => link.available === true).length,
            unavailable: storyLinks.filter(link => link.available === false).length,
            validationErrors: storyLinks.filter(link => link.validationError).length,
            availabilityErrors: storyLinks.filter(link => link.availabilityError).length
        };

        const contactPageStats = {
            total: contactPageLinks.length,
            valid: contactPageLinks.filter(link => link.valid === true).length,
            invalid: contactPageLinks.filter(link => link.valid === false).length,
            redirected: contactPageLinks.filter(link => link.redirected === true).length,
            available: contactPageLinks.filter(link => link.available === true).length,
            unavailable: contactPageLinks.filter(link => link.available === false).length,
            validationErrors: contactPageLinks.filter(link => link.validationError).length,
            availabilityErrors: contactPageLinks.filter(link => link.availabilityError).length
        };

        const wrongContactPathStats = {
            total: wrongContactPathLinks.length,
            valid: wrongContactPathLinks.filter(link => link.valid === true).length,
            invalid: wrongContactPathLinks.filter(link => link.valid === false).length,
            redirected: wrongContactPathLinks.filter(link => link.redirected === true).length,
            available: wrongContactPathLinks.filter(link => link.available === true).length,
            unavailable: wrongContactPathLinks.filter(link => link.available === false).length,
            validationErrors: wrongContactPathLinks.filter(link => link.validationError).length,
            availabilityErrors: wrongContactPathLinks.filter(link => link.availabilityError).length
        };

        const externalLinkStats = {
            total: externalLinkLinks.length,
            valid: externalLinkLinks.filter(link => link.valid === true).length,
            invalid: externalLinkLinks.filter(link => link.valid === false).length,
            redirected: externalLinkLinks.filter(link => link.redirected === true).length,
            available: externalLinkLinks.filter(link => link.available === true).length,
            unavailable: externalLinkLinks.filter(link => link.available === false).length,
            validationErrors: externalLinkLinks.filter(link => link.validationError).length,
            availabilityErrors: externalLinkLinks.filter(link => link.availabilityError).length
        };

        // Group links by category
        const linksByCategory = {};
        allLinks.forEach(link => {
            if (!linksByCategory[link.category]) {
                linksByCategory[link.category] = [];
            }
            
            const linkData = {
                text: link.text,
                href: link.href,
                position: link.position,
                section: link.section,
                sectionTitle: link.sectionTitle
            };
            
            // Add category-specific data
            if (link.category === 'cruise-with-id') {
                linkData.available = link.available;
                linkData.price = link.price;
                linkData.currency = link.currency;
                linkData.error = link.availabilityError;
            }
            else if (link.category === 'tour-with-id') {
                linkData.valid = link.valid;
                linkData.status = link.status;
                linkData.redirected = link.redirected;
                linkData.resolvedUrl = link.resolvedUrl;
                linkData.validationError = link.validationError;
                linkData.available = link.available;
                linkData.tourId = link.tourId;
                linkData.price = link.price;
                linkData.currency = link.currency;
                linkData.priceText = link.priceText;
                linkData.priceSelector = link.priceSelector;
                linkData.pageTitle = link.pageTitle;
                linkData.departureInfo = link.departureInfo;
                linkData.durationInfo = link.durationInfo;
                linkData.availabilityError = link.availabilityError;
            }
            else if (link.category.startsWith('multi-level/destination-')) {
                linkData.valid = link.valid;
                linkData.status = link.status;
                linkData.redirected = link.redirected;
                linkData.resolvedUrl = link.resolvedUrl;
                linkData.error = link.validationError;
            }
            else if (link.category === 'tour-activity') {
                linkData.valid = link.valid;
                linkData.status = link.status;
                linkData.redirected = link.redirected;
                linkData.resolvedUrl = link.resolvedUrl;
                linkData.validationError = link.validationError;
                linkData.available = link.available;
                linkData.pageType = link.pageType;
                linkData.experienceOptions = link.experienceOptions;
                linkData.activityOptions = link.activityOptions;
                linkData.activityPath = link.activityPath;
                linkData.isAvailableInExperience = link.isAvailableInExperience;
                linkData.isAvailableInActivity = link.isAvailableInActivity;
                linkData.availabilityError = link.availabilityError;
            }
            else if (link.category === 'cruise-ship') {
                linkData.valid = link.valid;
                linkData.status = link.status;
                linkData.redirected = link.redirected;
                linkData.resolvedUrl = link.resolvedUrl;
                linkData.validationError = link.validationError;
                linkData.available = link.available;
                linkData.shipOptions = link.shipOptions;
                linkData.shipName = link.shipName;
                linkData.normalizedShipName = link.normalizedShipName;
                linkData.normalizedOptions = link.normalizedOptions;
                linkData.toursUrl = link.toursUrl;
                linkData.availabilityError = link.availabilityError;
            }
            else if (link.category === 'destination-special-page') {
                linkData.valid = link.valid;
                linkData.status = link.status;
                linkData.redirected = link.redirected;
                linkData.resolvedUrl = link.resolvedUrl;
                linkData.validationError = link.validationError;
            }
            else if (link.category === 'multi-level/stories/story-name') {
                linkData.valid = link.valid;
                linkData.status = link.status;
                linkData.redirected = link.redirected;
                linkData.resolvedUrl = link.resolvedUrl;
                linkData.validationError = link.validationError;
                linkData.available = link.available;
                linkData.availabilityError = link.availabilityError;
            }
            else if (link.category === 'contact-page') {
                linkData.valid = link.valid;
                linkData.status = link.status;
                linkData.redirected = link.redirected;
                linkData.resolvedUrl = link.resolvedUrl;
                linkData.validationError = link.validationError;
                linkData.available = link.available;
                linkData.availabilityError = link.availabilityError;
            }
            else if (link.category === 'wrong-contact-path') {
                linkData.valid = link.valid;
                linkData.status = link.status;
                linkData.redirected = link.redirected;
                linkData.resolvedUrl = link.resolvedUrl;
                linkData.validationError = link.validationError;
                linkData.available = link.available;
                linkData.availabilityError = link.availabilityError;
            }
            else if (link.category === 'external-link') {
                linkData.valid = link.valid;
                linkData.status = link.status;
                linkData.redirected = link.redirected;
                linkData.resolvedUrl = link.resolvedUrl;
                linkData.validationError = link.validationError;
                linkData.available = link.available;
                linkData.availabilityError = link.availabilityError;
            }
            
            linksByCategory[link.category].push(linkData);
        });

        // Group links by section AND title
        const linksBySection = {};
        allLinks.forEach(link => {
            const sectionType = link.section;
            const sectionTitle = link.sectionTitle || 'No Title';
            
            if (!linksBySection[sectionType]) {
                linksBySection[sectionType] = [];
            }
            
            let titleGroup = linksBySection[sectionType].find(group => group.title === sectionTitle);
            
            if (!titleGroup) {
                titleGroup = {
                    title: sectionTitle,
                    links: []
                };
                linksBySection[sectionType].push(titleGroup);
            }
            
            const linkData = {
                text: link.text,
                href: link.href,
                position: link.position,
                category: link.category
            };
            
            // Add category-specific data for sections
            if (link.category === 'cruise-with-id') {
                linkData.available = link.available;
                linkData.price = link.price;
                linkData.currency = link.currency;
                linkData.availabilityError = link.availabilityError;
            }
            else if (link.category === 'tour-with-id') {
                linkData.valid = link.valid;
                linkData.available = link.available;
                linkData.tourId = link.tourId;
                linkData.price = link.price;
                linkData.currency = link.currency;
                linkData.priceText = link.priceText;
                linkData.pageTitle = link.pageTitle;
                linkData.departureInfo = link.departureInfo;
                linkData.durationInfo = link.durationInfo;
                linkData.validationError = link.validationError;
                linkData.availabilityError = link.availabilityError;
            }
            else if (link.category.startsWith('multi-level/destination-')) {
                linkData.valid = link.valid;
                linkData.status = link.status;
                linkData.redirected = link.redirected;
                linkData.resolvedUrl = link.resolvedUrl;
                linkData.validationError = link.validationError;
            }
            else if (link.category === 'tour-activity') {
                linkData.valid = link.valid;
                linkData.available = link.available;
                linkData.pageType = link.pageType;
                linkData.experienceOptions = link.experienceOptions;
                linkData.activityOptions = link.activityOptions;
                linkData.activityPath = link.activityPath;
                linkData.isAvailableInExperience = link.isAvailableInExperience;
                linkData.isAvailableInActivity = link.isAvailableInActivity;
                linkData.validationError = link.validationError;
                linkData.availabilityError = link.availabilityError;
            }
            else if (link.category === 'cruise-ship') {
                linkData.valid = link.valid;
                linkData.available = link.available;
                linkData.shipOptions = link.shipOptions;
                linkData.shipName = link.shipName;
                linkData.toursUrl = link.toursUrl;
                linkData.validationError = link.validationError;
                linkData.availabilityError = link.availabilityError;
            }
            else if (link.category === 'destination-special-page') {
                linkData.valid = link.valid;
                linkData.status = link.status;
                linkData.redirected = link.redirected;
                linkData.resolvedUrl = link.resolvedUrl;
                linkData.validationError = link.validationError;
            }
            else if (link.category === 'multi-level/stories/story-name') {
                linkData.valid = link.valid;
                linkData.available = link.available;
                linkData.validationError = link.validationError;
                linkData.availabilityError = link.availabilityError;
            }
            else if (link.category === 'contact-page') {
                linkData.valid = link.valid;
                linkData.available = link.available;
                linkData.validationError = link.validationError;
                linkData.availabilityError = link.availabilityError;
            }
            else if (link.category === 'wrong-contact-path') {
                linkData.valid = link.valid;
                linkData.available = link.available;
                linkData.validationError = link.validationError;
                linkData.availabilityError = link.availabilityError;
            }
            else if (link.category === 'external-link') {
                linkData.valid = link.valid;
                linkData.available = link.available;
                linkData.validationError = link.validationError;
                linkData.availabilityError = link.availabilityError;
            }
            
            titleGroup.links.push(linkData);
        });

        

        return {
            url: targetUrl,
            success: true,
            error: null,
            stats: {
                totalLinks: allLinks.length,
                totalIntroLinks: introLinks.length,
                totalMainLinks: mainLinks.length,
                categoryCounts,
                sectionCounts,
                sectionStats,
                cruiseAvailability: cruiseStats,
                destinationValidation: destinationStats,
                tourWithIdValidation: tourWithIdStats,
                tourActivityValidation: tourActivityStats,
                cruiseShipValidation: cruiseShipStats,
                destinationSpecialPageValidation: destinationSpecialPageStats,
                storyValidation: storyStats,
                contactPageValidation: contactPageStats,
                wrongContactPathValidation: wrongContactPathStats,
                externalLinkValidation: externalLinkStats
            },
            linksByCategory,
            linksBySection,
            sectionCategoryMatrix,
            detailedResults: {
                cruiseLinks: cruiseLinks.map(link => ({
                    text: link.text,
                    href: link.href,
                    section: link.section,
                    sectionTitle: link.sectionTitle,
                    available: link.available,
                    price: link.price,
                    currency: link.currency,
                    error: link.availabilityError
                })),
                destinationLinks: destinationLinks.map(link => ({
                    text: link.text,
                    href: link.href,
                    section: link.section,
                    sectionTitle: link.sectionTitle,
                    valid: link.valid,
                    status: link.status,
                    redirected: link.redirected,
                    resolvedUrl: link.resolvedUrl,
                    error: link.validationError
                })),
                tourActivityLinks: tourActivityLinks.map(link => ({
                    text: link.text,
                    href: link.href,
                    section: link.section,
                    sectionTitle: link.sectionTitle,
                    valid: link.valid,
                    status: link.status,
                    redirected: link.redirected,
                    resolvedUrl: link.resolvedUrl,
                    validationError: link.validationError,
                    available: link.available,
                    pageType: link.pageType,
                    experienceOptions: link.experienceOptions,
                    activityOptions: link.activityOptions,
                    activityPath: link.activityPath,
                    isAvailableInExperience: link.isAvailableInExperience,
                    isAvailableInActivity: link.isAvailableInActivity,
                    availabilityError: link.availabilityError
                })),
                cruiseShipLinks: cruiseShipLinks.map(link => ({
                    text: link.text,
                    href: link.href,
                    section: link.section,
                    sectionTitle: link.sectionTitle,
                    valid: link.valid,
                    status: link.status,
                    redirected: link.redirected,
                    resolvedUrl: link.resolvedUrl,
                    validationError: link.validationError,
                    available: link.available,
                    shipOptions: link.shipOptions,
                    shipName: link.shipName,
                    normalizedShipName: link.normalizedShipName,
                    normalizedOptions: link.normalizedOptions,
                    toursUrl: link.toursUrl,
                    availabilityError: link.availabilityError
                })),
                destinationSpecialPageLinks: destinationSpecialPageLinks.map(link => ({
                    text: link.text,
                    href: link.href,
                    section: link.section,
                    sectionTitle: link.sectionTitle,
                    valid: link.valid,
                    status: link.status,
                    redirected: link.redirected,
                    resolvedUrl: link.resolvedUrl,
                    validationError: link.validationError
                })),
                storyLinks: storyLinks.map(link => ({
                    text: link.text,
                    href: link.href,
                    section: link.section,
                    sectionTitle: link.sectionTitle,
                    valid: link.valid,
                    status: link.status,
                    redirected: link.redirected,
                    resolvedUrl: link.resolvedUrl,
                    validationError: link.validationError,
                    available: link.available,
                    availabilityError: link.availabilityError
                })),
                contactPageLinks: contactPageLinks.map(link => ({
                    text: link.text,
                    href: link.href,
                    section: link.section,
                    sectionTitle: link.sectionTitle,
                    valid: link.valid,
                    status: link.status,
                    redirected: link.redirected,
                    resolvedUrl: link.resolvedUrl,
                    validationError: link.validationError,
                    available: link.available,
                    availabilityError: link.availabilityError
                })),
                wrongContactPathLinks: wrongContactPathLinks.map(link => ({
                    text: link.text,
                    href: link.href,
                    section: link.section,
                    sectionTitle: link.sectionTitle,
                    valid: link.valid,
                    status: link.status,
                    redirected: link.redirected,
                    resolvedUrl: link.resolvedUrl,
                    validationError: link.validationError,
                    available: link.available,
                    availabilityError: link.availabilityError
                })),
                externalLinkLinks: externalLinkLinks.map(link => ({
                    text: link.text,
                    href: link.href,
                    section: link.section,
                    sectionTitle: link.sectionTitle,
                    valid: link.valid,
                    status: link.status,
                    redirected: link.redirected,
                    resolvedUrl: link.resolvedUrl,
                    validationError: link.validationError,
                    available: link.available,
                    availabilityError: link.availabilityError
                })),
                
                sectionBreakdown: Object.keys(linksBySection).map(section => ({
                    section,
                    totalLinks: sectionStats[section].total,
                    categories: sectionStats[section].categories,
                    titleGroups: linksBySection[section].map(titleGroup => ({
                        title: titleGroup.title,
                        linkCount: titleGroup.links.length,
                        links: titleGroup.links
                    }))
                }))
            }
        };

    } catch (error) {
        console.error(`❌ Error processing ${targetUrl}:`, error.message);
        
        return {
            url: targetUrl,
            success: false,
            error: error.message || 'Failed to process URL',
            stats: null,
            linksByCategory: null,
            linksBySection: null,
            detailedResults: null
        };
    }
}

// Batch audit endpoint
router.post('/', async (req, res) => {
    try {
        if (!req.body || !req.body.urls || !Array.isArray(req.body.urls)) {
            return res.status(400).json({
                error: 'Missing or invalid URLs array in request body',
                example: { 
                    urls: [
                        'https://www.adventure-life.com/bolivia/articles/witches-market-of-la-paz',
                        'https://www.adventure-life.com/peru/articles/cusco-witches-market'
                    ] 
                }
            });
        }

        const urls = req.body.urls;
        
        // Validate URL limit
        if (urls.length > 20) {
            return res.status(400).json({
                error: 'Too many URLs. Maximum 20 URLs per batch request.',
                provided: urls.length,
                maximum: 20
            });
        }

        // Validate URLs format
        const invalidUrls = [];
        urls.forEach((url, index) => {
            try {
                new URL(url);
            } catch (e) {
                invalidUrls.push({ index, url, error: 'Invalid URL format' });
            }
        });

        if (invalidUrls.length > 0) {
            return res.status(400).json({
                error: 'Invalid URLs found',
                invalidUrls
            });
        }

       
        const startTime = Date.now();

        // Process URLs with concurrency limit
        const batchConcurrencyLimit = pLimit(2); // Process 2 URLs concurrently
        
        const processingTasks = urls.map(url => 
            batchConcurrencyLimit(() => processSingleUrl(url))
        );

        const results = await Promise.allSettled(processingTasks);
        
        // Process results
        const successfulResults = [];
        const failedResults = [];
        
        results.forEach((result, index) => {
            if (result.status === 'fulfilled') {
                if (result.value.success) {
                    successfulResults.push(result.value);
                } else {
                    failedResults.push({
                        url: urls[index],
                        error: result.value.error
                    });
                }
            } else {
                failedResults.push({
                    url: urls[index],
                    error: result.reason?.message || 'Unknown error'
                });
            }
        });

        // Calculate aggregated summary
        const aggregatedStats = {
            totalLinksAcrossAllUrls: 0,
            totalCategoryCounts: {},
            totalSectionCounts: {},
            totalAvailable: 0,
            totalUnavailable: 0,
            totalValid: 0,
            totalInvalid: 0,
            totalRedirected: 0,
            totalErrors: 0
        };

        successfulResults.forEach(result => {
            if (result.stats) {
                aggregatedStats.totalLinksAcrossAllUrls += result.stats.totalLinks;
                
                // Aggregate category counts
                Object.keys(result.stats.categoryCounts || {}).forEach(category => {
                    aggregatedStats.totalCategoryCounts[category] = 
                        (aggregatedStats.totalCategoryCounts[category] || 0) + result.stats.categoryCounts[category];
                });
                
                // Aggregate section counts
                Object.keys(result.stats.sectionCounts || {}).forEach(section => {
                    aggregatedStats.totalSectionCounts[section] = 
                        (aggregatedStats.totalSectionCounts[section] || 0) + result.stats.sectionCounts[section];
                });

                // Aggregate availability stats
                if (result.stats.cruiseAvailability) {
                    aggregatedStats.totalAvailable += result.stats.cruiseAvailability.available || 0;
                    aggregatedStats.totalUnavailable += result.stats.cruiseAvailability.unavailable || 0;
                }
                
                if (result.stats.tourWithIdValidation) {
                    aggregatedStats.totalAvailable += result.stats.tourWithIdValidation.available || 0;
                    aggregatedStats.totalUnavailable += result.stats.tourWithIdValidation.unavailable || 0;
                    aggregatedStats.totalValid += result.stats.tourWithIdValidation.valid || 0;
                    aggregatedStats.totalInvalid += result.stats.tourWithIdValidation.invalid || 0;
                    aggregatedStats.totalRedirected += result.stats.tourWithIdValidation.redirected || 0;
                }
                
                // Aggregate validation stats
                if (result.stats.destinationValidation) {
                    aggregatedStats.totalValid += result.stats.destinationValidation.valid || 0;
                    aggregatedStats.totalInvalid += result.stats.destinationValidation.invalid || 0;
                    aggregatedStats.totalRedirected += result.stats.destinationValidation.redirected || 0;
                }
                
                if (result.stats.tourActivityValidation) {
                    aggregatedStats.totalAvailable += result.stats.tourActivityValidation.available || 0;
                    aggregatedStats.totalUnavailable += result.stats.tourActivityValidation.unavailable || 0;
                    aggregatedStats.totalValid += result.stats.tourActivityValidation.valid || 0;
                    aggregatedStats.totalInvalid += result.stats.tourActivityValidation.invalid || 0;
                    aggregatedStats.totalRedirected += result.stats.tourActivityValidation.redirected || 0;
                }
                
                if (result.stats.cruiseShipValidation) {
                    aggregatedStats.totalAvailable += result.stats.cruiseShipValidation.available || 0;
                    aggregatedStats.totalUnavailable += result.stats.cruiseShipValidation.unavailable || 0;
                    aggregatedStats.totalValid += result.stats.cruiseShipValidation.valid || 0;
                    aggregatedStats.totalInvalid += result.stats.cruiseShipValidation.invalid || 0;
                    aggregatedStats.totalRedirected += result.stats.cruiseShipValidation.redirected || 0;
                }
                
                if (result.stats.storyValidation) {
                    aggregatedStats.totalAvailable += result.stats.storyValidation.available || 0;
                    aggregatedStats.totalUnavailable += result.stats.storyValidation.unavailable || 0;
                    aggregatedStats.totalValid += result.stats.storyValidation.valid || 0;
                    aggregatedStats.totalInvalid += result.stats.storyValidation.invalid || 0;
                    aggregatedStats.totalRedirected += result.stats.storyValidation.redirected || 0;
                }
                
                if (result.stats.contactPageValidation) {
                    aggregatedStats.totalAvailable += result.stats.contactPageValidation.available || 0;
                    aggregatedStats.totalUnavailable += result.stats.contactPageValidation.unavailable || 0;
                    aggregatedStats.totalValid += result.stats.contactPageValidation.valid || 0;
                    aggregatedStats.totalInvalid += result.stats.contactPageValidation.invalid || 0;
                    aggregatedStats.totalRedirected += result.stats.contactPageValidation.redirected || 0;
                }
                
                if (result.stats.wrongContactPathValidation) {
                    aggregatedStats.totalInvalid += result.stats.wrongContactPathValidation.total || 0;
                }
                
                if (result.stats.externalLinkValidation) {
                    aggregatedStats.totalAvailable += result.stats.externalLinkValidation.available || 0;
                    aggregatedStats.totalUnavailable += result.stats.externalLinkValidation.unavailable || 0;
                    aggregatedStats.totalValid += result.stats.externalLinkValidation.valid || 0;
                    aggregatedStats.totalInvalid += result.stats.externalLinkValidation.invalid || 0;
                    aggregatedStats.totalRedirected += result.stats.externalLinkValidation.redirected || 0;
                }
            }
        });

        const endTime = Date.now();
        const durationMs = endTime - startTime;
        const durationMinutes = Math.round(durationMs / 1000 / 60 * 100) / 100;

      

        res.json({
            batchId: `batch_${Date.now()}`,
            completedAt: new Date().toISOString(),
            durationMinutes: durationMinutes,
            totalUrls: urls.length,
            successfulUrls: successfulResults.length,
            failedUrls: failedResults.length,
            results: successfulResults,
            failures: failedResults,
            summary: {
                totalLinksAcrossAllUrls: aggregatedStats.totalLinksAcrossAllUrls,
                aggregatedStats: aggregatedStats,
                averageLinksPerUrl: successfulResults.length > 0 ? 
                    Math.round(aggregatedStats.totalLinksAcrossAllUrls / successfulResults.length) : 0,
                topCategories: Object.entries(aggregatedStats.totalCategoryCounts)
                    .sort(([,a], [,b]) => b - a)
                    .slice(0, 5)
                    .map(([category, count]) => ({ category, count })),
                topSections: Object.entries(aggregatedStats.totalSectionCounts)
                    .sort(([,a], [,b]) => b - a)
                    .slice(0, 5)
                    .map(([section, count]) => ({ section, count }))
            }
        });

    } catch (error) {
        console.error('Batch audit error:', error.message);
        
        res.status(500).json({
            error: 'Internal server error during batch processing',
            details: error.message
        });
    }
});

export default router;