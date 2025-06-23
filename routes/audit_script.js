import express from 'express';
import axios from 'axios';
import { load } from 'cheerio';

const router = express.Router();

// Helper function to determine URL pattern (same as audit script)
function determineUrlPattern(href) {
  let path = href;
  
  // Handle full URLs (with domain)
  if (href.startsWith('http')) {
    try {
      const urlObj = new URL(href);
      const currentDomain = 'adventure-life.com'; // or get from request
      
      if (urlObj.hostname === currentDomain || 
          urlObj.hostname === `www.${currentDomain}`) {
        path = urlObj.pathname;
      } else {
        return 'external';
      }
    } catch (e) {
      return 'external';
    }
  }
  
  // Remove leading slash if present
  path = path.startsWith('/') ? path.substring(1) : path;
  
  // Split by slashes
  const segments = path.split('/');
  
  // Handle various URL patterns
  if (segments.length >= 2 && segments[1] === 'tours' && /^\d+$/.test(segments[2])) {
    return 'tour-with-id';
  }
  
  if (segments.length >= 3 && segments[1] === 'cruises' && /^\d+$/.test(segments[2])) {
    return 'cruise-ship';
  }
  
  if (segments.length >= 4 && segments[2] === 'cruises' && /^\d+$/.test(segments[3])) {
    return 'cruise-with-id';
  }
  
  if (segments.length >= 2 && segments[1] === 'tours' && segments[2] !== undefined && !/^\d+$/.test(segments[2])) {
    return 'tour-activity';
  }
  
  if (segments.length >= 2 && segments[1] === 'operators' && /^\d+$/.test(segments[2])) {
    return 'operator-with-id';
  }
  
  // Multi-level destinations
  if (segments.length === 1 && segments[0] !== '') {
    return 'multi-level/destination-1';
  }
  
  if (segments.length === 2 && !['tours', 'cruises', 'operators', 'articles', 'stories'].includes(segments[1])) {
    return 'multi-level/destination-2';
  }
  
  if (segments.length === 3 && !['tours', 'cruises', 'operators'].includes(segments[1])) {
    return 'multi-level/destination-3';
  }
  
  // Special destination pages
  if (segments.length >= 2 && ['land-tours', 'ships', 'videos', 'myTrips'].includes(segments[1])) {
    return 'destination-special-page';
  }
  
  // Articles and stories
  if (path.includes('/articles/')) return 'article';
  if (path.includes('/stories/')) return 'story';
  if (path.includes('/deals/')) return 'deal';
  
  return 'other';
}

// Helper function to extract text content
function extractTextContent(htmlString) {
  if (!htmlString) return '';
  
  return htmlString
    .replace(/<[^>]*>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, ' ')
    .trim();
}

// Function to determine section type based on classes
function determineSectionType($element) {
  const classList = $element.attr('class') || '';
  
  if (classList.includes('al-sumtiles')) return 'sumtiles';
  if (classList.includes('al-tiles')) return 'tiles';
  if (classList.includes('al-four')) return 'four';
  if (classList.includes('al-lp-table')) return 'table';
  if (classList.includes('al-text')) return 'text';
  if (classList.includes('al-stories')) return 'stories';
  if (classList.includes('al-articles')) return 'articles';
  
  return 'unknown';
}

// Function to check if link has description based on section type
function checkHasDescription($linkElement, sectionType) {
  const $link = $linkElement;
  
  // Check if it's in section title or button link
  const isInSectionTitle = $link.closest('.al-sec-title').length > 0;
  const isButtonLink = $link.hasClass('al-btn') || $link.find('.al-btn').length > 0;
  
  if (isInSectionTitle || isButtonLink) {
    return {
      hasDescription: false,
      reason: 'not_applicable',
      debugInfo: {
        isInSectionTitle,
        isButtonLink,
        sectionType
      }
    };
  }
  
  let hasDescription = false;
  let descriptionContent = '';
  let descriptionElementFound = false;
  
  switch (sectionType) {
    case 'four':
    case 'sumtiles':
      // Check .al-lnk-details
      const fourDetails = $link.find('.al-lnk-details');
      if (fourDetails.length > 0) {
        descriptionElementFound = true;
        descriptionContent = extractTextContent(fourDetails.html());
        hasDescription = descriptionContent.length > 0;
      }
      break;
      
    case 'table':
      // Check .al-lp-table-summary
      const tableSummary = $link.find('.al-lp-table-summary');
      if (tableSummary.length > 0) {
        descriptionElementFound = true;
        descriptionContent = extractTextContent(tableSummary.html());
        hasDescription = descriptionContent.length > 0;
      }
      break;
      
    case 'articles':
      // Only check if parent div has title attribute
      const parentDiv = $link.closest('div[title]');
      const parentDivHasTitle = parentDiv.length > 0;
      const titleAttributeValue = parentDiv.attr('title') || '';
      
      if (parentDivHasTitle) {
        const articleDetails = $link.find('.al-lnk-details');
        if (articleDetails.length > 0) {
          descriptionElementFound = true;
          descriptionContent = extractTextContent(articleDetails.html());
          hasDescription = descriptionContent.length > 0;
        }
      }
      
      return {
        hasDescription,
        reason: parentDivHasTitle ? 'checked' : 'parent_no_title',
        debugInfo: {
          isInSectionTitle,
          isButtonLink,
          parentDivHasTitle,
          titleAttributeValue,
          descriptionElementFound,
          descriptionContent: descriptionContent.substring(0, 100)
        }
      };
      
    default:
      // Other sections show as not applicable
      return {
        hasDescription: false,
        reason: 'not_applicable',
        debugInfo: {
          isInSectionTitle,
          isButtonLink,
          sectionType
        }
      };
  }
  
  return {
    hasDescription,
    reason: 'checked',
    debugInfo: {
      isInSectionTitle,
      isButtonLink,
      descriptionElementFound,
      descriptionContent: descriptionContent.substring(0, 100),
      sectionType
    }
  };
}

// Function to extract all links with metadata
function extractLinksWithMetadata($, selector, sectionName) {
  const links = [];
  
  $(selector).each((_, sectionElement) => {
    const $section = $(sectionElement);
    const sectionType = determineSectionType($section);
    
    $section.find('a').each((_, linkElement) => {
      const $link = $(linkElement);
      const href = $link.attr('href');
      
      if (!href || href === '#' || href.startsWith('javascript:')) {
        return; // Skip invalid links
      }
      
      // Extract text content with priority order
      let text = '';
      
      // 1. Try to get from .al-lnk-title h3
      const titleH3 = $link.find('.al-lnk-title h3');
      if (titleH3.length > 0) {
        text = extractTextContent(titleH3.html());
      }
      
      // 2. Try to get from any header tag
      if (!text) {
        const header = $link.find('h1, h2, h3, h4, h5, h6').first();
        if (header.length > 0) {
          text = extractTextContent(header.html());
        }
      }
      
      // 3. Use full link text as fallback
      if (!text) {
        text = extractTextContent($link.html());
      }
      
      // Clean up text
      text = text.substring(0, 200).trim();
      
      if (!text) {
        text = '[No text content]';
      }
      
      // Determine URL pattern
      const urlPattern = determineUrlPattern(href);
      
      // Check for description
      const descriptionCheck = checkHasDescription($link, sectionType);
      
      // Get section title
      const sectionTitle = $section.find('.al-sec-title h2').first();
      const sectionTitleText = sectionTitle.length > 0 ? extractTextContent(sectionTitle.html()) : '';
      
      links.push({
        text,
        href,
        urlPattern,
        section: sectionName,
        sectionType,
        sectionTitle: sectionTitleText,
        hasDescription: descriptionCheck.hasDescription,
        descriptionReason: descriptionCheck.reason,
        debugInfo: descriptionCheck.debugInfo
      });
    });
  });
  
  return links;
}

// Function to check link availability (simplified version)
async function checkLinkAvailability(link, baseUrl) {
  try {
    const fullUrl = link.href.startsWith('http') ? link.href : new URL(link.href, baseUrl).toString();
    
    // Basic availability check - just check if page loads
    const response = await axios.get(fullUrl, {
      timeout: 10000,
      validateStatus: function (status) {
        return status < 500; // Resolve only if status is less than 500
      }
    });
    
    if (response.status === 404) {
      return {
        available: false,
        status: 'broken_link_404',
        message: 'Page not found'
      };
    }
    
    if (response.status >= 400) {
      return {
        available: false,
        status: 'client_error',
        message: `HTTP ${response.status}`
      };
    }
    
    // For now, just check if page loads successfully
    // Future enhancement: add specific checks based on URL pattern
    return {
      available: true,
      status: 'available',
      message: 'Page loads successfully'
    };
    
  } catch (error) {
    if (error.code === 'ECONNABORTED') {
      return {
        available: false,
        status: 'timeout',
        message: 'Request timeout'
      };
    }
    
    return {
      available: false,
      status: 'error',
      message: error.message
    };
  }
}

router.post('/', async (req, res) => {
  const { url, checkAvailability = false } = req.body;

  try {
    console.log(`🔧 Starting enhanced link extraction for: ${url}`);
    
    const { data: html } = await axios.get(url);
    const $ = load(html);

    // Extract links from both sections with metadata
    const introLinks = extractLinksWithMetadata($, '.al-intro', 'intro');
    const mainLinks = extractLinksWithMetadata($, '#al-main', 'main');
    
    const allLinks = [...introLinks, ...mainLinks];
    
    console.log(`📊 Found ${allLinks.length} total links`);
    
    // Group links by URL pattern
    const linksByPattern = {};
    allLinks.forEach(link => {
      if (!linksByPattern[link.urlPattern]) {
        linksByPattern[link.urlPattern] = [];
      }
      linksByPattern[link.urlPattern].push(link);
    });
    
    // Group links by section type
    const linksBySection = {};
    allLinks.forEach(link => {
      if (!linksBySection[link.sectionType]) {
        linksBySection[link.sectionType] = [];
      }
      linksBySection[link.sectionType].push(link);
    });
    
    // Generate summary statistics
    const summary = {
      totalLinks: allLinks.length,
      introLinks: introLinks.length,
      mainLinks: mainLinks.length,
      linksByPattern: Object.keys(linksByPattern).map(pattern => ({
        pattern,
        count: linksByPattern[pattern].length
      })),
      linksBySection: Object.keys(linksBySection).map(section => ({
        section,
        count: linksBySection[section].length
      })),
      withDescriptions: allLinks.filter(link => link.hasDescription).length,
      withoutDescriptions: allLinks.filter(link => !link.hasDescription).length
    };
    
    let results = {
      message: 'Links extracted successfully',
      url,
      summary,
      links: allLinks,
      linksByPattern,
      linksBySection
    };
    
    // Optional availability checking
    if (checkAvailability) {
      console.log(`🔍 Checking availability for ${allLinks.length} links...`);
      
      const availabilityResults = [];
      let completed = 0;
      
      // Check availability in batches to avoid overwhelming the server
      const batchSize = 5;
      for (let i = 0; i < allLinks.length; i += batchSize) {
        const batch = allLinks.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (link) => {
          const result = await checkLinkAvailability(link, url);
          completed++;
          console.log(`✅ Checked ${completed}/${allLinks.length}: ${link.href}`);
          return {
            ...link,
            availability: result
          };
        });
        
        const batchResults = await Promise.all(batchPromises);
        availabilityResults.push(...batchResults);
        
        // Small delay between batches
        if (i + batchSize < allLinks.length) {
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      }
      
      // Update summary with availability stats
      const availableCount = availabilityResults.filter(link => link.availability.available).length;
      const unavailableCount = availabilityResults.filter(link => !link.availability.available).length;
      
      results.summary.availability = {
        checked: true,
        available: availableCount,
        unavailable: unavailableCount,
        availabilityRate: ((availableCount / allLinks.length) * 100).toFixed(2) + '%'
      };
      
      results.links = availabilityResults;
    }
    
    console.log(`✅ Extraction complete!`);
    res.status(200).json(results);

  } catch (error) {
    console.error(`❌ Error during extraction:`, error.message);
    res.status(500).json({ 
      error: 'Failed to extract and analyze links',
      message: error.message 
    });
  }
});

export default router;