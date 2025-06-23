import express from 'express';
import axios from 'axios';
import { load } from 'cheerio';

const router = express.Router();

router.post('/', async (req, res) => {
  const { url } = req.body;

  const getLinksFromPage = async (url) => {
    try {
      const { data: html } = await axios.get(url);
      const $ = load(html);

      const getLinksFromSelector = (selector) => {
        return $(selector)
          .find('a')
          .map((_, el) => $(el).attr('href'))
          .get()
          .filter(Boolean); // remove null/undefined
      };

      const introLinks = getLinksFromSelector('.al-intro');
      const mainLinks = getLinksFromSelector('#al-main');

      const allLinks = [...introLinks, ...mainLinks]; 

    //   console.log(`✅ Total links (with duplicates) from ${url}: ${allLinks.length}`);
    //   allLinks.forEach((link, i) => {
    //     console.log(`${i + 1}. ${link}`);
    //   });

      res.status(200).json({
        message: 'Links extracted successfully',
        url: allLinks,
        totalLinks: allLinks.length
      });

    } catch (error) {
      console.error(`❌ Error fetching or parsing:`, error.message);
      res.status(500).json({ error: 'Failed to fetch or parse the page' });
    }
  };

  await getLinksFromPage(url);
});

export default router;
