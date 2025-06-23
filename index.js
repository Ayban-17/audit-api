import express from 'express';
import extractLinksRoutes from './routes/extract_links.js';

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

app.use('/api/v1/extract-links', extractLinksRoutes);

app.listen(3000, () => {
    console.log('Server is running on port 3000');
});