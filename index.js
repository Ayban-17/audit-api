import dotenv from 'dotenv';
dotenv.config();

import express from 'express';
import cors from 'cors';
import auditScriptRoutes from './routes/audit_script.js';

const app = express();
const PORT = process.env.PORT || 3000;

// Enable CORS for all routes
app.use(cors());

// Parse JSON bodies
app.use(express.json());

// Serve static files (we'll add the HTML interface here)
app.use(express.static('public'));

app.get('/', (req, res) => {
  res.send('Hello World!');
});

// API routes
app.use('/api/v1/audit-script', auditScriptRoutes);

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});