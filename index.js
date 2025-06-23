import express from 'express';
import auditScriptRoutes from './routes/audit_script.js';

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

app.get('/', (req, res) => {
    res.send('Hello World');
});

app.use('/api/v1/audit-script', auditScriptRoutes);

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});