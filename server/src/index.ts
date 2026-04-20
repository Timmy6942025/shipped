import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import { GITHUB_TOKEN } from './config.js';
import { healthRouter } from './routes/health.js';
import { githubRouter } from './routes/github.js';
import { errorMiddleware } from './middleware/error.js';
import { cache } from './services/cache.js';

const app = express();
const PORT = 3001;

if (!GITHUB_TOKEN) {
  throw new Error('GITHUB_TOKEN is required');
}

app.use(cors());
app.use(express.json());

// Serve static frontend from public/
const __dirname = path.dirname(fileURLToPath(import.meta.url));
app.use(express.static(path.join(__dirname, '..', 'public')));

app.use('/api/health', healthRouter);
app.use('/api', githubRouter);

app.use(errorMiddleware);

cache.startCleanup();

app.listen(PORT, () => {
  console.log(`GitHub Ship Calendar server running on http://localhost:${PORT}`);
});
