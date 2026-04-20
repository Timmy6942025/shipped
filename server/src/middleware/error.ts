import { Request, Response, NextFunction } from 'express';

export function errorMiddleware(err: Error, _req: Request, res: Response, _next: NextFunction) {
  console.error('Server error:', err.message);
  
  if (err.message.includes('401') || err.message.includes('403')) {
    return res.status(401).json({ error: 'GitHub authentication failed. Check your token.' });
  }
  if (err.message.includes('404')) {
    return res.status(404).json({ error: 'User not found on GitHub.' });
  }
  if (err.message.includes('rate limit') || err.message.includes('429')) {
    return res.status(429).json({ error: 'GitHub API rate limit exceeded. Please try again later.' });
  }
  
  res.status(500).json({ error: err.message || 'Internal server error' });
}
