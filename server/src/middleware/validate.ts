import { Request, Response, NextFunction } from 'express';

// GitHub username: alphanumeric + hyphens, max 39 chars
const USERNAME_REGEX = /^[a-zA-Z0-9](?:[a-zA-Z0-9]|-(?=[a-zA-Z0-9])){0,38}$/;

export function validateUsername(req: Request, res: Response, next: NextFunction) {
  const username = req.params.username as string;
  if (!username || !USERNAME_REGEX.test(username)) {
    return res.status(400).json({ error: 'Invalid username format. Use alphanumeric characters and hyphens only (max 39 chars).' });
  }
  next();
}
