import { Router } from 'express';
import { getUser, getUserContributions, getUserRepos, getOrgContributions } from '../services/github.js';
import { validateUsername } from '../middleware/validate.js';

const router = Router();

// GET /api/user/:username — user profile
router.get('/user/:username', validateUsername, async (req, res, next) => {
  try {
    const user = await getUser(req.params.username as string);
    res.json({
      login: user.login,
      name: user.name,
      avatarUrl: user.avatar_url,
      publicRepos: user.public_repos,
      followers: user.followers,
      createdAt: user.created_at,
    });
  } catch (err) {
    next(err);
  }
});

// GET /api/user/:username/contributions — 12-month calendar + totals
router.get('/user/:username/contributions', validateUsername, async (req, res, next) => {
  try {
    const username = req.params.username as string;
    const forceRefresh = req.query.refresh === 'true';
    
    // Check if this is an organization
    const user = await getUser(username);
    if (user.type === 'Organization') {
      const data = await getOrgContributions(username, undefined, undefined, forceRefresh);
      return res.json({ ...data, createdAt: user.created_at });
    }
    
    const data = await getUserContributions(username, undefined, undefined, forceRefresh);
    res.json({ ...data, createdAt: user.created_at });
  } catch (err) {
    next(err);
  }
});

// GET /api/user/:username/stats — period stats for competition
router.get('/user/:username/stats', validateUsername, async (req, res, next) => {
  try {
    const { from, to, refresh } = req.query;
    if (!from || !to) {
      return res.status(400).json({ error: 'Missing required query parameters: from and to (ISO dates)' });
    }
    
    const username = req.params.username as string;
    const fromDate = from as string;
    const toDate = to as string;
    const forceRefresh = refresh === 'true';
    
    // Check if this is an organization
    const user = await getUser(username);
    if (user.type === 'Organization') {
      const data = await getOrgContributions(username, fromDate, toDate, forceRefresh);
      return res.json({ ...data, createdAt: user.created_at });
    }
    
    const data = await getUserContributions(username, fromDate, toDate, forceRefresh);
    res.json({ ...data, createdAt: user.created_at });
  } catch (err) {
    next(err);
  }
});

// GET /api/user/:username/repos — public repo list
router.get('/user/:username/repos', validateUsername, async (req, res, next) => {
  try {
    const repos = await getUserRepos(req.params.username as string);
    res.json(repos.map(repo => ({
      name: repo.name,
      fullName: repo.full_name,
      url: repo.html_url,
      description: repo.description,
      stars: repo.stargazers_count,
      forks: repo.forks_count,
      language: repo.language,
      updatedAt: repo.updated_at,
    })));
  } catch (err) {
    next(err);
  }
});

export { router as githubRouter };
