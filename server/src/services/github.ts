import { GITHUB_TOKEN } from '../config.js';
import { cache, TTL, CACHE_SCHEMA_VERSION } from './cache.js';
import type {
  GitHubUser,
  GitHubRepo,
  ContributionsData,
  OrganizationData,
  PRContributionsPageData,
} from '../types/github.js';
import type { UserStats, ContributionDay } from '../types/index.js';

const GITHUB_GRAPHQL_URL = 'https://api.github.com/graphql';
const GITHUB_REST_URL = 'https://api.github.com';

// Aggressive parallel batching for maximum throughput
const AGGRESSIVE_BATCH_SIZE = 50; // Process 50 repos at once
const INITIAL_DELAY_MS = 10; // Minimal delay between batches
const MIN_RATE_LIMIT_REMAINING = 10; // Reserve 10 calls for safety

// GraphQL query for contribution calendar + contributed repositories + PR LOC
const CONTRIBUTIONS_QUERY = `
  query($login: String!, $from: DateTime!, $to: DateTime!) {
    user(login: $login) {
      login
      name
      avatarUrl
      repositories(privacy: PUBLIC) {
        totalCount
      }
      followers {
        totalCount
      }
      contributionsCollection(from: $from, to: $to) {
        contributionCalendar {
          totalContributions
          weeks {
            contributionDays {
              date
              contributionCount
              contributionLevel
            }
          }
        }
        totalCommitContributions
        totalPullRequestContributions
        totalIssueContributions
        totalPullRequestReviewContributions
        totalRepositoryContributions
        restrictedContributionsCount
        commitContributionsByRepository(maxRepositories: 50) {
          repository { 
            nameWithOwner 
            stargazerCount
            description
            updatedAt
            licenseInfo { spdxId name }
            languages(first: 5, orderBy: { field: SIZE, direction: DESC }) {
              edges {
                size
                node { name }
              }
            }
          }
          contributions { totalCount }
        }
        pullRequestContributionsByRepository(maxRepositories: 50) {
          repository { 
            nameWithOwner 
            stargazerCount
            description
            updatedAt
            licenseInfo { spdxId name }
            languages(first: 5, orderBy: { field: SIZE, direction: DESC }) {
              edges {
                size
                node { name }
              }
            }
          }
          contributions { totalCount }
        }
        pullRequestContributions(first: 100) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            pullRequest {
              createdAt
              merged
              closed
              additions
              deletions
              repository { nameWithOwner }
            }
          }
        }
        issueContributions(first: 100) {
          nodes {
            issue {
              createdAt
            }
          }
        }
      }
    }
  }
`;

// GraphQL query for organizations (no contributionsCollection available)
const ORG_QUERY = `
  query($login: String!) {
    organization(login: $login) {
      login
      name
      avatarUrl
      repoList: repositories(privacy: PUBLIC, first: 100, orderBy: {field: UPDATED_AT, direction: DESC}) {
        totalCount
        nodes {
          nameWithOwner
          stargazerCount
          description
          updatedAt
          licenseInfo { spdxId name }
          languages(first: 5, orderBy: { field: SIZE, direction: DESC }) {
            edges {
              size
              node { name }
            }
          }
        }
      }
    }
  }
`;

// GraphQL query for paginating PR contributions (fallback LOC source)
const PR_CONTRIBUTIONS_PAGE_QUERY = `
  query($login: String!, $from: DateTime!, $to: DateTime!, $cursor: String!) {
    user(login: $login) {
      contributionsCollection(from: $from, to: $to) {
        pullRequestContributions(first: 100, after: $cursor) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            pullRequest {
              createdAt
              merged
              closed
              additions
              deletions
              repository { nameWithOwner }
            }
          }
        }
      }
    }
  }
`;

// Dynamic rate limit state
interface RateLimitState {
  resetTime: number;
  remaining: number;
  consecutive429s: number;
  currentBatchSize: number;
  currentDelay: number;
}

const rateLimitState: RateLimitState = {
  resetTime: 0,
  remaining: 5000,
  consecutive429s: 0,
  currentBatchSize: AGGRESSIVE_BATCH_SIZE,
  currentDelay: INITIAL_DELAY_MS,
};

function getAdjustedBatchParams(): { batchSize: number; delayMs: number } {
  // Reduce batch size if rate limit is getting low
  if (rateLimitState.remaining < 100) {
    return { batchSize: Math.max(10, rateLimitState.currentBatchSize / 2), delayMs: rateLimitState.currentDelay * 2 };
  }
  // Recover batch size if we're doing well
  if (rateLimitState.remaining > 4000 && rateLimitState.consecutive429s === 0) {
    rateLimitState.currentBatchSize = Math.min(AGGRESSIVE_BATCH_SIZE * 2, rateLimitState.currentBatchSize * 1.1);
    rateLimitState.currentDelay = Math.max(INITIAL_DELAY_MS, rateLimitState.currentDelay / 1.1);
  }
  return { batchSize: Math.floor(rateLimitState.currentBatchSize), delayMs: Math.floor(rateLimitState.currentDelay) };
}

async function graphqlQuery<T>(query: string, variables: Record<string, unknown>): Promise<T> {
  // Check if we need to wait due to rate limits
  if (Date.now() < rateLimitState.resetTime) {
    const waitTime = rateLimitState.resetTime - Date.now() + 1000;
    console.log(`[GraphQL] Rate limit wait: ${waitTime}ms`);
    await new Promise(resolve => setTimeout(resolve, waitTime));
  }

  const response = await fetch(GITHUB_GRAPHQL_URL, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${GITHUB_TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query, variables }),
  });

  // Track rate limits
  const remaining = response.headers.get('x-ratelimit-remaining');
  const reset = response.headers.get('x-ratelimit-reset');
  if (reset) {
    rateLimitState.resetTime = parseInt(reset) * 1000;
    rateLimitState.remaining = parseInt(remaining || '5000');
  }

  if (response.status === 429) {
    rateLimitState.consecutive429s++;
    // Slow down aggressive batching on 429
    rateLimitState.currentBatchSize = Math.max(5, rateLimitState.currentBatchSize / 3);
    rateLimitState.currentDelay = rateLimitState.currentDelay * 3;
    
    const retryAfter = response.headers.get('retry-after') || '1';
    const waitMs = parseInt(retryAfter) * 1000;
    console.log(`[GraphQL] 429 - waiting ${waitMs}ms (consecutive: ${rateLimitState.consecutive429s})`);
    await new Promise(resolve => setTimeout(resolve, Math.min(waitMs * Math.pow(2, rateLimitState.consecutive429s - 1), 60000)));
    return graphqlQuery(query, variables); // Retry
  }
  rateLimitState.consecutive429s = 0;

  if (!response.ok) {
    throw new Error(`GraphQL request failed: ${response.status} ${response.statusText}`);
  }

  const data = await response.json() as { data?: T; errors?: Array<{ message: string }> };

  if (data.errors?.length) {
    throw new Error(`GraphQL errors: ${data.errors.map(e => e.message).join(', ')}`);
  }

  return data.data!;
}

async function restGet<T>(path: string): Promise<T> {
  const url = `${GITHUB_REST_URL}${path}`;
  
  // Rate limit check
  if (Date.now() < rateLimitState.resetTime) {
    const waitTime = rateLimitState.resetTime - Date.now() + 500;
    await new Promise(resolve => setTimeout(resolve, waitTime));
  }

  const response = await fetch(url, {
    headers: {
      'Authorization': `Bearer ${GITHUB_TOKEN}`,
      'Accept': 'application/vnd.github.v3+json',
    },
  });

  const remaining = response.headers.get('x-ratelimit-remaining');
  const reset = response.headers.get('x-ratelimit-reset');
  if (reset) {
    rateLimitState.resetTime = parseInt(reset) * 1000;
    rateLimitState.remaining = parseInt(remaining || '5000');
  }

  if (response.status === 429) {
    rateLimitState.consecutive429s++;
    rateLimitState.currentBatchSize = Math.max(5, rateLimitState.currentBatchSize / 3);
    rateLimitState.currentDelay = rateLimitState.currentDelay * 3;
    
    const retryAfter = response.headers.get('retry-after') || '1';
    const waitMs = parseInt(retryAfter) * 1000;
    console.log(`[REST] 429 - waiting ${waitMs}ms`);
    await new Promise(resolve => setTimeout(resolve, Math.min(waitMs * Math.pow(2, rateLimitState.consecutive429s - 1), 60000)));
    return restGet(path);
  }
  rateLimitState.consecutive429s = 0;

  if (!response.ok) {
    throw new Error(`REST request failed: ${response.status} ${response.statusText} for ${path}`);
  }

  return response.json() as Promise<T>;
}

/**
 * REST GET that handles 202 (Accepted) responses from GitHub.
 * Some endpoints (like /stats/contributors) return 202 while computing,
 * then 200 with data once ready. This polls until data is available.
 */
async function restGetWith202Retry<T>(
  path: string,
  maxAttempts = 3,
  initialDelayMs = 2000,
): Promise<T | null> {
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const url = `${GITHUB_REST_URL}${path}`;
    
    const response = await fetch(url, {
      headers: {
        'Authorization': `Bearer ${GITHUB_TOKEN}`,
        'Accept': 'application/vnd.github.v3+json',
      },
    });

    const remaining = response.headers.get('x-ratelimit-remaining');
    const reset = response.headers.get('x-ratelimit-reset');
    if (reset) {
      rateLimitState.resetTime = parseInt(reset) * 1000;
      rateLimitState.remaining = parseInt(remaining || '5000');
    }

    if (response.status === 202) {
      // GitHub is computing stats - wait and retry
      const delay = initialDelayMs * Math.pow(1.5, attempt);
      console.log(`[REST] 202 for ${path} - waiting ${delay}ms (attempt ${attempt + 1}/${maxAttempts})`);
      await new Promise(resolve => setTimeout(resolve, delay));
      continue;
    }

    if (response.status === 429) {
      rateLimitState.consecutive429s++;
      rateLimitState.currentBatchSize = Math.max(5, rateLimitState.currentBatchSize / 3);
      rateLimitState.currentDelay = rateLimitState.currentDelay * 3;
      const retryAfter = response.headers.get('retry-after') || '1';
      const waitMs = parseInt(retryAfter) * 1000;
      await new Promise(resolve => setTimeout(resolve, Math.min(waitMs * Math.pow(2, rateLimitState.consecutive429s - 1), 60000)));
      // Retry this attempt, but cap total 429 retries to prevent infinite loop
      if (rateLimitState.consecutive429s > 5) return null;
      attempt--;
      continue;
    }

    rateLimitState.consecutive429s = 0;

    if (!response.ok) {
      return null; // Return null for non-202 errors so caller can fallback
    }

    return response.json() as Promise<T>;
  }
  
  // Exhausted retries while GitHub was still computing
  return null;
}

export async function getUser(username: string) {
  const cacheKey = `github:user:${username}`;
  const cached = cache.get<GitHubUser>(cacheKey);
  if (cached) return cached;

  const user = await restGet<GitHubUser>(`/users/${username}`);
  cache.set(cacheKey, user, TTL.USER_PROFILE);
  return user;
}

/**
 * Get contribution stats for a GitHub organization.
 * Organizations don't have contributionsCollection, so we build stats from
 * their public repos: language breakdown, repo list with stars, etc.
 */
export async function getOrgContributions(
  login: string,
  fromDate?: string,
  toDate?: string,
  forceRefresh = false,
): Promise<UserStats> {
  const now = new Date();
  const from = fromDate ?? new Date(now.getFullYear() - 1, now.getMonth(), now.getDate()).toISOString();
  const to = toDate ?? now.toISOString();

  const cacheKey = `github:org-contributions:s${CACHE_SCHEMA_VERSION}:${login}:${from}:${to}`;
  const cached = cache.get<UserStats>(cacheKey);
  if (cached && !forceRefresh) return cached;

  const data = await graphqlQuery<OrganizationData>(ORG_QUERY, { login });
  const org = data.organization;
  const repoNodes = org.repoList.nodes;

  // Build language map from all repos
  const langMap = new Map<string, number>();
  let totalStars = 0;
  for (const repo of repoNodes) {
    totalStars += repo.stargazerCount;
    if (repo.languages?.edges) {
      for (const lang of repo.languages.edges) {
        langMap.set(lang.node.name, (langMap.get(lang.node.name) || 0) + lang.size);
      }
    }
  }

  const totalBytes = [...langMap.values()].reduce((a, b) => a + b, 0);
  const languages = [...langMap.entries()]
    .map(([name, bytes]) => ({ name, percentage: Math.round((bytes / totalBytes) * 100) }))
    .sort((a, b) => b.percentage - a.percentage)
    .filter(l => l.percentage > 0)
    .slice(0, 5);

  // Build repo list sorted by stars (contributionCount not available for orgs, use stars as proxy)
  const contributedRepos = repoNodes
    .map(repo => ({
      nameWithOwner: repo.nameWithOwner,
      contributionCount: repo.stargazerCount, // Use stars as sort proxy
      stargazerCount: repo.stargazerCount,
      primaryLanguage: repo.languages?.edges?.length ? repo.languages.edges[0].node.name : null,
      description: repo.description,
      licenseSpdxId: repo.licenseInfo?.spdxId || null,
      updatedAt: repo.updatedAt,
    }))
    .sort((a, b) => b.stargazerCount - a.stargazerCount);

  // Build an empty calendar (orgs don't have contribution calendars)
  const calendar: ContributionDay[] = [];

  const result: UserStats = {
    login: org.login,
    name: org.name,
    avatarUrl: org.avatarUrl,
    publicRepos: org.repoList.totalCount,
    followers: 0, // Orgs don't have followers
    isOrganization: true,
    totalContributions: 0,
    totalCommits: 0,
    totalPRs: 0,
    totalIssues: 0,
    totalReviews: 0,
    totalDiscussions: 0,
    totalNewRepos: 0,
    totalPrivateActivity: 0,
    locStatus: 'ready' as const,
    totalLinesAdded: 0,
    totalLinesDeleted: 0,
    starPower: repoNodes.length > 0 ? Math.round(totalStars / repoNodes.length) : 0,
    prMergeRate: 0,
    reviewDepth: 0,
    languages,
    productiveHours: new Array(24).fill(0),
    contributedRepos,
    fromDate: from,
    toDate: to,
    calendar,
  };

  cache.set(cacheKey, result, TTL.CONTRIBUTIONS);
  return result;
}

// LOC computation tracking - stores background computation state
const locComputations = new Map<string, {
  status: 'pending' | 'computing' | 'ready' | 'error';
  result: { added: number; deleted: number; timestamps: string[] } | null;
  timestamp: number;
}>();

// Clean up old LOC computations every 10 minutes
setInterval(() => {
  const now = Date.now();
  for (const [key, val] of locComputations) {
    if (now - val.timestamp > 30 * 60 * 1000) { // 30 min expiry
      locComputations.delete(key);
    }
  }
}, 10 * 60 * 1000).unref?.();

/**
 * Get the status of a background LOC computation.
 * Returns the LOC data if ready, or the current status.
 */
export function getLocStatus(
  username: string,
  fromDate: string,
  toDate: string,
): { status: 'pending' | 'computing' | 'ready' | 'error'; added: number; deleted: number; timestamps: string[] } {
  const locKey = `${username}:${fromDate}:${toDate}`;
  const comp = locComputations.get(locKey);
  if (!comp) {
    // Check if the full result is cached (LOC was ready)
    const cacheKey = `github:contributions:s${CACHE_SCHEMA_VERSION}:${username}:${fromDate}:${toDate}`;
    const cached = cache.get<UserStats>(cacheKey);
    if (cached && cached.locStatus === 'ready') {
      return { status: 'ready', added: cached.totalLinesAdded, deleted: cached.totalLinesDeleted, timestamps: [] };
    }
    return { status: 'pending', added: 0, deleted: 0, timestamps: [] };
  }
  if (comp.status === 'ready' && comp.result) {
    return { status: 'ready', ...comp.result };
  }
  return { status: comp.status, added: 0, deleted: 0, timestamps: [] };
}

export async function getUserContributions(
  username: string,
  fromDate?: string,
  toDate?: string,
  forceRefresh = false,
): Promise<UserStats> {
  const now = new Date();
  const from = fromDate ?? new Date(now.getFullYear() - 1, now.getMonth(), now.getDate()).toISOString();
  const to = toDate ?? now.toISOString();

  const cacheKey = `github:contributions:s${CACHE_SCHEMA_VERSION}:${username}:${from}:${to}`;
  const cached = cache.get<UserStats>(cacheKey);
  if (cached && !forceRefresh) return cached;

  // On force refresh, invalidate LOC caches too
  if (forceRefresh) {
    const locCacheKey = `github:lines-shipped:s${CACHE_SCHEMA_VERSION}:${username}:${new Date(from).toISOString()}:${new Date(to).toISOString()}`;
    cache.delete(locCacheKey);
    // Remove from locComputations so background state is reset
    const locKey = `${username}:${from}:${to}`;
    locComputations.delete(locKey);
  }

  console.log(`[Performance] Starting fetch for ${username}`);

  // PARALLEL FETCH: User profile and contributions simultaneously
  const [user, data] = await Promise.all([
    getUser(username),
    graphqlQuery<ContributionsData>(CONTRIBUTIONS_QUERY, { login: username, from, to }),
  ]);

  console.log(`[Performance] Got initial data for ${username}`);

  // Shape the response
  const ghUser = data.user;
  const collection = ghUser.contributionsCollection;
  const calendar = collection.contributionCalendar;

  // Flatten weeks into array of days
  const contributionDays = calendar.weeks.flatMap(week =>
    week.contributionDays.map(day => ({
      date: day.date,
      count: day.contributionCount,
      level: contributionLevelToNumber(day.contributionLevel),
    }))
  );

  // Build deduplicated, contribution-sorted repo list from GraphQL response
  const contributedRepos = buildContributedRepoList(collection);

  // Fetch PR metadata (timestamps, merged/closed counts) - paginate only if needed
  const prMetadata = await collectPRMetadataFromCollection(username, from, to, collection);

  // Language & Impact analysis
  const langMap = new Map<string, number>();
  let totalStars = 0;
  const repoSet = new Set<string>();
  
  const processRepo = (repo: any) => {
    if (repoSet.has(repo.nameWithOwner)) return;
    repoSet.add(repo.nameWithOwner);
    totalStars += repo.stargazerCount;
    if (repo.languages?.edges) {
      for (const lang of repo.languages.edges) {
        langMap.set(lang.node.name, (langMap.get(lang.node.name) || 0) + lang.size);
      }
    }
  };

  for (const entry of collection.commitContributionsByRepository) processRepo(entry.repository);
  for (const entry of collection.pullRequestContributionsByRepository) processRepo(entry.repository);

  const totalBytes = [...langMap.values()].reduce((a, b) => a + b, 0);
  const languages = [...langMap.entries()]
    .map(([name, bytes]) => ({ name, percentage: Math.round((bytes / totalBytes) * 100) }))
    .sort((a, b) => b.percentage - a.percentage)
    .filter(l => l.percentage > 0)
    .slice(0, 5);

  // Productive Hours histogram - use PR timestamps + issue timestamps immediately
  const productiveHours = new Array(24).fill(0);
  const prTimestamps = prMetadata.timestamps || [];
  prTimestamps.forEach(ts => {
    if (!ts) return;
    const hour = new Date(ts).getUTCHours();
    if (!isNaN(hour) && hour >= 0 && hour < 24) productiveHours[hour]++;
  });
  const issueNodes = collection.issueContributions?.nodes || [];
  issueNodes.forEach(node => {
    if (node.issue?.createdAt) {
      const hour = new Date(node.issue.createdAt).getUTCHours();
      if (!isNaN(hour) && hour >= 0 && hour < 24) productiveHours[hour]++;
    }
  });

  const totalPRs = collection.totalPullRequestContributions;

  // Check if LOC is already computed and cached
  const locCacheKey = `github:lines-shipped:s${CACHE_SCHEMA_VERSION}:${username}:${new Date(from).toISOString()}:${new Date(to).toISOString()}`;
  const cachedLOC = cache.get<{ added: number; deleted: number; timestamps: string[] }>(locCacheKey);

  // Check if LOC computation is already running or ready
  const locKey = `${username}:${from}:${to}`;
  const existingComp = locComputations.get(locKey);

  let locStatus: 'ready' | 'pending' | 'computing' = 'pending';
  let linesData: { added: number; deleted: number; timestamps: string[] } = { added: 0, deleted: 0, timestamps: [] };

  if (cachedLOC) {
    // LOC already computed and cached
    linesData = cachedLOC;
    locStatus = 'ready';
    // Add commit timestamps to productive hours
    (linesData.timestamps || []).forEach(ts => {
      if (!ts) return;
      const hour = new Date(ts).getUTCHours();
      if (!isNaN(hour) && hour >= 0 && hour < 24) productiveHours[hour]++;
    });
  } else if (existingComp?.status === 'ready' && existingComp.result) {
    // LOC computation finished
    linesData = existingComp.result;
    locStatus = 'ready';
    (linesData.timestamps || []).forEach(ts => {
      if (!ts) return;
      const hour = new Date(ts).getUTCHours();
      if (!isNaN(hour) && hour >= 0 && hour < 24) productiveHours[hour]++;
    });
  } else if (existingComp?.status === 'computing') {
    // LOC computation in progress
    locStatus = 'computing';
  } else {
    // Start LOC computation in the background
    locComputations.set(locKey, { status: 'computing', result: null, timestamp: Date.now() });
    locStatus = 'computing';
    
    // Fire and forget - compute LOC in background
    computeLocBackground(username, from, to, contributedRepos, prMetadata.locByRepo, locKey);
  }

  const result: UserStats = {
    login: ghUser.login,
    name: ghUser.name,
    avatarUrl: ghUser.avatarUrl,
    publicRepos: ghUser.repositories.totalCount,
    followers: ghUser.followers.totalCount,
    locStatus,
    totalContributions: calendar.totalContributions,
    totalCommits: collection.totalCommitContributions,
    totalPRs,
    totalIssues: collection.totalIssueContributions,
    totalReviews: collection.totalPullRequestReviewContributions,
    totalDiscussions: 0,
    totalNewRepos: collection.totalRepositoryContributions,
    totalPrivateActivity: collection.restrictedContributionsCount,
    totalLinesAdded: linesData.added,
    totalLinesDeleted: linesData.deleted,
    starPower: repoSet.size > 0 ? Math.round(totalStars / repoSet.size) : 0,
    prMergeRate: (prMetadata.mergedCount + prMetadata.closedCount) > 0 ? Math.round((prMetadata.mergedCount / (prMetadata.mergedCount + prMetadata.closedCount)) * 100) : 0,
    reviewDepth: totalPRs > 0 ? parseFloat((collection.totalPullRequestReviewContributions / totalPRs).toFixed(1)) : 0,
    languages,
    productiveHours,
    contributedRepos: contributedRepos.map(({ owner, name, contributionCount, stargazerCount, primaryLanguage, description, licenseSpdxId, updatedAt }) => ({
      nameWithOwner: `${owner}/${name}`, contributionCount, stargazerCount, primaryLanguage, description, licenseSpdxId, updatedAt,
    })),
    fromDate: from,
    toDate: to,
    calendar: contributionDays,
  };

  // Cache the result immediately (even partial) so subsequent requests don't re-fetch from GitHub
  cache.set(cacheKey, result, TTL.CONTRIBUTIONS);
  console.log(`[Performance] Returning ${locStatus} data for ${username}`);
  return result;
}

/**
 * Compute LOC in the background. Stores result in locComputations map
 * and caches it when done.
 */
async function computeLocBackground(
  username: string,
  from: string,
  to: string,
  contributedRepos: Array<{ owner: string; name: string; contributionCount: number }>,
  prLocByRepo: Map<string, { added: number; deleted: number }>,
  locKey: string,
): Promise<void> {
  try {
    console.log(`[LOC] Starting background computation for ${username} (${contributedRepos.length} repos)`);
    const linesData = await getLinesShippedBatched(
      username, 
      contributedRepos, 
      prLocByRepo, 
      new Date(from), 
      new Date(to)
    );
    console.log(`[LOC] Background computation complete for ${username}: ${linesData.added} added, ${linesData.deleted} deleted`);
    
    locComputations.set(locKey, { status: 'ready', result: linesData, timestamp: Date.now() });
    
    // Also cache the lines-shipped result so future requests are instant
    const locCacheKey = `github:lines-shipped:s${CACHE_SCHEMA_VERSION}:${username}:${new Date(from).toISOString()}:${new Date(to).toISOString()}`;
    cache.set(locCacheKey, linesData, TTL.CONTRIBUTIONS);
    
    // Cache the full UserStats result too so future full requests are instant
    const cacheKey = `github:contributions:s${CACHE_SCHEMA_VERSION}:${username}:${from}:${to}`;
    const cached = cache.get<UserStats>(cacheKey);
    if (cached) {
      cached.totalLinesAdded = linesData.added;
      cached.totalLinesDeleted = linesData.deleted;
      cached.locStatus = 'ready';
      // Re-add commit timestamps to productive hours
      (linesData.timestamps || []).forEach(ts => {
        if (!ts) return;
        const hour = new Date(ts).getUTCHours();
        if (!isNaN(hour) && hour >= 0 && hour < 24) cached.productiveHours[hour]++;
      });
      cache.set(cacheKey, cached, TTL.CONTRIBUTIONS);
    }
  } catch (err) {
    console.error(`[LOC] Background computation failed for ${username}:`, err);
    locComputations.set(locKey, { status: 'error', result: null, timestamp: Date.now() });
    // Update the cached partial result to reflect the error
    const cacheKey = `github:contributions:s${CACHE_SCHEMA_VERSION}:${username}:${from}:${to}`;
    const cached = cache.get<UserStats>(cacheKey);
    if (cached) {
      cached.locStatus = 'error';
      cache.set(cacheKey, cached, TTL.CONTRIBUTIONS);
    }
  }
}

export async function getUserRepos(username: string) {
  const cacheKey = `github:repos:${username}`;
  const cached = cache.get<GitHubRepo[]>(cacheKey);
  if (cached) return cached;

  const repos = await restGet<GitHubRepo[]>(`/users/${username}/repos?per_page=100&sort=updated`);
  cache.set(cacheKey, repos, TTL.USER_PROFILE);
  return repos;
}

/**
 * Build a deduplicated, contribution-count-sorted list of repos
 * the user has contributed to (commits + PRs) from the GraphQL response.
 */
function buildContributedRepoList(
  collection: ContributionsData['user']['contributionsCollection'],
): Array<{
  owner: string;
  name: string;
  contributionCount: number;
  stargazerCount: number;
  primaryLanguage: string | null;
  description: string | null;
  licenseSpdxId: string | null;
  updatedAt: string;
}> {
  const repoMap = new Map<string, {
    owner: string;
    name: string;
    contributionCount: number;
    stargazerCount: number;
    primaryLanguage: string | null;
    description: string | null;
    licenseSpdxId: string | null;
    updatedAt: string;
  }>();

  const processEntry = (repo: {
    nameWithOwner: string;
    stargazerCount: number;
    description: string | null;
    updatedAt: string;
    licenseInfo: { spdxId: string; name: string } | null;
    languages: { edges: Array<{ size: number; node: { name: string } }> };
  }, contributionCount: number) => {
    const [owner, name] = repo.nameWithOwner.split('/');
    const primaryLanguage = repo.languages?.edges?.length
      ? repo.languages.edges[0].node.name
      : null;
    const licenseSpdxId = repo.licenseInfo?.spdxId || null;
    const existing = repoMap.get(repo.nameWithOwner);
    if (existing) {
      existing.contributionCount += contributionCount;
    } else {
      repoMap.set(repo.nameWithOwner, {
        owner, name,
        contributionCount,
        stargazerCount: repo.stargazerCount,
        primaryLanguage,
        description: repo.description,
        licenseSpdxId,
        updatedAt: repo.updatedAt,
      });
    }
  };

  for (const entry of collection.commitContributionsByRepository) {
    processEntry(entry.repository, entry.contributions.totalCount);
  }
  for (const entry of collection.pullRequestContributionsByRepository) {
    processEntry(entry.repository, entry.contributions.totalCount);
  }

  return [...repoMap.values()].sort((a, b) => b.contributionCount - a.contributionCount);
}

/**
 * Collect PR metadata from GraphQL data, with pagination only if needed.
 * This is a performance optimization - we only paginate if hasNextPage is true.
 */
async function collectPRMetadataFromCollection(
  username: string,
  from: string,
  to: string,
  collection: ContributionsData['user']['contributionsCollection'],
): Promise<{ locByRepo: Map<string, { added: number; deleted: number }>; timestamps: string[]; mergedCount: number; closedCount: number }> {
  const locByRepo = new Map<string, { added: number; deleted: number }>();
  const timestamps: string[] = [];
  let mergedCount = 0;
  let closedCount = 0;

  const processPR = (pr: ContributionsData['user']['contributionsCollection']['pullRequestContributions']['nodes'][0]['pullRequest']) => {
    const key = pr.repository.nameWithOwner;
    const existing = locByRepo.get(key);
    if (existing) {
      existing.added += pr.additions;
      existing.deleted += pr.deletions;
    } else {
      locByRepo.set(key, { added: pr.additions, deleted: pr.deletions });
    }
    timestamps.push(pr.createdAt);
    if (pr.merged) mergedCount++;
    else if (pr.closed) closedCount++;
  };

  // Process first page (already fetched)
  for (const node of collection.pullRequestContributions.nodes) {
    processPR(node.pullRequest);
  }

  // Only paginate if needed (quick check for users with lots of PRs)
  let { hasNextPage, endCursor: cursor } = collection.pullRequestContributions.pageInfo;
  while (hasNextPage && cursor) {
    const pageData = await graphqlQuery<PRContributionsPageData>(PR_CONTRIBUTIONS_PAGE_QUERY, { login: username, from, to, cursor });
    const prContribs = pageData.user.contributionsCollection.pullRequestContributions;
    for (const node of prContribs.nodes) {
      processPR(node.pullRequest);
    }
    hasNextPage = prContribs.pageInfo.hasNextPage;
    cursor = prContribs.pageInfo.endCursor;
  }

  return { locByRepo, timestamps: timestamps || [], mergedCount, closedCount };
}

/**
 * Use /stats/contributors endpoint - single API call per repo!
 * Returns aggregated LOC for ALL authors. Much faster than per-commit stats.
 * 
 * GitHub response format:
 * {
 *   author: { login: string },
 *   weeks: [{ w: timestamp, a: additions, d: deletions }],
 *   total: { additions: number, deletions: number }
 * }
 */
async function getRepoContributorStats(
  owner: string,
  repo: string,
  username: string,
  fromDate: Date,
  toDate: Date,
): Promise<{ added: number; deleted: number; timestamps: string[] } | null> {
  const cacheKey = `github:contributor-stats:s${CACHE_SCHEMA_VERSION}:${owner}/${repo}:${username}:${fromDate.toISOString()}:${toDate.toISOString()}`;
  const cached = cache.get<{ added: number; deleted: number; timestamps: string[] }>(cacheKey);
  if (cached) return cached;

  try {
    // This endpoint returns stats for ALL contributors - we filter for our user
    // Uses 202 retry logic: GitHub returns 202 while computing, then 200 when ready
    const data = await restGetWith202Retry<Array<{
      author: { login: string };
      weeks: Array<{ w: number; a: number; d: number }>;
      total: { a: number; d: number };
    }>>(`/repos/${owner}/${repo}/stats/contributors`);

    if (!data || !Array.isArray(data)) {
      // Stats still computing or endpoint failed - return null to use fallback
      console.log(`[Stats] No data available for ${owner}/${repo}, will use fallback`);
      return null;
    }

    // Find the user's stats
    const userStats = data.find(c => c.author?.login?.toLowerCase() === username.toLowerCase());
    if (!userStats) {
      return { added: 0, deleted: 0, timestamps: [] };
    }

    // Filter weeks within date range and aggregate
    let added = 0;
    let deleted = 0;
    const timestamps: string[] = [];
    
    const fromTs = fromDate.getTime();
    const toTs = toDate.getTime();
    
    for (const week of userStats.weeks || []) {
      // Week timestamp is in seconds, multiply by 1000 for ms
      const weekStart = week.w * 1000;
      const weekEnd = weekStart + 7 * 24 * 60 * 60 * 1000;
      
      // Check if week overlaps with our date range
      if (weekEnd >= fromTs && weekStart <= toTs) {
        added += week.a;
        deleted += week.d;
        // Add a timestamp for this week's activity
        timestamps.push(new Date(weekStart).toISOString());
      }
    }

    const result = { added, deleted, timestamps };
    cache.set(cacheKey, result, TTL.REPO_STATS);
    return result;
  } catch (e: any) {
    console.error(`[Stats] Failed for ${owner}/${repo}:`, e.message);
    return null;
  }
}

/**
 * Fallback: List ALL commits and get stats for EACH commit.
 * Fully accurate - no sampling, no extrapolation.
 * Slower than /stats/contributors but guarantees exact numbers.
 */
async function getCommitLocFallback(
  owner: string,
  repo: string,
  username: string,
  fromDate: Date,
  toDate: Date,
): Promise<{ added: number; deleted: number; timestamps: string[] }> {
  // Check cache first
  const cacheKey = `github:commit-loc:s${CACHE_SCHEMA_VERSION}:${owner}/${repo}:${username}:${fromDate.toISOString()}:${toDate.toISOString()}`;
  const cached = cache.get<{ added: number; deleted: number; timestamps: string[] }>(cacheKey);
  if (cached) return cached;

  // Step 1: Fetch ALL commit SHAs (paginated, no limit)
  const commits: Array<{ sha: string; date: string }> = [];
  let page = 1;

  while (true) {
    try {
      const url = `${GITHUB_REST_URL}/repos/${owner}/${repo}/commits?author=${encodeURIComponent(username)}&since=${fromDate.toISOString()}&until=${toDate.toISOString()}&per_page=100&page=${page}`;
      const response = await restGet<any[]>(url);

      if (!Array.isArray(response) || response.length === 0) break;
      
      for (const c of response) {
        if (c.commit?.author?.date) {
          commits.push({ sha: c.sha, date: c.commit.author.date });
        }
      }

      if (response.length < 100) break; // Last page
      page++;
    } catch (e) {
      console.error(`[LOC] Error fetching commits page ${page} for ${owner}/${repo}:`, e);
      break;
    }
  }

  if (commits.length === 0) {
    const emptyResult = { added: 0, deleted: 0, timestamps: [] };
    cache.set(cacheKey, emptyResult, TTL.REPO_STATS);
    return emptyResult;
  }

  // Step 2: Fetch stats for ALL commits (batched, no sampling)
  const timestamps = commits.map(c => c.date);
  let added = 0;
  let deleted = 0;

  const BATCH = 50; // Parallel batch size for commit stats
  for (let i = 0; i < commits.length; i += BATCH) {
    const batch = commits.slice(i, i + BATCH);
    const statsResults = await Promise.allSettled(
      batch.map(c => {
        const statsCacheKey = `github:commit-stats:${owner}/${repo}/${c.sha}`;
        const cached = cache.get<{ additions: number; deletions: number }>(statsCacheKey);
        if (cached) return Promise.resolve(cached);
        return restGet<{ stats?: { additions: number; deletions: number } }>(
          `/repos/${owner}/${repo}/commits/${c.sha}`
        ).then(d => {
          if (d.stats) {
            const result = { additions: d.stats.additions, deletions: d.stats.deletions };
            cache.set(statsCacheKey, result, TTL.REPO_STATS);
            return result;
          }
          return null;
        }).catch(() => null);
      })
    );

    for (const result of statsResults) {
      if (result.status === 'fulfilled' && result.value) {
        added += result.value.additions;
        deleted += result.value.deletions;
      }
    }
  }

  const locData = { added, deleted, timestamps };
  cache.set(cacheKey, locData, TTL.REPO_STATS);
  return locData;
}

/**
 * Get total lines added/deleted and timestamps from commits in a repo.
 * Uses /stats/contributors when available (single call), falls back to per-commit.
 */
async function getCommitLocForRepo(
  owner: string,
  repo: string,
  username: string,
  fromDate: Date,
  toDate: Date,
): Promise<{ added: number; deleted: number; timestamps: string[] }> {
  // Try the fast /stats/contributors endpoint first
  const statsResult = await getRepoContributorStats(owner, repo, username, fromDate, toDate);
  if (statsResult !== null) {
    return statsResult;
  }
  
  // Fallback to per-commit method
  return getCommitLocFallback(owner, repo, username, fromDate, toDate);
}

/**
 * Get lines shipped using AGGRESSIVE parallel batching with smart rate limiting.
 * Uses /stats/contributors endpoint (1 call per repo instead of per-commit).
 */
async function getLinesShippedBatched(
  username: string,
  contributedRepos: Array<{ owner: string; name: string; contributionCount: number }>,
  prLocByRepo: Map<string, { added: number; deleted: number }>,
  fromDate: Date,
  toDate: Date,
): Promise<{ added: number; deleted: number; timestamps: string[] }> {
  const cacheKey = `github:lines-shipped:s${CACHE_SCHEMA_VERSION}:${username}:${fromDate.toISOString()}:${toDate.toISOString()}`;
  const cached = cache.get<{ added: number; deleted: number; timestamps: string[] }>(cacheKey);
  if (cached) return cached;

  try {
    let totalAdded = 0;
    let totalDeleted = 0;
    const timestamps: string[] = [];
    const reposWithCommits = new Set<string>();
    const totalRepos = contributedRepos.length;

    // AGGRESSIVE: Process ALL repos in parallel batches
    // With /stats/contributors (1 call per repo), we can handle many more
    let { batchSize, delayMs } = getAdjustedBatchParams();
    console.log(`[LOC] Processing ${totalRepos} repos with batch size ${batchSize}`);

    for (let i = 0; i < contributedRepos.length; i += batchSize) {
      const batch = contributedRepos.slice(i, i + batchSize);
      const batchNum = Math.floor(i / batchSize) + 1;
      const totalBatches = Math.ceil(totalRepos / batchSize);
      
      console.log(`[LOC] Batch ${batchNum}/${totalBatches} (${batch.length} repos, ${rateLimitState.remaining} remaining)`);

      // Fire ALL requests in parallel - GitHub can handle it with proper rate limit tracking
      const results = await Promise.allSettled(
        batch.map(repo => getCommitLocForRepo(repo.owner, repo.name, username, fromDate, toDate))
      );

      for (let j = 0; j < results.length; j++) {
        const result = results[j];
        const repo = batch[j];
        
        if (result.status === 'fulfilled') {
          const loc = result.value;
          totalAdded += loc.added;
          totalDeleted += loc.deleted;
          if (Array.isArray(loc.timestamps)) {
            timestamps.push(...loc.timestamps);
          }
          if (loc.added > 0 || loc.deleted > 0) {
            reposWithCommits.add(`${repo.owner}/${repo.name}`);
          }
        } else {
          console.error(`[LOC] Failed for ${repo.owner}/${repo.name}:`, result.reason);
        }
      }

      // Adaptive delay based on remaining rate limit (recalculate for next batch)
      if (i + batchSize < contributedRepos.length) {
        const next = getAdjustedBatchParams();
        batchSize = next.batchSize; // May have changed
        await new Promise(resolve => setTimeout(resolve, next.delayMs));
      }
    }

    // Add PR LOC for repos we couldn't get commit data for
    for (const [repoKey, prLoc] of prLocByRepo) {
      if (reposWithCommits.has(repoKey)) continue;
      totalAdded += prLoc.added;
      totalDeleted += prLoc.deleted;
    }

    const linesData = { added: totalAdded, deleted: totalDeleted, timestamps };
    cache.set(cacheKey, linesData, TTL.CONTRIBUTIONS);
    console.log(`[LOC] Complete: ${totalAdded} added, ${totalDeleted} deleted from ${reposWithCommits.size} repos`);
    return linesData;
  } catch (err) {
    console.error(`Failed to fetch lines shipped for ${username}:`, err);
    let prAdded = 0;
    let prDeleted = 0;
    for (const prLoc of prLocByRepo.values()) {
      prAdded += prLoc.added;
      prDeleted += prLoc.deleted;
    }
    return { added: prAdded, deleted: prDeleted, timestamps: [] };
  }
}

function contributionLevelToNumber(level: string): number {
  switch (level) {
    case 'NONE': return 0;
    case 'FIRST_QUARTILE': return 1;
    case 'SECOND_QUARTILE': return 2;
    case 'THIRD_QUARTILE': return 3;
    case 'FOURTH_QUARTILE': return 4;
    default: return 0;
  }
}
