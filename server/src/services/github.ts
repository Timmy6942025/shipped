import { GITHUB_TOKEN } from '../config.js';
import { cache, TTL, CACHE_SCHEMA_VERSION } from './cache.js';
import type {
  GitHubUser,
  GitHubRepo,
  ContributionsData,
  OrganizationData,
  PRContributionsPageData,
  RepoStatsData,
} from '../types/github.js';
import type { UserStats, ContributionDay } from '../types/index.js';

const GITHUB_GRAPHQL_URL = 'https://api.github.com/graphql';
const GITHUB_REST_URL = 'https://api.github.com';

// Max repos to fetch LOC for (prevents extreme rate limit usage)
const MAX_LOC_REPOS = 10;
// Max commits per repo to fetch stats for
const MAX_COMMIT_STATS = 100;
// Batch size for parallel repo LOC fetching
const LOC_BATCH_SIZE = 10;
// Delay between batches to respect rate limits
const LOC_BATCH_DELAY_MS = 100;
// Batch size for parallel commit stats within a repo
const STATS_BATCH_SIZE = 20;

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

// Rate limit tracking
let lastRateLimitReset = 0;
let consecutive429s = 0;

async function graphqlQuery<T>(query: string, variables: Record<string, unknown>): Promise<T> {
  // Check if we need to wait due to rate limits
  if (Date.now() < lastRateLimitReset) {
    const waitTime = lastRateLimitReset - Date.now() + 1000;
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
    lastRateLimitReset = parseInt(reset) * 1000;
  }

  if (response.status === 429) {
    consecutive429s++;
    const retryAfter = response.headers.get('retry-after') || '1';
    const waitMs = parseInt(retryAfter) * 1000;
    console.log(`[GraphQL] 429 - waiting ${waitMs}ms (consecutive: ${consecutive429s})`);
    await new Promise(resolve => setTimeout(resolve, Math.min(waitMs * Math.pow(2, consecutive429s - 1), 30000)));
    return graphqlQuery(query, variables); // Retry
  }
  consecutive429s = 0;

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
  
  // Rate limit check for REST too
  if (Date.now() < lastRateLimitReset) {
    const waitTime = lastRateLimitReset - Date.now() + 1000;
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
    lastRateLimitReset = parseInt(reset) * 1000;
  }

  if (response.status === 429) {
    consecutive429s++;
    const retryAfter = response.headers.get('retry-after') || '1';
    const waitMs = parseInt(retryAfter) * 1000;
    console.log(`[REST] 429 - waiting ${waitMs}ms`);
    await new Promise(resolve => setTimeout(resolve, Math.min(waitMs * Math.pow(2, consecutive429s - 1), 30000)));
    return restGet(path);
  }
  consecutive429s = 0;

  if (!response.ok) {
    throw new Error(`REST request failed: ${response.status} ${response.statusText} for ${path}`);
  }

  return response.json() as Promise<T>;
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
    totalLinesAdded: 0,
    totalLinesDeleted: 0,
    starPower: repoNodes.length > 0 ? Math.round(totalStars / repoNodes.length) : 0,
    prMergeRate: 0,
    reviewDepth: 0,
    languages,
    productiveHours: new Array(24).fill(0),
    contributedRepos,
    calendar,
  };

  cache.set(cacheKey, result, TTL.CONTRIBUTIONS);
  return result;
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

  // PARALLEL: Fetch LOC data for top repos concurrently (with batching)
  const topRepos = contributedRepos.slice(0, MAX_LOC_REPOS);
  console.log(`[Performance] Fetching LOC for top ${topRepos.length} repos`);
  
  const linesData = await getLinesShippedBatched(
    username, 
    topRepos, 
    prMetadata.locByRepo, 
    new Date(from), 
    new Date(to)
  );

  console.log(`[Performance] Got LOC data for ${username}`);

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

  // Productive Hours histogram
  const productiveHours = new Array(24).fill(0);
  const allTimestamps = [...(prMetadata.timestamps || []), ...(linesData.timestamps || [])];
  
  console.log(`[Persona] Processing ${allTimestamps.length} timestamps for ${username}`);
  
  allTimestamps.forEach(ts => {
    if (!ts) return;
    const hour = new Date(ts).getUTCHours();
    if (!isNaN(hour) && hour >= 0 && hour < 24) {
      productiveHours[hour]++;
    }
  });
  
  const issueNodes = collection.issueContributions?.nodes || [];
  issueNodes.forEach(node => {
    if (node.issue?.createdAt) {
      const hour = new Date(node.issue.createdAt).getUTCHours();
      if (!isNaN(hour) && hour >= 0 && hour < 24) {
        productiveHours[hour]++;
      }
    }
  });

  const totalPRs = collection.totalPullRequestContributions;
  const result: UserStats = {
    login: ghUser.login,
    name: ghUser.name,
    avatarUrl: ghUser.avatarUrl,
    publicRepos: ghUser.repositories.totalCount,
    followers: ghUser.followers.totalCount,
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
    calendar: contributionDays,
  };

  cache.set(cacheKey, result, TTL.CONTRIBUTIONS);
  console.log(`[Performance] Completed fetch for ${username}`);
  return result;
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
 * List the user's commits in a repo (paginated, filtered by author + date).
 * Optimized: reduced pages, parallel fetching within the repo.
 */
async function listUserCommits(
  owner: string,
  repo: string,
  username: string,
  fromDate: Date,
  toDate: Date,
): Promise<Array<{ sha: string; date: string }>> {
  const commits: Array<{ sha: string; date: string }> = [];
  let page = 1;
  const MAX_PAGES = 3; // Reduced for performance

  while (page <= MAX_PAGES) {
    try {
      const url = `${GITHUB_REST_URL}/repos/${owner}/${repo}/commits?author=${encodeURIComponent(username)}&since=${fromDate.toISOString()}&until=${toDate.toISOString()}&per_page=100&page=${page}`;
      const response = await restGet<any[]>(url);

      if (!Array.isArray(response) || response.length === 0) break;
      
      for (const c of response) {
        if (c.commit?.author?.date) {
          commits.push({ sha: c.sha, date: c.commit.author.date });
        }
      }

      if (response.length < 100) break;
      page++;
    } catch (e) {
      console.error(`[REST] Error fetching commits for ${owner}/${repo}:`, e);
      break;
    }
  }

  return commits;
}

/**
 * Fetch stats (additions/deletions) for a single commit.
 */
async function getCommitStats(
  owner: string,
  repo: string,
  sha: string,
): Promise<{ additions: number; deletions: number } | null> {
  const cacheKey = `github:commit-stats:${owner}/${repo}/${sha}`;
  const cached = cache.get<{ additions: number; deletions: number }>(cacheKey);
  if (cached) return cached;

  try {
    const data = await restGet<{ stats?: { additions: number; deletions: number } }>(
      `/repos/${owner}/${repo}/commits/${sha}`
    );

    if (!data.stats) return null;

    const result = { additions: data.stats.additions, deletions: data.stats.deletions };
    cache.set(cacheKey, result, TTL.REPO_STATS);
    return result;
  } catch {
    return null;
  }
}

/**
 * Get total lines added/deleted and timestamps from commits in a repo.
 */
async function getCommitLocForRepo(
  owner: string,
  repo: string,
  username: string,
  fromDate: Date,
  toDate: Date,
): Promise<{ added: number; deleted: number; timestamps: string[] }> {
  const cacheKey = `github:commit-loc:s${CACHE_SCHEMA_VERSION}:${owner}/${repo}:${username}:${fromDate.toISOString()}:${toDate.toISOString()}`;
  const cached = cache.get<{ added: number; deleted: number; timestamps: string[] }>(cacheKey);
  if (cached) return cached;

  const commits = await listUserCommits(owner, repo, username, fromDate, toDate);
  if (commits.length === 0) return { added: 0, deleted: 0, timestamps: [] };

  const timestamps = commits.map(c => c.date);
  
  let added = 0;
  let deleted = 0;

  try {
    // Only fetch stats for first MAX_COMMIT_STATS commits
    const statsBatch = commits.slice(0, MAX_COMMIT_STATS);
    
    // Batch the stats requests
    for (let i = 0; i < statsBatch.length; i += STATS_BATCH_SIZE) {
      const batch = statsBatch.slice(i, i + STATS_BATCH_SIZE);
      const statsResults = await Promise.allSettled(
        batch.map(c => getCommitStats(owner, repo, c.sha))
      );

      for (const result of statsResults) {
        if (result.status === 'fulfilled' && result.value) {
          added += result.value.additions;
          deleted += result.value.deletions;
        }
      }
    }
  } catch (e) {
    console.error(`[LOC] Throttled or failed stats for ${owner}/${repo}:`, e);
  }

  const locData = { added, deleted, timestamps };
  cache.set(cacheKey, locData, TTL.REPO_STATS);
  return locData;
}

/**
 * Get lines shipped using PARALLEL batched fetching.
 * This is the major performance optimization.
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

    // Process repos in parallel batches
    for (let i = 0; i < contributedRepos.length; i += LOC_BATCH_SIZE) {
      const batch = contributedRepos.slice(i, i + LOC_BATCH_SIZE);
      
      console.log(`[LOC] Processing batch ${Math.floor(i / LOC_BATCH_SIZE) + 1}/${Math.ceil(contributedRepos.length / LOC_BATCH_SIZE)} (${batch.length} repos)`);
      
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

      // Small delay between batches to avoid overwhelming rate limits
      if (i + LOC_BATCH_SIZE < contributedRepos.length) {
        await new Promise(resolve => setTimeout(resolve, LOC_BATCH_DELAY_MS));
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
