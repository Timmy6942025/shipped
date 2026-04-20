import { GITHUB_TOKEN } from '../config.js';
import { cache, TTL } from './cache.js';
import type {
  GitHubUser,
  GitHubRepo,
  ContributionsData,
  PRContributionsPageData,
} from '../types/github.js';
import type { UserStats } from '../types/index.js';

const GITHUB_GRAPHQL_URL = 'https://api.github.com/graphql';
const GITHUB_REST_URL = 'https://api.github.com';

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
        commitContributionsByRepository(maxRepositories: 100) {
          repository { nameWithOwner }
          contributions { totalCount }
        }
        pullRequestContributionsByRepository(maxRepositories: 100) {
          repository { nameWithOwner }
          contributions { totalCount }
        }
        pullRequestContributions(first: 100) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            pullRequest {
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

async function graphqlQuery<T>(query: string, variables: Record<string, unknown>): Promise<T> {
  const response = await fetch(GITHUB_GRAPHQL_URL, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${GITHUB_TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query, variables }),
  });

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
  const response = await fetch(url, {
    headers: {
      'Authorization': `Bearer ${GITHUB_TOKEN}`,
      'Accept': 'application/vnd.github.v3+json',
    },
  });

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

export async function getUserContributions(
  username: string,
  fromDate?: string,
  toDate?: string,
): Promise<UserStats> {
  const now = new Date();
  const from = fromDate ?? new Date(now.getFullYear() - 1, now.getMonth(), now.getDate()).toISOString();
  const to = toDate ?? now.toISOString();

  const cacheKey = `github:contributions:${username}:${from}:${to}`;
  const cached = cache.get<UserStats>(cacheKey);
  if (cached) return cached;

  // Fetch contribution calendar + contributed repo list from GraphQL
  const data = await graphqlQuery<ContributionsData>(CONTRIBUTIONS_QUERY, {
    login: username,
    from,
    to,
  });

  // Shape the response
  const user = data.user;
  const calendar = user.contributionsCollection.contributionCalendar;

  // Flatten weeks into array of days
  const contributionDays = calendar.weeks.flatMap(week =>
    week.contributionDays.map(day => ({
      date: day.date,
      count: day.contributionCount,
      level: contributionLevelToNumber(day.contributionLevel),
    }))
  );

  // Build deduplicated, contribution-sorted repo list from GraphQL response
  // (already filtered to the from/to period by the GraphQL query)
  const contributedRepos = buildContributedRepoList(user.contributionsCollection);

  // Collect PR LOC per repo from GraphQL (always available, covers PRs)
  const prLocByRepo = await collectPRLocByRepo(username, from, to, user.contributionsCollection);

  // Fetch TOTAL LOC for all contributed repos using hybrid approach:
  // REST repo stats (covers commits+PRs+everything) as primary,
  // GraphQL PR LOC as fallback per repo when REST stats are unavailable
  const linesData = await getLinesShipped(username, contributedRepos, prLocByRepo, new Date(from), new Date(to));

  const result = {
    login: user.login,
    name: user.name,
    avatarUrl: user.avatarUrl,
    publicRepos: user.repositories.totalCount,
    followers: user.followers.totalCount,
    totalContributions: calendar.totalContributions,
    totalCommits: user.contributionsCollection.totalCommitContributions,
    totalPRs: user.contributionsCollection.totalPullRequestContributions,
    totalIssues: user.contributionsCollection.totalIssueContributions,
    totalReviews: user.contributionsCollection.totalPullRequestReviewContributions,
    totalLinesAdded: linesData.added,
    totalLinesDeleted: linesData.deleted,
    calendar: contributionDays,
  };

  cache.set(cacheKey, result, TTL.CONTRIBUTIONS);
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
): Array<{ owner: string; name: string; contributionCount: number }> {
  const repoMap = new Map<string, { owner: string; name: string; contributionCount: number }>();

  for (const entry of collection.commitContributionsByRepository) {
    const [owner, name] = entry.repository.nameWithOwner.split('/');
    const existing = repoMap.get(entry.repository.nameWithOwner);
    if (existing) {
      existing.contributionCount += entry.contributions.totalCount;
    } else {
      repoMap.set(entry.repository.nameWithOwner, { owner, name, contributionCount: entry.contributions.totalCount });
    }
  }

  for (const entry of collection.pullRequestContributionsByRepository) {
    const [owner, name] = entry.repository.nameWithOwner.split('/');
    const existing = repoMap.get(entry.repository.nameWithOwner);
    if (existing) {
      existing.contributionCount += entry.contributions.totalCount;
    } else {
      repoMap.set(entry.repository.nameWithOwner, { owner, name, contributionCount: entry.contributions.totalCount });
    }
  }

  return [...repoMap.values()].sort((a, b) => b.contributionCount - a.contributionCount);
}

/**
 * List the user's commits in a repo (paginated, filtered by author + date).
 * Returns an array of commit SHAs.
 */
async function listUserCommits(
  owner: string,
  repo: string,
  username: string,
  fromDate: Date,
  toDate: Date,
): Promise<string[]> {
  const shas: string[] = [];
  let page = 1;
  const MAX_PAGES = 10; // safety limit (10 pages × 100 = 1000 commits per repo)

  while (page <= MAX_PAGES) {
    const url = `${GITHUB_REST_URL}/repos/${owner}/${repo}/commits?author=${username}&since=${fromDate.toISOString()}&until=${toDate.toISOString()}&per_page=100&page=${page}`;
    const response = await fetch(url, {
      headers: {
        'Authorization': `Bearer ${GITHUB_TOKEN}`,
        'Accept': 'application/vnd.github.v3+json',
      },
    });

    if (!response.ok) break; // repo may not exist or be private

    const commits = await response.json() as Array<{ sha: string }>;
    if (commits.length === 0) break;

    for (const c of commits) {
      shas.push(c.sha);
    }

    if (commits.length < 100) break; // last page
    page++;
  }

  return shas;
}

/**
 * Fetch stats (additions/deletions) for a single commit.
 * The REST endpoint GET /repos/:owner/:repo/commits/:sha always returns stats.
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
    const url = `${GITHUB_REST_URL}/repos/${owner}/${repo}/commits/${sha}`;
    const response = await fetch(url, {
      headers: {
        'Authorization': `Bearer ${GITHUB_TOKEN}`,
        'Accept': 'application/vnd.github.v3+json',
      },
    });

    if (!response.ok) return null;

    const data = await response.json() as { stats?: { additions: number; deletions: number } };
    if (!data.stats) return null;

    const result = { additions: data.stats.additions, deletions: data.stats.deletions };
    cache.set(cacheKey, result, TTL.REPO_STATS);
    return result;
  } catch {
    return null;
  }
}

/**
 * Get total lines added/deleted from ALL commits in a repo by the user
 * within the date range. Uses individual commit stats (always available).
 */
async function getCommitLocForRepo(
  owner: string,
  repo: string,
  username: string,
  fromDate: Date,
  toDate: Date,
): Promise<{ added: number; deleted: number }> {
  const cacheKey = `github:commit-loc:${owner}/${repo}:${username}:${fromDate.toISOString()}:${toDate.toISOString()}`;
  const cached = cache.get<{ added: number; deleted: number }>(cacheKey);
  if (cached) return cached;

  const shas = await listUserCommits(owner, repo, username, fromDate, toDate);
  console.log(`[LOC] ${owner}/${repo}: ${shas.length} commits by ${username}`);
  if (shas.length === 0) return { added: 0, deleted: 0 };

  let added = 0;
  let deleted = 0;

  // Fetch commit stats in batches of 25 to reduce round-trips
  const BATCH_SIZE = 25;
  for (let i = 0; i < shas.length; i += BATCH_SIZE) {
    const batch = shas.slice(i, i + BATCH_SIZE);
    const statsResults = await Promise.allSettled(
      batch.map(sha => getCommitStats(owner, repo, sha))
    );

    for (const result of statsResults) {
      if (result.status === 'fulfilled' && result.value) {
        added += result.value.additions;
        deleted += result.value.deletions;
      }
    }
  }

  const locData = { added, deleted };
  cache.set(cacheKey, locData, TTL.REPO_STATS);
  return locData;
}

/**
 * Collect PR LOC per repo from the GraphQL response, with pagination.
 * This always works and provides at least PR additions/deletions.
 * Returns a map of repo nameWithOwner -> { added, deleted }.
 */
async function collectPRLocByRepo(
  username: string,
  from: string,
  to: string,
  collection: ContributionsData['user']['contributionsCollection'],
): Promise<Map<string, { added: number; deleted: number }>> {
  const repoMap = new Map<string, { added: number; deleted: number }>();

  const addPR = (pr: { additions: number; deletions: number; repository: { nameWithOwner: string } }) => {
    const key = pr.repository.nameWithOwner;
    const existing = repoMap.get(key);
    if (existing) {
      existing.added += pr.additions;
      existing.deleted += pr.deletions;
    } else {
      repoMap.set(key, { added: pr.additions, deleted: pr.deletions });
    }
  };

  // Sum the first page from the initial query
  for (const node of collection.pullRequestContributions.nodes) {
    addPR(node.pullRequest);
  }

  // Paginate through remaining pages
  let hasNextPage = collection.pullRequestContributions.pageInfo.hasNextPage;
  let cursor = collection.pullRequestContributions.pageInfo.endCursor;

  while (hasNextPage && cursor) {
    const pageData = await graphqlQuery<PRContributionsPageData>(
      PR_CONTRIBUTIONS_PAGE_QUERY,
      { login: username, from, to, cursor },
    );
    const prContribs = pageData.user.contributionsCollection.pullRequestContributions;
    for (const node of prContribs.nodes) {
      addPR(node.pullRequest);
    }
    hasNextPage = prContribs.pageInfo.hasNextPage;
    cursor = prContribs.pageInfo.endCursor;
  }

  return repoMap;
}

/**
 * Sum lines added/deleted across all repos the user has contributed to
 * within the given date range. Uses a hybrid approach:
 *
 * 1. REST repo stats (primary): covers ALL code changes (commits, PRs, merges).
 *    Only available when GitHub has pre-computed stats for the repo.
 * 2. GraphQL PR LOC (fallback per repo): covers PR additions/deletions.
 *    Always available, but misses direct commits.
 *
 * For each repo, we try REST stats first. If that fails (202/computing),
 * we fall back to the PR LOC data for that repo.
 */
async function getLinesShipped(
  username: string,
  contributedRepos: Array<{ owner: string; name: string; contributionCount: number }>,
  prLocByRepo: Map<string, { added: number; deleted: number }>,
  fromDate: Date,
  toDate: Date,
): Promise<{ added: number; deleted: number }> {
  const cacheKey = `github:lines-shipped:${username}:${fromDate.toISOString()}:${toDate.toISOString()}`;
  const cached = cache.get<{ added: number; deleted: number }>(cacheKey);
  if (cached) return cached;

  try {
    let totalAdded = 0;
    let totalDeleted = 0;
    const reposWithCommits = new Set<string>();

    // Fetch commit LOC for all contributed repos in batches of 5
    // (each repo may require many individual commit stat requests)
    const BATCH_SIZE = 5;
    for (let i = 0; i < contributedRepos.length; i += BATCH_SIZE) {
      const batch = contributedRepos.slice(i, i + BATCH_SIZE);
      const commitLocResults = await Promise.allSettled(
        batch.map(repo => getCommitLocForRepo(repo.owner, repo.name, username, fromDate, toDate))
      );

      for (let j = 0; j < commitLocResults.length; j++) {
        const result = commitLocResults[j];
        const repo = batch[j];
        const repoKey = `${repo.owner}/${repo.name}`;
        if (result.status === 'fulfilled') {
          totalAdded += result.value.added;
          totalDeleted += result.value.deleted;
          if (result.value.added > 0 || result.value.deleted > 0) {
            reposWithCommits.add(repoKey);
          }
        }
      }
    }

    // Add PR LOC only for repos where the user had NO direct commits.
    // For repos with commits, commit stats already include PR commits
    // (normally-merged PRs appear as authored commits in the repo).
    // For repos without commits, PR LOC covers contributions to
    // others' repos (e.g., fork PRs, open PRs).
    for (const [repoKey, prLoc] of prLocByRepo) {
      if (reposWithCommits.has(repoKey)) continue;
      totalAdded += prLoc.added;
      totalDeleted += prLoc.deleted;
    }

    const linesData = { added: totalAdded, deleted: totalDeleted };
    cache.set(cacheKey, linesData, TTL.CONTRIBUTIONS);
    return linesData;
  } catch (err) {
    // Gracefully degrade — fall back to pure PR LOC if commit stats fail
    console.error(`Failed to fetch lines shipped for ${username}:`, err);
    let prAdded = 0;
    let prDeleted = 0;
    for (const prLoc of prLocByRepo.values()) {
      prAdded += prLoc.added;
      prDeleted += prLoc.deleted;
    }
    return { added: prAdded, deleted: prDeleted };
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
