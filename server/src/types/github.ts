export interface GitHubUser {
  login: string;
  name: string | null;
  avatar_url: string;
  public_repos: number;
  followers: number;
  following: number;
  bio: string | null;
  location: string | null;
  created_at: string;
  type: 'User' | 'Organization';
}

export interface GitHubRepo {
  id: number;
  name: string;
  full_name: string;
  html_url: string;
  description: string | null;
  stargazers_count: number;
  forks_count: number;
  language: string | null;
  updated_at: string;
}

export interface ContributionsData {
  user: {
    login: string;
    name: string | null;
    avatarUrl: string;
    repositories: { totalCount: number };
    followers: { totalCount: number };
    contributionsCollection: {
      contributionCalendar: {
        totalContributions: number;
        weeks: Array<{
          contributionDays: Array<{
            date: string;
            contributionCount: number;
            contributionLevel: string;
          }>;
        }>;
      };
      totalCommitContributions: number;
      totalPullRequestContributions: number;
      totalIssueContributions: number;
      totalPullRequestReviewContributions: number;
      totalRepositoryContributions: number;
      totalRepositoryContributionsByRepository: Array<{
        repository: { nameWithOwner: string };
      }>;
      restrictedContributionsCount: number;
      commitContributionsByRepository: Array<{
        repository: {
          nameWithOwner: string;
          stargazerCount: number;
          description: string | null;
          updatedAt: string;
          licenseInfo: { spdxId: string; name: string } | null;
          languages: {
            edges: Array<{
              size: number;
              node: { name: string };
            }>;
          };
        };
        contributions: { totalCount: number };
      }>;
      pullRequestContributionsByRepository: Array<{
        repository: {
          nameWithOwner: string;
          stargazerCount: number;
          description: string | null;
          updatedAt: string;
          licenseInfo: { spdxId: string; name: string } | null;
          languages: {
            edges: Array<{
              size: number;
              node: { name: string };
            }>;
          };
        };
        contributions: { totalCount: number };
      }>;
      pullRequestContributions: {
        pageInfo: {
          hasNextPage: boolean;
          endCursor: string;
        };
        nodes: Array<{
          pullRequest: {
            createdAt: string;
            merged: boolean;
            closed: boolean;
            additions: number;
            deletions: number;
            repository: { nameWithOwner: string };
          };
        }>;
      };
      issueContributions: {
        nodes: Array<{
          issue: {
            createdAt: string;
          };
        }>;
      };
    };
  };
}

export interface OrganizationData {
  organization: {
    login: string;
    name: string | null;
    avatarUrl: string;
    repoList: {
      totalCount: number;
      nodes: Array<{
        nameWithOwner: string;
        stargazerCount: number;
        description: string | null;
        updatedAt: string;
        licenseInfo: { spdxId: string; name: string } | null;
        languages: {
          edges: Array<{
            size: number;
            node: { name: string };
          }>;
        };
      }>;
    };
  };
}

export interface PRContributionsPageData {
  user: {
    contributionsCollection: {
      pullRequestContributions: {
        pageInfo: {
          hasNextPage: boolean;
          endCursor: string;
        };
        nodes: Array<{
          pullRequest: {
            createdAt: string;
            merged: boolean;
            closed: boolean;
            additions: number;
            deletions: number;
            repository: { nameWithOwner: string };
          };
        }>;
      };
    };
  };
}

