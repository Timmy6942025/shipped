// Re-export the same types so server responses match client expectations

// A single day of contributions on the calendar heatmap
export interface ContributionDay {
  date: string;
  count: number;
  level: number;
}

// Full user stats returned from the API
export interface UserStats {
  login: string;
  name: string | null;
  avatarUrl: string;
  publicRepos: number;
  followers: number;
  isOrganization?: boolean; // true when the login is a GitHub org, not a user
  totalContributions: number;
  totalCommits: number;
  totalPRs: number;
  totalIssues: number;
  totalReviews: number;
  totalDiscussions: number;
  totalNewRepos: number;
  totalPrivateActivity: number;
  locStatus?: 'ready' | 'pending' | 'computing' | 'error';
  totalLinesAdded: number;
  totalLinesDeleted: number;
  starPower: number; // Average stars of repos contributed to
  prMergeRate: number; // Percentage
  reviewDepth: number; // Reviews per PR ratio
  languages: Array<{ name: string; percentage: number }>;
  productiveHours: number[]; // 24-hour histogram
  contributedRepos: Array<{
    nameWithOwner: string;
    contributionCount: number;
    stargazerCount: number;
    primaryLanguage: string | null;
    description: string | null;
    licenseSpdxId: string | null;
    updatedAt: string;
  }>;
  fromDate?: string;
  toDate?: string;
  calendar: ContributionDay[];
  createdAt?: string;
}

// Competition configuration (encoded in URL for sharing)
export interface Competition {
  members: string[];
  startDate: string;
  duration: 7 | 14 | 30;
  name?: string;
}

// A single entry on the competition leaderboard
export interface LeaderboardEntry {
  username: string;
  score: number;
  commits: number;
  prs: number;
  issues: number;
  reviews: number;
}

// API error response shape
export interface ApiError {
  error: string;
}
