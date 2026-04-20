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
  totalContributions: number;
  totalCommits: number;
  totalPRs: number;
  totalIssues: number;
  totalReviews: number;
  totalLinesAdded: number;
  totalLinesDeleted: number;
  calendar: ContributionDay[];
  createdAt?: string; // added by route handler from user profile
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
