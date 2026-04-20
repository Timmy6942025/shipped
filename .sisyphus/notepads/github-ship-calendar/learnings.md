# Learnings

## TypeScript Type Definitions (T3)
- Created matching type definitions in both client and server `src/types/index.ts`
- Types: `ContributionDay`, `UserStats`, `Competition`, `LeaderboardEntry`, `ApiError`
- `react-activity-calendar` expects `{ date: 'yyyy-MM-dd', count: number, level: 0-4 }`
- Both `bunx tsc --noEmit` passed cleanly
- Server already had `server/src/types/github.ts` with GitHub API response types from T2
