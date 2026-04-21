interface CacheEntry<T> {
  value: T;
  expiresAt: number;
}

const DEFAULT_MAX_SIZE = 500;
const CLEANUP_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

export class Cache {
  private store = new Map<string, CacheEntry<unknown>>();
  private readonly maxSize: number;
  private cleanupTimer: ReturnType<typeof setInterval> | null = null;

  constructor(maxSize = DEFAULT_MAX_SIZE) {
    this.maxSize = maxSize;
  }

  /**
   * Start periodic cleanup of expired entries.
   * Call this once at server startup.
   */
  startCleanup(): void {
    if (this.cleanupTimer) return;
    this.cleanupTimer = setInterval(() => this.evictExpired(), CLEANUP_INTERVAL_MS);
    // Allow the process to exit even if the timer is running
    if (this.cleanupTimer.unref) this.cleanupTimer.unref();
  }

  /**
   * Stop periodic cleanup (for tests or graceful shutdown).
   */
  stopCleanup(): void {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
  }

  get<T>(key: string): T | null {
    const entry = this.store.get(key);
    if (!entry) return null;
    if (Date.now() > entry.expiresAt) {
      this.store.delete(key);
      return null;
    }
    // Move to end (most-recently-used) for LRU ordering
    this.store.delete(key);
    this.store.set(key, entry);
    return entry.value as T;
  }

  set<T>(key: string, value: T, ttlMs: number): void {
    // Delete first so the key moves to the end (most-recently-used)
    this.store.delete(key);
    this.store.set(key, { value, expiresAt: Date.now() + ttlMs });
    // Evict least-recently-used entries if over capacity
    while (this.store.size > this.maxSize) {
      const oldestKey = this.store.keys().next().value;
      if (oldestKey !== undefined) {
        this.store.delete(oldestKey);
      }
    }
  }

  has(key: string): boolean {
    const entry = this.store.get(key);
    if (!entry) return false;
    if (Date.now() > entry.expiresAt) {
      this.store.delete(key);
      return false;
    }
    return true;
  }

  /**
   * Remove all expired entries. Called periodically by the cleanup timer.
   */
  private evictExpired(): void {
    const now = Date.now();
    for (const [key, entry] of this.store) {
      if (now > entry.expiresAt) {
        this.store.delete(key);
      }
    }
  }
}

export const cache = new Cache();

/** Bump this when cached data shapes change to invalidate all stale entries.
 *  Versioned key families (use s${CACHE_SCHEMA_VERSION}):
 *    github:contributions, github:commit-loc, github:lines-shipped
 *  Unversioned (stable shapes):
 *    github:user, github:repos, github:commit-stats
 */
export const CACHE_SCHEMA_VERSION = 4;

// TTL constants
export const TTL = {
  CONTRIBUTIONS: 5 * 60 * 1000,  // 5 minutes
  USER_PROFILE: 60 * 60 * 1000,  // 1 hour
  REPO_STATS: 10 * 60 * 1000,   // 10 minutes
};
