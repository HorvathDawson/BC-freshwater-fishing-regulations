import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    poolOptions: {
      workers: {
        wrangler: { configPath: './wrangler.toml' },
        miniflare: {
          r2Buckets: ['BUCKET'],
          bindings: { SHARD_VERSION: 'v8' },
        },
        isolatedStorage: false,
      },
    },
  },
});
