import { defineConfig } from 'vitest/config';
import path from 'node:path';

const r = (p: string) => path.resolve(__dirname, p);

export default defineConfig({
  resolve: {
    alias: {
      '@order-system/common': r('packages/common/src/index.ts'),
      '@order-system/order-service': r('services/order/src/index.ts'),
      '@order-system/inventory-service': r('services/inventory/src/index.ts'),
      '@order-system/payment-service': r('services/payment/src/index.ts'),
      '@order-system/shipping-service': r('services/shipping/src/index.ts'),
      '@order-system/notification-service': r('services/notification/src/index.ts'),
    },
  },
  test: {
    include: ['tests/**/*.test.ts'],
    environment: 'node',
    testTimeout: 30000,
    hookTimeout: 30000,
    // SQLite files + real timers: keep test files sequential for determinism.
    fileParallelism: false,
  },
});
