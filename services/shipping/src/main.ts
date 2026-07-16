import { runService } from '@order-system/common';
import { createShippingService } from './service';

runService('shipping', async ({ broker, dbPath, logger }) =>
  createShippingService({ broker, dbPath, logger }),
).catch((err) => {
  console.error(err);
  process.exit(1);
});
