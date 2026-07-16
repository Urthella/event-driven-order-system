import { runService } from '@order-system/common';
import { createInventoryService } from './service';

runService('inventory', async ({ broker, dbPath, logger }) =>
  createInventoryService({ broker, dbPath, logger }),
).catch((err) => {
  console.error(err);
  process.exit(1);
});
