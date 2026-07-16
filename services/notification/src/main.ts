import { runService } from '@order-system/common';
import { createNotificationService } from './service';

runService('notification', async ({ broker, dbPath, logger }) =>
  createNotificationService({ broker, dbPath, logger }),
).catch((err) => {
  console.error(err);
  process.exit(1);
});
