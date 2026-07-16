import { runService } from '@order-system/common';
import { createPaymentService } from './service';

runService('payment', async ({ broker, dbPath, logger }) =>
  createPaymentService({ broker, dbPath, logger }),
).catch((err) => {
  console.error(err);
  process.exit(1);
});
