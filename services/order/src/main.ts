import { runService } from '@order-system/common';
import { createOrderService } from './service';

runService('order', async ({ broker, dbPath, logger }) =>
  createOrderService({
    broker,
    dbPath,
    logger,
    httpPort: Number(process.env.PORT ?? 3000),
  }),
).catch((err) => {
  console.error(err);
  process.exit(1);
});
