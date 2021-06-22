import { RedisClient } from "redis";
import { initializeRedisQueue, addDelayedTask, pollQueue } from "./redis.utils";

const main = async () => {
  const redisClient: RedisClient = await initializeRedisQueue(
    `${process.env.REDIS_URL}`
  );

  const delayedQueue: string = `${process.env.REDIS_DELAYED_QUEUE}`;

  // Add Delayed Tasks
  addDelayedTask(redisClient, delayedQueue, { param: "test1" }, 5);
  addDelayedTask(redisClient, delayedQueue, { param: "test2" }, 1);
  addDelayedTask(redisClient, delayedQueue, { param: "test3" }, 10);

  // Poll Delayed Tasks
  await pollQueue(redisClient, delayedQueue);
};

main();
