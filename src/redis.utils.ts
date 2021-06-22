import { createClient, RedisClient } from "redis";
import { v4 as uuid } from "uuid";
import { sleep } from "./utils";

export const initializeRedisQueue = (redisUrl: string): Promise<RedisClient> =>
  new Promise((resolve, reject): void => {
    try {
      const redisQueueClient = createClient(redisUrl);
      redisQueueClient.on("ready", () => {
        console.log("Redis client for queue ready");
        resolve(redisQueueClient);
      });

      redisQueueClient.on("error", (err: Error) => {
        console.log(`Redis queue error `, err);
        reject(err);
      });
    } catch (err) {
      reject(err);
    }
  });

export const addDelayedTask = (
  redisClient: RedisClient,
  queue: string,
  item: any,
  delay: number
): void => {
  redisClient.zadd(
    queue,
    "CH",
    new Date().getTime() / 1000 + delay,
    JSON.stringify({ ...item, id: uuid() })
  );
};

export const getFirstItem = async (
  redisClient: RedisClient,
  queue: string
): Promise<Array<string>> => {
  return new Promise((resolve, reject) => {
    redisClient.zrange(
      queue,
      0,
      0,
      "withscores",
      (err: Error, reply: Array<string>) => {
        if (err) {
          reject(err);
        }
        resolve(reply);
      }
    );
  });
};

export const pollQueue = async (
  redisClient: RedisClient,
  queue: string
): Promise<void> => {
  while (true) {
    const [item, delay]: Array<any> = await getFirstItem(redisClient, queue);

    if (!item || delay > new Date().getTime() / 1000) {
      console.log("Waiting...");
      await sleep(1000);
      continue;
    }

    console.log("Deleting... ", item);
    redisClient.zrem(queue, item);
  }
};
