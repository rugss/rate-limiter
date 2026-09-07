import Redis from "ioredis"
import fs from "fs"
import path from "path"
import { fileURLToPath } from "url"

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)


//make redis connection

export const redis = new Redis({
    host: process.env.REDIS_URL || '127.0.0.1',
    port: Number(process.env.REDIS_PORT) || 6379,
    maxRetriesPerRequests:1,
    connectTimeout: 500,
    lazyConnect: false,

    retryStrategy(times){
        const delay = Math.min(times * 200, 2000);
        return delay;
    },

    enableOfflineQueue: false
});

redis.on('connect',()=>{
    console.log('Connected to Redis successfully')
})
redis.on('ready', () => {
  console.log('Redis is ready to accept commands');
});

const luaPath = path.join(__dirname,'rate-limiter.lua')
const luaScript = fs.readFileSync(luaPath,'utf-8')

redis.defineCommand('slidingWindowRateLimiter',{
    numberOfKeys: 1,
    lua: luaScript
});