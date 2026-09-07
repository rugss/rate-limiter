import {inMemoryLimiter} from "./inMemoryLimiter.js"
import {CircuitBreaker} from "./circuitBreaker.js"
import { redis } from './redisClient.js';


const breaker = new CircuitBreaker({failureThreshold: 3, cooldownPeriod: 10000});

export const resilientRateLimiter = ({ window_size, max_Requests }) => {
    return async(req,res,next)=>{
        const identifier = req.ip || 'anonymous';
        const key = `rate_limit:${identifier}`;
        const currentTime = Date.now();
        const requestId = `${currentTime}-${Math.random().toString(36).substring(2,9)}`;


        //check circuit breaker state

        if(breaker.canRequestRedis()){
            try{

                //enforce a strict 150ms timeout so a hanging redis doesn't block the API
                const timeoutPromise = new Promise((_,reject)=>
                setTimeout(()=>reject(new Error('Redis Timeout')),150)
            );

            const redisPromise = redis.slidingWindowRateLimiter(
                key,
                window_size,
                max_Requests,
                currentTime,
                requestId
            );

            const [allowed,remaining] = await Promise.race([redisPromise,timeoutPromise]);

            breaker.recordSuccess();

            res.setHeader('X-RateLimit-Limit',max_Requests);
            res.setHeader('X-RateLimit-Remaining',remaining);
            res.setHeader('X-RateLimit-Engine','Redis')

            if (allowed===1) return next();

            return res.status(429).json({error:"Too many requests, Try again after some time"})

            
            }
            catch(error)
            {
                breaker.recordFailure();
                console.error('Redis call failed, falling back to in-memory limiter',error.message)
            }
        }


        //fallback if redis is down

        const fallbackResult = inMemoryLimiter.isAllowed(identifier,max_Requests,window_size);

        res.setHeader('X-RateLimit-Limit', max_Requests);
        res.setHeader('X-RateLimit-Remaining', fallbackResult.remaining);
        res.setHeader('X-RateLimit-Engine', 'InMemory-Fallback');

        if(fallbackResult.allowed) return next();

        return res.status(429).json({error: 'Too Many Requests', 
      notice: 'Rate limit applied via local backup engine'
    });
    };
};

