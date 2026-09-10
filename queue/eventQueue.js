import {Queue} from "bullmq"


const conn = {
    host: process.env.REDIS_URL || '127.0.0.1',
    port: process.env.REDIS_PORT || 6379,
    maxRetriesPerRequest: null,
}


export const eventQueue = new Queue('ingestion-events',{
    conn,
    defaultJobOptions:{
        attempts:3, //retry the failed job upto 3 times
        backoff:{
            type:'exponential',
            delay:1000, // 1000 = 1s so it will have a delay of 1s then 2,3,4...
        },
        removeOnComplete:{
            count:1000, //Keeping the last 1000 jobs in redis for inspection
        },
        removeOnFail: false, //keeping failed jobs so we can inspect them
    }
})

