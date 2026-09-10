import { Worker } from "bullmq";
import dotenv from "dotenv"
dotenv.config()


const conn = {
    host: process.env.REDIS_URL || "127.0.0.1",
    port: process.env.REDIS_PORT || 6379,
    maxRetriesPerRequest: null,

}

const processedEvents = new Set();



export const eventWorker = new Worker(
    'ingestion-events',async(job)=>
        {
    const {eventId,type,payload,receivedAt}  = job.data;

    console.log(`[WORKER] Processing Job ID: ${job.id} | Event ID: ${eventId} | Type: ${type}`);
    console.log(`Attempt: ${job.attemptsMade + 1} of ${job.opts.attempts}`);


    if(processedEvents.has(eventId))
    {
        console.warn(`[DUPLICATE GUARD] Event ${eventId} was already processed! skipping execution`)
        return {status:'skipped',reason:'duplicate event'}

    }


    // to simulate workload like the worker will take some time to get the process job
    await new Promise((resolve)=>setTimeout(resolve,800));


    if (payload?.simulateFailure){
        console.error(`[ERROR SIMULATION] Forcing failure for event: ${eventId}`)
        throw new Error(`Simulated transient error on attempt ${job.attemptsMade +1}`)
    }


    processedEvents.add(eventId)

    console.log(`Work completed and event with event id: ${eventId} processed`)



    return{
        status:'success',
        processedAt: new Date().toISOString(),
        durationMs: Date.now() - new Date(receivedAt).getTime(),

    };
},
{
    connection:conn,
    concurrency:5 //procees upto 5 events simulataneously
}


)

eventWorker.on('complete',(job,result)=>{
    console.log(`Job with job id: ${job.id} completed with Result:`,result)
})

eventWorker.on('failed', (job, err) => {
  console.error(`Job ${job?.id} failed with error: "${err.message}". Attempts left: ${job.opts.attempts - job.attemptsMade}`);
});


