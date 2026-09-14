import { Worker } from "bullmq";
import dotenv from "dotenv"
import { pool } from "../db/db.js";
dotenv.config()


const conn = {
    host: process.env.REDIS_URL || "127.0.0.1",
    port: process.env.REDIS_PORT || 6379,
    maxRetriesPerRequest: null,

}




export const eventWorker = new Worker(
    'ingestion-events',async(job)=>
        {
    const {eventId,type,payload,receivedAt}  = job.data;

    console.log(`[WORKER] Processing Job ID: ${job.id} | Event ID: ${eventId} | Type: ${type}`);
    console.log(`Attempt: ${job.attemptsMade + 1} of ${job.opts.attempts}`);


    // to simulate workload like the worker will take some time to get the process job
    await new Promise((resolve)=>setTimeout(resolve,800));


    if (payload?.simulateFailure){
        console.error(`[ERROR SIMULATION] Forcing failure for event: ${eventId}`)
        throw new Error(`Simulated transient error on attempt ${job.attemptsMade +1}`)
    }

    
    const insertQuery = `
    INSERT INTO processed_events (event_id, idempotency_key, event_type, payload)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (idempotency_key) DO NOTHING
      RETURNING id;
    `;

    const values = [eventId, eventId,type,JSON.stringify(payload)]
    const result = await pool.query(insertQuery,values)

    if(result.rowCount === 0)
    {
        console.warn(`[DB IDEMPOTENCY GUARD] Event ${eventId} was already recorded in PostgreSQL! Skipping commit.`)
        return {status :'skipper', reason:"duplicate event"}
    }


    console.log(`✅ [DB COMMITTED] Event ${eventId} persisted with Row ID: ${result.rows[0].id}`);



    return{
        status:'success',
        recordId: result.rows[0].id,
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


