import express from "express"
import dotenv from "dotenv"
import { resilientRateLimiter } from "./controller/middleware.js"
import { randomUUID } from "crypto"
import { eventQueue } from "./queue/eventQueue.js"
import { tryCatch } from "bullmq"
import { dlqServiec } from "./queue/dlqService.js"
import { initDb } from "./db/db.js"
dotenv.config()

const app = express()

const port = process.env.PORT || 5000

app.use(express.json())

await initDb()

app.get("/",(req,res)=>{
    res.status(200).json({message:"yoo yoo"})
})


app.get("/rate-check",resilientRateLimiter({window_size:10000,max_Requests:5}),(req,res)=>{
    res.status(200).json({message:"rate limit check"})
})


app.post("/api/v1/events",resilientRateLimiter({window_size:10000,max_Requests:6}),async(req,res)=>{
    try{

        const {type,payload,idempotencyKey} = req.body

        if(!type || !payload){
            return res.status(400).json({
                error:'Validation Error',
                message:"Fields 'types' and 'payloads' are required"
            })
        }

        const eventId = idempotencyKey || randomUUID()

        const job = await eventQueue.add('process-event',{
            eventId,
            type,
            payload,
            receivedAt: new Date().toISOString(),
        },
        {
            jobId:`retry-${Date.now()}`
        }
    )

    console.log(`[GATEWAY] Job added or retrieved. ID: ${job.id}`);

    //202= Accepted
    return res.status(202).json({status:'Queued',eventId,jobId: job.id})

    }
    catch(error)
    {
        console.log("[INGESTION ERROR]",error)
        res.status(500).json({error:'Internal Server error'})
    }
})



app.get("/api/v1/dlq",async(req,res)=>{

    try {

        const failedJobs = await dlqServiec.getFailedJobs()
        return res.status(200).json({
            total: failedJobs.length,
            jobs: failedJobs
        });
        
    } catch (error) {
        console.error("[DLQ FETCH ERROR]",error)
        return res.status(500).json({error:"Failed to retrieve DLQ items"})
    }
})


app.post("/api/v1/dlq/:jobId/replay",async(req,res)=>{

    try {
        const{jobId} = req.params

        const result = await dlqServiec.retryJob(jobId)
        return res.status(200).json(result);
        
    } catch (error) {
        return res.status(400).json({error:error.message})
    }
})


app.post("/api/v1/dlq/:jobId",async(req,res)=>{

    try {

        const {jobId} = req.params

        const result = await dlqServiec.removeJob(jobId)

        return res.status(200).json(result)
        
    } catch (error) {
        return res.status(400).json({error: error.message})
    }
})

app.listen(port,()=>{
    console.log(`Server started on ${port}`)
})
