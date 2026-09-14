import {eventQueue} from "./eventQueue.js"

export const dlqServiec = {

    //get all permanetly failed jobs

    async getFailedJobs(start = 0,end = 20)
    {
        const failedJobs = await eventQueue.getFailed(start,end)



        return failedJobs.map((job)=>({

            jobId: job.id,
            eventId: job.data.eventId,
            type: job.data.type,
            payload: job.data.payload,  
            failedReason: job.failedReason,
            stacktrace: job.stacktrace?.[0] || null,
            attemptsMade: job.attemptsMade,
            failedAt: job.finishedOn ? new Date(job.finishedOn).toISOString() :null

        }));
    },

    //retry a specific job from the DLQ
    async retryJob(jobId){

        const job = await eventQueue.getJob(jobId);


        if(!job)
        {
            throw new Error( `Job with ID ${jobId} not found`)
        }


        const state = await job.getState();

        if (state !== 'failed'){
            throw new Error(`Cannot retry job in state: ${state}, only failed job can be retried`)
        }

        await job.retry();


        return{
            message: `Job ${jobId} re-enqueued for execution`,
            jobId: job.id,
            previousAttempts: job.attemptsMade
        };
    },

    //purge or delete a permamnently unfixable job


    async removeJob(jobId)
    {
        const job = await eventQueue.getJob(jobId)

        if(!job)
        {
            throw new Error(`Job with ID ${jobId} not found`);
        }

        await job.remove()

        return {
            message:`Job with jobId: ${jobId} permanently removed from DLQ`
        };
    }

}

