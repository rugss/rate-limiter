class inMemorySlidingWindow{
    constructor(){

        //map<key,array(timestamp)>
        this.storage = new Map();

        setInterval(()=> this.cleanup(),60000) //cleanup dead keys every 60 seconds
    }




    isAllowed(key,limit,windowSize){



        const currentTime = Date.now();

        const windowStartTime = currentTime-windowSize;

        let timestamps = this.storage.get(key) || [];


        timestamps = timestamps.filter(ts=> ts>windowStartTime);


        if (timestamps.length >= limit)
        {
            this.storage.set(key, timestamps);
            return { allowed: false, remaining: 0 };
        }

        timestamps.push(currentTime);
        this.storage.set(key, timestamps);

        return {
            allowed: true,
            remaining: limit - timestamps.length
        };
    
    }

    cleanup()
    {
        const now = Date.now();


        for (const[key,timestamps] of this.storage.entries())
        {
            if (timestamps.length=== 0 || timestamps[timestamps.length-1]< now-60000)
            {
                this.storage.delete(key);
            }
        }
    }
}
export const inMemoryLimiter = new inMemorySlidingWindow();