export class CircuitBreaker {
    constructor({ failureThreshold = 3,cooldownPeriod = 10000})
    {
        this.failureThreshold = failureThreshold;
        this.cooldownPeriod  = cooldownPeriod;

        this.state = 'CLOSED'; // the 3 states CLOSED,OPEN,HALF-OPEN
        this.failureCount = 0;
        this.nextAttempt = Date.now();
    }


    canRequestRedis()
    {
        if(this.state ==='CLOSED') return true;

        if(this.state ==='OPEN')
        {
            if(Date.now()>this.nextAttempt)
            {
                this.state = 'HALF-OPEN';
                console.log('[CIRCUIT BREAKER] Cooldown expired. Transitioning to HALF-OPEN (testing Redis health)');
                return true;
            }
            return false;
        }
        if (this.state ==='HALF-OPEN')
        {
            return false;
        }
        return true;
            
    }


    recordSuccess()
    {
        
        if (this.state = 'CLOSED')
        {
            console.log('[CIRCUIT BREAKER] Redis probe succeeded! Transitioning back to CLOSED.');
        }
        this.failureCount = 0;
        this.state = 'CLOSED';


    }


    recordFailure()
    {
        this.failureCount++;
        if(this.failureCount >= this.failureThreshold)
        {
            this.state = 'OPEN';
            this.nextAttempt = Date.now() + this.cooldownPeriod;
            console.warn(`Circuit breaker [TRIPPED] now state is OPEN and Falling back to in-memory limiter for ${this.cooldownPeriod}ms`)
        }
    }
}