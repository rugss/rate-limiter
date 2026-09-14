import pkg from "pg"
import dotenv from "dotenv"
dotenv.config()

const {Pool} = pkg

export const pool = new Pool({
    host:process.env.POSTGRES_HOST || '127.0.0.1',
    port: process.env.POSTGRES_PORT || 5432,
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
    database:process.env.POSTGRES_DATABASE || 'events_db',
    max:20,
    idleTimeoutMillis:30000,
    connectionTimeoutMillis:2000,

});


export const initDb = async()=>{
    const query = `
    
    CREATE TABLE IF NOT EXISTS processed_events(
    id SERIAL PRIMARY KEY,
      event_id VARCHAR(255) NOT NULL,
      idempotency_key VARCHAR(255) UNIQUE NOT NULL,
      event_type VARCHAR(100) NOT NULL,
      payload JSONB NOT NULL,
      processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_idempotency_key ON processed_events (idempotency_key);
    `;


    try {
        await pool.query(query)
        console.log('DB Postgresql connected')
        
    } catch (error) {
        console.error('DATABASE failed to initalise POSTGRESQL schema', error.message)
    }
}