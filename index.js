import express from "express"
import dotenv from "dotenv"
import { resilientRateLimiter } from "./controller/middleware.js"
dotenv.config()

const app = express()

const port = process.env.PORT || 5000

app.use(express.json())


app.get("/",(req,res)=>{
    res.status(200).json({message:"yoo yoo"})
})


app.get("/rate-check",resilientRateLimiter({window_size:10000,max_Requests:5}),(req,res)=>{
    res.status(200).json({message:"rate limit check"})
})

app.listen(port,()=>{
    console.log(`Server started on ${port}`)
})
