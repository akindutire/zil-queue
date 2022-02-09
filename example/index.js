import Queue from './../index.js'
import pkg from 'mongoose';
const { connect } = pkg;



connect('mongodb+srv://akin:akin009@cluster0.hsjpc.mongodb.net/meshedpointstore?retryWrites=true&w=majority',
    { useNewUrlParser: true, useUnifiedTopology: true, serverSelectionTimeoutMS: 10000 }, 
    () => {
        console.log('DB connected')
    }
)

new Queue(['high','video', 'share', 'default'], {useSJF: true, showQueueList: false})

const job1 = await Queue.add( 'video',
    async (a, b, c, basePath) => {
        
        const pkg = await import(basePath+'/example/exampleJob.js')
        await pkg.run(a, b, c)
    },
    [
        1, 
        2, 
        3,
        process.cwd()
    ],
    {maxRetry: 0, timeout: 5000}
    )


// const job2 = await Queue.add( 'high',
//     async (a, b, c) => {
//         console.log(`I am on high queue a=${a},b=${b},c=${c}`)
//     },
//     [
//         1, 
//         2, 
//         3
//     ],
//     {maxRetry: 0, timeout: 5000}
    // )
console.log(job1) //{id: 20, hash: def343dfhhehu3hh4, pos: 8}
