const Queue = require('./../index').default

new Queue(['high','video', 'share', 'default'], {useSJF: true, showQueueList: false})
 //Queue for share
const job1 = await Queue.add( 'video',
    async (a, b, c, basePath) => {
        let ExampleJob = require(basePath+'/job/exampleJob.js')
        await ExampleJob.run(a, b, c)
    },
    [
        1, 
        2, 
        3, 
        path.join(__dirname, '..')
    ],
    {maxRetry: 0, timeout: 5000}
    )


const job2 = await Queue.add( 'hight',
    async (a, b, c) => {
        console.log(`I am on high queue a=${a},b=${b},c=${c}`)
    },
    [
        1, 
        2, 
        3
    ],
    {maxRetry: 0, timeout: 5000}
    )
console.log(job1, job2) //{id: 20, hash: def343dfhhehu3hh4, pos: 8}
