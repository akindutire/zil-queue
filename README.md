# zil-queue
A simple queuing engine that works on principle of FIFO, SJF, and multi priority scheduling algorithms

![GitHub repo size](https://img.shields.io/github/repo-size/akindutire/zil-queue)
![node-current (tag)](https://img.shields.io/node/v/mongoose/latest)
![NPM](https://img.shields.io/npm/l/zil_queue)

## Installation
Use the package manager [npm](https://www.npmjs.com) to install it.

```bash
npm install @akindutire/zil-queue --save
```

## Dependency
- mongoose

## Usage

```node
const Queue = require('zil-queue')

 //start the queue engine
 // ['high', 'video', 'share', 'default'] is the multi priority list, task on the high queue will get executed before the video queue

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


//To remove a job from a queue
await Queue.remove('def343dfhhehu3hh4') //true or false

const job2 = await Queue.add( 'high',
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
```
   
## Contribution
Pull requests are welcome, for major change please open an issue to discuss the change.

## License
MIT