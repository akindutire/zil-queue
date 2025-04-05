# zil-queue
A simple queuing engine that works on principle of FIFO, SJF, and multi priority scheduling algorithms

![GitHub repo size](https://img.shields.io/github/repo-size/akindutire/zil-queue)
![node-current (tag)](https://img.shields.io/node/v/mongoose/latest)

## Installation
Use the package manager [npm](https://www.npmjs.com) to install it.

```bash
npm install @akindutire/zil-queue --save
```

## Dependency
- mongoose

## Usage

```node
const Queue = require('@akindutire/zil-queue')

 //start the queue engine
 // ['high', 'video', 'share', 'default'] is the multi priority list, task on the high queue will get executed before the video queue

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

console.log(job1) //{id: 20, hash: def343dfhhehu3hh4, pos: 8}

//To remove a job from a queue
await Queue.remove('def343dfhhehu3hh4') //true or false

```
   
## Contribution
Pull requests are welcome, for major change please open an issue to discuss the change.

## License
MIT