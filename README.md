# zil-queue
A simple queuing engine that works on principle of SJF, FIFO, and multi priority scheduling algorithms

## Installation
Use the package manager [npm](https://www.npmjs.com) to install it.

```bash
npm install zil-queue --save
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
const job = await Queue.add( 'video',
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

console.log(job) //{id: 20, hash: def343dfhhehu3hh4, pos: 8}

await Queue.remove('def343dfhhehu3hh4') //true or false
```
   
## Contribution
Pull requests are welcome, for major change please open an issue to discuss the change.

## License
GNU General Public License v3.0