# zil-queue

A simple queuing engine that works on principle of FIFO, SJF, and multi priority scheduling algorithms

![GitHub repo size](https://img.shields.io/github/repo-size/akindutire/zil-queue)
![node-current (tag)](https://img.shields.io/node/v/mongoose/latest)

## Installation

Use the package manager [npm](https://www.npmjs.com) to install it.

```bash
npm install @akindutire/zil-queue --save
```

## Usage

```node
const Jobber = require("@akindutire/zil-queue");

OR;

import Jobber from "@akindutire/zil-queue";

//start the queue engine
// ['high', 'podcast-email', 'share', 'default'] is the multi priority list, task on the high queue will get executed before the podcast-email queue

// Initialize the Jobber
new Jobber(
  [
    { name: "high", algo: "SJF" },
    { name: "podcast-email", algo: "FIFO" },
    { name: "default", algo: "FIFO" },
  ],
  {
    showQueueList: false,
  }
);

// Add a new task to a queue for instance "podcast-email"
// Closure is expect
const task1 = await Jobber.dispatch(
  "podcast-email",
  async (a, b, c, basePath) => {
    const pkg = await import(basePath + "/exampleJob.js");
    await pkg.run(a, b, c);
  },
  [1, 2, 3, process.cwd()],
  { maxRetry: 0, timeout: 5000 }
);

console.log(task1); //{id: 20, hash: def343dfhhehu3hh4, pos: 8}

//To remove a job from a queue
await Jobber.purge("def343dfhhehu3hh4"); //true or false
```

## Contribution

Pull requests are welcome, for major change please open an issue to discuss the change.

## License

MIT
