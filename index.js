
const path = require('path');
const {Worker, isMainThread, MessageChannel, parentPort} = require('worker_threads');
const events = require('events');
const serialize = require('serialize-javascript')
const QueueTask = require('./repo/QueueDao');

module.exports = class Queue {
    
    #currentJobIndex = -1
    #queuePriority = []
    #selections = []
    #queueWorker
    #options = {useSJF: false, showQueueList: true}
    static eventEmitter = new events.EventEmitter();

    constructor(queues, options) {
        if(isMainThread) {
            
            const startQueueWorker = new Promise( (resole, reject) => {
                console.log("======[zil-js-queue] Worker started============")
                this.#queueWorker =  new Worker( path.join(__dirname, "worker.js") );
                this.#queuePriority.push(...queues)
                this.#options = { ...this.#options, ...options }

                resole(true)
            } )
            
            startQueueWorker
                .then( async (status) => {
                    await this.stageSelection()
                    Promise.resolve(true)
                } )
                .then( (status) => {
                    //Listen to queue event
                    this.#queueWorker.on("message", async (msg) => {
                        if (msg === "MOVE_NEXT") {
                            
                            let j = this.#selections[this.#currentJobIndex]

                            if(j != undefined) {
                                this.#selections.splice(this.#currentJobIndex,1)
                                await QueueTask.remove(j.hash)
                            }
                           
                            this.next()

                        } else if(msg == "FAIL_THIS") {
                            let j = this.#selections[this.#currentJobIndex]

                            console.log(`[zil-js-queue] Failing item ${j.hash}`)

                            if(j != undefined) {
                                this.#selections.splice(this.#currentJobIndex,1)
                                await QueueTask.remove(j.hash)
                            }
                            
                            this.next()
                                                    
                        } else if(msg == "RETRY_THIS") {
                            
                            let j = this.#selections[this.#currentJobIndex]
                            //update job list
                            this.#selections[this.#currentJobIndex] = await QueueTask.release(j.hash)
                            
                            if (j.trial < j.maxRetry) {
                                console.log(`[zil-js-queue] Retrying item ${j.hash}`)
                                await this.process(this.#currentJobIndex)
                            }else{
                                this.#selections.splice(this.#currentJobIndex,1)
                                await QueueTask.remove(j.hash)
                                this.next()
                            }
                            
                        }

                    })

                    this.#queueWorker.on("error", async (err) => {

                        let j = this.#selections[this.#currentJobIndex]

                        console.log(`[zil-js-queue] Worker Failing item ${j.hash}`)

                        if(j != undefined) {
                            this.#selections.splice(this.#currentJobIndex,1)
                            await QueueTask.remove(j.hash)
                        }
                        
                        this.next()                  
                        
                    })

                    this.#queueWorker.on("messageerror", async (err) => {
                        //Worker couldn't read message properly
                        let j = this.#selections[this.#currentJobIndex]

                        console.log(`[zil-js-queue] Worker Failing item ${j.hash}`)

                        if(j != undefined) {
                            this.#selections.splice(this.#currentJobIndex,1)
                            await QueueTask.remove(j.hash)
                        }
                        
                        this.next()                  
                        
                    })

                    this.#queueWorker.on('exit', async() => {
                        console.log(`[zil-js-queue] Worker-Stopped`)
                        console.log(`[zil-js-queue] Restarting queue worker`)
                        for(let job of this.#selections) {
                            if(job.isLocked) {
                                await QueueTask.release()
                            }
                        }
                        new MeshedQueue(this.#queuePriority, this.#options)
                    })

                    this.#queueWorker.on('online', () => {
                        console.log(`[zil-js-queue] Actively executing`)
                    })

                    Promise.resolve(true)

                } ).then( (status) => {

                    //Listen for new Job, Pick up new task if tray is empty
                    console.log("[zil-js-queue] Now listening on task arrival")
                    MeshedQueue.eventEmitter.on("newTask", async () => {
                        console.log("[zil-js-queue] New task arrived")
                        if(this.#selections.length == 0) {
                            
                            await this.stageSelection()
                            this.next()

                        }
                    })

                    Promise.resolve(true)

                } ).then( (status) => {
                    //Look out for job every 5sec [free and unfailed job]
                    let intervalId = setInterval( async () => {
                        console.log("[zil-js-queue] Watchman finding free job")
                        if(this.#selections.length == 0) {
                            await this.stageSelection()
                            this.next()
                        }else{
                            console.log("[zil-js-queue] Watchman tray is not empty yet")
                        }
                    }, 60000 )

                    Promise.resolve(intervalId)
                }).then( (intervalId) => {
                    //Execute next on queue
                    
                    this.next()
                    
                } ).catch( (err) => {
                    console.log(err)
                } )
        }

    }

    stageSelection = async () => {
        //FIFO, SJF, Multi-level Priority
        for(let q of this.#queuePriority) {
            let c
            if (this.#options.useSJF) {
                c = await QueueTask.findByQueueName(q, true)
            }else{
                c = await QueueTask.findByQueueName(q, false)
            }
            this.#selections = [ ...this.#selections, ...c ] 
        }
        this.#currentJobIndex = -1;
        
        console.log(`[zil-js-queue] ${this.#selections.length} tasks queued`)
        if(this.#options.showQueueList) {
            console.log(this.#selections)
        }

    }

    static add = async (queue, payload, args = [], options = {} ) => {
        
        let defaultopts = { maxRetry: 3, timeout: 50000}

        options =  { ...defaultopts, ...options}
        
        
        if(typeof payload != 'function') {
            throw new Error("Only function is accepted as payload")
        }

        let fn = serialize(payload)

        const job = new QueueTask(queue, fn, args, options.maxRetry, options.timeout)
        
        const ajob = await job.create()
        const pos = await QueueTask.countJobs()

        //Inform queue
        MeshedQueue.eventEmitter.emit("newTask")
            
        return {id: ajob._seq, hash: ajob.hash, pos: pos}
        
    }

    process = async (jobStageIndex) => {
        
        const job = this.#selections[jobStageIndex]
        
        if (job !=  null || job != undefined) {
            if (!job.isLocked) {
                
                if(typeof job.payload != 'string'){
                    console.log(`[zil-js-queue] Skipping ${job.hash}`)
                    this.#selections.splice(jobStageIndex, 1)
                    this.next()
                }else{
                    
                    //Auto lock
                    await QueueTask.updateTrial(job.hash)
                    this.#queueWorker.postMessage({hash: job.hash, args: job.args.join(','), payload: job.payload, mr: job.maxRetry, ts: job.timeout, tr: job.trial})

                }
                
            }else{
                console.log(`[zil-js-queue] Skipping ${job.hash}`)
                this.#selections.splice(jobStageIndex, 1)
                this.next()
            }
        }else{
            //incase of disjoint
            if(this.#selections.length > 0){
                this.#selections.splice(jobStageIndex, 1)
                this.next()
            }else{
                console.log("[zil-js-queue] Queue relaxed, no task to process")
            }

        }

    }

    static remove = async (hash) => {
        const job = await QueueTask.basedOnHash(hash)
        if(job != null) {
            if(!job.isLocked) {
                //search if in memory too
                await QueueTask.remove(hash);
                return true
            }
        }

        return false
    }


    next = async () => {
        this.#currentJobIndex = 0 
        if(this.#selections.length > 0) {
            let j = this.#selections[0]
            console.log(`[zil-js-queue] Moving to item ${j.hash}`)
            await this.process(this.#currentJobIndex)
        }else{
            console.log("[zil-js-queue] Queue relaxed, no task to process")
        }
        
    }


}