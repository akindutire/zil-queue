import { getInstalledPath } from 'get-installed-path';
import { join, resolve } from 'path';
import { Worker, isMainThread, MessageChannel, parentPort } from 'worker_threads';
import { EventEmitter } from 'events';
import serialize from 'serialize-javascript';
import JobQueue from './repo/QueueDao';
import config from './config';

const __dirname = resolve();


export default class Queue {
    
    #currentJobIndex = -1
    #queuePriority = []
    #selections = []
    #queueWorker
    #options = {useSJF: false, showQueueList: true}
    static eventEmitter = new EventEmitter();
    static queues = [];

    constructor(queues, options) {
        if(isMainThread) {
            
            new Promise( (resolve, reject) => {
                resolve('start')
            }).then( async (data) => {

                let pkgPath
                try{

                    pkgPath = await getInstalledPath(config.packageName, {local: true})
    
                } catch(e) {
                    
                    pkgPath = __dirname
                    console.log("Using base dir "+pkgPath);

                }

                this.#queueWorker =  new Worker(join(pkgPath, "/service/worker/QueueWorker.js"));

                console.log(`------ ${config.cmd.tag} Worker started ------`);

                this.#queuePriority.push(...queues)
                Queue.queues = [...queues]
                this.#options = { ...this.#options, ...options }
            
                return Promise.resolve(true)
            } )
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
                            await JobQueue.remove(j.hash)
                        }
                        
                        this.next()

                    } else if(msg == "FAIL_THIS") {
                        let j = this.#selections[this.#currentJobIndex]

                        console.log(`${config.cmd.tag} Failing item ${j.hash}`)

                        if(j != undefined) {
                            this.#selections.splice(this.#currentJobIndex,1)
                            await JobQueue.remove(j.hash)
                        }
                        
                        this.next()
                                                
                    } else if(msg == "RETRY_THIS") {
                        
                        let j = this.#selections[this.#currentJobIndex]
                        //update job list
                        this.#selections[this.#currentJobIndex] = await release(j.hash)
                        
                        if (j.trial < j.maxRetry) {
                            console.log(`${config.cmd.tag} Retrying item ${j.hash}`)
                            await this.process(this.#currentJobIndex)
                        }else{
                            this.#selections.splice(this.#currentJobIndex,1)
                            await JobQueue.remove(j.hash)
                            this.next()
                        }
                        
                    }

                })

                this.#queueWorker.on("error", async (err) => {

                    let j = this.#selections[this.#currentJobIndex]

                    console.log(`${config.cmd.tag} Worker Failing item ${j.hash}`)

                    if(j != undefined) {
                        this.#selections.splice(this.#currentJobIndex,1)
                        await JobQueue.remove(j.hash)
                    }
                    
                    this.next()                  
                    
                })

                this.#queueWorker.on("messageerror", async (err) => {
                    //Worker couldn't read message properly
                    let j = this.#selections[this.#currentJobIndex]

                    console.log(`${config.cmd.tag} Worker Failing item ${j.hash}`)

                    if(j != undefined) {
                        this.#selections.splice(this.#currentJobIndex,1)
                        await JobQueue.remove(j.hash)
                    }
                    
                    this.next()                  
                    
                })

                this.#queueWorker.on('exit', async() => {
                    console.log(`${config.cmd.tag} Worker-Stopped`);
                    console.log(`${config.cmd.tag} Restarting queue worker`);
                    for(let job of this.#selections) {
                        if(job.isLocked) {
                            await JobQueue.release()
                        }
                    }
                    new Queue(this.#queuePriority, this.#options)
                })

                this.#queueWorker.on('online', () => {
                    console.log(`${config.cmd.tag} Actively executing`);
                })

                Promise.resolve(true)

            } ).then( (status) => {

                //Listen for new Job, Pick up new task if tray is empty
                console.log(`${config.cmd.tag} Now listening on task arrival`);
                Queue.eventEmitter.on("newTask", async () => {
                    console.log(`${config.cmd.tag} New task arrived`);
                    if(this.#selections.length == 0) {
                        
                        await this.stageSelection()
                        this.next()

                    }
                })

                Promise.resolve(true)

            } ).then( (status) => {
                //Look out for job every 5sec [free and unfailed job]
                let intervalId = setInterval( async () => {
                    console.log(`${config.cmd.tag} Watchman finding free job`);
                    if(this.#selections.length == 0) {
                        await this.stageSelection()
                        this.next()
                    }else{
                        console.log(`${config.cmd.tag} Watchman tray is not empty yet`);
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
        try{
            //FIFO, SJF, Multi-level Priority
            
            for(let q of this.#queuePriority) {
                let c
                if (this.#options.useSJF) {
                    c = await JobQueue.findByQueueName(q, true)
                }else{
                    c = await JobQueue.findByQueueName(q, false)
                }
                this.#selections = [ ...this.#selections, ...c ] 
            }
            this.#currentJobIndex = -1;
            
            console.log(`${config.cmd.tag} ${this.#selections.length} tasks queued`);
            if(this.#options.showQueueList) {
                console.log(this.#selections)
            }
        }catch(e) {
            console.log(e);
        }
    }

    static add = async (queue, payload, args = [], options = {} ) => {
        try{
        
            if(!Queue.queues.includes(queue)) {
                throw new Error(`${queue} not found on queue priority list`) 
            }

            let defaultopts = { maxRetry: 3, timeout: 50000}

            options =  { ...defaultopts, ...options}
            
            
            if(typeof payload != 'function') {
                throw new Error("Only function is accepted as payload")
            }

            let fn = serialize(payload)

            const job = new JobQueue(queue, fn, args, options.maxRetry, options.timeout)
            
            const ajob = await job.create()
            const pos = await JobQueue.countJobs()

            //Inform queue
            Queue.eventEmitter.emit("newTask")
                
            return { hash: ajob.hash, pos: pos }
        }catch(e) {
            console.log(e);
        }
        
    }

    process = async (jobStageIndex) => {
        try{
            const job = this.#selections[jobStageIndex]
            
            if (job !=  null || job != undefined) {
                if (!job.isLocked) {
                    
                    if(typeof job.payload != 'string'){
                        console.log(`${config.cmd.tag} Skipping ${job.hash}`)
                        this.#selections.splice(jobStageIndex, 1)
                        this.next()
                    }else{
                        
                        //Auto lock
                        await JobQueue.updateTrial(job.hash)
                        this.#queueWorker.postMessage({hash: job.hash, args: job.args.join(','), payload: job.payload, mr: job.maxRetry, ts: job.timeout, tr: job.trial})

                    }
                    
                }else{
                    console.log(`${config.cmd.tag} Skipping ${job.hash}`)
                    this.#selections.splice(jobStageIndex, 1)
                    this.next()
                }
            }else{
                //incase of disjoint
                if(this.#selections.length > 0){
                    this.#selections.splice(jobStageIndex, 1)
                    this.next()
                }else{
                    console.log(`${config.cmd.tag} Queue relaxed, no task to process`)
                }

            }
        }catch(e) {
            console.log(e);
        }
    }

    static remove = async (hash) => {
        try{
            const job = await JobQueue.basedOnHash(hash)
            if(job != null) {
                if(!job.isLocked) {
                    //search if in memory too
                    await JobQueue.remove(hash);
                    return true
                }
            }

            return false
        }catch(e) {
            console.log(e);
        }
    }


    next = async () => {
        try{
            this.#currentJobIndex = 0 
            if(this.#selections.length > 0) {
                let j = this.#selections[0]
                console.log(`${config.cmd.tag} Moving to item ${j.hash}`);
                await this.process(this.#currentJobIndex)
            }else{
                console.log(`${config.cmd.tag} Queue relaxed, no task to process`);
            }
        }catch(e) {
            console.log(e);
        }
    }


}