import './global'
import { join, resolve } from 'path';
import { Worker, isMainThread, MessageChannel, parentPort } from 'worker_threads';
import { EventEmitter } from 'events';
import serialize from 'serialize-javascript';
import { Config } from './structs/configStruct';
import { Queue } from './structs/queueStruct';

class zJobber {
    private eventEmitter: EventEmitter;
    private nodeWorker: Worker;
    private currentJobIndex: number = -1;
    private internalConfig: { [key :string] : string } = {tag: '[z-jobber] '}
    private tag: string = '';
    private stagedJobRefs: string[] = [];
    private queueFlatMap: string[] = [];
     
    constructor(private queues: Queue[], private config: Config) {
        if(isMainThread) {
            new Promise( (resolve, reject) => {
                    this.queueFlatMap =this.queues.flatMap( (q) => { return q.name; } );
                    resolve('start')
                })
                .then( async (data) => {
                    // Setup Node Service worker
                    this.nodeWorker =  new Worker(join("./worker/QueueWorker.js"));
                    this.tag = this.config.workerTag??this.internalConfig.tag
                    process.stdout.write(`------ ${this.tag} Worker started ------`);
                    return Promise.resolve(true)
                })
                .then ( async (status) => {
                    //Stage selections
                    process.stdout.write(`------ ${this.tag} Staging Job Refs... ------`);
                    await this.stageRefs()
                    return Promise.resolve(true)
                } )
                .then ( (status) => {
                    //Listen to queue event
                    this.nodeWorker.on("message", async (msg) => {
                        if (msg === "MOVE_NEXT") {
                            
                            let j = this.stagedJobRefs[this.currentJobIndex]

                            if(j != undefined) {
                                this.stagedJobRefs.splice(this.currentJobIndex,1)
                                await JobQueue.remove(j.hash)
                            }
                            
                            this.next()

                        } else if(msg == "FAIL_THIS") {
                            let j = this.stagedJobRefs[this.currentJobIndex]

                            process.stdout.write(`${this.tag} Failing item ${j.hash}`)

                            if(j != undefined) {
                                this.stagedJobRefs.splice(this.currentJobIndex,1)
                                await JobQueue.remove(j.hash)
                            }
                            
                            this.next()
                                                    
                        } else if(msg == "RETRY_THIS") {
                            
                            let j = this.stagedJobRefs[this.currentJobIndex]
                            //update job list
                            this.stagedJobRefs[this.currentJobIndex] = await this.release(j.hash)
                            
                            if (j.trial < j.maxRetry) {
                                process.stdout.write(`${this.tag} Retrying item ${j.hash}`)
                                await this.process(this.currentJobIndex)
                            }else{
                                this.stagedJobRefs.splice(this.currentJobIndex,1)
                                await JobQueue.remove(j.hash)
                                this.next()
                            }
                            
                        }

                    })

                    this.nodeWorker.on("error", async (err) => {

                        let j = this.stagedJobRefs[this.currentJobIndex]

                        process.stdout.write(`${this.tag} Worker Failing item ${j.hash}`)

                        if(j != undefined) {
                            this.stagedJobRefs.splice(this.currentJobIndex,1)
                            await JobQueue.remove(j.hash)
                        }
                        
                        this.next()                  
                        
                    })

                    this.nodeWorker.on("messageerror", async (err) => {
                        //Worker couldn't read message properly
                        let j = this.stagedJobRefs[this.currentJobIndex]

                        process.stdout.write(`${this.tag} Worker Failing item ${j.hash}`)

                        if(j != undefined) {
                            this.stagedJobRefs.splice(this.currentJobIndex,1)
                            await JobQueue.remove(j.hash)
                        }
                        
                        this.next()                  
                        
                    })

                    this.nodeWorker.on('exit', async() => {
                        process.stdout.write(`${this.tag} Worker-Stopped`);
                        process.stdout.write(`${this.tag} Restarting queue worker`);
                        for(let job of this.stagedJobRefs) {
                            if(job.isLocked) {
                                await JobQueue.release()
                            }
                        }
                        new zJobber(this.queues, this.config)
                    })

                    this.nodeWorker.on('online', () => {
                        process.stdout.write(`${this.tag} Actively executing`);
                    })

                    Promise.resolve(true)
                } )
                .then( (status) => {
                
                    //Listen for new Job, Pick up new task if tray is empty
                    process.stdout.write(`${this.tag} Now listening on task arrival`);
                    this.eventEmitter.on("newTask", async () => {
                        process.stdout.write(`${this.tag} New task arrived`);
                        if(this.stagedJobRefs.length == 0) {
                            
                            await this.stageRefs()
                            this.next()
    
                        }
                    })
    
                    Promise.resolve(true)
    
                } ).then( (status) => {
                    //Look out for job every 5sec [free and unfailed job]
                    let intervalId = setInterval( async () => {
                        process.stdout.write(`${this.tag} Watchman finding free job`);
                        if(this.stagedJobRefs.length == 0) {
                            await this.stageRefs()
                            this.next()
                        }else{
                            process.stdout.write(`${this.tag} Watchman tray is not empty yet`);
                        }
                    }, this.config.refreshPeriod )
    
                    Promise.resolve(intervalId)
                }).then( (intervalId) => {
                    
                    //Execute next on queue
                    this.next()
                    Promise.resolve(true)

                } ).then( (status) => {

                    //Add Jobber to global context
                    if(!global.zJobberCtx) {
                        global.zJobberCtx = this;
                    }

                }).catch( (err) => {
                    process.stdout.write(err)
                } )
        }
    }

    private stageRefs(): void {
        try{
            
            //Multi-level Priority
            
            for(let q of this.queues) {
                let c = await JobQueue.fetchFreeJobs(q)
                
                this.stagedJobRefs = [ ...this.stagedJobRefs, ...c ]
            }
            this.currentJobIndex = -1;
            
            process.stdout.write(`${this.tag} ${this.stagedJobRefs.length} tasks queued`);
            if(this.config.showQueueList) {
                process.stdout.write(this.stagedJobRefs.toString())
            }
        }catch(e) {
            console.log(e);
        }
    }

    private static async dispatch(queueName: string, payload: Function, args = [], options: Partial<{maxRetry: number, timeout: number}> ) : Promise<{ hash: string, pos: number }> {
        try{
            //Pick jobber from global context
            if(!zJobberCtx.queueFlatMap.includes(queueName)) {
                throw new Error(`${queueName} not found on queue priority list`) 
            }

            let defaultopts = { maxRetry: 3, timeout: 50000}

            options =  { ...defaultopts, ...options}

            let fn = serialize(payload)

            const job = new JobQueue(queue, fn, args, options.maxRetry, options.timeout)
            
            const ajob = await job.create()
            const pos = await JobQueue.countJobs()

            //Inform queue
            zJobberCtx.eventEmitter.emit("newTask")
                
            return { hash: ajob.hash, pos: pos }
        }catch(e) {
            console.log(e);
        }
    }
     
    private async next(): Promise<void> {
        try{
            this.currentJobIndex = 0 
            if(this.stagedJobRefs.length > 0) {
                let j = this.stagedJobRefs[0]
                process.stdout.write(`${this.tag} Moving to item ${j.hash}`);
                await this.process(this.currentJobIndex)
            }else{
                process.stdout.write(`${this.tag} Queue relaxed, no task to process`);
            }
        }catch(e) {
            process.stdout.write(e);
        }
    }

    private async process(jobStageIndex: number): Promise<void> {
        try{
            const job = this.stagedJobRefs[jobStageIndex]
            
            if (job !=  null || job != undefined) {
                if (!job.isLocked) {
                    
                    if(typeof job.payload != 'string'){
                        process.stdout.write(`${this.tag} Skipping ${job.hash}`)
                        this.stagedJobRefs.splice(jobStageIndex, 1)
                        this.next()
                    }else{
                        
                        //Auto lock
                        await JobQueue.updateTrial(job.hash)
                        this.nodeWorker.postMessage({hash: job.hash, args: job.args.join(','), payload: job.payload, mr: job.maxRetry, ts: job.timeout, tr: job.trial})

                    }
                    
                }else{
                    process.stdout.write(`${this.tag} Skipping ${job.hash}`)
                    this.stagedJobRefs.splice(jobStageIndex, 1)
                    this.next()
                }
            }else{
                //incase of disjoint
                if(this.stagedJobRefs.length > 0){
                    this.stagedJobRefs.splice(jobStageIndex, 1)
                    this.next()
                }else{
                    process.stdout.write(`${this.tag} Queue relaxed, no task to process`)
                }

            }
        }catch(e) {
            console.error(e);
        }
    }

}

export default zJobber;