import './global'
import { join, resolve } from 'path';
import { Worker, isMainThread, MessageChannel, parentPort } from 'worker_threads';
import { EventEmitter } from 'events';
import serialize from 'serialize-javascript';
import { Config } from './structs/configStruct';
import { Queue } from './structs/queueStruct';
import { JobStore } from './structs/jobStoreStruct';
import { JobStoreFactory } from './factory/jobStoreFactory';
import { Job } from './structs/jobStruct';

class zJobber {
    private eventEmitter: EventEmitter;
    private nodeWorker: Worker;
    private currentJobIndex: number = -1;
    private internalConfig: { [key :string] : string } = {tag: '[z-jobber] '}
    private tag: string = '';
    private stagedJobRefs: string[] = [];
    private queueFlatMap: string[] = [];
    private jobStore: JobStore
     
    constructor(private queues: Queue[], private config: Config) {
        if(isMainThread) {
            new Promise( (resolve, reject) => {

                    let l_flat_queue = new Set(this.queues.flatMap( (q) => { return q.name; } ))

                    let l_queue = []
                    for (const ulq of this.queues) {
                        if(l_flat_queue.has(ulq.name)) {
                            if(ulq.algo != "SJF")
                                ulq.algo = "FIFO"
                            
                            l_queue.push(ulq)
                        }
                    }

                    this.queues = l_queue
                    

                    this.jobStore = (new JobStoreFactory()).make(config.connection)

                    //Listen to uncaught exception at beginning of the app
                    process.on('uncaughtException', (e) => {
                        console.error(e);
                    });

                    process.on('unhandledRejection', (e) => {
                        console.error(e);
                    });

                    process.on('unhandledPromiseRejectionWarning', (e) => {
                        console.error(e);
                    });


                    resolve('start')
                })
                .then( async (data) => {
                    // Setup Node Service worker
                    this.nodeWorker =  new Worker(join("./worker/jobWorker.js"));
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
                            
                            let hash = this.stagedJobRefs[this.currentJobIndex]
                            let j: Job|null = await this.jobStore._fetchOne(hash)
                            if(j) {
                                this.stagedJobRefs.splice(this.currentJobIndex,1)
                                await this.jobStore._purge(j.hash)
                            }
                            
                            this.next()

                        } else if(msg == "FAIL_THIS") {
                            let hash = this.stagedJobRefs[this.currentJobIndex]

                            process.stdout.write(`${this.tag} Failing item ${hash}`)

                            if(hash != undefined) {
                                this.stagedJobRefs.splice(this.currentJobIndex,1)
                                await this.jobStore._fail(hash)
                            }
                            
                            this.next()
                                                    
                        } else if(msg == "RETRY_THIS") {
                            
                            let hash = this.stagedJobRefs[this.currentJobIndex]
                            //update job list
                            let j = await this.jobStore._release(hash)
                            if (j.trial < j.maxRetry) {
                                process.stdout.write(`${this.tag} Retrying item ${j.hash}`)
                                await this.process(this.currentJobIndex)
                            }else{
                                this.stagedJobRefs.splice(this.currentJobIndex,1)
                                await this.jobStore._purge(j.hash)
                                this.next()
                            }
                            
                        }

                    })

                    this.nodeWorker.on("error", async (err) => {

                        let hash = this.stagedJobRefs[this.currentJobIndex]

                        process.stdout.write(`${this.tag} Worker Failing item ${hash}`)
                        
                        this.stagedJobRefs.splice(this.currentJobIndex,1)
                        await this.jobStore._purge(hash)
                        
                        this.next()                  
                        
                    })

                    this.nodeWorker.on("messageerror", async (err) => {
                        //Worker couldn't read message properly
                        let hash = this.stagedJobRefs[this.currentJobIndex]

                        process.stdout.write(`${this.tag} : messageerror: Worker Failing item ${hash}`)

                        this.stagedJobRefs.splice(this.currentJobIndex,1)
                        await this.jobStore._purge(hash)
                        
                        this.next()                  
                        
                    })

                    this.nodeWorker.on('exit', async() => {
                        process.stdout.write(`${this.tag} Worker-Stopped`);
                        
                        //Release current job held
                        let hash = this.stagedJobRefs[this.currentJobIndex]
                        await this.jobStore._release(hash)   
                        
                        process.stdout.write(`${this.tag} Restarting queue worker`);
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
                            //if not task is running, load one new ones
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
                    console.error(err)
                } )
        }
    }

    private async stageRefs(): Promise<void> {
        try{
            
            //Multi-level Priority
            
            for(let q of this.queues) {
                let c = await this.jobStore._fetchFreeHashes(q)
                this.stagedJobRefs = [ ...this.stagedJobRefs, ...c ]
            }
            this.currentJobIndex = -1;
            
            process.stdout.write(`${this.tag} ${this.stagedJobRefs.length} tasks queued`);
            if(this.config.showQueueList) {
                process.stdout.write(this.stagedJobRefs.toString())
            }
        }catch(e) {
            console.error(e);
        }
    }

    public static async dispatch(queueName: string, payload: Function, args = [], options: Partial<{maxRetry: number, timeout: number}> ) : Promise<{ hash: string, pos: number }> {
        try{
            //Pick jobber from global context
            if(!zJobberCtx.queueFlatMap.includes(queueName)) {
                throw new Error(`${queueName} not found on queue priority list`) 
            }

            let defaultopts:{maxRetry: number, timeout: number}  = { maxRetry: 3, timeout: 50000}

            let newOptions =  { ...defaultopts, ...options}

            let fn = serialize(payload)
            
            const job = await global.zJobberCtx.jobStore._stash(queueName, fn, args, newOptions.maxRetry, newOptions.timeout)
          
            const pos = await global.zJobberCtx.jobStore._count()

            //Inform queue
            zJobberCtx.eventEmitter.emit("newTask")
                
            return { hash: job.hash, pos: pos }
        }catch(e) {
            console.error(e);
            throw e; // Re-throw the error after logging
        }
    }

    public static async purge(hash: string): Promise<boolean> {
        try {
            return await global.zJobberCtx.jobStore._purge(hash)
        } catch (e) {
            console.error(e);
            throw e; // Re-throw the error after logging
        }
    }
     
    private async next(): Promise<void> {
        try{
            this.currentJobIndex = 0 
            if(this.stagedJobRefs.length > 0) {
                let hash = this.stagedJobRefs[0]
                process.stdout.write(`${this.tag} Moving to item ${hash}`);
                await this.process(this.currentJobIndex)
            }else{
                process.stdout.write(`${this.tag} Queue relaxed, no task to process`);
            }
        }catch(e) {
            console.error(e);
        }
    }

    private async process(jobStageIndex: number): Promise<void> {
        try{
            const hash = this.stagedJobRefs[jobStageIndex]
            let job: Job|null = await this.jobStore._fetchOne(hash)

            if (job !=  null || job != undefined) {
                if (!job.isLocked) {
                    
                    if(typeof job.payload != 'string'){
                        process.stdout.write(`${this.tag} Skipping ${job.hash}`)
                        this.stagedJobRefs.splice(jobStageIndex, 1)
                        this.next()
                    }else{
                        
                        //Auto lock
                        await this.jobStore._updateTrial(job.hash)
                        this.nodeWorker.postMessage({tag: this.tag, hash: job.hash, args: job.args.join(','), payload: job.payload, mr: job.maxRetry, ts: job.timeout, tr: job.trial})

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