import './global'
import { join, resolve } from 'path';
import { Worker, isMainThread, MessageChannel, parentPort } from 'worker_threads';
import { EventEmitter } from 'events';
import serialize from 'serialize-javascript';
import { Config } from './structs/configStruct';
import { Queue } from './structs/queueStruct';
import { TaskStore } from './structs/taskStoreStruct';
import { TaskStoreFactory } from './factory/taskStoreFactory';
import { Task } from './structs/taskStruct';

class zJobber {
    private eventEmitter: EventEmitter;
    private nodeWorker: Worker;
    private currentJobIndex: number = -1;
    private internalConfig: { [key :string] : string } = {tag: '[z-jobber] '}
    private tag: string = '';
    private stagedTaskRefs: string[] = [];
    private queueFlatMap: string[] = [];
    private taskStore: TaskStore
     
    /**
     * Setup Jobber
     * @param queues 
     * @param config 
     */
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
                    

                    this.taskStore = (new TaskStoreFactory()).make(config.connection)

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
                            
                            let hash = this.stagedTaskRefs[this.currentJobIndex]
                            let j: Task|null = await this.taskStore._fetchOne(hash)
                            if(j) {
                                this.stagedTaskRefs.splice(this.currentJobIndex,1)
                                await this.taskStore._purge(j.hash)
                            }
                            
                            this.next()

                        } else if(msg == "FAIL_THIS") {
                            let hash = this.stagedTaskRefs[this.currentJobIndex]

                            process.stdout.write(`${this.tag} Failing item ${hash}`)

                            if(hash != undefined) {
                                this.stagedTaskRefs.splice(this.currentJobIndex,1)
                                await this.taskStore._fail(hash)
                            }
                            
                            this.next()
                                                    
                        } else if(msg == "RETRY_THIS") {
                            
                            let hash = this.stagedTaskRefs[this.currentJobIndex]
                            //update job list
                            let j = await this.taskStore._release(hash)
                            if (j.trial < j.maxRetry) {
                                process.stdout.write(`${this.tag} Retrying item ${j.hash}`)
                                await this.process(this.currentJobIndex)
                            }else{
                                this.stagedTaskRefs.splice(this.currentJobIndex,1)
                                await this.taskStore._purge(j.hash)
                                this.next()
                            }
                            
                        }

                    })

                    this.nodeWorker.on("error", async (err) => {

                        let hash = this.stagedTaskRefs[this.currentJobIndex]

                        process.stdout.write(`${this.tag} Worker Failing item ${hash}`)
                        
                        this.stagedTaskRefs.splice(this.currentJobIndex,1)
                        await this.taskStore._purge(hash)
                        
                        this.next()                  
                        
                    })

                    this.nodeWorker.on("messageerror", async (err) => {
                        //Worker couldn't read message properly
                        let hash = this.stagedTaskRefs[this.currentJobIndex]

                        process.stdout.write(`${this.tag} : messageerror: Worker Failing item ${hash}`)

                        this.stagedTaskRefs.splice(this.currentJobIndex,1)
                        await this.taskStore._purge(hash)
                        
                        this.next()                  
                        
                    })

                    this.nodeWorker.on('exit', async() => {
                        process.stdout.write(`${this.tag} Worker-Stopped`);
                        
                        //Release current job held
                        let hash = this.stagedTaskRefs[this.currentJobIndex]
                        await this.taskStore._release(hash)   
                        
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
                        if(this.stagedTaskRefs.length == 0) {
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
                        if(this.stagedTaskRefs.length == 0) {
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
                let c = await this.taskStore._fetchFreeHashes(q)
                this.stagedTaskRefs = [ ...this.stagedTaskRefs, ...c ]
            }
            this.currentJobIndex = -1;
            
            process.stdout.write(`${this.tag} ${this.stagedTaskRefs.length} tasks queued`);
            if(this.config.showQueueList) {
                process.stdout.write(this.stagedTaskRefs.toString())
            }
        }catch(e) {
            console.error(e);
        }
    }
     
    /**
     * Iterator 
     */
    private async next(): Promise<void> {
        try{
            this.currentJobIndex = 0 
            if(this.stagedTaskRefs.length > 0) {
                let hash = this.stagedTaskRefs[0]
                process.stdout.write(`${this.tag} Moving to item ${hash}`);
                await this.process(this.currentJobIndex)
            }else{
                process.stdout.write(`${this.tag} Queue relaxed, no task to process`);
            }
        }catch(e) {
            console.error(e);
        }
    }

    /**
     * Processor
     * @param jobStageIndex 
     */
    private async process(jobStageIndex: number): Promise<void> {
        try{
            const hash = this.stagedTaskRefs[jobStageIndex]
            let job: Task|null = await this.taskStore._fetchOne(hash)

            if (job !=  null || job != undefined) {
                if (!job.isLocked) {
                    
                    if(typeof job.payload != 'string'){
                        process.stdout.write(`${this.tag} Skipping ${job.hash}`)
                        this.stagedTaskRefs.splice(jobStageIndex, 1)
                        this.next()
                    }else{
                        
                        //Auto lock
                        await this.taskStore._updateTrial(job.hash)
                        this.nodeWorker.postMessage({tag: this.tag, hash: job.hash, args: job.args.join(','), payload: job.payload, mr: job.maxRetry, ts: job.timeout, tr: job.trial})

                    }
                    
                }else{
                    process.stdout.write(`${this.tag} Skipping ${job.hash}`)
                    this.stagedTaskRefs.splice(jobStageIndex, 1)
                    this.next()
                }
            }else{
                //incase of disjoint
                if(this.stagedTaskRefs.length > 0){
                    this.stagedTaskRefs.splice(jobStageIndex, 1)
                    this.next()
                }else{
                    process.stdout.write(`${this.tag} Queue relaxed, no task to process`)
                }

            }
        }catch(e) {
            console.error(e);
        }
    }

    // Code API
    /**
     * Register a new task
     * @param queueName 
     * @param payload 
     * @param args 
     * @param options 
     * @returns 
     */
    public static async dispatch(queueName: string, payload: Function, args = [], options: Partial<{maxRetry: number, timeout: number, delayPeriod: number}> ) : Promise<{ hash: string, pos: number }> {
        try{
            //Pick jobber from global context
            if(!zJobberCtx.queueFlatMap.includes(queueName)) {
                throw new Error(`${queueName} not found on queue priority list`) 
            }

            let defaultopts:{maxRetry: number, timeout: number, delayPeriod: number}  = { maxRetry: 3, timeout: 50000, delayPeriod: 0}

            let newOptions =  { ...defaultopts, ...options}

            let fn = serialize(payload)
            
            const task = await global.zJobberCtx.taskStore._stash(queueName, fn, args, newOptions.maxRetry, newOptions.timeout)
          
            const pos = await global.zJobberCtx.taskStore._count()

            //Inform queue
            zJobberCtx.eventEmitter.emit("newTask")
                
            return { hash: task.hash, pos: pos }
        }catch(e) {
            console.error(e);
            throw e; // Re-throw the error after logging
        }
    }

    /**
     * Remove a task entirely , free or failed
     * @param hash 
     * @returns 
     */
    public static async purge(hash: string): Promise<boolean> {
        try {
            return await global.zJobberCtx.taskStore._purge(hash)
        } catch (e) {
            console.error(e);
            throw e; // Re-throw the error after logging
        }
    }

    /**
     * Restore a failed task
     * @param hash 
     * @returns 
     */
    public static async restoreTask(hash: string) : Promise<boolean> {
        try {
            return await global.zJobberCtx.taskStore._restoreOneFailed(hash)
        } catch (e) {
            console.error(e);
            throw e; // Re-throw the error after logging
        }
    }

    /**
     * Retores all failed tasks
     * @returns 
     */
    public static async restoreAll() : Promise<number> {
        try {
            return await global.zJobberCtx.taskStore._restoreFailed()
        } catch (e) {
            console.error(e);
            throw e; // Re-throw the error after logging
        }
    }


    /**
     * Delay execution of a free task
     * @param hash 
     * @param period 
     * @returns 
     */
    public static async delayTask(hash: string, period: number) : Promise<boolean> {
        try {
            return await global.zJobberCtx.taskStore._delay(hash, period)
        } catch (e) {
            console.error(e);
            throw e; // Re-throw the error after logging
        }
    }
   
    /**
     * Get list of free tasks
     * @param queueName 
     * @returns 
     */
    public static async listFree(queueName: string) : Promise<Task[]> {
        try {
            let queues = global.zJobberCtx.queues.filter( (q) => { return q.name ==  queueName } )
            if(queues.length == 0)
                return []

            return await global.zJobberCtx.taskStore._fetchFree(queues[0])
        } catch (e) {
            console.error(e);
            throw e; // Re-throw the error after logging
        }
    }

    /**
     * Get list of failed tasks
     * @param queueName 
     * @returns 
     */
    public static async listFailed(queueName: string) : Promise<Task[]> {
        try {
            let queues = global.zJobberCtx.queues.filter( (q) => { return q.name ==  queueName } )
            if(queues.length == 0)
                return []

            return await global.zJobberCtx.taskStore._fetchFailed(queues[0])
        } catch (e) {
            console.error(e);
            throw e; // Re-throw the error after logging
        }
    }



}

export default zJobber;