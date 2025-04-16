import { DataSource, Repository } from "typeorm";
import { TaskOptions, TaskStore } from "../../../structs/taskStoreStruct";
import { Task } from "../../../structs/taskStruct";
import { Queue } from "../../../structs/queueStruct";
import { ZTaskRelInstructionStore } from "./datasource/taskSchema";
import md5 from "md5";
import { v4 as uuidv4 } from 'uuid';
import { ZFailedTaskRelInstructionStore } from "./datasource/failedTaskSchema";

export class RelationalTaskStore implements TaskStore {
    
    private taskInstructionStoreRepo: Repository<ZTaskRelInstructionStore>
    private failedTaskInstructionStoreRepo: Repository<ZFailedTaskRelInstructionStore>

    constructor(private readonly connectionDataSource: DataSource) {
        //Connect to DB
        connectionDataSource
            .initialize()
            .then( () => {
                this.taskInstructionStoreRepo = this.connectionDataSource.getRepository(ZTaskRelInstructionStore)
                this.failedTaskInstructionStoreRepo = this.connectionDataSource.getRepository(ZFailedTaskRelInstructionStore)
                process.stdout.write(`${connectionDataSource.options.type} Job store connection successful`)
            })
            .catch( (e) => { throw e; } )
    }

    private calculateTaskHash(queueName: string, payload: any) {
            try {
                let uid = uuidv4();
                return md5(`${queueName}-${payload}$-01-ca.${uid}`);
            } catch (err) {
                throw err;
            }
        }

    async _stash (queueName: string, payload: string, args: any[], options: TaskOptions) : Promise<Task> {
        const job = new ZTaskRelInstructionStore()

        job.hash = this.calculateTaskHash(queueName, payload);
        job.queue = queueName
        job.payload = payload
        job.args = args
        job.maxRetry = options.maxRetry
        job.timeout = options.timeout
        job.delay = options.delay
        job.createdAt = new Date().toISOString()
        job.modifiedAt = new Date().toISOString()
       
        await this.taskInstructionStoreRepo.save(job)

        return job as Task
    }

    async _lock (hash: string): Promise<boolean> {
        try{

            const job: ZTaskRelInstructionStore|null = await this.taskInstructionStoreRepo.findOne({ where: {hash: hash}})

            if ( job != null && !job.isLocked ){
                job.isLocked = true;
                job.modifiedAt = new Date().toISOString()
                await this.taskInstructionStoreRepo.save(job);
                return true;
            }
        
            return false;
            
        }catch(e){
            throw e;
        }
    }

    async _release (hash: string) : Promise<Task> {
        try{

            const job: ZTaskRelInstructionStore|null = await this.taskInstructionStoreRepo.findOne({ where: {hash: hash}})
            if ( job == null )
                throw Error(`Rel | Job ${hash} is not recognised in zJobber`)

            if ( job != null && !job.isLocked ){
                job.isLocked = false;
                job.modifiedAt = new Date().toISOString()
                return await this.taskInstructionStoreRepo.save(job) as Task;
            }
        
        
            return job as Task;
            
        }catch(e){
            throw e;
        }
    }

    async _purge (hash: string) : Promise<boolean> {
        try{
       
            await this.taskInstructionStoreRepo.delete({hash: hash});
            await this.failedTaskInstructionStoreRepo.delete({hash: hash});
            return true
        
        }catch(e){
            throw e;
        }
    }

    private async _purgeMainOnly (hash: string) : Promise<boolean> {
        try{
       
            await this.taskInstructionStoreRepo.delete({hash: hash});
            return true
        
        }catch(e){
            throw e;
        }
    }

    async _fail (hash: string): Promise<boolean> {
        try{

            const job: ZTaskRelInstructionStore|null = await this.taskInstructionStoreRepo.findOne({ where: {hash: hash}})

            if ( job != null ){
                job.isFailed = false;
                job.isLocked = false;
                job.modifiedAt = new Date().toISOString()
                await this.failedTaskInstructionStoreRepo.save(job);
                await this._purgeMainOnly(hash)
                return true;
            }
        
            return false;
            
        }catch(e){
            throw e;
        }
    }

    async _restoreFailed(): Promise<number> {
        try {
            const failedJobs = await this.failedTaskInstructionStoreRepo.find();
    
            for (const job of failedJobs) {
                job.isFailed = false;
                job.isLocked = false;
                job.trial = 0;
                job.modifiedAt = new Date().toISOString();
            }
    
            const result = await this.taskInstructionStoreRepo.save(failedJobs);
            await this.failedTaskInstructionStoreRepo.delete({})
            return result.length;
        } catch (e) {
            throw e;
        }
    }

    async _restoreOneFailed(hash: string): Promise<boolean> {
        try {
            const job = await this.failedTaskInstructionStoreRepo.findOne({ where: { hash } });
    
            if (!job) return false;
    
            job.isFailed = false;
            job.isLocked = false;
            job.trial = 0;
            job.modifiedAt = new Date().toISOString();
    
            await this.taskInstructionStoreRepo.save(job);
            await this.failedTaskInstructionStoreRepo.delete({ hash })

            return true;
        } catch (e) {
            throw e;
        }
    }
    
    async _count () : Promise<number> {
        try{
            return await this.taskInstructionStoreRepo.count()
        }catch(e){
            throw e;
        }
    }

    async _fetchFree (q: Queue) : Promise<Task[]> {
        try{
       
            let result
            if(q.algo == "SJF") {
                result = await this.taskInstructionStoreRepo.find({
                    where: {
                        isFailed: false, isLocked: false, queue: q.name.trim()
                    },
                    order: {timeout: 'ASC'}
                },
                )
                    
            } else if( q.algo == "FIFO" ) {
                result = await this.taskInstructionStoreRepo.find({
                    where: {
                        isFailed: false, isLocked: false, queue: q.name.trim()
                    },
                    order: {createdAt: 'ASC'}
                },
                )
            } else {
                throw Error(`Queue scheduling algorithm ${q?.algo} not supported`)
            }

            if(result === null){
                return [];                
            }
            return result.map( r => r as Task )
        }catch(e){
            throw e;
        }
    }
    
    async _fetchFailed(q: Queue): Promise<Task[]> {
        try {
            let result;
            if (q.algo === "SJF") {
                result = await this.failedTaskInstructionStoreRepo.find({
                    where: {
                        queue: q.name.trim()
                    },
                    order: { timeout: 'ASC' }
                });
            } else if (q.algo === "FIFO") {
                result = await this.failedTaskInstructionStoreRepo.find({
                    where: {
                        queue: q.name.trim()
                    },
                    order: { createdAt: 'ASC' }
                });
            } else {
                throw Error(`Queue scheduling algorithm ${q?.algo} not supported`);
            }
    
            return result?.map(r => r as Task) ?? [];
    
        } catch (e) {
            throw e;
        }
    }

    
    async _fetchFreeHashes (q: Queue) : Promise<string[]> {
        try{
       
            let result
            if(q.algo == "SJF") {
                result = await this.taskInstructionStoreRepo.find({
                    where: {
                        isFailed: false, isLocked: false, queue: q.name.trim()
                    },
                    order: {timeout: 'ASC'},
                    select: ['hash']
                },
                )
                    
            } else if( q.algo == "FIFO" ) {
                result = await this.taskInstructionStoreRepo.find({
                    where: {
                        isFailed: false, isLocked: false, queue: q.name.trim()
                    },
                    order: {createdAt: 'ASC'},
                    select: ['hash']
                },
                )
            } else {
                throw Error(`Queue scheduling algorithm ${q?.algo} not supported`)
            }

            if(result === null){
                return [];                
            }
            return result.map( r => r.hash )   
        }catch(e){
            throw e;
        }
    }

    async _fetchOne (hash: string) : Promise<Task | null> {
        try{
            return await this.taskInstructionStoreRepo.findOne({ where: {hash: hash}}) as Task
        } catch (e) {
            throw e;
        }
    }

    async _fetchLocked (q: Queue) : Promise<Task[]> {
        try{
       
            let result
            if(q.algo == "SJF") {
                result = await this.taskInstructionStoreRepo.find({
                    where: {
                        isLocked: true, queue: q.name.trim()
                    },
                    order: {timeout: 'ASC'}
                },
                )
                    
            } else if( q.algo == "FIFO" ) {
                result = await this.taskInstructionStoreRepo.find({
                    where: {
                        isLocked: true, queue: q.name.trim()
                    },
                    order: {createdAt: 'ASC'}
                },
                )
            } else {
                throw Error(`Queue scheduling algorithm ${q?.algo} not supported`)
            }

            if(result === null){
                return [];                
            }
            return result.map( r => r as Task )
        }catch(e){
            throw e;
        }
    }

    async _updateTrial (hash: string) : Promise<boolean>{
        try{

            const result = await this.taskInstructionStoreRepo.update(
                { hash: hash },
                { 
                  trial: () => "trial + 1", 
                  isLocked: true,
                }
              );
              
            return (result?.affected ?? 0) > 0;
            
        }catch(e){
            throw e;
        }
    }

    async _delay (hash: string, period: number) : Promise<boolean>{
        try{

            const task = await this.taskInstructionStoreRepo.findOne({ where: {hash} })
            if(!task?.isLocked) {
                const result = await this.taskInstructionStoreRepo.update(
                    { hash: hash },
                    { 
                      delay: period
                    }
                  );
                  return (result?.affected ?? 0) > 0;
            }
              
            return false
            
        }catch(e){
            throw e;
        }
    }

    async _disconnect(): Promise<void> {
        try {
            if (this.connectionDataSource.isInitialized) {
                await this.connectionDataSource.destroy();
                process.stdout.write(`${this.connectionDataSource.options.type} Job store disconnected\n`);
            }
        } catch (e) {
            throw e;
        }
    }
}