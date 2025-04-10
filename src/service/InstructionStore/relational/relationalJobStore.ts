import { DataSource, Repository } from "typeorm";
import { JobStore } from "../../../structs/jobStoreStruct";
import { Job } from "../../../structs/jobStruct";
import { Queue } from "../../../structs/queueStruct";
import { ZJobberRelInstructionStore } from "./datasource/jobSchema";
import md5 from "md5";
import { v4 as uuidv4 } from 'uuid';

export class RelationalJobStore implements JobStore {
     
    private jobInstructionStoreRepo: Repository<ZJobberRelInstructionStore>

    constructor(private readonly connectionDataSource: DataSource) {
        //Connect to DB
        connectionDataSource
            .initialize()
            .then( () => {
                this.jobInstructionStoreRepo = this.connectionDataSource.getRepository(ZJobberRelInstructionStore)
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

    async _stash (queueName: string, payload: string, args: any[], maxRetry: number, timeout: number) : Promise<Job> {
        const job = new ZJobberRelInstructionStore()

        job.hash = this.calculateTaskHash(queueName, payload);
        job.queue = queueName
        job.payload = payload
        job.args = args
        job.maxRetry = maxRetry
        job.timeout = timeout
        job.createdAt = new Date().toISOString()
        job.modifiedAt = new Date().toISOString()
       
        await this.jobInstructionStoreRepo.save(job)

        return job as Job
    }

    async _lock (hash: string): Promise<boolean> {
        try{

            const job: ZJobberRelInstructionStore|null = await this.jobInstructionStoreRepo.findOne({ where: {hash: hash}})

            if ( job != null && !job.isLocked ){
                job.isLocked = true;
                job.modifiedAt = new Date().toISOString()
                await this.jobInstructionStoreRepo.save(job);
                return true;
            }
        
            return false;
            
        }catch(e){
            throw e;
        }
    }

    async _release (hash: string) : Promise<Job> {
        try{

            const job: ZJobberRelInstructionStore|null = await this.jobInstructionStoreRepo.findOne({ where: {hash: hash}})
            if ( job == null )
                throw Error(`Rel | Job ${hash} is not recognised in zJobber`)

            if ( job != null && !job.isLocked ){
                job.isLocked = false;
                job.modifiedAt = new Date().toISOString()
                return await this.jobInstructionStoreRepo.save(job) as Job;
            }
        
        
            return job as Job;
            
        }catch(e){
            throw e;
        }
    }

    async _purge (hash: string) : Promise<boolean> {
        try{
       
            await this.jobInstructionStoreRepo.delete({hash: hash});
            return true
        
        }catch(e){
            throw e;
        }
    }

    async _fail (hash: string): Promise<boolean> {
        try{

            const job: ZJobberRelInstructionStore|null = await this.jobInstructionStoreRepo.findOne({ where: {hash: hash}})

            if ( job != null && !job.isLocked ){
                job.isFailed = true;
                job.isLocked = false;
                job.modifiedAt = new Date().toISOString()
                await this.jobInstructionStoreRepo.save(job);
                return true;
            }
        
            return false;
            
        }catch(e){
            throw e;
        }
    }
    
    async _count () : Promise<number> {
        try{
            return await this.jobInstructionStoreRepo.count()
        }catch(e){
            throw e;
        }
    }

    async _fetchFree (q: Queue) : Promise<Job[]> {
        try{
       
            let result
            if(q.algo == "SJF") {
                result = await this.jobInstructionStoreRepo.find({
                    where: {
                        isFailed: false, isLocked: false, queue: q.name.trim()
                    },
                    order: {timeout: 'ASC'}
                },
                )
                    
            } else if( q.algo == "FIFO" ) {
                result = await this.jobInstructionStoreRepo.find({
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
            return result.map( r => r as Job )   
        }catch(e){
            throw e;
        }
    }
    
    async _fetchFreeHashes (q: Queue) : Promise<string[]> {
        try{
       
            let result
            if(q.algo == "SJF") {
                result = await this.jobInstructionStoreRepo.find({
                    where: {
                        isFailed: false, isLocked: false, queue: q.name.trim()
                    },
                    order: {timeout: 'ASC'},
                    select: ['hash']
                },
                )
                    
            } else if( q.algo == "FIFO" ) {
                result = await this.jobInstructionStoreRepo.find({
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

    async _fetchOne (hash: string) : Promise<Job | null> {
        try{
            return await this.jobInstructionStoreRepo.findOne({ where: {hash: hash}}) as Job
        } catch (e) {
            throw e;
        }
    }

    async _fetchLocked (q: Queue) : Promise<Job[]> {
        try{
       
            let result
            if(q.algo == "SJF") {
                result = await this.jobInstructionStoreRepo.find({
                    where: {
                        isLocked: true, queue: q.name.trim()
                    },
                    order: {timeout: 'ASC'}
                },
                )
                    
            } else if( q.algo == "FIFO" ) {
                result = await this.jobInstructionStoreRepo.find({
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
            return result.map( r => r as Job )   
        }catch(e){
            throw e;
        }
    }

    async _updateTrial (hash: string) : Promise<boolean>{
        try{

            const result = await this.jobInstructionStoreRepo.update(
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

    async _disconnect(): Promise<void> {
        
    }
}