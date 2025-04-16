import md5 from 'md5';
import pkg from 'mongoose';
const { startSession, Schema, model } = pkg;
import { v4 as uuidv4 } from 'uuid';
import { TaskOptions, TaskStore } from '../../../structs/taskStoreStruct';
import { Task } from '../../../structs/taskStruct';
import { Queue } from '../../../structs/queueStruct';
const { connect } = pkg;

export class MongoTaskStore implements TaskStore {
    
    constructor(options: { uri: string } ){ 
        //Connect to DB
        this.connect(options.uri)  
    }

    private async connect(uri: string) {
        try{
            const connection = await  connect(uri, {  serverSelectionTimeoutMS: 10000 });
            process.stdout.write("Mongo: Job store connection successful")
            return connection
        } catch (e) {
            throw e
        }
        
    }

    private calculateTaskHash(queueName: string, payload: any){
        try{
            let uid = uuidv4();
            return md5(`${queueName}-${payload}$-01-ca.${uid}`);
        }catch(err){
            throw err;
        }
    }

    async _stash(queueName: string, payload: string, args: any[], options: TaskOptions) : Promise<Task> {
        try{
            const session = await startSession();
            try{
             
                let stashedProcess = await session.withTransaction( async () => {
                    
                    let hash = this.calculateTaskHash(queueName, payload);
            
                    return await m.create({
                        queue: queueName,
                        hash: hash,
                        payload: payload,
                        args: args,
                        timeout: options.timeout,
                        trial: 0,
                        isFailed: false,
                        maxRetry: options.maxRetry,
                        delay: options.delay,
                        createdAt : new Date().toISOString(),
                        modifiedAt : new Date().toISOString()
                    }); 
                
                })

                return {
                    ...stashedProcess
                }
            }finally {
                session.endSession();
            }
        }catch(e){
            throw e;
        }
    }

    async _count() : Promise<number> {
        try{
       
            const count = await m.countDocuments();
            return count;   
        
        }catch(e){
            throw e;
        }
       
    }

    async _fetchFree(q: Queue) : Promise<Task[]>{
        try{
       
            let result
            if(q.algo == "SJF") {
                result = await m.find({isFailed: false, isLocked: false, queue: q.name.trim()}).sort({timeout: 1});
            } else if( q.algo == "FIFO" ) {
                result = await m.find({isFailed: false, isLocked: false, queue: q.name.trim()}).sort({createdAt: 1});
            } else {
                throw Error(`Queue scheduling algorithm ${q?.algo} not supported`)
            }

            if(result === null){
                return [];                
            }

            return result;   
        
        }catch(e){
            throw e;
        }
       
    }

    async _fetchFailed(q: Queue) : Promise<Task[]>{
        try{
       
            let result
            if(q.algo == "SJF") {
                result = await f.find({isFailed: false, isLocked: false, queue: q.name.trim()}).sort({timeout: 1});
            } else if( q.algo == "FIFO" ) {
                result = await f.find({isFailed: false, isLocked: false, queue: q.name.trim()}).sort({createdAt: 1});
            } else {
                throw Error(`Queue scheduling algorithm ${q?.algo} not supported`)
            }

            if(result === null){
                return [];                
            }

            return result;   
        
        }catch(e){
            throw e;
        }
       
    }

    async _fetchFreeHashes(q: Queue) : Promise<string[]>{
        try{
       
            let result
            if(q.algo == "SJF") {
                result = await m.find({isFailed: false, isLocked: false, queue: q.name.trim()}).select(['hash']).sort({timeout: 1});
            } else if( q.algo == "FIFO" ) {
                result = await m.find({isFailed: false, isLocked: false, queue: q.name.trim()}).select(['hash']).sort({createdAt: 1});
            } else {
                throw Error(`Queue scheduling algorithm ${q?.algo} not supported`)
            }

            if(result === null){
                return [];                
            }

            return result;   
        
        }catch(e){
            throw e;
        }
       
    }

    async _fetchOne(hash: string) : Promise<Task|null>{
        try{
       
            return await m.findOne({isFailed: false, hash: hash});

        }catch(e){
            throw e;
        }
       
    }

    async _fetchLocked() : Promise<Task[]>{
        try{
            return await m.find({isLocked: false});
        }catch(e){
            throw e;
        }
       
    }

    async _fail(hash: string): Promise<boolean> {
        const result = await m.findOne({ hash });
        if (!result) return false;
        await f.create({ ...result.toObject(), isFailed: false, isLocked: false });
        await this._purgeMainOnly(hash);
        return true;
    }

    async _restoreFailed(): Promise<number> {
        const failedTasks = await f.find({});
        let count = 0;
        for (const task of failedTasks) {
            await m.create({ ...task.toObject(), isFailed: false, isLocked: false, trial: 0 });
            await f.deleteOne({ hash: task.hash });
            count++;
        }
        return count;
    }

    async _restoreOneFailed(hash: string): Promise<boolean> {
        const failedTask = await f.findOne({ hash });
        if (!failedTask) return false;
        await m.create({ ...failedTask.toObject(), isFailed: false, isLocked: false, trial: 0 });
        await f.deleteOne({ hash });
        return true;
    }

    async _lock(hash: string): Promise<boolean> {
        try{

            const result = await m.findOne({hash: hash});

            if ( result != null || !result.isLocked ){
                result.isLocked = true;
                result.modifiedAt = new Date().toISOString()
                await result.save();
                return true;
            }
        
            return false;
            
        }catch(e){
            throw e;
        }
       
    }

    async _release(hash: string): Promise<Task> {
        try{

            const result = await m.findOne({hash: hash});

            if ( result != null ){
                result.isLocked = false;
                result.modifiedAt = new Date().toISOString()
                let r = await result.save();
                return r;
            }
        
            return result;
            
        }catch(e){
            throw e;
        }
       
    }

    async _purge(hash: string): Promise<boolean> {
        try{
       
            await m.deleteOne({hash: hash});
            await f.deleteOne({hash: hash});
            return true
        
        }catch(e){
            throw e;
        }
       
    }

    private async _purgeMainOnly(hash: string): Promise<boolean> {
        try{
       
            await m.deleteOne({hash: hash});
            return true
        
        }catch(e){
            throw e;
        }
       
    }

    async _updateTrial(hash: string) : Promise<boolean>{
        try{

            const result = await m.findOne({hash: hash});

            if ( !result?.isLocked ){
                result.trial = parseInt(result.trial) + 1;
                result.isLocked = true
                result.modifiedAt = new Date().toISOString()
                await result.save();
                return true;
            }
        
            return false;
            
        }catch(e){
            throw e;
        }
       
    }

    async _delay (hash: string, period: number) : Promise<boolean>{
        try{

            const task = await m.findOne({ where: {hash} })
            if(!task?.isLocked) {
                task.delay = period
                task.modifiedAt = new Date().toISOString()
                await task.save()
            }
              
            return true
            
        }catch(e){
            throw e;
        }
    }

    async _disconnect(): Promise<void> {
        
    }
   
}

const schema = new Schema({

    queue : {
        type: String,
        required: [true, "Queue name required"],
        default: 'default'
    },

    hash : {
        type: String,
        required: [true, "Hash calculation failed"]
    },
    payload:  {
        type: String,
        required: [true, 'Payload is required']
    },

    args:  {
        type: Array,
        default: []
    },

    maxRetry:  {
        type: Number,
        required: [3, 'Retry option is required']
    },
    trial:  {
        type: Number,
        required: [0, 'trial is required']
    },
    timeout: {
        type: Number,
        default: 0
    },

    delay: {
        type: Number,
        default: 0
    },

    isLocked: {
        type: Boolean,
        default: false
    },
    
    isFailed: {
        type: Boolean,
        default: false
    },

    createdAt : {
        type: String,
        default:  new Date().toISOString(),
        immutable: true,
    },

    modifiedAt : {
        type: String,
        default:  new Date().toISOString(),
    }

});

const m = model('z_jobber_task_instruction_store', schema);
const f = model('z_jobber_failed_task_instruction_store', schema);




export const QueueSchema = schema;