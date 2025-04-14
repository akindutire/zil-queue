import md5 from 'md5';
import pkg from 'mongoose';
const { startSession, Schema, model } = pkg;
import { v4 as uuidv4 } from 'uuid';
import { JobStore } from '../../../structs/taskStoreStruct';
import { Job } from '../../../structs/jobStruct';
import { Queue } from '../../../structs/queueStruct';
const { connect } = pkg;

export class MongoJobStore implements JobStore {
    
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

    async _stash(queueName: string, payload: string, args: any[], maxRetry:number, timeout:number) : Promise<Job> {
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
                        timeout: timeout,
                        trial: 0,
                        isFailed: false,
                        maxRetry: maxRetry,
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

    async _fetchFree(q: Queue) : Promise<Job[]>{
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

    async _fetchOne(hash: string) : Promise<Job|null>{
        try{
       
            return await m.findOne({isFailed: false, hash: hash});

        }catch(e){
            throw e;
        }
       
    }

    async _fetchLocked() : Promise<Job[]>{
        try{
            return await m.find({isLocked: false});
        }catch(e){
            throw e;
        }
       
    }

    async _fail(hash: string): Promise<boolean> {
        try{
       
            const result = await m.findOne({hash: hash});

            if(result === null){
                return false;                
            }else{
                
                result.isFailed = true
                result.isLocked = false

                await result.save();
                
                return true;
            }
        
        }catch(e){
            throw e;
        }
       
    }

    async _updateTrial(hash: string) : Promise<boolean>{
        try{

            const result = await m.findOne({hash: hash});

            if ( !result.isLocked ){
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

    async _release(hash: string): Promise<Job> {
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

const m = model('z_jobber_instruction_store', schema);




export const QueueSchema = schema;