import md5 from 'md5';
import pkg from 'mongoose';
const { startSession, Schema, model: _model } = pkg;
import * as ms from 'mongoose-sequence'
const AutoIncrement = ms(mongoose);
import { v4 as uuidv4 } from 'uuid';

export default  class JobQueue{
    
    constructor(queueName, payload, args, maxRetry, timeout){
        this.payload = payload;
        this.maxRetry = maxRetry;
        this.timeout = timeout;
        this.queueName = queueName
        this.isNew = true
        this.args = args
    }
 

    calculateTaskHash(){
        try{
            let uid = uuidv4();
            return md5(`${this.queueName}-${this.payload}$-01-ca.${uid}`);
        }catch(err){
            throw err;
        }
    }

    async create() {
        try{
            const session = await startSession();
            try{

                await session.withTransaction( async () => {
                    
                    this.hash = this.calculateTaskHash();
            
                    await model.create({
                        queue_name: this.queueName,
                        hash: this.hash,
                        payload: this.payload,
                        args: this.args,
                        timeout: this.timeout,
                        trial: 0,
                        isFailed: false,
                        maxRetry: this.maxRetry,
                        createdAt : new Date().toISOString(),
                        modifiedAt : new Date().toISOString()
                    });       
                
                })

                return this
            } catch(err){
                throw err;
            }finally {
                session.endSession();
            }
                
        
        }catch(e){
            throw e;
        }
    }

    static async countJobs() {
        try{
       
            const count = await model.countDocuments({isFailed: false});
            return count;   
        
        }catch(e){
            throw e;
        }
       
    }

    static async fetch() {
        try{
       
            const result = await model.find({isFailed: false});

            if(result === null){
                return null;                
            }

            return result;   
        
        }catch(e){
            throw e;
        }
       
    }


    static findByQueueName = async (q, useSJF = false) => {
        try{
       
            let result
            if(useSJF) {
                result = await model.find({queue_name: q.trim(), isFailed: false, isLocked: false}).sort({timeout: 1});
            }else{
                result = await model.find({queue_name: q.trim(), isFailed: false, isLocked: false});
            }

            if(result === null){
                return [];                
            }

            return result;   
        
        }catch(e){
            throw e;
        }
    }

    static async fetchFirstShouldBeProcessed() {
        try{
       
            const result = await model.findOne({isFailed: false});

            if(result === null){
                return null;                
            }

            return result;   
        
        }catch(e){
            throw e;
        }
       
    }

    static async fetchFailed() {
        try{
       
            const result = await model.find({isFailed: true});

            if(result === null){
                return null;                
            }

            return result;   
        
        }catch(e){
            throw e;
        }
       
    }

    static async fail(hash) {
        try{
       
            const result = await model.findOne({hash: hash});

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

    static async findBySeq(seq) {
        try{
       
            const result = await model.findOne({_seq: seq});

            if(result === null){
                return null;                
            }

            return result;   
        
        }catch(e){
            throw e;
        }
    }

    static async find(contentId) {
        try{
       
            const result = await model.findOne({foreign_content_id: contentId});

            if(result === null){
                return null;                
            }

            return result;   
        
        }catch(e){
            throw e;
        }
       
    }

    static async basedOnHash(hash) {
        try{

            const result = await model.findOne({hash: hash});

            if(result === null){
                return null;                
            }

            return result;   
        
        }catch(e){
            throw e;
        }
    }

    static async updateTrial(hash) {
        try{

            const result = await model.findOne({hash: hash});

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

    static async lock(hash) {
        try{

            const result = await model.findOne({hash: hash});

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

    static async release(hash) {
        try{

            const result = await model.findOne({hash: hash});

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

    static async remove(hash) {
        try{
       
            await model.deleteOne({hash: hash});
        
        }catch(e){
            throw e;
        }
       
    }

   
}

const schema = new Schema({

    _seq : {
        type: Number,
        unique: true
    },

    queue_name : {
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

schema.plugin(AutoIncrement, {inc_field: '_seq'});

const model = _model('Queue', schema, 'zil_task_queue');




export const QueueSchema = schema;