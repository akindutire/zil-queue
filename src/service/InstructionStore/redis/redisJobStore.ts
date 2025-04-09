import { createClient } from 'redis';
import md5 from 'md5';
import { v4 as uuidv4 } from 'uuid';
import { JobStore } from '../../../structs/jobStoreStruct';
import { Job } from '../../../structs/jobStruct';
import { Queue } from '../../../structs/queueStruct';

export class RedisJobStore implements JobStore {
    private client: any;
    
    constructor(options: { url?: string } = {}) {
        this.client = createClient({
            url: options.url || process.env.REDIS_URL || 'redis://localhost:6379'
        });
        
        this.client.on('error', (err: any) => console.error('Redis Client Error', err));
        
        // Connect automatically
        this.connect();
    }
    
    private async connect() {
        if (!this.client.isOpen) {
            await this.client.connect();
        }
    }
    
    private calculateTaskHash(queueName: string, payload: any) {
        try {
            let uid = uuidv4();
            return md5(`${queueName}-${payload}$-01-ca.${uid}`);
        } catch (err) {
            throw err;
        }
    }
    
    private getJobKey(hash: string): string {
        return `job:${hash}`;
    }
    
    private getQueueKey(queueName: string): string {
        return `queue:${queueName}`;
    }
    
    private async jobExists(hash: string): Promise<boolean> {
        return await this.client.exists(this.getJobKey(hash)) === 1;
    }
    
    async _stash(queueName: string, payload: string, args: any[], maxRetry: number, timeout: number): Promise<Job> {
        try {
            const hash = this.calculateTaskHash(queueName, payload);
            const jobKey = this.getJobKey(hash);
            const queueKey = this.getQueueKey(queueName);
            const now = new Date().toISOString();
            
            const job: Job = {
                queue: queueName,
                hash: hash,
                payload: payload,
                args: args,
                timeout: timeout,
                trial: 0,
                isFailed: false,
                isLocked: false,
                maxRetry: maxRetry,
                createdAt: now,
                modifiedAt: now
            };
            
            const multi = this.client.multi();
            
            // Store job details
            multi.hSet(jobKey, {
                queue: queueName,
                hash: hash,
                payload: JSON.stringify(payload),
                args: JSON.stringify(args),
                timeout: timeout,
                trial: 0,
                isFailed: "false",
                isLocked: "false",
                maxRetry: maxRetry,
                createdAt: now,
                modifiedAt: now
            });
            
            // Add job hash to queue sorted set with score based on created time
            if (timeout > 0) {
                // For SJF algorithm, we use timeout as the score
                multi.zAdd(queueKey, { score: timeout, value: hash });
            } else {
                // For FIFO algorithm, we use timestamp as the score
                multi.zAdd(queueKey, { score: Date.now(), value: hash });
            }
            
            await multi.exec();
            
            return job;
        } catch (err) {
            throw err;
        }
    }
    
    async _count(): Promise<number> {
        try {
            const keys = await this.client.keys('job:*');
            if (!keys) return 0;
            
            let count = 0;
            for (const key of keys) {
                const isFailed = await this.client.hGet(key, 'isFailed');
                if (isFailed === 'false') {
                    count++;
                }
            }
            
            return count;
        } catch (err) {
            throw err;
        }
    }
    
    async _fetchFree(q: Queue): Promise<Job[]> {
        try {
            const queueKey = this.getQueueKey(q.name.trim());
            
            // Get all job hashes from this queue, sorted appropriately
            const jobHashes = await this.client.zRange(queueKey, 0, -1);
            
            if (!jobHashes || jobHashes.length === 0) {
                return [];
            }
            
            const jobs: Job[] = [];
            
            for (const hash of jobHashes) {
                const jobKey = this.getJobKey(hash);
                const jobData = await this.client.hGetAll(jobKey);
                
                if (jobData && jobData.isFailed === 'false' && jobData.isLocked === 'false') {
                    jobs.push({
                        ...jobData,
                        payload: JSON.parse(jobData.payload),
                        args: JSON.parse(jobData.args),
                        trial: parseInt(jobData.trial),
                        isFailed: jobData.isFailed === 'true',
                        isLocked: jobData.isLocked === 'true',
                        timeout: parseInt(jobData.timeout),
                        maxRetry: parseInt(jobData.maxRetry)
                    });
                }
            }
            
            // Sort based on algorithm
            if (q.algo === 'SJF') {
                jobs.sort((a, b) => a.timeout - b.timeout);
            } else if (q.algo === 'FIFO') {
                jobs.sort((a, b) => {
                    return new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime();
                });
            } else {
                throw Error(`Queue scheduling algorithm ${q?.algo} not supported`);
            }
            
            return jobs;
        } catch (err) {
            throw err;
        }
    }
    
    async _fetchFreeHashes(q: Queue): Promise<string[]> {
        try {
            const jobs = await this._fetchFree(q);
            return jobs.map(job => job.hash);
        } catch (err) {
            throw err;
        }
    }
    
    async _fetchOne(hash: string): Promise<Job | null> {
        try {
            const jobKey = this.getJobKey(hash);
            const exists = await this.jobExists(hash);
            
            if (!exists) {
                return null;
            }
            
            const jobData = await this.client.hGetAll(jobKey);
            
            if (!jobData || jobData.isFailed === 'true') {
                return null;
            }
            
            return {
                ...jobData,
                payload: JSON.parse(jobData.payload),
                args: JSON.parse(jobData.args),
                trial: parseInt(jobData.trial),
                isFailed: jobData.isFailed === 'true',
                isLocked: jobData.isLocked === 'true',
                timeout: parseInt(jobData.timeout),
                maxRetry: parseInt(jobData.maxRetry)
            };
        } catch (err) {
            throw err;
        }
    }
    
    async _fetchLocked(): Promise<Job[]> {
        try {
            const keys = await this.client.keys('job:*');
            if (!keys) return [];
            
            const jobs: Job[] = [];
            
            for (const key of keys) {
                const jobData = await this.client.hGetAll(key);
                
                if (jobData && jobData.isLocked === 'true') {
                    jobs.push({
                        ...jobData,
                        payload: JSON.parse(jobData.payload),
                        args: JSON.parse(jobData.args),
                        trial: parseInt(jobData.trial),
                        isFailed: jobData.isFailed === 'true',
                        isLocked: jobData.isLocked === 'true',
                        timeout: parseInt(jobData.timeout),
                        maxRetry: parseInt(jobData.maxRetry)
                    });
                }
            }
            
            return jobs;
        } catch (err) {
            throw err;
        }
    }
    
    async _fail(hash: string): Promise<boolean> {
        try {
            const jobKey = this.getJobKey(hash);
            const exists = await this.jobExists(hash);
            
            if (!exists) {
                return false;
            }
            
            await this.client.hSet(jobKey, {
                isFailed: 'true',
                isLocked: 'false',
                modifiedAt: new Date().toISOString()
            });
            
            return true;
        } catch (err) {
            throw err;
        }
    }
    
    async _updateTrial(hash: string): Promise<boolean> {
        try {
            const jobKey = this.getJobKey(hash);
            const jobData = await this.client.hGetAll(jobKey);
            
            if (!jobData) {
                return false;
            }
            
            if (jobData.isLocked === 'false') {
                const newTrial = parseInt(jobData.trial) + 1;
                
                await this.client.hSet(jobKey, {
                    trial: newTrial,
                    isLocked: 'true',
                    modifiedAt: new Date().toISOString()
                });
                
                return true;
            }
            
            return false;
        } catch (err) {
            throw err;
        }
    }
    
    async _lock(hash: string): Promise<boolean> {
        try {
            const jobKey = this.getJobKey(hash);
            const jobData = await this.client.hGetAll(jobKey);
            
            if (!jobData || jobData.isLocked === 'true') {
                return false;
            }
            
            await this.client.hSet(jobKey, {
                isLocked: 'true',
                modifiedAt: new Date().toISOString()
            });
            
            return true;
        } catch (err) {
            throw err;
        }
    }
    
    async _release(hash: string): Promise<Job> {
        try {
            const jobKey = this.getJobKey(hash);
            const jobData = await this.client.hGetAll(jobKey);
            
            if (!jobData) {
                throw new Error(`Job with hash ${hash} not found`);
            }
            
            await this.client.hSet(jobKey, {
                isLocked: 'false',
                modifiedAt: new Date().toISOString()
            });
            
            // Get updated job data
            const updatedJobData = await this.client.hGetAll(jobKey);
            
            return {
                ...updatedJobData,
                payload: JSON.parse(updatedJobData.payload),
                args: JSON.parse(updatedJobData.args),
                trial: parseInt(updatedJobData.trial),
                isFailed: updatedJobData.isFailed === 'true',
                isLocked: updatedJobData.isLocked === 'true',
                timeout: parseInt(updatedJobData.timeout),
                maxRetry: parseInt(updatedJobData.maxRetry)
            };
        } catch (err) {
            throw err;
        }
    }
    
    async _purge(hash: string): Promise<boolean> {
        try {
            const jobKey = this.getJobKey(hash);
            const exists = await this.jobExists(hash);
            
            if (exists) {
                // Get the queue name for this job
                const queueName = await this.client.hGet(jobKey, 'queue');
                
                const multi = this.client.multi();
                
                // Remove job from hash storage
                multi.del(jobKey);
                
                // Remove job from queue sorted set if queue name exists
                if (queueName) {
                    const queueKey = this.getQueueKey(queueName);
                    multi.zRem(queueKey, hash);
                }
                
                await multi.exec();
            }
            
            return true;
        } catch (err) {
            throw err;
        }
    }
    
    async disconnect() {
        if (this.client && this.client.isOpen) {
            await this.client.quit();
        }
    }
}