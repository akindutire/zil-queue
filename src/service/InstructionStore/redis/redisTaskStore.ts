import { createClient } from 'redis';
import md5 from 'md5';
import { v4 as uuidv4 } from 'uuid';
import { TaskOptions, TaskStore } from '../../../structs/taskStoreStruct';
import { Task } from '../../../structs/taskStruct';
import { Queue } from '../../../structs/queueStruct';

export class RedisTaskStore implements TaskStore {
    private client: any;
    
    constructor(options: { uri?: string } = {}) {
        try{
            this.client = createClient({
                url: options.uri || process.env.REDIS_URL || 'redis://localhost:6379',
                database: 1
            });
            
            // //Connect to DB
            this.connect();
        } catch (e) {
            throw e
        }
    }
    
    private async connect() {
        if (!this.client.isOpen) {
            await this.client.connect();
            process.stdout.write("Redis: Job store connection successful")
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
    
    private getTaskKey(hash: string): string {
        return `0x1_z_task_instn:${hash}`;
    }

    private getFailedKey(hash: string): string {
        return `0x1_z_failed_task:${hash}`;
    }
    
    private getQueueKey(queueName: string): string {
        return `0x1_z_task_queue:${queueName}`;
    }
    
    private async taskExists(hash: string): Promise<boolean> {
        return await this.client.exists(this.getTaskKey(hash)) === 1;
    }
    
    async _stash(queueName: string, payload: string, args: any[], options: TaskOptions): Promise<Task> {
        try {
            const hash = this.calculateTaskHash(queueName, payload);
            const taskKey = this.getTaskKey(hash);
            const queueKey = this.getQueueKey(queueName);
            const now = new Date().toISOString();
            
            const task: Task = {
                queue: queueName,
                hash: hash,
                payload: payload,
                args: args,
                timeout: options.timeout,
                trial: 0,
                isFailed: false,
                isLocked: false,
                maxRetry: options.maxRetry,
                delay: options.delay,
                createdAt: now,
                modifiedAt: now
            };
            
            const multi = this.client.multi();
            
            // Store job details
            multi.hSet(taskKey, {
                queue: queueName,
                hash: hash,
                payload: JSON.stringify(payload),
                args: JSON.stringify(args),
                timeout: options.timeout,
                trial: 0,
                isFailed: "false",
                isLocked: "false",
                maxRetry: options.maxRetry,
                delay: options.delay,
                createdAt: now,
                modifiedAt: now
            });
            
            // Add job hash to queue sorted set with score based on created time
            if (options.timeout > 0) {
                // For SJF algorithm, we use timeout as the score
                multi.zAdd(queueKey, { score: options.timeout, value: hash });
            } else {
                // For FIFO algorithm, we use timestamp as the score
                multi.zAdd(queueKey, { score: Date.now(), value: hash });
            }
            
            await multi.exec();
            
            return task;
        } catch (err) {
            throw err;
        }
    }
    
    async _count(): Promise<number> {
        try {
            const keys = await this.client.keys('task:*');
            if (!keys) return 0;
            
            let count = keys.length;
            // for (const key of keys) {
            //     const isFailed = await this.client.hGet(key, 'isFailed');
            //     if (isFailed === 'false') {
            //         count++;
            //     }
            // }
            
            return count;
        } catch (err) {
            throw err;
        }
    }
    
    async _fetchFree(q: Queue): Promise<Task[]> {
        try {
            const queueKey = this.getQueueKey(q.name.trim());
            
            // Get all job hashes from this queue, sorted appropriately
            const taskHashes = await this.client.zRange(queueKey, 0, -1);
            
            if (!taskHashes || taskHashes.length === 0) {
                return [];
            }
            
            const tasks: Task[] = [];
            
            for (const hash of taskHashes) {
                const taskKey = this.getTaskKey(hash);
                const taskData = await this.client.hGetAll(taskKey);
                
                if (taskData && taskData.isFailed === 'false' && taskData.isLocked === 'false') {
                    tasks.push({
                        ...taskData,
                        payload: JSON.parse(taskData.payload),
                        args: JSON.parse(taskData.args),
                        trial: parseInt(taskData.trial),
                        isFailed: taskData.isFailed === 'true',
                        isLocked: taskData.isLocked === 'true',
                        timeout: parseInt(taskData.timeout),
                        maxRetry: parseInt(taskData.maxRetry)
                    });
                }
            }
            
            // Sort based on algorithm
            if (q.algo === 'SJF') {
                tasks.sort((a, b) => a.timeout - b.timeout);
            } else if (q.algo === 'FIFO') {
                tasks.sort((a, b) => {
                    return new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime();
                });
            } else {
                throw Error(`Queue scheduling algorithm ${q?.algo} not supported`);
            }
            
            return tasks;
        } catch (err) {
            throw err;
        }
    }
    
    async _fetchFailed(): Promise<Task[]> {
        try {
            const keys = await this.client.keys(this.getFailedKey("*"));
            if (!keys || keys.length === 0) return [];
    
            const failedTasks: Task[] = [];
    
            for (const key of keys) {
                const data = await this.client.get(key);
                if (data) {
                    const task = JSON.parse(data) as Task;
                    failedTasks.push(task);
                }
            }
    
            return failedTasks;
        } catch (err) {
            throw err;
        }
    }
    
    async _fetchFreeHashes(q: Queue): Promise<string[]> {
        try {
            const tasks = await this._fetchFree(q);
            return tasks.map(job => job.hash);
        } catch (err) {
            throw err;
        }
    }
    
    async _fetchOne(hash: string): Promise<Task | null> {
        try {
            const taskKey = this.getTaskKey(hash);
            const exists = await this.taskExists(hash);
            
            if (!exists) {
                return null;
            }
            
            const taskData = await this.client.hGetAll(taskKey);
            
            if (!taskData || taskData.isFailed === 'true') {
                return null;
            }
            
            return {
                ...taskData,
                payload: JSON.parse(taskData.payload),
                args: JSON.parse(taskData.args),
                trial: parseInt(taskData.trial),
                isFailed: taskData.isFailed === 'true',
                isLocked: taskData.isLocked === 'true',
                timeout: parseInt(taskData.timeout),
                maxRetry: parseInt(taskData.maxRetry)
            };
        } catch (err) {
            throw err;
        }
    }
    
    async _fetchLocked(): Promise<Task[]> {
        try {
            const keys = await this.client.keys('task:*');
            if (!keys) return [];
            
            const tasks: Task[] = [];
            
            for (const key of keys) {
                const taskData = await this.client.hGetAll(key);
                
                if (taskData && taskData.isLocked === 'true') {
                    tasks.push({
                        ...taskData,
                        payload: JSON.parse(taskData.payload),
                        args: JSON.parse(taskData.args),
                        trial: parseInt(taskData.trial),
                        isFailed: taskData.isFailed === 'true',
                        isLocked: taskData.isLocked === 'true',
                        timeout: parseInt(taskData.timeout),
                        maxRetry: parseInt(taskData.maxRetry)
                    });
                }
            }
            
            return tasks;
        } catch (err) {
            throw err;
        }
    }
    
    async _fail(hash: string): Promise<boolean> {
        try {
            const taskKey = this.getTaskKey(hash);
            const exists = await this.taskExists(hash);
            if (!exists) return false;

            const taskData = await this.client.hGetAll(taskKey);
            if (!taskData) return false;

            await this.client.hSet(taskKey, {
                isFailed: 'true',
                modifiedAt: new Date().toISOString()
            });

            const failedKey = this.getFailedKey(hash);
            await this.client.set(failedKey, JSON.stringify({
                ...taskData,
                isFailed: true,
                isLocked: false,
                modifiedAt: new Date().toISOString()
            }));

            await this._purgeMainOnly(hash);
            return true;
        } catch (err) {
            throw err;
        }
    }
    
    async _restoreFailed(): Promise<number> {
        try {
            const keys = await this.client.keys('0x1_z_failed_task:*');
            let restoredCount = 0;
            for (const key of keys) {
                const task: Task = JSON.parse(await this.client.get(key));
                await this._stash(task.queue, task.payload, task.args, {maxRetry: task.maxRetry, timeout: task.timeout, delay: task.delay});
                await this.client.del(key);
                restoredCount++;
            }
            return restoredCount;
        } catch (err) {
            throw err;
        }
    }

    async _restoreOneFailed(hash: string): Promise<boolean> {
        try {
            const failedKey = this.getFailedKey(hash);
            const exists = await this.client.exists(failedKey);
            if (!exists) return false;

            const task: Task = JSON.parse(await this.client.get(failedKey));
            await this._stash(task.queue, task.payload, task.args, {maxRetry: task.maxRetry, timeout: task.timeout, delay: task.delay});
            await this.client.del(failedKey);
            return true;
        } catch (err) {
            throw err;
        }
    }
    
    async _lock(hash: string): Promise<boolean> {
        try {
            const taskKey = this.getTaskKey(hash);
            const taskData = await this.client.hGetAll(taskKey);
            
            if (!taskData || taskData.isLocked === 'true') {
                return false;
            }
            
            await this.client.hSet(taskKey, {
                isLocked: 'true',
                modifiedAt: new Date().toISOString()
            });
            
            return true;
        } catch (err) {
            throw err;
        }
    }
    
    async _release(hash: string): Promise<Task> {
        try {
            const taskKey = this.getTaskKey(hash);
            const taskData = await this.client.hGetAll(taskKey);
            
            if (!taskData) {
                throw new Error(`Job with hash ${hash} not found`);
            }
            
            await this.client.hSet(taskKey, {
                isLocked: 'false',
                modifiedAt: new Date().toISOString()
            });
            
            // Get updated job data
            const updatedtaskData = await this.client.hGetAll(taskKey);
            
            return {
                ...updatedtaskData,
                payload: JSON.parse(updatedtaskData.payload),
                args: JSON.parse(updatedtaskData.args),
                trial: parseInt(updatedtaskData.trial),
                isFailed: updatedtaskData.isFailed === 'true',
                isLocked: updatedtaskData.isLocked === 'true',
                timeout: parseInt(updatedtaskData.timeout),
                maxRetry: parseInt(updatedtaskData.maxRetry)
            };
        } catch (err) {
            throw err;
        }
    }
    
    async _purge(hash: string): Promise<boolean> {
        try {
            const taskKey = this.getTaskKey(hash);
            let exists = await this.taskExists(hash);
            
            if (exists) {
                // Get the queue name for this job
                const queueName = await this.client.hGet(taskKey, 'queue');
                
                const multi = this.client.multi();
                
                // Remove job from hash storage
                multi.del(taskKey);
                
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

    private async _purgeMainOnly(hash: string): Promise<boolean> {
        try {
            const taskKey = this.getTaskKey(hash);
            let exists = await this.taskExists(hash);
            
            if (exists) {
                // Get the queue name for this job
                const queueName = await this.client.hGet(taskKey, 'queue');
                
                const multi = this.client.multi();
                
                // Remove job from hash storage
                multi.del(taskKey);
                
                // Remove job from queue sorted set if queue name exists
                if (queueName) {
                    const queueKey = this.getQueueKey(queueName);
                    multi.zRem(queueKey, hash);
                }
                

                const failedKey = this.getFailedKey(hash);
                exists = await this.taskExists(hash);
                if(exists) {
                    multi.del(failedKey);
                }
                
                await multi.exec();
            }

            

            return true;
        } catch (err) {
            throw err;
        }
    }
    

    async _updateTrial(hash: string): Promise<boolean> {
        try {
            const taskKey = this.getTaskKey(hash);
            const taskData = await this.client.hGetAll(taskKey);
            
            if (!taskData) {
                return false;
            }
            
            if (taskData.isLocked === 'false') {
                const newTrial = parseInt(taskData.trial) + 1;
                
                await this.client.hSet(taskKey, {
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

    async _delay (hash: string, period: number) : Promise<boolean>{
        try {
            const taskKey = this.getTaskKey(hash);
            const taskData = await this.client.hGetAll(taskKey);
            
            if (!taskData) {
                return false;
            }
            
            if (taskData.isLocked === 'false') {
                
                await this.client.hSet(taskKey, {
                    delay: period,
                    modifiedAt: new Date().toISOString()
                });
                
                return true;
            }
            
            return false;
        } catch (err) {
            throw err;
        }
    }

    async _disconnect(): Promise<void> {
        if (this.client && this.client.isOpen) {
            await this.client.quit();
        }
    }
}