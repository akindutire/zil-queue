import { Job } from "./jobStruct"
import { Queue } from "./queueStruct"

export interface JobStore {
    _stash: (queueName: string, payload: {[key: string|number] : string}, args: any[], maxRetry:number, timeout:number) => Promise<Job>,
    _lock: (hash: string) => Promise<boolean>,
    _release: (hash: string) => Promise<Job>,
    _purge: (hash: string) => Promise<boolean>,
    _fail: (hash: string) => Promise<boolean>,
    _count: () => Promise<number>,
    _fetchFree: (queue: Queue) => Promise<Job[]>,
    _fetchFreeHashes: (queue: Queue) => Promise<string[]>,
    _fetchOne: (hash: string) => Promise<Job|null>
    _fetchLocked: (queue: Queue) => Promise<Job[]>,
    _updateTrial: (hash: string) => Promise<boolean>
}