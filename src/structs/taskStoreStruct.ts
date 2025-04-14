import { Task } from "./taskStruct"
import { Queue } from "./queueStruct"

export interface TaskStore {
    _stash: (queueName: string, payload: string, args: any[], maxRetry:number, timeout:number) => Promise<Task>,
    _lock: (hash: string) => Promise<boolean>,
    _release: (hash: string) => Promise<Task>,
    _purge: (hash: string) => Promise<boolean>,
    _fail: (hash: string) => Promise<boolean>,
    _count: () => Promise<number>,
    _fetchFree: (queue: Queue) => Promise<Task[]>,
    _fetchFreeHashes: (queue: Queue) => Promise<string[]>,
    _fetchOne: (hash: string) => Promise<Task|null>
    _fetchLocked: (queue: Queue) => Promise<Task[]>,
    _updateTrial: (hash: string) => Promise<boolean>
    _disconnect: () => Promise<void> 
}