import { Task } from "./taskStruct"
import { Queue } from "./queueStruct"

export interface TaskStore {
    _stash: (queueName: string, payload: string, args: any[], newOptions: TaskOptions) => Promise<Task>,
    _lock: (hash: string) => Promise<boolean>,
    _release: (hash: string) => Promise<Task>,
    _purge: (hash: string) => Promise<boolean>,
    _fail: (hash: string) => Promise<boolean>,
    _restoreFailed: () => Promise<number>,
    _restoreOneFailed: (hash: string) => Promise<boolean>,
    _count: () => Promise<number>,
    _fetchFree: (queue: Queue) => Promise<Task[]>,
    _fetchFailed: (queue: Queue) => Promise<Task[]>,
    _fetchFreeHashes: (queue: Queue) => Promise<string[]>,
    _fetchOne: (hash: string) => Promise<Task|null>
    _fetchLocked: (queue: Queue) => Promise<Task[]>,
    _updateTrial: (hash: string) => Promise<boolean>
    _delay: (hash: string, period: number) => Promise<boolean>,
    _disconnect: () => Promise<void> 
}

export interface TaskOptions {
    maxRetry: number, 
    timeout: number, // In seconds
    delay: number //In seconds
}