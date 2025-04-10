import { DataSource } from "typeorm";
import { JobStore } from "../../../structs/jobStoreStruct";
import { Job } from "../../../structs/jobStruct";
import { Queue } from "../../../structs/queueStruct";

export class RelationalJobStore implements JobStore {

    constructor(connectionDataSource: DataSource) {
        //Connect to DB
        connectionDataSource.initialize()
        process.stdout.write(`${connectionDataSource.options.type} Job store connection successful`)
    }

    _stash: (queueName: string, payload: string, args: any[], maxRetry: number, timeout: number) => Promise<Job>;
    _lock: (hash: string) => Promise<boolean>;
    _release: (hash: string) => Promise<Job>;
    _purge: (hash: string) => Promise<boolean>;
    _fail: (hash: string) => Promise<boolean>;
    _count: () => Promise<number>;
    _fetchFree: (queue: Queue) => Promise<Job[]>;
    _fetchFreeHashes: (queue: Queue) => Promise<string[]>;
    _fetchOne: (hash: string) => Promise<Job | null>;
    _fetchLocked: (queue: Queue) => Promise<Job[]>;
    _updateTrial: (hash: string) => Promise<boolean>;

    async _disconnect(): Promise<void> {
        
    }
}