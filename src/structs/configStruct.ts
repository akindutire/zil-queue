import { DataSourceOptions } from "typeorm";

export interface Config {
    showQueueList: boolean, 
    refreshPeriod: number,
    connection: string | DataSourceOptions,
    workerTag?: string
}