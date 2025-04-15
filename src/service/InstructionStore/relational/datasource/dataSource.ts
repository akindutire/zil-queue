import { DataSource, DataSourceOptions } from "typeorm"
import { ZTaskRelInstructionStore } from "./taskSchema";
import { ZFailedTaskRelInstructionStore } from "./failedTaskSchema";
export class JobberDataSource {
    private source : DataSource
    constructor(connectionOptions: DataSourceOptions) {
        this.source = new DataSource({...connectionOptions, synchronize: true, logging: false, entities: [ZTaskRelInstructionStore, ZFailedTaskRelInstructionStore]});
    }

    getDataSource() : DataSource {
        return this.source
    }
}
