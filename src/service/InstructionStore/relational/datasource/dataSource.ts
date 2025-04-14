import { DataSource, DataSourceOptions } from "typeorm"
import { ZTaskRelInstructionStore } from "./taskSchema";
export class JobberDataSource {
    private source : DataSource
    constructor(connectionOptions: DataSourceOptions) {
        this.source = new DataSource({...connectionOptions, synchronize: true, logging: false, entities: [ZTaskRelInstructionStore]});
    }

    getDataSource() : DataSource {
        return this.source
    }
}
