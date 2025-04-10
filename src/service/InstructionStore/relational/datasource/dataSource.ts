import { DataSource, DataSourceOptions } from "typeorm"
import { ZJobberRelInstructionStore } from "./jobSchema";
export class JobberDataSource {
    private source : DataSource
    constructor(connectionOptions: DataSourceOptions) {
        this.source = new DataSource({...connectionOptions, synchronize: true, logging: false, entities: [ZJobberRelInstructionStore]});
    }

    getDataSource() : DataSource {
        return this.source
    }
}
