import { DataSource, DataSourceOptions } from "typeorm"
export class JobberDataSource {
    private source : DataSource
    constructor(connectionOptions: DataSourceOptions) {
        
        this.source = new DataSource({...connectionOptions, synchronize: true, logging: false, entities: []});
    }

    getDataSource() : DataSource {
        return this.source
    }
}
