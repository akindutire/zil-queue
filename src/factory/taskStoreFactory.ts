import { DataSourceOptions } from "typeorm";
import { MongoJobStore } from "../service/InstructionStore/mongo/mongoJobStore";
import { RedisJobStore } from "../service/InstructionStore/redis/redisJobStore";
import { TaskStore } from "../structs/taskStoreStruct";
import BaseTaskStoreFactory, { InstructionStoreVariant } from "./baseTaskStoreFactory";
import { JobberDataSource } from "../service/InstructionStore/relational/datasource/dataSource";
import { RelationalJobStore } from "../service/InstructionStore/relational/relationalJobStore";

export class TaskStoreFactory extends BaseTaskStoreFactory {

    private taskStoreInstance: TaskStore
    public override make(jobStoreConnection: string | DataSourceOptions): TaskStore {
        if (!this.taskStoreInstance) {
            let databaseType: InstructionStoreVariant|string
            if (typeof jobStoreConnection == 'string') {
                databaseType = this.detectDatabaseTypeFromConnectionString(jobStoreConnection)
            } else {
                databaseType = jobStoreConnection.type
            }
            
            if(typeof jobStoreConnection == 'string') {
                if (databaseType === 'MONGO') {
                    this.taskStoreInstance = new MongoJobStore({ uri: jobStoreConnection } )
                } else if(databaseType === "REDIS") {
                    this.taskStoreInstance = new RedisJobStore({ uri: jobStoreConnection } )
                } else{
                    throw Error(`Unknown database connection '${jobStoreConnection}', can not deduce data store in use, only MONGO, REDIS connection string is supported, consider DataSourceOptions for Postgres, Mysql, Sqlite and CockroachDB`)
                }
            } else {
                let supportedType: string[] = ['cockroachdb', 'postgres', 'mysql', 'mariadb', 'mysql', "sqlite"]
                if (!supportedType.includes(jobStoreConnection.type)) {
                    throw Error(`Unsupported database driver ${jobStoreConnection.type} provided, only ${supportedType.join(',')}`);
                }
                const jobberDataSource = (new JobberDataSource(jobStoreConnection)).getDataSource();
                this.taskStoreInstance = new RelationalJobStore(jobberDataSource)
            }

            return this.taskStoreInstance

        } else {
            return this.taskStoreInstance
        }
    }

    private detectDatabaseTypeFromConnectionString(connectionString: string) : InstructionStoreVariant|string {
        if (connectionString.startsWith('mongodb://') || connectionString.startsWith('mongodb+srv://')) {
            return "MONGO";
        }
        if (connectionString.startsWith('postgres://') || connectionString.startsWith('postgresql://')) {
            return 'RELATIONAL';
        }
        if (connectionString.startsWith('mysql://')) {
            return 'RELATIONAL';
        }
        if (connectionString.startsWith('redis://')) {
            return 'REDIS';
        }

        if (connectionString.startsWith('file:')) {
            return 'RELATIONAL';
        }

        return '';
    }
}