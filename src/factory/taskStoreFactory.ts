import { DataSourceOptions } from "typeorm";
import { MongoTaskStore } from "../service/InstructionStore/mongo/mongoTaskStore";
import { RedisTaskStore } from "../service/InstructionStore/redis/redisTaskStore";
import { TaskStore } from "../structs/taskStoreStruct";
import BaseTaskStoreFactory, { InstructionStoreVariant } from "./baseTaskStoreFactory";
import { JobberDataSource } from "../service/InstructionStore/relational/datasource/dataSource";
import { RelationalTaskStore } from "../service/InstructionStore/relational/relationalTaskStore";

export class TaskStoreFactory extends BaseTaskStoreFactory {

    private taskStoreInstance: TaskStore
    public override make(taskStoreConnection: string | DataSourceOptions): TaskStore {
        if (!this.taskStoreInstance) {
            let databaseType: InstructionStoreVariant|string
            if (typeof taskStoreConnection == 'string') {
                databaseType = this.detectDatabaseTypeFromConnectionString(taskStoreConnection)
            } else {
                databaseType = taskStoreConnection.type
            }
            
            if(typeof taskStoreConnection == 'string') {
                if (databaseType === 'MONGO') {
                    this.taskStoreInstance = new MongoTaskStore({ uri: taskStoreConnection } )
                } else if(databaseType === "REDIS") {
                    this.taskStoreInstance = new RedisTaskStore({ uri: taskStoreConnection } )
                } else{
                    throw Error(`Unknown database connection '${taskStoreConnection}', can not deduce data store in use, only MONGO, REDIS connection string is supported, consider DataSourceOptions for Postgres, Mysql, Sqlite and CockroachDB`)
                }
            } else {
                let supportedType: string[] = ['cockroachdb', 'postgres', 'mysql', 'mariadb', 'mysql', "sqlite"]
                if (!supportedType.includes(taskStoreConnection.type)) {
                    throw Error(`Unsupported database driver ${taskStoreConnection.type} provided, only ${supportedType.join(',')}`);
                }
                const jobberDataSource = (new JobberDataSource(taskStoreConnection)).getDataSource();
                this.taskStoreInstance = new RelationalTaskStore(jobberDataSource)
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