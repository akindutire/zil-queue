import { MongoJobStore } from "../service/InstructionStore/mongo/mongoJobStore";
import { RedisJobStore } from "../service/InstructionStore/redis/redisJobStore";
import { JobStore } from "../structs/jobStoreStruct";
import BaseJobStoreFactory, { InstructionStoreVariant } from "./baseJobStoreFactory";

export class JobStoreFactory extends BaseJobStoreFactory {

    private jobStoreInstance: JobStore
    public override make(jobStoreConnectionString: string): JobStore {
        if (!this.jobStoreInstance) {
            let databaseType: InstructionStoreVariant|null = this.detectDatabaseTypeFromConnectionString(jobStoreConnectionString)
            if (databaseType === 'MONGO') {
                this.jobStoreInstance = new MongoJobStore()
            } else if(databaseType === "REDIS") {
                this.jobStoreInstance = new RedisJobStore({ url: jobStoreConnectionString } )
            } else{
                throw Error(`Unknown database connection '${jobStoreConnectionString}', can not deduce data store in use, only MONGO, POSTGRES, MYSQL and REDIS connection string is supported`)
            }
        } else {
            return this.jobStoreInstance
        }

        return this.jobStoreInstance
    }

    private detectDatabaseTypeFromConnectionString(connectionString: string) : InstructionStoreVariant|null {
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

        return null;
    }
}