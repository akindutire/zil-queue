import { MongoJobStore } from "../service/InstructionStore/mongo/mongoJobStore";
import { JobStore } from "../structs/jobStoreStruct";
import BaseJobStoreFactory, { InstructionStoreVariant } from "./baseJobStoreFactory";

export class JobStoreFactory extends BaseJobStoreFactory {

    private jobStoreInstance: JobStore
    protected override make(jobStoreConnectionString: string): JobStore {
        if (!this.jobStoreInstance) {
            if (this.detectDatabaseTypeFromConnectionString(jobStoreConnectionString) === 'REDIS') {
                this.jobStoreInstance = new MongoJobStore()
            }else{
                throw Error(`Unknown database connection '${jobStoreConnectionString}', can not deduce data store in use, only MONGO, POSTGRES, MYSQL and REDIS connection string is supported`)
            }
        } else {
            return this.jobStoreInstance
        }

        return this.jobStoreInstance
    }

    private detectDatabaseTypeFromConnectionString(connectionString: string) : InstructionStoreVariant|undefined {
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
      }
}