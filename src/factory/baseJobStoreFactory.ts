import { JobStore } from "../structs/jobStoreStruct";

abstract class BaseJobStoreFactory {
    protected abstract make(jobStoreConnectionString: string): JobStore 
}

export type InstructionStoreVariant = "REDIS" | "RELATIONAL" | "MONGO"

export default BaseJobStoreFactory;