import { TaskStore } from "../structs/taskStoreStruct";

abstract class BaseTaskStoreFactory {
    protected abstract make(jobStoreConnectionString: string): TaskStore 
}

export type InstructionStoreVariant = "REDIS" | "RELATIONAL" | "MONGO"

export default BaseTaskStoreFactory;