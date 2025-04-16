export interface Task {
    queue: string
    hash: string,
    payload: string,
    isLocked: boolean,
    isFailed: boolean,
    args: any[],
    maxRetry: number,
    trial: number,
    timeout: number,
    delay: number,
    createdAt: string,
    modifiedAt: string,
}