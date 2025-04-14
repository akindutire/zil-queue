export interface Job {
    queue: string
    hash: string,
    payload: string,
    isLocked: boolean,
    isFailed: boolean,
    args: any[],
    maxRetry: number,
    trial: number,
    timeout: number,
    createdAt: string,
    modifiedAt: string,
}