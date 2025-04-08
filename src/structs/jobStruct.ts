export interface Job {
    queue: string
    hash: string,
    payload: {[key: string|number] : string},
    isLocked: boolean,
    isFailed: boolean,
    args: any[],
    maxRetry: number,
    trial: number,
    timeout: number,
    createdAt: string,
    modifiedAt: string,
}