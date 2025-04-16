import { parentPort } from 'worker_threads';

// Define the job interface
interface Job {
  tag: string;
  hash: string;
  payload: string | any;
  args: string;
  tr: number;   // trial count
  mr: number;   // max retry
}

// Type for messages sent back to the parent
type WorkerMessage = 'MOVE_NEXT' | 'FAIL_THIS' | 'RETRY_THIS';

// Check if parentPort exists (it should in a worker thread)
if (parentPort) {
  parentPort.on('message', async (job: Job) => {
    console.log(`${job.tag} Processing item ${job.hash}`);

    try {
      try {
        if (typeof job.payload === 'string') {
          // Using eval is generally risky - consider alternatives if possible
          // eslint-disable-next-line no-eval
          const fn = eval('(' + job.payload + ')');
          
          const args: string[] = job.args.split(',');
          
          if (args.length > 0) {
            await fn.apply(null, args);
          } else {
            await fn();
          }
          
          parentPort?.postMessage('MOVE_NEXT' as WorkerMessage);
        } else {
          parentPort?.postMessage('FAIL_THIS' as WorkerMessage);
        }
      } catch (ee) {
        console.log("Job specific error", ee);
        
        if (job.tr < job.mr) {
          parentPort?.postMessage('RETRY_THIS' as WorkerMessage);
        } else {
          parentPort?.postMessage('FAIL_THIS' as WorkerMessage);
        }
      }
    } catch (ex) {
      // Worker on error event would pickup
      console.log("Wkr ", ex);
      parentPort?.postMessage('FAIL_THIS' as WorkerMessage);
    }
  });
} else {
  throw new Error('This script must be run as a worker thread');
}