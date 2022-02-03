const QueueTask = require('./queueDao')
const { parentPort } = require('worker_threads');


parentPort.on('message', async (job) => {
   
    console.log(`[zil-js-queue-worker]-Processing item ${job.hash}`)
   
   

    try {
    
        try{
    
            if(typeof job.payload == 'string') {
                
                const fn = eval('(' + job.payload + ')')

                const args = job.args.split(',')

                if(args.length > 0) {
                    await fn.apply(null, args)
                }else{
                    await fn()
                }

                parentPort.postMessage("MOVE_NEXT")

                
            }else {

                parentPort.postMessage("FAIL_THIS")
                
            }
          
    
            
        } catch (ee) {
            console.log("Job specific error", ee)
            if(job.tr < job.mr) {
                parentPort.postMessage("RETRY_THIS")
            }else{
                parentPort.postMessage("FAIL_THIS")
            }
        }
        
    
    } catch(ex) {

        //Worker on error event would pickup
        console.log("Wkr ", ex)
        parentPort.postMessage("FAIL_THIS")
    }
 
})