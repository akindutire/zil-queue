export async function run(a,b,c) {
    
    try {
        return await new Promise((resolve, reject) => {
            setTimeout(() => {
                console.log("I want to resolve")
                resolve(true)
            }, 2000)
        })
    } catch (e) {
        return console.log(e.message)
    }
    

}

