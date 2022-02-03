module.exports = class ExampleJob {
    
    static run = (a,b,c) => {

        return await new Promise( (resolve, reject) => {
            setTimeout( () => {
                console.log("I want to resolve")
                resolve(true)
            }, 2000 )
        } ).catch ( e => console.log(e.message) )
    }

}