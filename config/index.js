const dotenv = require('dotenv');
dotenv.config();

export default config = {

    env: process.env.NODE_ENV,
    cmd: {
        tag: '[zil-queue] '
    }
}