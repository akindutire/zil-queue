const dotenv = require('dotenv');
dotenv.config();

module.exports = {

    env: process.env.NODE_ENV,
    cmd: {
        tag: '[zil-queue] '
    }
}