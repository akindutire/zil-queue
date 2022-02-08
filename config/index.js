import {config} from 'dotenv';

config();

export default {

    env: process.env.NODE_ENV,
    cmd: {
        tag: '[zil-queue] '
    },
    packageName: '@akindutire/zil-queue'
}