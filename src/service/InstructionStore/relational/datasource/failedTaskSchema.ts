import 'reflect-metadata'
import { Column, CreateDateColumn, Entity, PrimaryGeneratedColumn, Table, UpdateDateColumn } from "typeorm";

@Entity()
export class ZFailedTaskRelInstructionStore {

    @PrimaryGeneratedColumn()
    id: number

    @Column({nullable: false})
    queue: string

    @Column({nullable: false, unique: true, update: false})
    hash: string

    @Column()
    payload: string

    @Column({default: false})
    isLocked: boolean

    @Column({default: false})
    isFailed: boolean

    @Column()
    args: number|string|boolean[]

    @Column()
    maxRetry: number

    @Column({default: 0})
    trial: number

    @Column({default: -1})
    timeout: number

    @Column({default: 0})
    delay: number

    @CreateDateColumn({default: new Date().toISOString()})
    createdAt: string

    @UpdateDateColumn({default: new Date().toISOString()})
    modifiedAt: string

}