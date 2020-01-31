import {Kinesis} from 'aws-sdk';

declare module 'aws-kinesis-agg' {
    export interface UserRecord {
        partitionKey: string;
        explicitPartitionKey: string;
        sequencenumber: string;
        subSequencenumber: number;
        data: Buffer;
    }

    export interface EncodedRecord {
        partitionKey: string,
        data: Buffer;
    }

    export function deaggregate(kinesisRecord: Kinesis.Types.Record, 
        computeChecksums: boolean,
        perRecordCallback: (err: Error, userRecords?: UserRecord) => void,
        afterRecordCallback: (err?: Error, errorUserRecord?: UserRecord) => void
    ): void;

    export function deaggregateSync(kinesisRecord: Kinesis.Types.Record, 
        computeChecksums: boolean,
        afterRecordCallback: (err: Error, userRecords?: UserRecord[]) => void
    ): void;

    export function aggregate(records: any[], 
        encodedRecordHandler: (encodedRecord: EncodedRecord, callback: (err?: Error, data?: Kinesis.Types.PutRecordOutput) => void) => void,
        afterPutAggregatedRecords: () => void, 
        errorCallback: (error: Error, data?: EncodedRecord) => void,
        queueSize?: number
    ): void;
}
