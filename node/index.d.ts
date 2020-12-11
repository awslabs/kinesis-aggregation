import {Kinesis} from 'aws-sdk';
import {KinesisStreamRecordPayload} from 'aws-lambda/trigger/kinesis-stream';

declare module 'aws-kinesis-agg' {
    export interface UserRecord {
        partitionKey: string;
        explicitPartitionKey?: string;
        sequenceNumber: string;
        subSequenceNumber: number;
        data: string;
    }

    export interface EncodedRecord {
        partitionKey: string,
        data: Buffer;
    }

    export function deaggregate(kinesisRecord: KinesisStreamRecordPayload, 
        computeChecksums: boolean,
        perRecordCallback: (err: Error, userRecords?: UserRecord) => void,
        afterRecordCallback: (err?: Error, errorUserRecord?: UserRecord) => void
    ): void;

    export function deaggregateSync(kinesisRecord: KinesisStreamRecordPayload, 
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
