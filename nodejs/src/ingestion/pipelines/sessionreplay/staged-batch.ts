import { Message } from 'node-rdkafka'

import { SessionBatchRecorder } from './sessions/session-batch-recorder'

/** How a staged runner reaches the ingester's recorder: it records under the batch lock, hands back the messages it recorded, and leaves the flush decision to the ingester. */
export interface StagedBatchCommitter {
    currentRecorder(): SessionBatchRecorder
    commit(
        maxOffsets: Map<number, number>,
        record: (recorder: SessionBatchRecorder) => Promise<Message[]>
    ): Promise<void>
}

/** Processes poll batches in overlapping stages; the promise resolves once the batch is recorded and its offsets tracked. */
export interface StagedBatchRunner {
    run(messages: Message[], committer: StagedBatchCommitter): Promise<void>
}

/** One batch per stage: prepare on N+2, anonymize on N+1 and commit on N is the deepest a staged runner goes. */
export const STAGED_BATCH_LOOKAHEAD = 3

/** A staged batch is done once its commit stage has run, which can sit behind two earlier batches and a flush, so this is longer than the consumer's default for a quick background task. It stays under librdkafka's max.poll.interval.ms (5 minutes): at the lookahead the consumer waits on the oldest batch, and a member that waits longer than that is fenced before this timeout could fire. */
export const STAGED_BATCH_TIMEOUT_MS = 4 * 60 * 1000

/** A revoke drains every batch in flight before the revoke hook flushes, so the budget covers three commit stages, each of which can spend the key store's 45 s retry budget. */
export const STAGED_BATCH_REBALANCE_TIMEOUT_MS = 2 * 60 * 1000
