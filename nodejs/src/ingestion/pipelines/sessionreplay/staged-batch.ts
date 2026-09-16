import { Message } from 'node-rdkafka'

import { SessionReplayBatchProgress } from './session-replay-pipeline'
import { SessionBatchRecorder } from './sessions/session-batch-recorder'

/** How a staged runner reaches the ingester's recorder: it records under the batch lock and leaves the flush decision to the ingester. */
export interface StagedBatchCommitter {
    currentRecorder(): SessionBatchRecorder
    commit(
        progress: SessionReplayBatchProgress,
        record: (recorder: SessionBatchRecorder) => Promise<void>
    ): Promise<void>
}

/** Processes poll batches in overlapping stages; the promise resolves once the batch is recorded and its offsets tracked. */
export interface StagedBatchRunner {
    run(messages: Message[], committer: StagedBatchCommitter): Promise<void>
}

/** One batch per stage: prepare on N+2, anonymize on N+1 and commit on N is the deepest a staged runner goes. */
export const STAGED_BATCH_LOOKAHEAD = 3

/** A staged batch is done once its commit stage has run, which can sit behind two earlier batches and a flush, so this matches the lane's loop stall threshold rather than the consumer's default for a quick background task. */
export const STAGED_BATCH_TIMEOUT_MS = 10 * 60 * 1000
