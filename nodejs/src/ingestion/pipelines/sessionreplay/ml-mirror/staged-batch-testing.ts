import { Message } from 'node-rdkafka'

import { SessionReplayPipelineConfig } from '~/ingestion/pipelines/sessionreplay'
import { SessionBatchRecorder } from '~/ingestion/pipelines/sessionreplay/sessions/session-batch-recorder'
import { StagedBatchCommitter } from '~/ingestion/pipelines/sessionreplay/staged-batch'

import {
    MlMirrorCollection,
    MlMirrorImageScrubProducer,
    MlMirrorPipelineOptions,
    MlMirrorUrlFetchProducer,
    createMlMirrorAnonymizePipeline,
    createMlMirrorPreparePipeline,
} from './ml-mirror-pipeline'
import { MlMirrorStagedBatchRunner } from './staged-batch-runner'

/** Test-only: the runner the ML mirror server builds, from one pipeline config. */
export function buildMlMirrorStagedRunner(
    config: SessionReplayPipelineConfig,
    mlOptions: MlMirrorPipelineOptions,
    imageScrub?: MlMirrorImageScrubProducer,
    collection?: MlMirrorCollection,
    urlFetch?: MlMirrorUrlFetchProducer
): MlMirrorStagedBatchRunner {
    return new MlMirrorStagedBatchRunner(
        createMlMirrorPreparePipeline(config, mlOptions),
        createMlMirrorAnonymizePipeline(config, mlOptions, imageScrub, collection, urlFetch),
        config.promiseScheduler
    )
}

/** Test-only: a committer that records into one fixed recorder and never flushes. */
export function recordingCommitter(recorder: SessionBatchRecorder): StagedBatchCommitter {
    return {
        currentRecorder: () => recorder,
        commit: async (_maxOffsets, record) => {
            await record(recorder)
        },
    }
}

/** Test-only: runs one poll batch to completion against one recorder. */
export function runMlMirrorBatch(
    runner: MlMirrorStagedBatchRunner,
    messages: Message[],
    recorder: SessionBatchRecorder
): Promise<void> {
    return runner.run(messages, recordingCommitter(recorder))
}
