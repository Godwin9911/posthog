import { Message } from 'node-rdkafka'
import pLimit from 'p-limit'

import { PromiseScheduler } from '~/common/utils/promise-scheduler'
import { BatchingContext, BatchingPipeline } from '~/ingestion/framework/batching-pipeline'
import { ChunkPipelineResultWithContext } from '~/ingestion/framework/chunk-pipeline.interface'
import { createBatch } from '~/ingestion/framework/helpers'
import { OkResultWithContext } from '~/ingestion/framework/pipeline.interface'
import { isOkResult } from '~/ingestion/framework/results'
import { SessionReplayBatchProgress } from '~/ingestion/pipelines/sessionreplay/session-replay-pipeline'
import { StagedBatchCommitter, StagedBatchRunner } from '~/ingestion/pipelines/sessionreplay/staged-batch'

import { MlBatchStage, MlMirrorMetrics } from './metrics'
import { MlAnonymizePipeline, MlPreparePipeline } from './ml-mirror-pipeline'

/**
 * Runs poll batches through three stages that each hold one batch at a time, in batch order, so
 * the I/O-bound prepare and commit stages of neighbouring batches overlap the CPU-bound anonymize
 * stage of this one. The anonymize stage never admits a batch while an earlier one is still in it,
 * so a later batch's CPU work cannot delay an earlier batch. Offsets and lag are tracked in the
 * commit stage, in batch order, as before.
 */
export class MlMirrorStagedBatchRunner implements StagedBatchRunner {
    private readonly stages: Record<MlBatchStage, ReturnType<typeof pLimit>> = {
        prepare: pLimit(1),
        anonymize: pLimit(1),
        commit: pLimit(1),
    }

    constructor(
        private readonly prepare: MlPreparePipeline,
        private readonly anonymize: MlAnonymizePipeline,
        private readonly promiseScheduler: PromiseScheduler
    ) {}

    public run(messages: Message[], committer: StagedBatchCommitter): Promise<void> {
        const queuedAt = performance.now()
        const prepared = this.stages.prepare(() =>
            this.timed('prepare', queuedAt, async () => {
                // The recorder stamped here only satisfies the input type: the deferred record step rebinds to the recorder current at commit time.
                const recorder = committer.currentRecorder()
                const batch = createBatch(messages.map((message) => ({ message, sessionBatchRecorder: recorder })))
                const elements = await drainBatch(this.prepare, batch, this.promiseScheduler)
                return { elements }
            })
        )
        const anonymized = this.stages.anonymize(async () => {
            const { elements, finishedAt } = await prepared
            return this.timed('anonymize', finishedAt, async () => {
                const survivors = elements.flatMap(({ result }) => (isOkResult(result) ? [result.value] : []))
                const anonymizedElements = survivors.length
                    ? await drainBatch(this.anonymize, createBatch(survivors), this.promiseScheduler)
                    : []
                return { progress: progressOf(elements, anonymizedElements), handle: survivors[0]?.mlBatch }
            })
        })
        return this.stages.commit(async () => {
            const { progress, handle, finishedAt } = await anonymized
            await this.timed('commit', finishedAt, async () => {
                await committer.commit(
                    progress,
                    (recorder) => handle?.commit(recorder, this.promiseScheduler) ?? Promise.resolve()
                )
                return {}
            })
        })
    }

    private async timed<T extends object>(
        stage: MlBatchStage,
        readyAt: number,
        run: () => Promise<T>
    ): Promise<T & { finishedAt: number }> {
        const startedAt = performance.now()
        const result = await run()
        const finishedAt = performance.now()
        MlMirrorMetrics.observeMlBatchStage(stage, startedAt - readyAt, finishedAt - startedAt)
        return { ...result, finishedAt }
    }
}

type StageElements<TOutput, COutput extends BatchingContext, R extends string> = ChunkPipelineResultWithContext<
    TOutput,
    COutput,
    R
>

// Each stage pipeline holds one batch at a time and the runner feeds each stage in batch order, so draining to null returns exactly the batch just fed.
async function drainBatch<TInput, TOutput, CInput, COutput extends BatchingContext, R extends string>(
    pipeline: BatchingPipeline<TInput, TOutput, CInput, Record<never, object>, COutput, R>,
    batch: OkResultWithContext<TInput, CInput>[],
    promiseScheduler: PromiseScheduler
): Promise<StageElements<TOutput, COutput, R>> {
    const feedResult = await pipeline.feed(batch, {})
    if (!feedResult.ok) {
        throw new Error(`ML mirror stage rejected feed: ${feedResult.kind} (${feedResult.reason})`)
    }
    let elements: StageElements<TOutput, COutput, R> | undefined
    let batchResult = await pipeline.next()
    while (batchResult !== null) {
        for (const sideEffect of batchResult.sideEffects ?? []) {
            void promiseScheduler.schedule(sideEffect)
        }
        elements = batchResult.elements
        batchResult = await pipeline.next()
    }
    if (!elements) {
        throw new Error('ML mirror stage returned no batch')
    }
    return elements
}

// Every message reaches a terminal result in the prepare stage or the anonymize stage, so the prepare elements carry every offset and the anonymize elements say which messages were recorded.
function progressOf<R extends string>(
    prepared: ChunkPipelineResultWithContext<unknown, { message: Message }, R>,
    anonymized: ChunkPipelineResultWithContext<unknown, { message: Message }, R>
): SessionReplayBatchProgress {
    const maxOffsets = new Map<number, number>()
    for (const { context } of prepared) {
        const { partition, offset } = context.message
        const current = maxOffsets.get(partition)
        if (current === undefined || offset > current) {
            maxOffsets.set(partition, offset)
        }
    }
    const okMessages = anonymized.flatMap(({ result, context }) => (isOkResult(result) ? [context.message] : []))
    return { maxOffsets, okMessages }
}
