import { Message } from 'node-rdkafka'
import pLimit from 'p-limit'

import { PromiseScheduler } from '~/common/utils/promise-scheduler'
import { BatchingContext, BatchingPipeline } from '~/ingestion/framework/batching-pipeline'
import { ChunkPipelineResultWithContext } from '~/ingestion/framework/chunk-pipeline.interface'
import { createBatch } from '~/ingestion/framework/helpers'
import { OkResultWithContext } from '~/ingestion/framework/pipeline.interface'
import { isOkResult } from '~/ingestion/framework/results'
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

    // Once a batch fails, later batches must not commit: their offsets would be tracked past the failed batch's messages, and the consumer stores whatever was tracked when it stops.
    private failure?: unknown

    public run(messages: Message[], committer: StagedBatchCommitter): Promise<void> {
        const queuedAt = performance.now()
        const prepared = this.stages.prepare(() =>
            this.timed('prepare', queuedAt, async () => {
                if (!messages.length) {
                    return { elements: [], handle: undefined }
                }
                // The recorder stamped here only satisfies the input type: the deferred record step rebinds to the recorder current at commit time.
                const recorder = committer.currentRecorder()
                const batch = createBatch(messages.map((message) => ({ message, sessionBatchRecorder: recorder })))
                const { elements, batchContext } = await drainBatch(this.prepare, batch, this.promiseScheduler)
                return { elements, handle: batchContext.mlBatch }
            })
        )
        const anonymized = this.stages.anonymize(async () => {
            const { elements, handle, finishedAt } = await prepared
            return this.timed('anonymize', finishedAt, async () => {
                const survivors = elements.flatMap(({ result }) => (isOkResult(result) ? [result.value] : []))
                if (survivors.length) {
                    await drainBatch(this.anonymize, createBatch(survivors), this.promiseScheduler)
                }
                return { maxOffsets: maxOffsetsOf(elements), handle }
            })
        })
        const committed = this.stages.commit(async () => {
            const { maxOffsets, handle, finishedAt } = await anonymized
            await this.timed('commit', finishedAt, async () => {
                await committer.commit(
                    maxOffsets,
                    (recorder) => handle?.commit(recorder, this.promiseScheduler) ?? Promise.resolve([])
                )
                return {}
            })
        })
        // Each stage's rejection reaches the caller through the stage after it, so the intermediate promises must not count as unhandled while they wait for a slot.
        prepared.catch(() => undefined)
        anonymized.catch(() => undefined)
        return committed
    }

    private async timed<T extends object>(
        stage: MlBatchStage,
        readyAt: number,
        run: () => Promise<T>
    ): Promise<T & { finishedAt: number }> {
        if (this.failure !== undefined) {
            throw this.failure
        }
        const startedAt = performance.now()
        try {
            const result = await run()
            const finishedAt = performance.now()
            MlMirrorMetrics.observeMlBatchStage(stage, startedAt - readyAt, finishedAt - startedAt)
            return { ...result, finishedAt }
        } catch (error) {
            this.failure ??= error
            throw error
        }
    }
}

type StageElements<TOutput, COutput extends BatchingContext, R extends string> = ChunkPipelineResultWithContext<
    TOutput,
    COutput,
    R
>

// Each stage pipeline holds one batch at a time and the runner feeds each stage in batch order, so draining to null returns exactly the batch just fed.
async function drainBatch<TInput, TOutput, CInput, CBatch, COutput extends BatchingContext, R extends string>(
    pipeline: BatchingPipeline<TInput, TOutput, CInput, CBatch, COutput, R>,
    batch: OkResultWithContext<TInput, CInput>[],
    promiseScheduler: PromiseScheduler
): Promise<{ elements: StageElements<TOutput, COutput, R>; batchContext: CBatch }> {
    const feedResult = await pipeline.feed(batch, {})
    if (!feedResult.ok) {
        throw new Error(`ML mirror stage rejected feed: ${feedResult.kind} (${feedResult.reason})`)
    }
    let drained: { elements: StageElements<TOutput, COutput, R>; batchContext: CBatch } | undefined
    let batchResult = await pipeline.next()
    while (batchResult !== null) {
        for (const sideEffect of batchResult.sideEffects ?? []) {
            void promiseScheduler.schedule(sideEffect)
        }
        drained = { elements: batchResult.elements, batchContext: batchResult.batchContext }
        batchResult = await pipeline.next()
    }
    if (!drained) {
        throw new Error('ML mirror stage returned no batch')
    }
    return drained
}

// Every message reaches a terminal result in the prepare stage or later, and every disposition advances the offset, so the prepare elements carry every offset of the batch.
function maxOffsetsOf<R extends string>(
    prepared: ChunkPipelineResultWithContext<unknown, { message: Message }, R>
): Map<number, number> {
    const maxOffsets = new Map<number, number>()
    for (const { context } of prepared) {
        const { partition, offset } = context.message
        const current = maxOffsets.get(partition)
        if (current === undefined || offset > current) {
            maxOffsets.set(partition, offset)
        }
    }
    return maxOffsets
}
