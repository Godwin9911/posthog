import { Message } from 'node-rdkafka'
import pLimit from 'p-limit'

import { PromiseScheduler } from '~/common/utils/promise-scheduler'
import { PipelineResult, PipelineResultType, ok } from '~/ingestion/framework/results'
import { SessionBatchRecorder } from '~/ingestion/pipelines/sessionreplay/sessions/session-batch-recorder'
import { SessionKey } from '~/ingestion/pipelines/sessionreplay/shared/types'

import { MlKeyBatchController } from './keys/batch-controller'
import { MlKeyBatch, MlSessionKeys } from './keys/key-store'
import { MlMirrorMetrics } from './metrics'

export interface MlDeferrableInput {
    message: Message
    team: { teamId: number }
    headers: { session_id: string }
    sessionKey: SessionKey
    sessionBatchRecorder: SessionBatchRecorder
    mlKeys?: MlSessionKeys
}

type SideEffectSink = (promises: Promise<unknown>[]) => Promise<void>
/** Runs the deferred step and answers with the message it recorded, or nothing when the step skipped or failed it. */
type DeferredAction = (recorder: SessionBatchRecorder, sideEffects: SideEffectSink) => Promise<Message | undefined>

/**
 * One poll batch's publication state: the keys it prepared and the record and produce steps it put off
 * until those keys are committed. The handle rides on every element of the batch and on the batch
 * context, so the commit stage reaches it whether or not any message survived.
 */
export class MlBatchHandle {
    public keys?: MlKeyBatch
    private deferred: DeferredAction[] = []
    private readonly publish = pLimit(8)

    constructor(private readonly keyManager?: MlKeyBatchController) {}

    public defer<T extends MlDeferrableInput>(
        input: T,
        action: (input: T) => Promise<PipelineResult<T>>
    ): Promise<PipelineResult<T>> {
        this.deferred.push(async (recorder, sideEffects) => {
            const key = this.keyManager
                ? this.keyManager.sessionKey(this.keys, input.headers.session_id, input.team.teamId)
                : input.sessionKey
            if (key.sessionState === 'deleted') {
                return undefined
            }
            // The keys are the ones the commit settled on, which differ from the prepared ones when another writer stored the session first.
            const mlKeys = this.keys?.get(input.team.teamId, input.headers.session_id)
            const result = await action({ ...input, sessionKey: key, sessionBatchRecorder: recorder, mlKeys })
            if (result.type !== PipelineResultType.OK) {
                return undefined
            }
            await sideEffects(result.sideEffects ?? [])
            return input.message
        })
        return Promise.resolve(ok(input))
    }

    // The recorder is the one current when the commit runs, not when the batch was fed: a flush can land between the two, and a record into the flushed recorder would be lost. Delivery acks go to the consumer's scheduler, which drains before offsets commit, so publication runs at enqueue speed.
    public async commit(recorder: SessionBatchRecorder, scheduler?: PromiseScheduler): Promise<Message[]> {
        if (this.keys && this.keyManager) {
            await this.keyManager.commit(this.keys)
        }
        const startedAt = performance.now()
        const sideEffects: SideEffectSink = async (promises) => {
            if (!promises.length) {
                return
            }
            if (scheduler) {
                for (const promise of promises) {
                    void scheduler.schedule(promise)
                }
            } else {
                await Promise.all(promises)
            }
        }
        const actions = this.deferred
        this.deferred = []
        const recorded = await Promise.all(actions.map((action) => this.publish(() => action(recorder, sideEffects))))
        MlMirrorMetrics.observeMlKeyPhase('publish', performance.now() - startedAt)
        return recorded.filter((message): message is Message => message !== undefined)
    }
}
