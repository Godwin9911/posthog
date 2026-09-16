import { LRUCache } from 'lru-cache'

import { logger } from '~/common/utils/logger'
import { sessionStartMonth } from '~/ingestion/pipelines/sessionreplay/ml-mirror/session-identifier-format'

import { MlDataKey, MlKeyEncryption } from './crypto'
import { DynamoItem, MlKeyDynamoDB } from './dynamodb'
import {
    MlKeyIdentity,
    MlSessionIdentity,
    TableKey,
    imageKeyId,
    keySessionMonth,
    monthKeyIndexId,
    sessionKeyId,
    tableKeyString,
    teamBlockId,
} from './schema'

// Commits retry transient DynamoDB and KMS failures; the budget counts the re-reads as well as the waits and stays under the consumer's loop stall threshold.
const COMMIT_ATTEMPTS = 10
const COMMIT_BUDGET_MS = 45_000
const COMMIT_BACKOFF_BASE_MS = 100
const COMMIT_BACKOFF_CAP_MS = 3_000
const TRANSIENT_ERRORS = new Set([
    'ProvisionedThroughputExceededException',
    'ThrottlingException',
    'RequestLimitExceeded',
    'InternalServerError',
    'ServiceUnavailableException',
    'TransactionConflictException',
    'KMSInternalException',
    'DependencyTimeoutException',
    'TimeoutError',
    'AbortError',
])
const TRANSIENT_ERROR_CODES = new Set(['ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'ETIMEDOUT', 'EAI_AGAIN'])

function isTransientError(error: unknown): boolean {
    if (!(error instanceof Error)) {
        return false
    }
    const { code, $retryable, $fault } = error as Error & { code?: string; $retryable?: unknown; $fault?: string }
    return (
        TRANSIENT_ERRORS.has(error.name) ||
        TRANSIENT_ERROR_CODES.has(code ?? '') ||
        $retryable !== undefined ||
        $fault === 'server'
    )
}

function commitRetryDelayMs(attempt: number): number {
    return Math.random() * Math.min(COMMIT_BACKOFF_CAP_MS, COMMIT_BACKOFF_BASE_MS * 2 ** attempt)
}

export interface MlSessionKeys {
    session: MlDataKey
    image: MlDataKey
}

function storedKeyId(identity: MlKeyIdentity): TableKey {
    return identity.sessionId
        ? sessionKeyId(identity.teamId, identity.sessionId)
        : imageKeyId(identity.teamId, keySessionMonth(identity))
}

/** What batches in flight at the same time share, so a session that spans consecutive batches gets one key and one write. */
export interface MlInFlightKeys {
    pendingCandidate(id: string): MlDataKey | undefined
    rememberPending(id: string, key: MlDataKey): void
    rememberCommitted(id: string): void
    wasCommitted(id: string): boolean
}

const NO_IN_FLIGHT_KEYS: MlInFlightKeys = {
    pendingCandidate: () => undefined,
    rememberPending: () => undefined,
    rememberCommitted: () => undefined,
    wasCommitted: () => false,
}

export class MlSessionKeyStore implements MlInFlightKeys {
    // A later batch reads DynamoDB before an earlier batch has persisted its keys, so the candidates of uncommitted batches and the ids committed since stand in for what the table does not show yet.
    private readonly pending = new Map<string, MlDataKey>()
    private readonly committed: LRUCache<string, true>

    constructor(
        private readonly db: MlKeyDynamoDB,
        private readonly encryption: MlKeyEncryption,
        committedMax = 100_000
    ) {
        this.committed = new LRUCache({ max: committedMax })
    }

    public async prepare(identities: MlSessionIdentity[]): Promise<MlKeyBatch> {
        const eligible = identities.filter((identity) => {
            try {
                sessionStartMonth(identity.sessionId)
                return true
            } catch {
                return false
            }
        })
        const batch = new MlKeyBatch(this.db, this.encryption, eligible, this)
        await batch.read()
        return batch
    }

    public pendingCandidate(id: string): MlDataKey | undefined {
        return this.pending.get(id)
    }

    public rememberPending(id: string, key: MlDataKey): void {
        this.pending.set(id, key)
    }

    public rememberCommitted(id: string): void {
        this.pending.delete(id)
        this.committed.set(id, true)
    }

    public wasCommitted(id: string): boolean {
        return this.committed.has(id)
    }
}

export class MlKeyBatch {
    private state = new Map<string, DynamoItem>()
    private readonly candidates = new Map<string, MlDataKey>()
    private readonly keys = new Map<string, MlDataKey>()
    private committed = false

    constructor(
        private readonly db: MlKeyDynamoDB,
        private readonly encryption: MlKeyEncryption,
        private readonly identities: MlSessionIdentity[],
        private readonly inFlight: MlInFlightKeys = NO_IN_FLIGHT_KEYS
    ) {}

    public async read(deadline?: AbortSignal): Promise<void> {
        this.keys.clear()
        const initial = this.identities.flatMap((identity) => [
            teamBlockId(identity.teamId),
            sessionKeyId(identity.teamId, identity.sessionId),
        ])
        this.state = await this.db.read(initial, deadline)
        const keyIdentities = new Map<string, MlKeyIdentity>()
        for (const identity of this.identities) {
            const id = tableKeyString(sessionKeyId(identity.teamId, identity.sessionId))
            if (
                this.state.has(tableKeyString(teamBlockId(identity.teamId))) ||
                this.state.get(id)?.deleted?.BOOL === true
            ) {
                continue
            }
            for (const sessionId of [identity.sessionId, undefined]) {
                const keyIdentity = {
                    teamId: identity.teamId,
                    organizationId: identity.organizationId,
                    ...(sessionId ? { sessionId } : { sessionMonth: sessionStartMonth(identity.sessionId) }),
                }
                keyIdentities.set(tableKeyString(storedKeyId(keyIdentity)), keyIdentity)
            }
        }
        const remaining = [...keyIdentities.values()].filter((identity) => !identity.sessionId).map(storedKeyId)
        for (const [id, item] of await this.db.read(remaining, deadline)) {
            this.state.set(id, item)
        }
        await Promise.all(
            [...keyIdentities].map(async ([id, identity]) => {
                const item = this.state.get(id)
                if (item?.deleted?.BOOL === true) {
                    return
                }
                if (item) {
                    if (!item.wrapped_key?.B || item.organization_id?.S !== identity.organizationId) {
                        throw new Error('Invalid stored ML key identity')
                    }
                    this.keys.set(id, await this.encryption.decrypt(identity, Buffer.from(item.wrapped_key.B)))
                } else {
                    let candidate = this.candidates.get(id) ?? this.inFlight.pendingCandidate(id)
                    if (!candidate) {
                        candidate = await this.encryption.generate(identity)
                        this.inFlight.rememberPending(id, candidate)
                    }
                    this.candidates.set(id, candidate)
                    this.keys.set(id, candidate)
                }
            })
        )
    }

    public get(teamId: number, sessionId: string): MlSessionKeys | undefined {
        const id = tableKeyString(sessionKeyId(teamId, sessionId))
        const session = this.keys.get(id)
        if (!session) {
            return undefined
        }
        const image = this.keys.get(tableKeyString(imageKeyId(teamId, sessionStartMonth(sessionId))))
        return image ? { session, image } : undefined
    }

    // The index entry goes first and is idempotent, so every stored key has an index entry even when the key put fails or a retried put reports the batch's own write as a competitor's. An index entry without a key is harmless: the month sweep leaves a tombstone that a later key put respects.
    private async persist(deadline: AbortSignal): Promise<void> {
        const before = [...this.keys.keys()]
        const results = await Promise.allSettled(
            [...this.keys].map(async ([id, key]) => {
                if (this.state.has(id) || this.inFlight.wasCommitted(id)) {
                    return
                }
                const location = storedKeyId(key.identity)
                await this.db.put(
                    monthKeyIndexId(key.identity, location),
                    { key_pk: { S: location.pk }, key_sk: { S: location.sk } },
                    deadline
                )
                const created = await this.db.putIfAbsent(
                    location,
                    {
                        wrapped_key: { B: key.wrapped },
                        organization_id: { S: key.identity.organizationId },
                        team_id: { N: String(key.identity.teamId) },
                        session_month: { S: keySessionMonth(key.identity) },
                    },
                    deadline
                )
                if (created) {
                    this.encryption.rememberCommitted(key)
                    this.inFlight.rememberCommitted(id)
                }
            })
        )
        const failures = results.filter((result): result is PromiseRejectedResult => result.status === 'rejected')
        const failure = failures.find(({ reason }) => !isTransientError(reason)) ?? failures[0]
        if (failure) {
            throw failure.reason
        }
        await this.read(deadline)
        const dropped = before.filter((id) => !this.keys.has(id)).length
        if (dropped) {
            logger.info('🔑', 'ml_key_commit_dropped_blocked', { dropped })
        }
    }

    public async commit(): Promise<void> {
        if (this.committed) {
            throw new Error('ML batch already committed')
        }
        const startedAt = Date.now()
        const deadline = AbortSignal.timeout(COMMIT_BUDGET_MS)
        for (let attempt = 0; attempt < COMMIT_ATTEMPTS; attempt++) {
            try {
                await this.persist(deadline)
                this.committed = true
                return
            } catch (error) {
                const delayMs = commitRetryDelayMs(attempt)
                if (
                    !isTransientError(error) ||
                    deadline.aborted ||
                    attempt === COMMIT_ATTEMPTS - 1 ||
                    Date.now() - startedAt + delayMs > COMMIT_BUDGET_MS
                ) {
                    throw error
                }
                logger.warn('🔑', 'ml_key_commit_retry', {
                    attempt: attempt + 1,
                    errorName: error instanceof Error ? error.name : undefined,
                    error: String(error),
                })
                await new Promise((resolve) => setTimeout(resolve, delayMs))
                await this.read(deadline)
            }
        }
    }
}
