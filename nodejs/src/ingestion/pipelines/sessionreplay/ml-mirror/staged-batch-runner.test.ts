import { DateTime } from 'luxon'
import { Message } from 'node-rdkafka'

import { EventIngestionRestrictionManager } from '~/common/utils/event-ingestion-restrictions'
import { PromiseScheduler } from '~/common/utils/promise-scheduler'
import { createApplyEventRestrictionsStep, createParseHeadersStep } from '~/ingestion/common/steps/event-preprocessing'
import { TopHogRegistry } from '~/ingestion/framework/extensions/tophog'
import { ok } from '~/ingestion/framework/results'
import { ParsedMessageData } from '~/ingestion/pipelines/sessionreplay/kafka/types'
import { SessionBatchRecorder } from '~/ingestion/pipelines/sessionreplay/sessions/session-batch-recorder'
import { SessionFilter } from '~/ingestion/pipelines/sessionreplay/sessions/session-filter'
import { SessionTracker } from '~/ingestion/pipelines/sessionreplay/sessions/session-tracker'
import {
    RetentionResolution,
    RetentionService,
} from '~/ingestion/pipelines/sessionreplay/shared/retention/retention-service'
import { SessionMap, SessionSet } from '~/ingestion/pipelines/sessionreplay/shared/session-map'
import { TeamService } from '~/ingestion/pipelines/sessionreplay/shared/teams/team-service'
import { createMockKeyStore } from '~/ingestion/pipelines/sessionreplay/shared/test-helpers'
import { StagedBatchCommitter } from '~/ingestion/pipelines/sessionreplay/staged-batch'
import { TeamForReplay } from '~/ingestion/pipelines/sessionreplay/teams/types'
import { createMockIngestionOutputs } from '~/tests/helpers/mock-ingestion-outputs'

import { createParseAndAnonymizeMessageStep } from './parse-and-anonymize-step'
import { MlMirrorStagedBatchRunner } from './staged-batch-runner'
import { buildMlMirrorStagedRunner } from './staged-batch-testing'

jest.mock('~/ingestion/common/steps/event-preprocessing', () => ({
    createParseHeadersStep: jest.fn(),
    createApplyEventRestrictionsStep: jest.fn(),
}))
jest.mock('./parse-and-anonymize-step', () => ({
    createParseAndAnonymizeMessageStep: jest.fn(),
}))

const mockCreateParseHeadersStep = createParseHeadersStep as jest.Mock
const mockCreateApplyEventRestrictionsStep = createApplyEventRestrictionsStep as jest.Mock
const mockCreateParseAndAnonymizeMessageStep = createParseAndAnonymizeMessageStep as jest.Mock

describe('ml-mirror staged batch runner', () => {
    const SESSION_A = '01a0a4f0-3200-7000-8000-000000000001'
    const SESSION_B = '01a0a4f0-3200-7000-8000-000000000002'
    const OPTED_IN_TOKEN = 'opted-in'
    const OPTED_OUT_TOKEN = 'opted-out'
    const now = DateTime.now()

    const retentionService = {
        resolveSessionRetentions: jest.fn().mockImplementation((sessions: SessionSet) => {
            const resolutions = new SessionMap<RetentionResolution>()
            for (const s of sessions) {
                resolutions.set(s.teamId, s.sessionId, { resolved: true, retentionPeriod: '30d' })
            }
            return Promise.resolve(resolutions)
        }),
    } as unknown as RetentionService
    const sessionTracker = {
        hasSeen: jest.fn().mockImplementation((sessions: SessionSet) => {
            const map = new SessionMap<boolean>()
            for (const { teamId, sessionId } of sessions) {
                map.set(teamId, sessionId, true)
            }
            return Promise.resolve(map)
        }),
        markSeen: jest.fn().mockResolvedValue(undefined),
    } as unknown as SessionTracker
    const sessionFilter = {
        handleNewSessions: jest.fn().mockResolvedValue(new SessionSet()),
        isBlocked: jest.fn().mockResolvedValue(new SessionSet()),
    } as unknown as SessionFilter
    const keyStore = createMockKeyStore()
    const teamService = {
        getTeamByToken: jest.fn().mockImplementation((token: string) =>
            Promise.resolve({
                teamId: 1,
                consoleLogIngestionEnabled: false,
                aiTrainingOptedIn: token === OPTED_IN_TOKEN,
            } satisfies TeamForReplay)
        ),
        getRetentionPeriodByTeamId: jest.fn().mockResolvedValue(30),
    } as unknown as TeamService

    let promiseScheduler: PromiseScheduler
    // Per `${sessionId}:${offset}`: a promise the mocked scrub awaits before completing.
    let scrubGates: Map<string, Promise<void>>
    let scrubStarts: Set<string>

    beforeEach(() => {
        jest.clearAllMocks()
        promiseScheduler = new PromiseScheduler()
        scrubGates = new Map()
        scrubStarts = new Set()

        mockCreateParseHeadersStep.mockReturnValue((input: { message: Message }) => {
            const headers: Record<string, string> = {}
            for (const header of input.message.headers || []) {
                for (const [key, value] of Object.entries(header)) {
                    headers[key] = Buffer.isBuffer(value) ? value.toString() : (value as string)
                }
            }
            return Promise.resolve(ok({ ...input, headers }))
        })
        mockCreateApplyEventRestrictionsStep.mockReturnValue((input: unknown) => Promise.resolve(ok(input)))
        mockCreateParseAndAnonymizeMessageStep.mockReturnValue(
            async (input: { message: Message; headers: Record<string, string> }) => {
                const scrubKey = `${input.headers.session_id}:${input.message.offset}`
                scrubStarts.add(scrubKey)
                await (scrubGates.get(scrubKey) ?? Promise.resolve())
                const parsedMessage: ParsedMessageData = {
                    metadata: {
                        partition: input.message.partition,
                        topic: input.message.topic,
                        rawSize: input.message.size,
                        offset: input.message.offset,
                        timestamp: input.message.timestamp!,
                    },
                    distinct_id: 'user-123',
                    session_id: input.headers.session_id,
                    token: input.headers.token,
                    eventsByWindowId: {},
                    preSerialized: {
                        lines: Buffer.from('["window-1",{}]\n'),
                        events: [],
                        consoleLogCount: 0,
                        consoleWarnCount: 0,
                        consoleErrorCount: 0,
                    },
                    eventsRange: { start: now, end: now },
                    snapshot_source: null,
                    snapshot_library: null,
                }
                return ok({ ...input, parsedMessage })
            }
        )
    })

    function buildRunner(): MlMirrorStagedBatchRunner {
        return buildMlMirrorStagedRunner(
            {
                outputs: createMockIngestionOutputs(),
                eventIngestionRestrictionManager: {} as unknown as EventIngestionRestrictionManager,
                overflowMode: 'disabled',
                promiseScheduler,
                teamService,
                retentionService,
                sessionTracker,
                sessionFilter,
                keyStore,
                sessionKeyResolutionMaxConcurrency: 20,
                topHog: {
                    registerSum: jest.fn().mockReturnValue({ record: jest.fn() }),
                    registerMax: jest.fn().mockReturnValue({ record: jest.fn() }),
                    registerAverage: jest.fn().mockReturnValue({ record: jest.fn() }),
                } as unknown as TopHogRegistry,
                isDebugLoggingEnabled: () => false,
            },
            { anonymizeMaxConcurrency: 2 }
        )
    }

    function message(sessionId: string, offset: number, token = OPTED_IN_TOKEN): Message {
        return {
            partition: 0,
            offset,
            topic: 'test-topic',
            value: Buffer.from('irrelevant, the scrub step is mocked'),
            key: Buffer.from('k'),
            timestamp: Date.now(),
            headers: [
                { token: Buffer.from(token) },
                { session_id: Buffer.from(sessionId) },
                { distinct_id: Buffer.from('user-123') },
            ],
            size: 10,
        } as unknown as Message
    }

    function recorder(): jest.Mocked<SessionBatchRecorder> {
        return {
            record: jest.fn().mockResolvedValue(undefined),
            getRetention: jest.fn().mockReturnValue(undefined),
        } as unknown as jest.Mocked<SessionBatchRecorder>
    }

    function gate(key: string): () => void {
        let release!: () => void
        scrubGates.set(key, new Promise<void>((resolve) => (release = resolve)))
        return release
    }

    async function until(condition: () => boolean): Promise<void> {
        for (let i = 0; i < 5000 && !condition(); i++) {
            await new Promise(setImmediate)
        }
        if (!condition()) {
            throw new Error('condition not reached while a scrub gate was held')
        }
    }

    it('prepares the next batch while the current one scrubs, and scrubs it only once the current one is done', async () => {
        const releaseA = gate(`${SESSION_A}:1`)
        const releaseB = gate(`${SESSION_B}:2`)
        const runner = buildRunner()
        const committed: number[] = []
        const committer: StagedBatchCommitter = {
            currentRecorder: recorder,
            commit: async (_maxOffsets, record) => {
                committed.push(...(await record(recorder())).map((m) => m.offset))
            },
        }

        const first = runner.run([message(SESSION_A, 1)], committer)
        const second = runner.run([message(SESSION_B, 2)], committer)
        try {
            await until(() => scrubStarts.has(`${SESSION_A}:1`))
            // The second batch's prepare stage ran to its end (the seen-mark is its last step) while the first batch's scrub was still held.
            await until(() => (sessionTracker.markSeen as jest.Mock).mock.calls.length === 2)
            expect(scrubStarts.has(`${SESSION_B}:2`)).toBe(false)
            releaseA()
            await until(() => scrubStarts.has(`${SESSION_B}:2`))
        } finally {
            releaseA()
            releaseB()
        }
        await Promise.all([first, second])

        expect(committed).toEqual([1, 2])
    })

    it('commits batches in order even when a later batch finishes scrubbing first', async () => {
        const releaseA = gate(`${SESSION_A}:1`)
        const runner = buildRunner()
        const committed: number[] = []
        const committer: StagedBatchCommitter = {
            currentRecorder: recorder,
            commit: async (_maxOffsets, record) => {
                committed.push(...(await record(recorder())).map((m) => m.offset))
            },
        }

        const first = runner.run([message(SESSION_A, 1)], committer)
        const second = runner.run([message(SESSION_B, 2)], committer)
        try {
            await until(() => scrubStarts.has(`${SESSION_A}:1`))
            await new Promise(setImmediate)
            expect(committed).toEqual([])
        } finally {
            releaseA()
        }
        await Promise.all([first, second])

        expect(committed).toEqual([1, 2])
    })

    it('records into the recorder current at commit time, not the one current at feed time', async () => {
        const releaseA = gate(`${SESSION_A}:1`)
        const runner = buildRunner()
        const fedWith = recorder()
        const flushedInto = recorder()
        let current = fedWith
        const committer: StagedBatchCommitter = {
            currentRecorder: () => current,
            commit: async (_maxOffsets, record) => {
                await record(current)
            },
        }

        const run = runner.run([message(SESSION_A, 1)], committer)
        try {
            await until(() => scrubStarts.has(`${SESSION_A}:1`))
            current = flushedInto
        } finally {
            releaseA()
        }
        await run

        expect(fedWith.record).not.toHaveBeenCalled()
        expect(flushedInto.record).toHaveBeenCalledTimes(1)
    })

    it('advances the offset past dropped messages but reports only recorded ones as recorded', async () => {
        const runner = buildRunner()
        let maxOffsets: Map<number, number> | undefined
        let recorded: number[] = []
        const committer: StagedBatchCommitter = {
            currentRecorder: recorder,
            commit: async (batchMaxOffsets, record) => {
                recorded = (await record(recorder())).map((m) => m.offset)
                maxOffsets = batchMaxOffsets
            },
        }

        await runner.run([message(SESSION_A, 5, OPTED_OUT_TOKEN), message(SESSION_B, 6)], committer)

        expect(maxOffsets?.get(0)).toBe(6)
        expect(recorded).toEqual([6])
    })

    it('commits an empty poll batch so the ingester can still flush on age', async () => {
        const runner = buildRunner()
        const commit = jest.fn().mockImplementation((_maxOffsets, record) => record(recorder()))

        await runner.run([], { currentRecorder: recorder, commit })

        expect(commit).toHaveBeenCalledTimes(1)
        expect(commit.mock.calls[0][0]).toEqual(new Map())
    })

    it('never commits a later batch once an earlier batch has failed', async () => {
        const releaseA = gate(`${SESSION_A}:1`)
        mockCreateParseAndAnonymizeMessageStep.mockReturnValue(
            async (input: { message: Message; headers: Record<string, string> }) => {
                scrubStarts.add(`${input.headers.session_id}:${input.message.offset}`)
                await (scrubGates.get(`${input.headers.session_id}:${input.message.offset}`) ?? Promise.resolve())
                throw new Error('addon crashed')
            }
        )
        const runner = buildRunner()
        const commit = jest.fn().mockImplementation((_maxOffsets, record) => record(recorder()))
        const committer: StagedBatchCommitter = { currentRecorder: recorder, commit }

        const first = runner.run([message(SESSION_A, 1)], committer)
        const second = runner.run([message(SESSION_B, 2)], committer)
        await until(() => scrubStarts.has(`${SESSION_A}:1`))
        releaseA()

        await expect(first).rejects.toThrow('addon crashed')
        await expect(second).rejects.toThrow('addon crashed')
        expect(commit).not.toHaveBeenCalled()
    })

    it('still tracks offsets for a batch that drops every message', async () => {
        const runner = buildRunner()
        let maxOffsets: Map<number, number> | undefined
        let recorded: Message[] = []
        const committer: StagedBatchCommitter = {
            currentRecorder: recorder,
            commit: async (batchMaxOffsets, record) => {
                recorded = await record(recorder())
                maxOffsets = batchMaxOffsets
            },
        }

        await runner.run([message(SESSION_A, 7, OPTED_OUT_TOKEN)], committer)

        expect(maxOffsets?.get(0)).toBe(7)
        expect(recorded).toEqual([])
        expect(scrubStarts.size).toBe(0)
    })
})
