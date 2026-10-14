// Workflow runs of a Space: background work made visible. A paused run says
// why; a step with an unknown outcome waits for the user to say what
// happened before anything is repeated.

import { create } from '@bufbuild/protobuf'
import {
  CancelWorkflowRunRequestSchema,
  GetWorkflowRunRequestSchema,
  ListWorkflowRunsRequestSchema,
  ResolveWorkflowStepRequestSchema,
  ResumeWorkflowRunRequestSchema,
  RunStatus,
  StepStatus,
  type WorkflowRun,
} from '@elri/proto/elri/v1/workflow/workflow_pb.ts'
import { useCallback, useEffect, useState } from 'react'
import { type CoreClients, errorText } from './core.ts'
import { coalesce, watch } from './events.ts'

interface Props {
  clients: CoreClients
  spaceId: string
}

const RUN: Record<number, string> = {
  [RunStatus.RUNNING]: 'running',
  [RunStatus.PAUSED]: 'paused',
  [RunStatus.DONE]: 'done',
  [RunStatus.FAILED]: 'failed',
  [RunStatus.CANCELLED]: 'cancelled',
}
const STEP: Record<number, string> = {
  [StepStatus.INTENT]: 'running',
  [StepStatus.DONE]: 'done',
  [StepStatus.FAILED]: 'failed',
  [StepStatus.UNKNOWN]: 'unknown outcome',
}

export function WorkflowsPanel({ clients, spaceId }: Props) {
  const [runs, setRuns] = useState<WorkflowRun[]>([])
  const [open, setOpen] = useState<WorkflowRun | null>(null)
  const [resultText, setResultText] = useState('')
  const [error, setError] = useState<string | null>(null)

  const refresh = useCallback(async () => {
    try {
      const res = await clients.workflows.listWorkflowRuns(
        create(ListWorkflowRunsRequestSchema, { spaceId, limit: 100 }),
      )
      setRuns(res.runs)
      setOpen((cur) => {
        if (!cur) return null
        void clients.workflows
          .getWorkflowRun(create(GetWorkflowRunRequestSchema, { id: cur.id }))
          .then((r) => setOpen(r.run ?? null))
        return cur
      })
      setError(null)
    } catch (e) {
      setError(errorText(e))
    }
  }, [clients, spaceId])

  useEffect(() => {
    void refresh()
    const reload = coalesce(() => void refresh(), 500)
    const timer = setInterval(reload, 3000)
    const stop = watch(clients, [spaceId], (ev) => {
      if (ev.event.case === 'notice' || ev.event.case === 'lagged') reload()
    })
    return () => {
      clearInterval(timer)
      stop()
    }
  }, [clients, spaceId, refresh])

  const act = async (f: () => Promise<unknown>) => {
    try {
      await f()
      setError(null)
      await refresh()
    } catch (e) {
      setError(errorText(e))
    }
  }

  const show = async (id: string) => {
    const res = await clients.workflows.getWorkflowRun(create(GetWorkflowRunRequestSchema, { id }))
    setOpen(res.run ?? null)
  }

  return (
    <section aria-labelledby="workflows-heading" className="stack">
      <h2 id="workflows-heading" style={{ margin: 0 }}>
        Workflows
      </h2>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {runs.length === 0 ? (
        <p className="muted">No runs in this Space.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th scope="col">Started</th>
              <th scope="col">Script</th>
              <th scope="col">Status</th>
              <th scope="col">Why</th>
              <th scope="col">
                <span className="visually-hidden">Actions</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {runs.map((r) => (
              <tr key={r.id} className={r.status === RunStatus.PAUSED ? 'attention' : undefined}>
                <td className="muted">{new Date(Number(r.createdAtMs)).toLocaleString()}</td>
                <td>
                  <code>{r.scriptId}</code>
                </td>
                <td>{RUN[r.status] ?? '?'}</td>
                <td className="muted">{r.reason}</td>
                <td>
                  <button type="button" onClick={() => void show(r.id)}>
                    Steps
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {open && (
        <div className="section" aria-live="polite">
          <h3>
            Run <code>{open.id}</code> · {RUN[open.status] ?? '?'}
          </h3>
          {open.reason && <p className="muted">{open.reason}</p>}
          <table>
            <thead>
              <tr>
                <th scope="col">Step</th>
                <th scope="col">Status</th>
                <th scope="col">Result</th>
                <th scope="col">
                  <span className="visually-hidden">Actions</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {open.steps.map((s) => (
                <tr
                  key={s.name}
                  className={s.status === StepStatus.UNKNOWN ? 'attention' : undefined}
                >
                  <td>{s.name}</td>
                  <td>{STEP[s.status] ?? '?'}</td>
                  <td className="muted">{s.resultJson || s.error}</td>
                  <td>
                    {(s.status === StepStatus.UNKNOWN || s.status === StepStatus.FAILED) &&
                      open.status !== RunStatus.RUNNING && (
                        <div className="stack">
                          <button
                            type="button"
                            onClick={() =>
                              act(() =>
                                clients.workflows.resolveWorkflowStep(
                                  create(ResolveWorkflowStepRequestSchema, {
                                    id: open.id,
                                    step: s.name,
                                    outcome: { case: 'retry', value: true },
                                  }),
                                ),
                              )
                            }
                          >
                            It did not happen: run it again
                          </button>
                          <div className="row">
                            <input
                              aria-label={`result of step ${s.name} as JSON`}
                              placeholder="result JSON"
                              value={resultText}
                              onChange={(e) => setResultText(e.target.value)}
                            />
                            <button
                              type="button"
                              onClick={() =>
                                act(() =>
                                  clients.workflows.resolveWorkflowStep(
                                    create(ResolveWorkflowStepRequestSchema, {
                                      id: open.id,
                                      step: s.name,
                                      outcome: { case: 'resultJson', value: resultText },
                                    }),
                                  ),
                                )
                              }
                            >
                              It happened: record this result
                            </button>
                          </div>
                        </div>
                      )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {(open.status === RunStatus.PAUSED || open.status === RunStatus.FAILED) && (
            <div className="row" style={{ marginTop: 8 }}>
              <button
                type="button"
                className="primary"
                onClick={() =>
                  act(() =>
                    clients.workflows.resumeWorkflowRun(
                      create(ResumeWorkflowRunRequestSchema, { id: open.id }),
                    ),
                  )
                }
              >
                Resume
              </button>
              <button
                type="button"
                className="danger"
                onClick={() =>
                  act(() =>
                    clients.workflows.cancelWorkflowRun(
                      create(CancelWorkflowRunRequestSchema, { id: open.id }),
                    ),
                  )
                }
              >
                Cancel run
              </button>
            </div>
          )}
        </div>
      )}
    </section>
  )
}
