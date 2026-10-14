// Chat with the agents of a Space. The answer streams in as it is
// written; every tool call the agent makes is shown with its result.
// Conversations are Objects of the Space, so they follow the Space.

import { create } from '@bufbuild/protobuf'
import {
  CreateAgentRequestSchema,
  GetConversationRequestSchema,
  GrantAgentRequestSchema,
  ListAgentsRequestSchema,
  ListConversationsRequestSchema,
  ListSkillsRequestSchema,
  SendRequestSchema,
  StartConversationRequestSchema,
} from '@elri/proto/elri/v1/ai/ai_pb.ts'
import {
  type Agent,
  type Conversation,
  type Message,
  MessageRole,
  type Skill,
} from '@elri/proto/elri/v1/ai/domain_pb.ts'
import { type FormEvent, useCallback, useEffect, useRef, useState } from 'react'
import { type CoreClients, errorText } from './core.ts'

interface Props {
  clients: CoreClients
  spaceId: string
  onOpenSettings: () => void
}

interface Shown {
  key: number
  role: 'user' | 'assistant' | 'tool'
  text: string
  tool?: string
  args?: string
  result?: string
  failed?: boolean
}

interface TurnError {
  code: string
  message: string
}

let nextKey = 0

function shown(m: Message): Shown {
  const key = nextKey++
  if (m.role === MessageRole.TOOL)
    return {
      key,
      role: 'tool',
      text: '',
      tool: m.tool,
      args: m.argumentsJson,
      result: m.result,
      failed: m.failed,
    }
  return { key, role: m.role === MessageRole.USER ? 'user' : 'assistant', text: m.text }
}

export function ChatPanel({ clients, spaceId, onOpenSettings }: Props) {
  const [agents, setAgents] = useState<Agent[]>([])
  const [agentId, setAgentId] = useState<string>('')
  const [conversations, setConversations] = useState<Conversation[]>([])
  const [conversationId, setConversationId] = useState<string>('')
  const [messages, setMessages] = useState<Shown[]>([])
  const [draft, setDraft] = useState('')
  const [text, setText] = useState('')
  const [busy, setBusy] = useState(false)
  const [turnError, setTurnError] = useState<TurnError | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [creating, setCreating] = useState(false)
  const end = useRef<HTMLDivElement>(null)

  const loadAgents = useCallback(async () => {
    try {
      const res = await clients.agents.listAgents(create(ListAgentsRequestSchema, { spaceId }))
      setAgents(res.agents)
      setAgentId((cur) => cur || res.agents[0]?.id || '')
      setCreating(res.agents.length === 0)
    } catch (e) {
      setError(errorText(e))
    }
  }, [clients, spaceId])

  const loadConversations = useCallback(async () => {
    if (!agentId) {
      setConversations([])
      return
    }
    try {
      const res = await clients.agents.listConversations(
        create(ListConversationsRequestSchema, { spaceId, agentId }),
      )
      setConversations(res.conversations)
    } catch (e) {
      setError(errorText(e))
    }
  }, [clients, spaceId, agentId])

  const loadConversation = useCallback(
    async (id: string) => {
      if (!id) {
        setMessages([])
        return
      }
      const res = await clients.agents.getConversation(
        create(GetConversationRequestSchema, { spaceId, id }),
      )
      setMessages((res.conversation?.messages ?? []).map(shown))
    },
    [clients, spaceId],
  )

  useEffect(() => {
    void loadAgents()
  }, [loadAgents])
  useEffect(() => {
    void loadConversations()
  }, [loadConversations])
  useEffect(() => {
    void loadConversation(conversationId)
  }, [loadConversation, conversationId])
  useEffect(() => {
    if (messages.length > 0 || draft) end.current?.scrollIntoView({ block: 'end' })
  }, [messages, draft])

  const agent = agents.find((a) => a.id === agentId)

  const send = async (ev: FormEvent) => {
    ev.preventDefault()
    const message = text.trim()
    if (!message || !agentId || busy) return
    setBusy(true)
    setTurnError(null)
    setText('')
    try {
      let conv = conversationId
      if (!conv) {
        const res = await clients.agents.startConversation(
          create(StartConversationRequestSchema, {
            spaceId,
            agentId,
            title: message.slice(0, 60),
          }),
        )
        conv = res.conversation?.id ?? ''
        setConversationId(conv)
        await loadConversations()
      }
      for await (const e of clients.agents.send(
        create(SendRequestSchema, { spaceId, conversationId: conv, text: message }),
      )) {
        const ev = e.event
        switch (ev.case) {
          case 'textDelta':
            setDraft((d) => d + ev.value)
            break
          case 'message':
            setDraft('')
            setMessages((m) => [...m, shown(ev.value)])
            break
          case 'toolCall':
            setMessages((m) => [
              ...m,
              {
                key: nextKey++,
                role: 'tool',
                text: '',
                tool: ev.value.tool,
                args: ev.value.argumentsJson,
              },
            ])
            break
          case 'toolResult':
            setMessages((m) => {
              const next = [...m]
              for (let i = next.length - 1; i >= 0; i--) {
                const t = next[i]
                if (t.role === 'tool' && t.result === undefined) {
                  next[i] = { ...t, result: ev.value.result, failed: ev.value.failed }
                  break
                }
              }
              return next
            })
            break
          case 'error':
            setTurnError({ code: ev.value.code, message: ev.value.message })
            // A refused turn kept nothing; the message waits to be sent again.
            if (ev.value.code === 'consent' || ev.value.code === 'sign_in') setText(message)
            break
        }
      }
    } catch (e) {
      setTurnError({ code: 'transport', message: errorText(e) })
      setText(message)
    } finally {
      setDraft('')
      setBusy(false)
    }
  }

  const grant = async () => {
    if (!agent) return
    try {
      await clients.agents.grantAgent(
        create(GrantAgentRequestSchema, {
          spaceId,
          agentId: agent.id,
          expectedVersion: agent.version,
        }),
      )
      setTurnError(null)
    } catch (e) {
      setError(errorText(e))
    }
  }

  return (
    <section aria-labelledby="chat-heading" className="chat">
      <div className="chat-side stack">
        <h2 id="chat-heading" style={{ margin: 0 }}>
          Chat
        </h2>
        {error && (
          <p className="error" role="alert">
            {error}
          </p>
        )}
        <label className="stack">
          <span className="muted">agent</span>
          <select
            value={agentId}
            onChange={(e) => {
              setAgentId(e.target.value)
              setConversationId('')
            }}
          >
            {agents.map((a) => (
              <option key={a.id} value={a.id}>
                {a.name}
              </option>
            ))}
          </select>
        </label>
        <button type="button" onClick={() => setCreating((c) => !c)}>
          {creating ? 'Cancel' : 'New agent'}
        </button>
        <button
          type="button"
          disabled={!agentId}
          onClick={() => {
            setConversationId('')
            setMessages([])
          }}
        >
          New conversation
        </button>
        <ul className="list" aria-label="conversations">
          {conversations.map((c) => (
            <li key={c.id} className={c.id === conversationId ? 'active' : ''}>
              <button type="button" className="linklike" onClick={() => setConversationId(c.id)}>
                {c.title}
              </button>
            </li>
          ))}
        </ul>
      </div>

      <div className="chat-main">
        {creating ? (
          <NewAgent
            clients={clients}
            spaceId={spaceId}
            onCreated={async (a) => {
              setCreating(false)
              await loadAgents()
              setAgentId(a.id)
            }}
          />
        ) : (
          <>
            <div className="chat-log" aria-live="polite">
              {messages.length === 0 && !draft && (
                <p className="muted">
                  {agent ? `Ask ${agent.name} something.` : 'Create an agent to start.'}
                </p>
              )}
              {messages.map((m) =>
                m.role === 'tool' ? (
                  <details key={m.key} className={`chat-tool${m.failed ? ' failed' : ''}`}>
                    <summary>
                      {m.failed ? '✗' : m.result === undefined ? '…' : '✓'} {m.tool}{' '}
                      <code>{toolSummary(m.args)}</code>
                    </summary>
                    {m.args && <pre>{pretty(m.args)}</pre>}
                    {m.result !== undefined && <pre>{pretty(m.result)}</pre>}
                  </details>
                ) : (
                  <div key={m.key} className={`chat-msg ${m.role}`}>
                    {m.text}
                  </div>
                ),
              )}
              {draft && <div className="chat-msg assistant">{draft}</div>}
              {turnError && (
                <div className="error" role="alert">
                  {turnError.message}
                  {turnError.code === 'consent' && turnError.message.includes('not granted') && (
                    <button type="button" onClick={() => void grant()} style={{ marginLeft: 8 }}>
                      Allow this agent
                    </button>
                  )}
                  {(turnError.code === 'consent' || turnError.code === 'sign_in') &&
                    !turnError.message.includes('not granted') && (
                      <button type="button" onClick={onOpenSettings} style={{ marginLeft: 8 }}>
                        Open settings
                      </button>
                    )}
                </div>
              )}
              <div ref={end} />
            </div>
            <form className="row chat-input" onSubmit={send}>
              <label htmlFor="chat-text" className="visually-hidden">
                message
              </label>
              <textarea
                id="chat-text"
                rows={2}
                value={text}
                disabled={!agentId}
                onChange={(e) => setText(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault()
                    void send(e)
                  }
                }}
                placeholder={
                  busy ? 'working…' : 'Message (Enter sends, Shift+Enter for a new line)'
                }
                style={{ flex: 1 }}
              />
              <button type="submit" className="primary" disabled={busy || !text.trim() || !agentId}>
                Send
              </button>
            </form>
          </>
        )}
      </div>
    </section>
  )
}

function toolSummary(args?: string): string {
  if (!args) return ''
  try {
    const a = JSON.parse(args) as Record<string, unknown>
    return String(a.method ?? a.target ?? a.name ?? '')
  } catch {
    return ''
  }
}

function pretty(s: string): string {
  try {
    const v = JSON.parse(s) as Record<string, unknown>
    if (typeof v.request === 'string') {
      try {
        v.request = JSON.parse(v.request)
      } catch {
        // left as text
      }
    }
    return JSON.stringify(v, null, 2)
  } catch {
    return s
  }
}

function NewAgent({
  clients,
  spaceId,
  onCreated,
}: {
  clients: CoreClients
  spaceId: string
  onCreated: (a: Agent) => Promise<void>
}) {
  const [name, setName] = useState('Assistant')
  const [instructions, setInstructions] = useState('')
  const [model, setModel] = useState('')
  const [effort, setEffort] = useState('')
  const [skills, setSkills] = useState<Skill[]>([])
  const [chosen, setChosen] = useState<Set<string>>(new Set())
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    void clients.agents
      .listSkills(create(ListSkillsRequestSchema, { spaceId }))
      .then((r) => setSkills(r.skills))
      .catch(() => setSkills([]))
  }, [clients, spaceId])

  const submit = async (ev: FormEvent) => {
    ev.preventDefault()
    try {
      const res = await clients.agents.createAgent(
        create(CreateAgentRequestSchema, {
          spaceId,
          agent: {
            name: name.trim(),
            instructions,
            model: model.trim(),
            effort,
            skills: [...chosen],
          },
        }),
      )
      if (res.agent) await onCreated(res.agent)
    } catch (e) {
      setError(errorText(e))
    }
  }

  return (
    <form className="stack" onSubmit={submit} style={{ maxWidth: 640 }}>
      <h3>New agent</h3>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      <input aria-label="agent name" value={name} onChange={(e) => setName(e.target.value)} />
      <textarea
        aria-label="instructions"
        rows={6}
        value={instructions}
        onChange={(e) => setInstructions(e.target.value)}
        placeholder="What this agent is for, how it should behave"
      />
      <div className="row">
        <input
          aria-label="model"
          value={model}
          onChange={(e) => setModel(e.target.value)}
          placeholder="model (empty: device default)"
          style={{ flex: 1 }}
        />
        <select aria-label="effort" value={effort} onChange={(e) => setEffort(e.target.value)}>
          {['', 'none', 'minimal', 'low', 'medium', 'high', 'xhigh'].map((x) => (
            <option key={x} value={x}>
              {x || 'default effort'}
            </option>
          ))}
        </select>
      </div>
      <fieldset style={{ border: 0, padding: 0 }}>
        <legend className="muted">skills (none chosen: all built-in ones)</legend>
        {skills.map((s) => (
          <label key={s.id} className="row">
            <input
              type="checkbox"
              checked={chosen.has(s.id)}
              onChange={(e) =>
                setChosen((c) => {
                  const n = new Set(c)
                  if (e.target.checked) n.add(s.id)
                  else n.delete(s.id)
                  return n
                })
              }
            />
            {s.name} <span className="muted">— {s.description}</span>
          </label>
        ))}
      </fieldset>
      <button type="submit" className="primary" disabled={!name.trim()}>
        Create agent
      </button>
    </form>
  )
}
