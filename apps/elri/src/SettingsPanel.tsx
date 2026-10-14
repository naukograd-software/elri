// Device settings: how this device reaches the network, semantic search,
// AI accounts and defaults, and which Spaces may be sent to the provider.
// Nothing here is synchronized; credentials stay in the credential store.

import { create } from '@bufbuild/protobuf'
import {
  AddApiKeyRequestSchema,
  GetSignInRequestSchema,
  ListAccountsRequestSchema,
  ListModelsRequestSchema,
  SignInState,
  SignOutRequestSchema,
  StartSignInRequestSchema,
} from '@elri/proto/elri/v1/ai/ai_pb.ts'
import { type Account, AccountKind, type Model } from '@elri/proto/elri/v1/ai/domain_pb.ts'
import {
  GetSemanticStatusRequestSchema,
  PrepareSemanticRequestSchema,
  SemanticState,
  type SemanticStatus,
} from '@elri/proto/elri/v1/search/search_pb.ts'
import {
  GetSettingsRequestSchema,
  ProxyMode,
  ResolveProxyRequestSchema,
  type Settings,
  UpdateSettingsRequestSchema,
} from '@elri/proto/elri/v1/settings/settings_pb.ts'
import type { Space } from '@elri/proto/elri/v1/space/domain_pb.ts'
import { useCallback, useEffect, useState } from 'react'
import { type CoreClients, errorText, openSignIn } from './core.ts'

interface Props {
  clients: CoreClients
  spaces: Space[]
}

const EFFORTS = ['', 'none', 'minimal', 'low', 'medium', 'high', 'xhigh']

const SEMANTIC: Record<number, string> = {
  [SemanticState.OFF]: 'off',
  [SemanticState.MISSING]: 'model not downloaded',
  [SemanticState.DOWNLOADING]: 'downloading',
  [SemanticState.LOADING]: 'loading',
  [SemanticState.READY]: 'ready',
  [SemanticState.FAILED]: 'failed',
}

export function SettingsPanel({ clients, spaces }: Props) {
  const [settings, setSettings] = useState<Settings | null>(null)
  const [proxyUrl, setProxyUrl] = useState('')
  const [noProxy, setNoProxy] = useState('')
  const [resolved, setResolved] = useState('')
  const [semantic, setSemantic] = useState<SemanticStatus | null>(null)
  const [accounts, setAccounts] = useState<Account[]>([])
  const [models, setModels] = useState<Model[]>([])
  const [label, setLabel] = useState('personal')
  const [keyLabel, setKeyLabel] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [signing, setSigning] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const refresh = useCallback(async () => {
    try {
      const s = (await clients.settings.getSettings(create(GetSettingsRequestSchema))).settings
      if (s) {
        setSettings(s)
        setProxyUrl(s.proxy?.url ?? '')
        setNoProxy(s.proxy?.noProxy ?? '')
      }
      const r = await clients.settings.resolveProxy(create(ResolveProxyRequestSchema, {}))
      setResolved(r.proxy ? `${r.proxy} (${r.source})` : `direct (${r.source})`)
      setSemantic(
        (await clients.search.getSemanticStatus(create(GetSemanticStatusRequestSchema))).status ??
          null,
      )
      const acc = (await clients.ai.listAccounts(create(ListAccountsRequestSchema))).accounts
      setAccounts(acc)
      if (acc.some((a) => !a.needsSignIn)) {
        try {
          setModels(
            (await clients.ai.listModels(create(ListModelsRequestSchema, { accountId: '' })))
              .models,
          )
        } catch {
          setModels([])
        }
      }
      setError(null)
    } catch (e) {
      setError(errorText(e))
    }
  }, [clients])

  useEffect(() => {
    void refresh()
  }, [refresh])

  // While the model downloads, follow its progress.
  useEffect(() => {
    const busy =
      semantic?.state === SemanticState.DOWNLOADING || semantic?.state === SemanticState.LOADING
    if (!busy) return
    const t = setInterval(() => void refresh(), 1000)
    return () => clearInterval(t)
  }, [semantic, refresh])

  const save = async (patch: Parameters<typeof create<typeof UpdateSettingsRequestSchema>>[1]) => {
    try {
      const res = await clients.settings.updateSettings(create(UpdateSettingsRequestSchema, patch))
      if (res.settings) setSettings(res.settings)
      setError(null)
      await refresh()
    } catch (e) {
      setError(errorText(e))
    }
  }

  const ai = settings?.ai
  const saveAi = (change: Partial<NonNullable<typeof ai>>) =>
    save({
      ai: {
        defaultAccountId: ai?.defaultAccountId ?? '',
        defaultModel: ai?.defaultModel ?? '',
        defaultEffort: ai?.defaultEffort ?? '',
        authorizedSpaceIds: ai?.authorizedSpaceIds ?? [],
        ...change,
      },
    })

  const signIn = async (accountId: string) => {
    try {
      const started = await clients.ai.startSignIn(
        create(StartSignInRequestSchema, { accountId, label: accountId ? '' : label.trim() }),
      )
      setSigning('waiting for the browser…')
      await openSignIn(started.authorizeUrl)
      for (;;) {
        await new Promise((r) => setTimeout(r, 1000))
        const st = await clients.ai.getSignIn(
          create(GetSignInRequestSchema, { attemptId: started.attemptId }),
        )
        if (st.state === SignInState.DONE) break
        if (st.state === SignInState.FAILED) throw new Error(st.error)
      }
      setSigning(null)
      await refresh()
    } catch (e) {
      setSigning(null)
      setError(errorText(e))
    }
  }

  const proxyMode = settings?.proxy?.mode ?? ProxyMode.SYSTEM
  const allowed = new Set(ai?.authorizedSpaceIds ?? [])

  return (
    <section aria-labelledby="settings-heading" className="stack" style={{ maxWidth: 720 }}>
      <h2 id="settings-heading" style={{ margin: 0 }}>
        Settings
      </h2>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}

      <div className="section stack">
        <h3>AI accounts</h3>
        {accounts.length === 0 && <p className="muted">No account yet.</p>}
        <ul className="list">
          {accounts.map((a) => (
            <li key={a.id}>
              <span>
                {a.label}{' '}
                <span className="muted">
                  {a.kind === AccountKind.CHATGPT ? `ChatGPT ${a.email}` : 'API key'}
                </span>
                {a.needsSignIn && <span className="badge attention">signed out</span>}
              </span>
              <span className="row">
                {a.kind === AccountKind.CHATGPT && a.needsSignIn && (
                  <button type="button" onClick={() => void signIn(a.id)}>
                    Sign in again
                  </button>
                )}
                <button
                  type="button"
                  className="danger"
                  onClick={async () => {
                    try {
                      const r = await clients.ai.signOut(
                        create(SignOutRequestSchema, { accountId: a.id }),
                      )
                      if (!r.revoked)
                        setError(
                          'Signed out here; the provider did not confirm. You can disconnect elri in ChatGPT settings.',
                        )
                      await refresh()
                    } catch (e) {
                      setError(errorText(e))
                    }
                  }}
                >
                  Sign out
                </button>
              </span>
            </li>
          ))}
        </ul>
        <div className="row">
          <label htmlFor="acct-label" className="visually-hidden">
            account label
          </label>
          <input
            id="acct-label"
            value={label}
            onChange={(e) => setLabel(e.target.value)}
            placeholder="label"
            style={{ width: 160 }}
          />
          <button
            type="button"
            className="primary"
            disabled={signing !== null || !label.trim()}
            onClick={() => void signIn('')}
          >
            Sign in with ChatGPT
          </button>
          {signing && (
            <span className="muted" role="status">
              {signing}
            </span>
          )}
        </div>
        <form
          className="row"
          onSubmit={async (e) => {
            e.preventDefault()
            try {
              await clients.ai.addApiKey(
                create(AddApiKeyRequestSchema, { label: keyLabel.trim(), apiKey: apiKey.trim() }),
              )
              setApiKey('')
              setKeyLabel('')
              await refresh()
            } catch (err) {
              setError(errorText(err))
            }
          }}
        >
          <input
            aria-label="API key label"
            value={keyLabel}
            onChange={(e) => setKeyLabel(e.target.value)}
            placeholder="label"
            style={{ width: 160 }}
          />
          <input
            aria-label="OpenAI API key"
            type="password"
            value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            placeholder="or an OpenAI API key"
            style={{ flex: 1 }}
          />
          <button type="submit" disabled={!keyLabel.trim() || !apiKey.trim()}>
            Add key
          </button>
        </form>
      </div>

      <div className="section stack">
        <h3>AI defaults</h3>
        <label className="row">
          account
          <select
            value={ai?.defaultAccountId ?? ''}
            onChange={(e) => void saveAi({ defaultAccountId: e.target.value })}
          >
            <option value="">first signed-in</option>
            {accounts.map((a) => (
              <option key={a.id} value={a.id}>
                {a.label}
              </option>
            ))}
          </select>
        </label>
        <label className="row">
          model
          <select
            value={ai?.defaultModel ?? ''}
            onChange={(e) => void saveAi({ defaultModel: e.target.value })}
          >
            <option value="">choose…</option>
            {ai?.defaultModel && !models.some((m) => m.slug === ai.defaultModel) && (
              <option value={ai.defaultModel}>{ai.defaultModel}</option>
            )}
            {models.map((m) => (
              <option key={m.slug} value={m.slug}>
                {m.displayName}
              </option>
            ))}
          </select>
        </label>
        <label className="row">
          effort
          <select
            value={ai?.defaultEffort ?? ''}
            onChange={(e) => void saveAi({ defaultEffort: e.target.value })}
          >
            {EFFORTS.map((x) => (
              <option key={x} value={x}>
                {x || 'model default'}
              </option>
            ))}
          </select>
        </label>
        <fieldset style={{ border: 0, padding: 0 }}>
          <legend className="muted">Spaces whose data may be sent to the AI provider</legend>
          {spaces.map((s) => (
            <label key={s.id} className="row">
              <input
                type="checkbox"
                checked={allowed.has(s.id)}
                onChange={(e) => {
                  const next = new Set(allowed)
                  if (e.target.checked) next.add(s.id)
                  else next.delete(s.id)
                  void saveAi({ authorizedSpaceIds: [...next] })
                }}
              />
              {s.name}
            </label>
          ))}
        </fieldset>
      </div>

      <div className="section stack">
        <h3>Network</h3>
        <label className="row">
          proxy
          <select
            value={proxyMode}
            onChange={(e) =>
              void save({
                proxy: { mode: Number(e.target.value) as ProxyMode, url: proxyUrl, noProxy },
              })
            }
          >
            <option value={ProxyMode.SYSTEM}>system settings</option>
            <option value={ProxyMode.ENVIRONMENT}>environment (HTTPS_PROXY)</option>
            <option value={ProxyMode.MANUAL}>manual</option>
            <option value={ProxyMode.NONE}>none</option>
          </select>
        </label>
        {proxyMode === ProxyMode.MANUAL && (
          <form
            className="stack"
            onSubmit={(e) => {
              e.preventDefault()
              void save({ proxy: { mode: ProxyMode.MANUAL, url: proxyUrl, noProxy } })
            }}
          >
            <input
              aria-label="proxy URL"
              value={proxyUrl}
              onChange={(e) => setProxyUrl(e.target.value)}
              placeholder="http://user:password@host:3128 or socks5://host:1080"
            />
            <input
              aria-label="hosts that bypass the proxy"
              value={noProxy}
              onChange={(e) => setNoProxy(e.target.value)}
              placeholder="bypass: localhost, .corp"
            />
            <button type="submit">Save proxy</button>
          </form>
        )}
        <p className="muted">requests to the provider go: {resolved}</p>
      </div>

      <div className="section stack">
        <h3>Semantic search</h3>
        <label className="row">
          <input
            type="checkbox"
            checked={settings?.semantic?.enabled ?? true}
            onChange={(e) => void save({ semantic: { enabled: e.target.checked } })}
          />
          search by meaning with a local model (nothing leaves the device)
        </label>
        {semantic && (
          <p className="muted" role="status">
            {SEMANTIC[semantic.state] ?? '?'}
            {semantic.state === SemanticState.DOWNLOADING &&
              ` ${Math.round((Number(semantic.bytesDone) / Math.max(1, Number(semantic.bytesTotal))) * 100)}%`}
            {semantic.pending > 0n && ` · ${semantic.pending} objects waiting`}
            {semantic.error && ` · ${semantic.error}`}
          </p>
        )}
        {(semantic?.state === SemanticState.MISSING ||
          semantic?.state === SemanticState.FAILED) && (
          <button
            type="button"
            onClick={async () => {
              try {
                await clients.search.prepareSemantic(create(PrepareSemanticRequestSchema))
                await refresh()
              } catch (e) {
                setError(errorText(e))
              }
            }}
          >
            Download the model (about 450 MB)
          </button>
        )}
      </div>
    </section>
  )
}
