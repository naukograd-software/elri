// Local backups: a verified copy of every Space and the device database at
// a chosen directory. What the copy lacks is said, not hidden; a backup on
// the same disk is not protection against losing that disk.

import { create } from '@bufbuild/protobuf'
import {
  CreateBackupRequestSchema,
  type CreateBackupResponse,
} from '@elri/proto/elri/v1/backup/backup_pb.ts'
import { type FormEvent, useState } from 'react'
import { type CoreClients, errorText } from './core.ts'

interface Props {
  clients: CoreClients
}

export function BackupPanel({ clients }: Props) {
  const [dest, setDest] = useState('')
  const [last, setLast] = useState<CreateBackupResponse | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleCreate = async (ev: FormEvent) => {
    ev.preventDefault()
    if (!dest.trim()) return
    setBusy(true)
    try {
      const res = await clients.backup.createBackup(
        create(CreateBackupRequestSchema, { destDir: dest.trim() }),
      )
      setLast(res)
      setError(null)
    } catch (e) {
      setError(errorText(e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <section aria-labelledby="backup-heading" className="stack" style={{ maxWidth: 640 }}>
      <h2 id="backup-heading" style={{ margin: 0 }}>
        Backups
      </h2>
      <p className="muted">
        Writes every Space, the device database, attachments and unsynchronized work to a directory,
        then reopens the copy to check it. Restore with <code>elri backup restore</code> while the
        application is closed.
      </p>
      <form onSubmit={handleCreate} className="row">
        <label htmlFor="backup-dest" className="visually-hidden">
          destination directory
        </label>
        <input
          id="backup-dest"
          value={dest}
          onChange={(e) => setDest(e.target.value)}
          placeholder="/path/to/backups"
          style={{ flex: 1 }}
        />
        <button type="submit" className="primary" disabled={busy || !dest.trim()}>
          {busy ? 'Writing…' : 'Back up now'}
        </button>
      </form>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {last && (
        <div className="section" aria-live="polite">
          <h3>Last backup</h3>
          <p>
            <code>{last.dir}</code>
            <br />
            {new Date(Number(last.createdAtMs)).toLocaleString()} ·{' '}
            {(Number(last.bytes) / (1024 * 1024)).toFixed(1)} MiB ·{' '}
            <span className={last.verified ? '' : 'error'}>
              {last.verified ? 'verified' : 'NOT verified'}
            </span>
          </p>
          {last.omissions.length > 0 && (
            <p className="error">Not included: {last.omissions.join('; ')}</p>
          )}
          <table>
            <thead>
              <tr>
                <th scope="col">Space</th>
                <th scope="col">Cursor</th>
                <th scope="col">Pending</th>
                <th scope="col">Rejected</th>
              </tr>
            </thead>
            <tbody>
              {last.spaces.map((s) => (
                <tr key={s.id}>
                  <td>{s.name}</td>
                  <td className="muted">{s.cursor.toString()}</td>
                  <td className="muted">{s.pending}</td>
                  <td className={s.rejected > 0 ? 'error' : 'muted'}>{s.rejected}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}
