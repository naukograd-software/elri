// Field values as the renderer edits them: a typed scalar with a text form.

import { create } from '@bufbuild/protobuf'
import { type Value, ValueSchema } from '@elri/proto/elri/v1/object/domain_pb.ts'

export type ValueKind = 'string' | 'int' | 'double' | 'bool' | 'time' | 'ref'

export const VALUE_KINDS: ValueKind[] = ['string', 'int', 'double', 'bool', 'time', 'ref']

export function kindOf(v: Value): ValueKind {
  switch (v.value.case) {
    case 'int':
      return 'int'
    case 'double':
      return 'double'
    case 'bool':
      return 'bool'
    case 'timeMs':
      return 'time'
    case 'ref':
      return 'ref'
    default:
      return 'string'
  }
}

export function textOf(v: Value): string {
  switch (v.value.case) {
    case 'string':
      return v.value.value
    case 'int':
      return v.value.value.toString()
    case 'double':
      return String(v.value.value)
    case 'bool':
      return v.value.value ? 'true' : 'false'
    case 'timeMs':
      return new Date(Number(v.value.value)).toISOString()
    case 'ref':
      return v.value.value
    default:
      return ''
  }
}

export function parseValue(kind: ValueKind, text: string): Value | string {
  const t = text.trim()
  switch (kind) {
    case 'string':
      return create(ValueSchema, { value: { case: 'string', value: text } })
    case 'int': {
      if (!/^-?\d+$/.test(t)) return 'expected an integer'
      return create(ValueSchema, { value: { case: 'int', value: BigInt(t) } })
    }
    case 'double': {
      const n = Number(t)
      if (t === '' || Number.isNaN(n)) return 'expected a number'
      return create(ValueSchema, { value: { case: 'double', value: n } })
    }
    case 'bool': {
      if (t !== 'true' && t !== 'false') return 'expected true or false'
      return create(ValueSchema, { value: { case: 'bool', value: t === 'true' } })
    }
    case 'time': {
      const ms = Date.parse(t)
      if (Number.isNaN(ms)) return 'expected a date/time'
      return create(ValueSchema, { value: { case: 'timeMs', value: BigInt(ms) } })
    }
    case 'ref': {
      if (!/^[0-9a-f-]{36}$/i.test(t)) return 'expected an object id'
      return create(ValueSchema, { value: { case: 'ref', value: t } })
    }
  }
}
