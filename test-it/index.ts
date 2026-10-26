import { strict as assert } from 'node:assert'
import { it, describe } from 'node:test'
import path from 'node:path'
import { listYears } from '../lib/process.ts'

import processingConfigSchema from '../processing-config-schema.json' with { type: 'json' }

describe('Accidents processing', () => {
  it('should expose a processing config schema for users', async () => {
    assert.equal(processingConfigSchema.type, 'object')
  })

  it('should list the years from the downloaded caracteristiques files', async () => {
    assert.deepEqual(await listYears(path.join(import.meta.dirname, 'resources')), [2024])
  })
})
