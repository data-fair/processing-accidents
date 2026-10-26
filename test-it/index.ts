import { strict as assert } from 'node:assert'
import { it, describe } from 'node:test'
import os from 'node:os'
import path from 'node:path'
import fs from 'fs-extra'
import type { ProcessingConfig } from '../types/processingConfig/index.ts'
import process, { listYears, OUTPUT_FILE } from '../lib/process.ts'

import processingConfigSchema from '../processing-config-schema.json' with { type: 'json' }

const log = { step: async () => {}, task: async () => {}, progress: async () => {}, info: async () => {}, debug: async () => {}, warning: async () => {}, error: async () => {} }

// test-it/resources holds one 2024 accident between two bicycles, extracted from the real BAAC files,
// plus a pedestrian (catu 3, locp " -1") attached to one of the bicycles
const runOnResources = async (processingConfig: Partial<ProcessingConfig> = {}) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'processing-accidents-'))
  await fs.copy(path.join(import.meta.dirname, 'resources'), dir)
  await process({ datasetMode: 'create', ...processingConfig } as ProcessingConfig, dir, log)
  const lines = (await fs.readFile(path.join(dir, OUTPUT_FILE), 'utf8')).split('\n').filter(Boolean)
  await fs.remove(dir)
  return lines
}

describe('Accidents processing', () => {
  it('should expose a processing config schema for users', async () => {
    assert.equal(processingConfigSchema.type, 'object')
  })

  it('should list the years from the downloaded caracteristiques files', async () => {
    assert.deepEqual(await listYears(path.join(import.meta.dirname, 'resources')), [2024])
  })

  // since 2019 a cyclist has locp " -1" (empty in 2018), not "0": the pedestrian test relies on catu
  it('should keep the cyclists and drop the pedestrian they hit', async () => {
    const lines = await runOnResources()
    assert.equal(lines.length, 3)
    assert.ok(lines[1].startsWith('202400000014,2024-03-25,2024,mars,lundi,15:10,75,75119,'))
  })

  it('should filter on the department', async () => {
    assert.equal((await runOnResources({ filter: 75 })).length, 3)
    assert.equal((await runOnResources({ filter: 56 })).length, 1)
  })
})
