process.env.NODE_ENV = 'test'
const config = require('config')
const testUtils = require('@data-fair/processings-test-utils')
const { download } = require('../lib/download.js')

describe('download', () => {
  it('should download the file', async () => {
    const context = testUtils.context({
      pluginConfig: {
        url: 'https://www.data.gouv.fr/api/1/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2021/'
      },
      processingConfig: {
        year: 2020
      },
      tmpDir: 'data/'
    }, config, false)
    await download(context.pluginConfig, context.processingConfig, context.tmpDir, context.axios, context.log)
  })
})
