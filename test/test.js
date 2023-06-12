process.env.NODE_ENV = 'test'
const config = require('config')
const testUtils = require('@data-fair/processings-test-utils')
const accident = require('../')

describe('gloab test', function () {
  it('should create a dataset and load data', async function () {
    this.timeout(100000000000)
    const context = testUtils.context({
      processingConfig: {
        datasetMode: 'update',
        url: 'https://www.data.gouv.fr/api/1/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2021/',
        dataset: { id: 'accidents-test', title: 'accidents test' }
      },
      tmpDir: 'data'
    }, config, false)
    await accident.run(context)
  })
})
