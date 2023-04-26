const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const pump = util.promisify(require('pump'))

const filtreUrl = async (urlSource, year, axios) => {
  const json = await axios.get(urlSource)
  const filtre = []
  const resources = json.data.resources
  for (let i = 0; i < resources.length; i++) {
    // the 2021 file is 'carcteristiques' instead of 'caracteristiques'
    if (resources[i].url.includes('csv') && ((resources[i].url.includes('vehicules') || resources[i].url.includes('usagers') || resources[i].url.includes('lieux') || resources[i].url.includes('caracteristiques') || resources[i].url.includes('carcteristiques')))) {
      if (path.parse(new URL(resources[i].url).pathname).name.includes(`${year}`)) {
        filtre.push(resources[i].url)
      }
    }
  }
  return filtre
}

exports.download = async (pluginConfig, processingConfig, dir = 'data', axios, log) => {
  await log.step('Filtre des fichiers')
  const urls = await filtreUrl(pluginConfig.url, processingConfig.year, axios)
  await log.step('Téléchargement du fichier')

  for (const url of urls) {
    let res
    try {
      res = await axios.get(url, { responseType: 'stream' })
    } catch (err) {
      if (err.status === 404) {
        await log.warning('Le fichier n\'existe pas')
        return
      }
      throw err
    }

    const fileName = path.parse(new URL(url).pathname).name + '.csv'
    const file = `${dir}/${fileName}`

    if (await fs.pathExists(file)) {
      await log.warning(`Le fichier ${file} existe déjà`)
    } else {
      // creating empty file before streaming seems to fix some weird bugs with NFS
      await fs.ensureFile(file)
      await log.info(`Récupération du fichier ${file}`)
      await pump(res.data, fs.createWriteStream(file))
      await log.info(`Fichier récupéré dans ${file}`)
    }
  }
}
