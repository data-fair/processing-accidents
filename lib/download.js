const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const pump = util.promisify(require('pump'))
const iconv = require('iconv-lite')

const filtreUrl = async (urlSource, axios) => {
  const json = await axios.get(urlSource)
  const filtre = []
  const resources = json.data.resources
  for (let i = 0; i < resources.length; i++) {
    // the 2021 file is 'carcteristiques' instead of 'caracteristiques'
    if (resources[i].url.includes('csv') && ((resources[i].url.includes('vehicules') || resources[i].url.includes('usagers') || resources[i].url.includes('lieux') || resources[i].url.includes('caracteristiques') || resources[i].url.includes('carcteristiques')))) {
      filtre.push(resources[i].url)
    }
  }
  return filtre
}

module.exports = async (processingConfig, dir = 'data', axios, log) => {
  await log.step('Filtre des fichiers')
  const urls = await filtreUrl(processingConfig.url, axios)
  await log.step('Téléchargement du fichier')

  for (const url of urls) {
    let res
    try {
      res = await axios.get(url, { responseType: 'stream' })
    } catch (err) {
      if (err.status === 404) {
        await log.error('URL mal renseigné ou fichier indisponible')
        return
      }
      throw err
    }
    // pour le cas du fichier 2021
    let fileName = path.parse(new URL(url).pathname).name + '.csv'
    if (fileName.includes('carcteristiques')) {
      fileName = fileName.replace('carcteristiques', 'caracteristiques')
    }
    if (fileName.includes('_')) {
      fileName = fileName.replace('_', '-')
    }
    const file = path.join(dir, fileName)
    if (await fs.pathExists(file)) {
      await log.warning(`Le fichier ${file} existe déjà`)
    } else {
      const decodeStream = iconv.decodeStream('iso-8859-1')
      // creating empty file before streaming seems to fix some weird bugs with NFS
      await fs.ensureFile(file)
      await log.info(`Récupération du fichier ${file}`)
      await pump(res.data.pipe(decodeStream), fs.createWriteStream(file))
      await log.info(`Fichier récupéré dans ${file}`)
    }
  }
}
