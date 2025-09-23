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
    if (
      resources[i].url.includes('csv') &&
      (
        (
          (
            resources[i].url.includes('vehicules') &&
            !resources[i].url.includes('immatricules')
          ) ||
          resources[i].url.includes('usagers') ||
          resources[i].url.includes('lieux') ||
          resources[i].url.includes('caracteristiques') ||
          resources[i].url.includes('carcteristiques') || // pour le cas des fichier 2021 et 2022
          resources[i].url.includes('caract') // pour le cas du fichier 2023
        )
      )
    ) {
      filtre.push(resources[i].url)
    }
  }
  return filtre
}

module.exports = async (processingConfig, dir = 'data', axios, log) => {
  await log.step('Récupération des fichiers à télécharger')
  const urls = await filtreUrl(processingConfig.url, axios)
  await log.info(`Fichiers à télécharger : ${urls.length}`)
  await log.step('Téléchargement des fichiers')
  await log.task('Téléchargement des fichiers')
  await log.progress('Téléchargement des fichiers', 0, urls.length)

  for (let i = 0; i < urls.length; i++) {
    const url = urls[i]

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
    let fileName = path.parse(new URL(url).pathname).name + '.csv'
    if (fileName.includes('_')) {
      fileName = fileName.replace('_', '-')
    }
    // pour le cas des fichier 2021 et 2022
    if (fileName.includes('carcteristiques-')) {
      fileName = fileName.replace('carcteristiques-', 'caracteristiques-')
    }
    // pour le cas du fichier 2023
    if (fileName.includes('caract-')) {
      fileName = fileName.replace('caract-', 'caracteristiques-')
    }
    const file = path.join(dir, fileName)
    if (await fs.pathExists(file)) {
      await log.warning(`Le fichier ${file} existe déjà`)
    } else {
      const decodeStream = iconv.decodeStream('iso-8859-1')
      // creating empty file before streaming seems to fix some weird bugs with NFS
      await fs.ensureFile(file)
      await log.info(`Récupération du fichier ${fileName}`)
      await pump(res.data.pipe(decodeStream), fs.createWriteStream(file))
      await log.info(`Fichier récupéré dans ${file}`)

      // Correction spécifique pour le fichier caracteristiques-2022.csv
      if (fileName === 'caracteristiques-2022.csv') {
        await log.info('Correction de l\'en-tête du fichier caracteristiques-2022.csv')
        const fileContent = await fs.readFile(file, 'utf-8')
        const correctedContent = fileContent.replace('"Accident_Id"', '"Num_Acc"')
        await fs.writeFile(file, correctedContent, 'utf-8')
        await log.info('En-tête corrigé : Accident_Id → Num_Acc')
      }
    }

    await log.progress('Téléchargement des fichiers', i, urls.length)
  }
}
