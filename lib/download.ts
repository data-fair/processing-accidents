import fs from 'fs-extra'
import path from 'path'
import { pipeline } from 'stream/promises'
import iconv from 'iconv-lite'
import type { AxiosInstance } from 'axios'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import { isStopped } from './process.ts'

// data.gouv redirects renamed datasets and static files, context.axios follows none by default
const externalOptions = { maxRedirects: 5, timeout: 5 * 60 * 1000 }

const filtreUrl = async (urlSource: string, axios: AxiosInstance): Promise<string[]> => {
  const resources: { url: string }[] = (await axios.get(urlSource, externalOptions)).data.resources
  return resources.map(r => r.url).filter(url =>
    url.includes('csv') &&
    (
      (url.includes('vehicules') && !url.includes('immatricules')) ||
      url.includes('usagers') ||
      url.includes('lieux') ||
      url.includes('caracteristiques') ||
      url.includes('carcteristiques') || // pour le cas des fichier 2021 et 2022
      url.includes('caract') // pour le cas des fichiers 2023 et 2024
    )
  )
}

export default async (processingConfig: ProcessingConfig, dir: string, axios: AxiosInstance, log: ProcessingContext<ProcessingConfig>['log']): Promise<void> => {
  await log.step('Récupération des fichiers à télécharger')
  const urls = await filtreUrl(processingConfig.url ?? '', axios)
  await log.info(`Fichiers à télécharger : ${urls.length}`)
  await log.step('Téléchargement des fichiers')
  await log.task('Téléchargement des fichiers')
  await log.progress('Téléchargement des fichiers', 0, urls.length)

  for (let i = 0; i < urls.length; i++) {
    if (isStopped()) return
    const url = urls[i]

    let fileName = path.parse(new URL(url).pathname).name + '.csv'
    if (fileName.includes('_')) {
      fileName = fileName.replace('_', '-')
    }
    // pour le cas des fichier 2021 et 2022
    if (fileName.includes('carcteristiques-')) {
      fileName = fileName.replace('carcteristiques-', 'caracteristiques-')
    }
    // pour le cas des fichiers 2023 et 2024
    if (fileName.includes('caract-')) {
      fileName = fileName.replace('caract-', 'caracteristiques-')
    }
    const file = path.join(dir, fileName)
    if (await fs.pathExists(file)) {
      await log.warning(`Le fichier ${file} existe déjà`)
    } else {
      const res = await axios.get(url, { ...externalOptions, responseType: 'stream' })
      // creating empty file before streaming seems to fix some weird bugs with NFS
      await fs.ensureFile(file)
      await log.info(`Récupération du fichier ${fileName}`)
      await pipeline(res.data, iconv.decodeStream('iso-8859-1'), fs.createWriteStream(file))
      await log.info(`Fichier récupéré dans ${file}`)

      // Correction spécifique pour le fichier caracteristiques-2022.csv
      if (fileName === 'caracteristiques-2022.csv') {
        await log.info('Correction de l\'en-tête du fichier caracteristiques-2022.csv')
        const fileContent = await fs.readFile(file, 'utf-8')
        await fs.writeFile(file, fileContent.replace('"Accident_Id"', '"Num_Acc"'), 'utf-8')
        await log.info('En-tête corrigé : Accident_Id → Num_Acc')
      }
    }

    await log.progress('Téléchargement des fichiers', i + 1, urls.length)
  }
}
