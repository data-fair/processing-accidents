import fs from 'fs-extra'
import path from 'path'
import { json2csv } from 'json-2-csv'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import processFormat2018 from './processFormat2018.ts' // Pour traiter les fichiers de 2005 à 2018
import processFormat2021, { type Vehicule } from './processFormat2021.ts' // Pour traiter les fichiers à partir de 2019

export const OUTPUT_FILE = 'accidentsVelo.csv'

let shouldBeStopped = false
export const setShouldBeStopped = (v: boolean) => { shouldBeStopped = v }
export const isStopped = (): boolean => shouldBeStopped

// fields to keep for the final file in order to have the same output format
const fields = {
  // caracteristiques
  Num_Acc: 'identifiant accident',
  date: 'date',
  an: 'annee',
  mois: 'mois',
  jour: 'jour',
  hrmn: 'heure',
  dep: 'departement', // need improvements
  com: 'commune',
  lat: 'lat',
  long: 'lon',
  agg: 'en agglomeration',
  int: 'type intersection',
  col: 'type collision',
  lum: 'luminosite',
  atm: 'conditions atmosperiques',
  // lieux
  catr: 'type route',
  circ: 'circulation',
  nbv: 'nb voies',
  prof: 'profil long route',
  plan: 'trace plan route',
  lartpc: 'largeur TPC',
  larrout: 'largeur route',
  surf: 'etat surface',
  infra: 'amenagement',
  situ: 'situation',
  // usagers
  grav: 'gravite accident',
  sexe: 'sexe',
  age: 'age',
  trajet: 'motif deplacement',
  secuexist: 'existence securite',
  equipement: 'equipement(s) securite',
  // vehicules
  obs: 'obstacle fixe heurte',
  obsm: 'obstacle mobile heurte',
  choc: 'localisation choc',
  manv: 'manoeuvre avant accident',
  vehiculeid: 'identifiant vehicule',
  typevehicules: 'type autres vehicules',
  manoeuvehicules: 'manoeuvre autres vehicules',
  numVehicules: 'nombre autres vehicules'
}
const fieldKeys = Object.keys(fields)

const parseManoeuvehicules = (manoeuvres: string): string => {
  return manoeuvres.split(',').map(manoeuvehicule => manoeuvehicule.replace(' ', '')).join('/')
}

/** Years to process, read from the downloaded caracteristiques files (one per published year). */
export const listYears = async (dir: string): Promise<number[]> => {
  return (await fs.readdir(dir))
    .map(f => f.match(/^caracteristiques-(\d{4})\.csv$/)?.[1])
    .filter((y): y is string => y !== undefined)
    .map(Number)
    .sort((a, b) => a - b)
}

export default async (processingConfig: ProcessingConfig, dir: string, log: ProcessingContext<ProcessingConfig>['log']): Promise<void> => {
  await log.step('Traitement des données')
  const filePath = path.join(dir, OUTPUT_FILE)
  await fs.writeFile(filePath, fieldKeys.join(',') + '\n')
  // dep is a string code ("01", "2A", "971"), the filter an integer
  const filter = processingConfig.filter ? String(processingConfig.filter).padStart(2, '0') : undefined

  const years = await listYears(dir)
  await log.info(`Années à traiter : ${years[0]} à ${years[years.length - 1]}`)
  for (const year of years) {
    if (shouldBeStopped) return
    const accidentsVelos = (year < 2019) ? await processFormat2018(year, dir, log) : await processFormat2021(year, dir, log) // change her for a new format
    const usagersVelo: Record<string, unknown>[] = []
    accidentsVelos.forEach(accident => {
      Object.values(accident.vehicules).forEach((vehicule: Vehicule, i, vehicules) => {
        if (vehicule.catv === '1') {
          const autreVehicules = vehicules.filter((v, j) => i !== j)
          const autreVehiculesNonVelos = autreVehicules.filter(v => v.catv !== '1')
          if (vehicule.usagers) {
            vehicule.usagers.forEach(usager => {
              usagersVelo.push(Object.assign({
                obs: vehicule.obs,
                obsm: vehicule.obsm,
                choc: vehicule.choc,
                manv: vehicule.manv,
                vehiculeid: vehicule.id,
                typevehicules: autreVehicules.length ? (autreVehiculesNonVelos.length === 0 ? '1' : autreVehiculesNonVelos.map(v => v.catv).filter((v, i, s) => s.indexOf(v) === i).join(', ')) : undefined,
                manoeuvehicules: ((autreVehiculesNonVelos.length === 0) ? autreVehicules.filter(v => v.catv === '1') : autreVehiculesNonVelos).map(v => v.manv).filter((v, i, s) => s.indexOf(v) === i).join(', '),
                numVehicules: (autreVehiculesNonVelos.length === 0) ? autreVehicules.length : autreVehiculesNonVelos.length
              }, usager, accident))
            })
          }
        }
      })
    })
    const accidentWrite: Record<string, unknown>[] = []
    for (const accident of usagersVelo) {
      if (typeof accident.typevehicules === 'string' && accident.typevehicules.length > 2) {
        accident.typevehicules = accident.typevehicules.split(',').join('/')
      }
      if (typeof accident.manoeuvehicules === 'string' && accident.manoeuvehicules.length > 2) {
        accident.manoeuvehicules = parseManoeuvehicules(accident.manoeuvehicules)
      }
      if (filter && accident.dep !== filter) continue
      accidentWrite.push(Object.fromEntries(fieldKeys.map(f => [f, accident[f] || ''])))
    }
    if (accidentWrite.length > 0) {
      await log.info('Ecriture du fichier csv')
      // drop the header line, already written once at the top of the file
      const csv = (await json2csv(accidentWrite)).replace(/^(.*\n)/, '')
      await fs.appendFile(filePath, csv + '\n')
    } else {
      await log.warning(`Aucune donnée récupérée pour ${year}`)
    }
    await log.info(`Accidents velo ${year} terminé`)
  }
}
