import fs from 'fs-extra'
import path from 'path'
import csv from 'csv-parser'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'

export type Usager = Record<string, string | number>
export interface Vehicule {
  catv?: string
  obs?: string
  obsm?: string
  choc?: string
  manv?: string
  id?: string
  usagers?: Usager[]
  pietons?: Usager[]
  [key: string]: unknown
}
export interface Accident {
  date: string
  vehicules: Record<string, Vehicule>
  [key: string]: unknown
}

const JOURS = ['dimanche', 'lundi', 'mardi', 'mercredi', 'jeudi', 'vendredi', 'samedi']
const MOIS = ['janvier', 'février', 'mars', 'avril', 'mai', 'juin', 'juillet', 'août', 'septembre', 'octobre', 'novembre', 'décembre']

const keyCaracteristiques = new Set(['Num_Acc', 'an', 'mois', 'jour', 'hrmn', 'col', 'agg', 'dep', 'com', 'lat', 'long', 'int', 'atm', 'lum'])
const keyLieux = new Set(['catr', 'circ', 'nbv', 'prof', 'plan', 'lartpc', 'larrout', 'surf', 'infra', 'situ'])
const keyUsagers = new Set(['grav', 'sexe', 'an_nais', 'trajet', 'secu', 'secu1', 'secu2', 'secu3', 'locp'])
const keyVehicules = new Set(['catv', 'obs', 'obsm', 'choc', 'manv', 'num_veh'])

function equipementSecu (line: string): string {
  const [secu1, secu2, secu3] = line.split('/')
  const equipements = []
  let compt = 0
  for (let secu of [secu1, secu2, secu3]) {
    secu = secu.trim()
    if (secu !== '-1' && secu !== '0' && (secu !== '8' || compt === 2)) {
      equipements.push(secu)
    }
    compt++
  }
  return equipements.join('/')
}

export default async (year: number, dir: string, log: ProcessingContext<ProcessingConfig>['log']): Promise<Accident[]> => {
  const accidents: Record<string, Accident> = {}
  const separateur = ';'
  const readStreamC = fs.createReadStream(path.join(dir, `caracteristiques-${year}.csv`))
  const csvParserC = csv({ separator: separateur })

  for await (const row of readStreamC.pipe(csvParserC) as AsyncIterable<Record<string, string>>) {
    if (row.Num_Acc !== undefined) {
      accidents[row.Num_Acc] = accidents[row.Num_Acc] || ({} as Accident)
      accidents[row.Num_Acc].date = row.an + '-' + row.mois.padStart(2, '0') + '-' + row.jour.padStart(2, '0')
      const mDate = new Date(accidents[row.Num_Acc].date)
      row.jour = JOURS[mDate.getDay()]
      row.mois = MOIS[mDate.getMonth()]
      for (const [key, value] of Object.entries(row)) {
        if (keyCaracteristiques.has(key)) {
          accidents[row.Num_Acc][key] = value
        }
      }
    }
  }
  const readStreamL = fs.createReadStream(path.join(dir, `lieux-${year}.csv`))
  const csvParserL = csv({ separator: separateur })
  for await (const row of readStreamL.pipe(csvParserL) as AsyncIterable<Record<string, string>>) {
    if (row.Num_Acc !== undefined) {
      for (const [key, value] of Object.entries(row)) {
        if (keyLieux.has(key)) {
          accidents[row.Num_Acc][key] = value
        }
      }
    }
  }
  const readStreamV = fs.createReadStream(path.join(dir, `vehicules-${year}.csv`))
  const csvParserV = csv({ separator: separateur })
  for await (const row of readStreamV.pipe(csvParserV) as AsyncIterable<Record<string, string>>) {
    if (row.Num_Acc !== undefined) {
      accidents[row.Num_Acc].vehicules = accidents[row.Num_Acc].vehicules || {}
      const v: Vehicule = accidents[row.Num_Acc].vehicules[row.num_veh] = accidents[row.Num_Acc].vehicules[row.num_veh] || {}
      for (const [key, value] of Object.entries(row)) {
        if (keyVehicules.has(key)) {
          v[key] = value
        }
      }
      v.id = row.Num_Acc + row.num_veh
      accidents[row.Num_Acc].vehicules[row.num_veh] = v
    }
  }
  const readStreamU = fs.createReadStream(path.join(dir, `usagers-${year}.csv`))
  const csvParserU = csv({ separator: separateur })
  for await (const row of readStreamU.pipe(csvParserU) as AsyncIterable<Record<string, string>>) {
    if (row.Num_Acc !== undefined) {
      const v: Vehicule = accidents[row.Num_Acc].vehicules[row.num_veh] || {}
      v.usagers = v.usagers || []
      v.pietons = v.pietons || []
      const u: Usager = {}
      for (const [key, value] of Object.entries(row)) {
        if (keyUsagers.has(key)) {
          u[key] = value
        }
      }
      u.age = Number(accidents[row.Num_Acc].date.substring(0, 4)) - Number(row.an_nais)
      u.equipement = `${u.secu1}/${u.secu2}/${u.secu3}`
      const equipement = u.equipement
      u.equipement = equipementSecu(equipement)
      const containsValue = (u.equipement !== '')
      u.secuexist = (containsValue) ? 1 : 2
      // a non-pedestrian user has locp "0" until 2017, "" in 2018 and " -1" since 2019
      if (!['', '0', '-1'].includes(row.locp.trim())) {
        v.pietons.push(u)
      } else {
        v.usagers.push(u)
      }
      accidents[row.Num_Acc].vehicules[row.num_veh] = v
    }
  }
  const accidentsVelos = Object.values(accidents).filter(accident => {
    return Object.values(accident.vehicules).find(v => v.catv === '1')
  })
  return accidentsVelos
}
