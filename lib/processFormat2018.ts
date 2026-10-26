import fs from 'fs-extra'
import path from 'path'
import csv from 'csv-parser'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import type { Accident, Usager, Vehicule } from './processFormat2021.ts'

const JOURS = ['dimanche', 'lundi', 'mardi', 'mercredi', 'jeudi', 'vendredi', 'samedi']
const MOIS = ['janvier', 'février', 'mars', 'avril', 'mai', 'juin', 'juillet', 'août', 'septembre', 'octobre', 'novembre', 'décembre']

const keyCaracteristiques = new Set(['Num_Acc', 'an', 'mois', 'jour', 'hrmn', 'col', 'agg', 'dep', 'com', 'lat', 'long', 'int', 'atm', 'lum'])
const keyLieux = new Set(['catr', 'circ', 'nbv', 'prof', 'plan', 'lartpc', 'larrout', 'surf', 'infra', 'situ'])
const keyUsagers = new Set(['grav', 'sexe', 'an_nais', 'trajet', 'secu', 'secu1', 'secu2', 'secu3', 'locp'])
const keyVehicules = new Set(['catv', 'obs', 'obsm', 'choc', 'manv', 'num_veh'])
function codeDep (code: string): string {
  if (code.length === 2) { code = '0' + code }
  let c = code.padStart(3, '0')
  if (code === '201') return '2A'
  if (code === '202') return '2B'

  if (c.charAt(2) === '0') c = c.substring(0, 2)
  return c
}

function codeCom (dep: string, com: string): string {
  const d = (dep.length === 3) ? codeDep(dep) : dep
  return d + com.padStart(3, '0').substring(d.length - 2)
}

function latSign (code: string): string {
  if (code === 'R' || code === 'Y') return '-'
  return ''
}

function lonSign (code: string): string {
  if (code === 'A' || code === 'G') return '-'
  return ''
}

export default async (year: number, dir: string, log: ProcessingContext<ProcessingConfig>['log']): Promise<Accident[]> => {
  const accidents: Record<string, Accident> = {}
  const separateur = ','
  const readStreamC = fs.createReadStream(path.join(dir, `caracteristiques-${year}.csv`))
  const csvParserC = csv({ separator: (year === 2009) ? '\t' : separateur })

  for await (const row of readStreamC.pipe(csvParserC) as AsyncIterable<Record<string, string>>) {
    if (row.Num_Acc !== undefined) {
      accidents[row.Num_Acc] = accidents[row.Num_Acc] || ({} as Accident)
      row.dep = codeDep(row.dep)
      row.com = codeCom(row.dep, row.com)
      row.an = '20' + row.an.padStart(2, '0')
      accidents[row.Num_Acc].date = row.an + '-' + row.mois.padStart(2, '0') + '-' + row.jour.padStart(2, '0')
      const mDate = new Date(accidents[row.Num_Acc].date)
      row.jour = JOURS[mDate.getDay()]
      row.mois = MOIS[mDate.getMonth()]
      row.hrmn = row.hrmn.padStart(4, '0').substring(0, 2) + ':' + row.hrmn.padStart(4, '0').substring(2)
      // empty or 0 means not geolocated, keep it empty instead of placing the accident at 0,0
      row.lat = Number(row.lat) ? latSign(row.gps) + Number(row.lat) / 100000 : ''
      row.long = Number(row.long) ? lonSign(row.gps) + Number(row.long) / 100000 : ''
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
      for (let [key, value] of Object.entries(row)) {
        if (keyLieux.has(key)) {
          if ((key === 'circ' || key === 'prof' || key === 'plan' || key === 'surf') && value === '0') {
            value = '-1'
          }
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
      for (let [key, value] of Object.entries(row)) {
        if (keyVehicules.has(key)) {
          if ((key === 'catv' || key === 'manv') && value.charAt(0) === '0') {
            value = value.slice(1)
          }
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
      u.age = Number(row.an_nais) ? Number(accidents[row.Num_Acc].date.substring(0, 4)) - Number(row.an_nais) : ''
      u.equipement = (row.secu[0] === undefined) ? 8 : row.secu[0]
      if (row.secu !== '00') {
        u.secuexist = (row.secu[1]) ? row.secu[1] : 3
      } else {
        u.secuexist = 3
      }

      // a pedestrian is attached to the vehicle that hit them: catu 3 (locp is not reliably filled)
      if (row.catu.trim() === '3') {
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
