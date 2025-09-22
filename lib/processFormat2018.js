const fs = require('fs-extra')
const path = require('path')
const csv = require('csv-parser')

const keyCaracteristiques = new Set(['Num_Acc', 'an', 'mois', 'jour', 'hrmn', 'col', 'agg', 'dep', 'com', 'lat', 'long', 'int', 'atm', 'lum'])
const keyLieux = new Set(['catr', 'circ', 'nbv', 'prof', 'plan', 'lartpc', 'larrout', 'surf', 'infra', 'situ'])
const keyUsagers = new Set(['grav', 'sexe', 'an_nais', 'trajet', 'secu', 'secu1', 'secu2', 'secu3', 'locp'])
const keyVehicules = new Set(['catv', 'obs', 'obsm', 'choc', 'manv', 'num_veh'])
function codeDep (code) {
  if (code.length === 2) { code = '0' + code }
  let c = code.padStart(3, '0')
  if (code === '201') return '2A'
  if (code === '202') return '2B'

  if (c.charAt(2) === '0') c = c.substring(0, 2)
  return c
}

function codeCom (dep, com) {
  const d = (dep.length === 3) ? codeDep(dep) : dep
  return d + com.padStart(3, '0').substring(d.length - 2)
}

function latSign (code) {
  if (code === 'R' || code === 'Y') return '-'
  return ''
}

function lonSign (code) {
  if (code === 'A' || code === 'G') return '-'
  return ''
}

module.exports = async (year, dir, log) => {
  const accidents = {}
  const separateur = ','
  const readStreamC = fs.createReadStream(path.join(dir, `caracteristiques-${year}.csv`))
  const csvParserC = csv({ columns: true, separator: (year === 2009) ? '\t' : separateur })

  for await (const row of readStreamC.pipe(csvParserC)) {
    if (row.Num_Acc !== undefined) {
      accidents[row.Num_Acc] = accidents[row.Num_Acc] || {}
      row.dep = codeDep(row.dep)
      row.com = codeCom(row.dep, row.com)
      row.an = '20' + row.an.padStart(2, '0')
      accidents[row.Num_Acc].date = row.an + '-' + row.mois.padStart(2, '0') + '-' + row.jour.padStart(2, '0')
      const mDate = new Date(accidents[row.Num_Acc].date)
      row.jour = mDate.toLocaleDateString('fr-FR', { weekday: 'long' })// very slow
      row.mois = mDate.toLocaleDateString('fr-FR', { month: 'long' })
      if (row.hrmn && row.hrmn.trim() !== '') {
        const hrmn = row.hrmn.trim()
        if (hrmn.length <= 2) row.hrmn = hrmn + ':00' // "9" -> "9:00", "15" -> "15:00"
        else { // "154" -> "1:54", "1530" -> "15:30"
          const padded = hrmn.padStart(4, '0')
          row.hrmn = padded.substring(0, 2) + ':' + padded.substring(2)
        }
      } else {
        row.hrmn = ''
      }
      row.lat = latSign(row.gps) + Number(row.lat) / 100000
      row.lat.replace(',', '.')
      row.long = lonSign(row.gps) + Number(row.long) / 100000
      row.long.replace(',', '.')
      for (const [key, value] of Object.entries(row)) {
        if (keyCaracteristiques.has(key)) {
          accidents[row.Num_Acc][key] = value
        }
      }
    }
  }
  const readStreamL = fs.createReadStream(path.join(dir, `lieux-${year}.csv`))
  const csvParserL = csv({ columns: true, separator: separateur })
  for await (const row of readStreamL.pipe(csvParserL)) {
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
  const csvParserV = csv({ columns: true, separator: separateur })
  for await (const row of readStreamV.pipe(csvParserV)) {
    if (row.Num_Acc !== undefined) {
      accidents[row.Num_Acc].vehicules = accidents[row.Num_Acc].vehicules || {}
      const v = accidents[row.Num_Acc].vehicules[row.num_veh] = accidents[row.Num_Acc].vehicules[row.num_veh] || {}
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
  const csvParserU = csv({ columns: true, separator: separateur })
  for await (const row of readStreamU.pipe(csvParserU)) {
    if (row.Num_Acc !== undefined) {
      const v = accidents[row.Num_Acc].vehicules[row.num_veh] || {}
      v.usagers = v.usagers || []
      v.pietons = v.pietons || []
      const u = {}

      for (const [key, value] of Object.entries(row)) {
        if (keyUsagers.has(key)) {
          u[key] = value
        }
      }
      u.age = Number(accidents[row.Num_Acc].date.substring(0, 4)) - Number(row.an_nais)
      u.equipement = (u.secu[0] === undefined) ? 8 : u.secu[0]
      if (u.secu !== '00') {
        u.secuexist = (u.secu[1]) ? u.secu[1] : 3
      } else {
        u.secuexist = 3
      }

      if (row.locp !== '0') {
        v.pietons.push(u)
      } else {
        if (!v) log.warning('missing vehicule for', row.num_veh, ', accident has vehicules:', Object.keys(accidents[row.Num_Acc].vehicules).join(', '))
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
