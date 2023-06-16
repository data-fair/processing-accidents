const fs = require('fs-extra')
const path = require('path')
const csv = require('csv-parser')

const keyCaracteristiques = new Set(['Num_Acc', 'an', 'mois', 'jour', 'hrmn', 'col', 'agg', 'dep', 'com', 'lat', 'long', 'int', 'atm', 'lum'])
const keyLieux = new Set(['catr', 'circ', 'nbv', 'prof', 'plan', 'lartpc', 'larrout', 'surf', 'infra', 'situ'])
const keyUsagers = new Set(['grav', 'sexe', 'an_nais', 'trajet', 'secu', 'secu1', 'secu2', 'secu3', 'locp'])
const keyVehicules = new Set(['catv', 'obs', 'obsm', 'choc', 'manv', 'num_veh'])

function equipementSecu (line) {
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

module.exports = async (year, dir, log) => {
  const accidents = {}
  const separateur = ';'
  const readStreamC = fs.createReadStream(path.join(dir, `caracteristiques-${year}.csv`))
  const csvParserC = csv({ columns: true, separator: separateur })

  for await (const row of readStreamC.pipe(csvParserC)) {
    if (row.Num_Acc !== undefined) {
      accidents[row.Num_Acc] = accidents[row.Num_Acc] || {}
      accidents[row.Num_Acc].date = row.an + '-' + row.mois.padStart(2, '0') + '-' + row.jour.padStart(2, '0')
      const mDate = new Date(accidents[row.Num_Acc].date)
      row.jour = mDate.toLocaleDateString('fr-FR', { weekday: 'long' })// very slow
      row.mois = mDate.toLocaleDateString('fr-FR', { month: 'long' })
      row.lat.replace(',', '.')
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
      for (const [key, value] of Object.entries(row)) {
        if (keyLieux.has(key)) {
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
      for (const [key, value] of Object.entries(row)) {
        if (keyVehicules.has(key)) {
          if (keyVehicules.has(key)) {
            v[key] = value
          }
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
      u.equipement = `${u.secu1}/${u.secu2}/${u.secu3}`
      const equipement = u.equipement
      u.equipement = equipementSecu(equipement)
      const containsValue = (u.equipement !== '')
      u.secuexist = (containsValue) ? 1 : 2
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
