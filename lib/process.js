const fs = require('fs-extra')
const path = require('path')
const processFormat2018 = require('./processFormat2018')// Pour traiter les fichiers de 2005 à 2018
const processFormat2021 = require('./processFormat2021')// Pour traiter les fichiers de 2019 à 2021

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

const typevehicules = {
  10: 'VU seul 1,5T <= PTAC <= 3,5T avec ou sans remorque',
  11: 'VU (10) + caravane',
  12: 'VU (10) + remorque',
  13: 'PL seul 3,5T <PTCA <= 7,5T',
  14: 'PL seul > 7,5T',
  15: 'PL > 3,5T + remorque',
  16: 'Tracteur routier seul',
  17: 'Tracteur routier + semi-remorque',
  18: 'Transport en commun',
  19: 'Tramway',
  20: 'Engin spécial',
  21: 'Tracteur agricole',
  30: 'Scooter < 50 cm3',
  31: 'Motocyclette > 50 cm3 et <= 125 cm3',
  32: 'Scooter > 50 cm3 et <= 125 cm3',
  33: 'Motocyclette > 125 cm3',
  34: 'Scooter > 125 cm3',
  35: 'Quad léger <= 50 cm3 (Quadricycle à moteur non carrossé)',
  36: 'Quad lourd > 50 cm3 (Quadricycle à moteur non carrossé)',
  37: 'Autobus',
  38: 'Autocar',
  39: 'Train',
  40: 'Tramway',
  41: '3RM <= 50 cm3',
  42: '3RM > 50 cm3 et <= 125 cm3',
  43: '3RM > 125 cm3',
  50: 'EDP à moteur',
  60: 'EDP sans moteur',
  80: 'VAE',
  99: 'Autre véhicule',
  0: 'Indeterminable',
  1: 'Bicyclette',
  2: 'Cyclomoteur <50cm3',
  3: 'Voiturette (Quadricycle à moteur carrossé) ',
  4: 'Scooter immatriculé',
  5: 'Motocyclette',
  6: 'Side car',
  7: 'Vl seul',
  8: 'Vl + caravane',
  9: 'Vl + remorque'
}
const parseVehicules = (vehicules) => {
  let vehiculesParsed = ''
  const vehiculesTab = vehicules.split(',')
  vehiculesTab.forEach(vehicule => {
    vehiculesParsed += typevehicules[vehicule] + ','
  })
  // remove last comma
  vehiculesParsed = vehiculesParsed.slice(0, -1)
  return vehiculesParsed
}

const manoeuvehicules = {
  0: 'Inconnue',
  1: 'Sans changement de direction',
  2: 'Même sens, même file',
  3: 'Entre 2 files',
  4: 'En marche arrière',
  5: 'A contresens',
  6: 'En franchissant le terre-plein central',
  7: 'Dans le couloir bus, dans le me sens',
  8: 'Dans le couloir bus, dans le sens inverse',
  9: "En s'insérant",
  10: 'En faisant demi-tour sur la chaussée',
  11: 'Changeant de file à gauche',
  12: 'Changeant de file à droite',
  13: 'Déporté à gauche',
  14: 'Déporté à droite',
  15: 'Tournant à gauche',
  16: 'Tournant à droite',
  17: 'Dépassant à gauche',
  18: 'Dépassant à droite',
  19: 'Traversant la chaussée',
  20: 'Manoeuvre de stationnement',
  21: "Manoeuvre d'évitemment",
  22: 'Ouverture de porte',
  23: 'Arrêté (horstationnement)',
  24: 'En stationnement (avec occupants)',
  25: 'Circulant sur troitoir',
  26: 'Autres manoeuvres',
  '-1': 'Non renseigné'
}
const parseManoeuvehicules = (manoeuvres) => {
  let manoeuvehiculesParsed = ''
  const manoeuvehiculesTab = manoeuvres.split(',')
  manoeuvehiculesTab.forEach(manoeuvehicule => {
    manoeuvehiculesParsed += manoeuvehicules[manoeuvehicule.replace(' ', '')] + ','
  })
  return manoeuvehiculesParsed
}

const years = [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2016, 2017, 2018, 2019, 2020, 2021]
module.exports = async (processingConfig, dir, log) => {
  const writeStream = fs.createWriteStream(path.join(dir, 'accidentsVelo.csv'), { flags: 'w' })
  writeStream.write(Object.keys(fields).join(',') + '\n')
  try {
    for (const year of years) {
      const accidentsVelos = (year < 2019) ? await processFormat2018(year, dir, log) : await processFormat2021(year, dir, log) // change her for a new format
      const usagersVelo = []
      accidentsVelos.forEach(accident => {
        Object.values(accident.vehicules).forEach((vehicule, i, vehicules) => {
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

      Object.values(usagersVelo).forEach(accident => {
        if (accident.typevehicules !== undefined && accident.typevehicules.length > 2) {
          accident.typevehicules = parseVehicules(accident.typevehicules)
        }
        if (accident.manoeuvehicules !== undefined && accident.manoeuvehicules.length > 2) {
          accident.manoeuvehicules = parseManoeuvehicules(accident.manoeuvehicules)
        }
        if (processingConfig.filter === undefined) {
          writeStream.write(Object.keys(fields).map(f => accident[f] ? `"${accident[f]}"` : '').join(',') + '\n')
        } else {
          if (accident.dep === processingConfig.filter) {
            writeStream.write(Object.keys(fields).map(f => accident[f] ? `"${accident[f]}"` : '').join(',') + '\n')
          }
        }
      })
    }
    writeStream.end()
  } catch (err) {
    console.error(err)
  }
}
