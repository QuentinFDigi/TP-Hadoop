# imports
import sys
import csv
import socket
import logging

# Config du logging
logging.basicConfig(
     filename='/datavolume1/TP/mapper.log',
     level=logging.INFO,
     format='%(asctime)s - %(levelname)s - %(message)s'
)

# Contexte
logging.info("=== CONTEXTE D'EXECUTION ===")
logging.info("Hostname: %s", socket.gethostname())
logging.info("============================")

# lire le csv
reader = csv.reader(sys.stdin, quotechar='"')

# Ignorer l'en-tete
header = next(reader, None)

# boucler sur les donnees
for line_numer, line in enumerate(reader):

    # Verification du nombre de colonnes
    if len(line) != 25:
            logging.warning("Ligne %d invalide (colonnes=%d)", line_numer, len(line))
            continue

    # extraire uniquement les champs souhaite
    try:
        danceability = int(line[17])
        energy = int(line[19])
        streams = int(line[8])
    except ValueError:
        logging.warning("Conversion invalide: danceability=%i energy=%i streams=%i",danceability, energy, streams)
        continue

    # afficher les champs
    print('%i\t%i\t%i' % (danceability, energy, streams))