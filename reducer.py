# imports
import sys
import happybase
import socket
import logging

# Config du logging
logging.basicConfig(
     filename='/datavolume1/TP/reducer.log',
     level=logging.INFO,
     format='%(asctime)s - %(levelname)s - %(message)s'
)

IP = '127.0.0.1'
PORT = 9090
TABLE_NAME = 'streams_by_dance_energy'

logging.info("=== CONTEXTE D'EXECUTION ===")
logging.info("Hostname: %s", socket.gethostname())
logging.info("Connexion HBase %s:%s/%s" % (IP, PORT, TABLE_NAME))
logging.info("============================")

# Connexion à HBase
try:
    logging.info("Tentative de connexion %s:%s/%s" % (IP, PORT, TABLE_NAME))
    connection = happybase.Connection('127.0.0.1', 9090)
    logging.info("Entre connexion et open")
    #connection = happybase.Connection('node197707-hadoop-2024-d10b-etudiant01.sh1.hidora.com', 11397)
    connection.open()
    logging.info("Connexion HBase etablie")
except Exception as e:
    logging.warning("Erreur connexion HBase : %s", e)
    sys.exit(1)

# Recuperation de la table
table = connection.table('streams_by_dance_energy')

# Variables pour stocker les donnees
current_key = None
current_sum = 0
current_count = 0
processed_lines = 0
written_keys = 0

# Boucler sur les donnees
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        danceability, energy, streams = line.split("\t")
    except ValueError:
        logging.warning("Split invalide: %r" % (line))
        continue
    
    try:
        danceability = int(danceability)
        energy = int(energy)
        streams = int(streams)
    except ValueError:
        logging.warning("Conversion invalide: danceability=%i energy=%i streams=%i",danceability, energy, streams)
        continue
    
    processed_lines += 1

    # Transformation en cle composite
    key = "%d_%d" % (danceability, energy)

    if key == current_key:
        current_sum += streams
        current_count +=1
    else:
        if current_key is not None:
            average = current_sum / current_count

            # Stockage en HBase
            table.put(
                current_key.encode(),
                {
                    b'data:total_streams': str(current_sum).encode(),
                    b'data:count': str(current_count).encode(),
                    b'data:average_streams': str(round(average, 2)).encode()
                }
            )

            # Sortie Hadoop (HDFS)
            print('%s\t%d\t%d\t%.2f' % (current_key, current_sum, current_count, average))

            logging.info("Cle %s ecrite (sum=%d, count=%d, avg=%.2f", current_key, current_sum, current_count, average)

        current_key = key
        current_sum = streams
        current_count = 1

# Derniere cle
if current_key is not None:
    average = current_sum / current_count

    # Stockage en HBase
    table.put(
        current_key.encode(),
        {
            b'data:total_streams': str(current_sum).encode(),
            b'data:count': str(current_count).encode(),
            b'data:average_streams': str(round(average, 2)).encode()
        }
    )

    # Sortie Hadoop (HDFS)
    print('%s\t%d\t%d\t%.2f' % (current_key, current_sum, current_count, average))

    logging.info("Cle %s ecrite (sum=%d, count=%d, avg=%.2f", current_key, current_sum, current_count, average)

# Fermer la connection
connection.close()
logging.info("Reducer termine")
logging.info("Lignes traitees : %d", processed_lines)