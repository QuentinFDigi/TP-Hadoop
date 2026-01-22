# Imports
import happybase
import logging
import socket

# VIARIABLES
# IP = 'node197707-hadoop-2024-d10b-etudiant01.sh1.hidora.com'
# PORT = 11397
TABLE_NAME = 'streams_by_dance_energy'
COLUMN_FAMILIES = {
    'data': {}
}

# LOGGER
logging.basicConfig(
    filename='/datavolume1/TP/init.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.info("=== CONTEXTE D'EXECUTION ===")
logging.info("Hostname: %s", socket.gethostname())
logging.info("============================")

# Connexion à HBase
connection = happybase.Connection('127.0.0.1', 9090)
connection.open()

try:
    connection.delete_table(TABLE_NAME, disable=True)
except:
    pass

connection.create_table(TABLE_NAME, COLUMN_FAMILIES)

logging.info("Table '%s' created successfully.", TABLE_NAME)

connection.close()