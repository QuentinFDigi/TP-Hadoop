#!/usr/bin/env python3

import happybase
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import socket
import logging

# Configuration du logging
logging.basicConfig(filename='/datavolume1/TP/hbase_to_pdf.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logging.info("=== CONTEXTE D'EXECUTION ===")
logging.info("Hostname: %s", socket.gethostname())
logging.info("============================")

# Connexion Hbase
IP = '127.0.0.1'
PORT = 9090
TABLE_NAME = 'streams_by_dance_energy'

try:
    connection = happybase.Connection(IP, PORT)
    connection.open()
    table = connection.table(TABLE_NAME)
    logging.info("Connexion HBase reussie a %s:%s/%s", IP, PORT, TABLE_NAME)
except Exception as e:
    logging.error("Erreur connexion HBase: %s", e)
    raise

# Lecture des données depuis HBase
data = []

for key, line in table.scan():
    dance_energy = key.decode()
    total_streams = int(line.get(b'data:total_streams', b'0'))
    count = int(line.get(b'data:count', b'0'))
    average = float(line.get(b'data:average_streams', b'0'))
    data.append((dance_energy, total_streams, count, average))

connection.close()
logging.info("Lecture HBase terminee (%d lignes)", len(data))

# Tri et selection du top 10
data_sorted = sorted(data, key=lambda x: x[3], reverse=True)
top_data = data_sorted[:10]

labels = [x[0] for x in top_data]
values = [x[3] for x in top_data]

# Generation du PDF
output_file = "/datavolume1/TP/top_dance_energy.pdf"
with PdfPages(output_file) as pdf:
    plt.figure(figsize=(10,10))
    plt.pie(values, labels=labels,autopct='%1.1f%%',startangle=140)
    plt.title("Top 10 des combinaisons danceability & energy selon la moyenne de streams")
    pdf.savefig()
    plt.close()

logging.info("PDF Pie Chart genere: %s", output_file)