import petl as etl
import sys
import psycopg2
import pdb; pdb.set_trace()
from connections import *

# Setup connection
env = 'dev'
conn = 'siglos'
conn = set_connections(env, conn)

host = conn[0]
database = conn[1]
user = conn[2]
password = conn[3]
port = conn[4]

# Connect to the database
try:
    src_siglos_conn = psycopg2.connect(database=database, user=user,
                        password=password, host=host, port=port)
    print 'Connection successful...'
except psycopg2.OperationalError as e:
    print('Unable to connect!\n{0}').format(e)
    print "I am unable to connect to the database. Host: " + host + "  Database: " + database
    sys.exit(1)

src_siglos_exchange_rates = etl.fromdb(src_siglos_conn,
                            """
                            SELECT
                            cur.id,
                            cur.title,
                            cval.rate AS usd_raw,
                            cval.rate_aud AS aud_raw,
                            cval.rate_gbp AS gbp_raw,
                            cval.rate_cad AS cad_raw,
                            cval.id AS val_id,
                            cval.validity_start,
                            cval.validity_end,
                            cval.rate,
                            cval.rate_aud,
                            cval.rate_gbp,
                            cval.rate_cad,
                            cval.created,
                            cval.updated
                            FROM currencies cur
                            JOIN currency_validities cval
                                ON (cur.id = cval.currency_id)
                            --WHERE cval.validity_start = date_trunc('month', current_date) + '1month'
                            WHERE cval.validity_start = '2016-12-01'
                            AND cval.updated > '2013-08-30 18:25:02.599015'
                            """)

for row in src_siglos_exchange_rates:
    print row['cur.id']
