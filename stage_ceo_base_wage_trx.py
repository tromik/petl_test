import petl as etl
import psycopg2

env = 'test'

if env == 'test':
    database = 'xyz'
    host = 'xyz'
    user = 'xyz'
    password = 'xyz'
    port = '5432'

# Connect to the database
try:
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port_num)
    print 'Connection successful...'
except psycopg2.OperationalError as e:
    print('Unable to connect!\n{0}').format(e)
    print "I am unable to connect to the database. Host: " + host_name + "  Database: " + database_name
    sys.exit(1)

src_ceo_base_wages = etl.fromdb(lambda: conn.cursor(name='source'),
                                """
                                SELECT * FROM service s
                                where s."Modified_Date" = '2016-11-08';
                                """)
