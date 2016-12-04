import petl as etl
import sys
import psycopg2
from connections import *

# Setup connection
env = 'dev'
conn = 'datamart'
conn = set_connections(env, conn)

host = conn[0]
database = conn[1]
user = conn[2]
password = conn[3]
port = conn[4]

# Connect to the database
try:
    src_datamart_conn = psycopg2.connect(database=database, user=user,
                        password=password, host=host, port=port)
    print 'Connection successful...'
except psycopg2.OperationalError as e:
    print('Unable to connect!\n{0}').format(e)
    print "I am unable to connect to the database. Host: " + host + "  Database: " + database
    sys.exit(1)

# Construct source query, retrieve customers from services, airfare, refunds, payments, and customer changes
src_query = """
            SELECT b."Customer_Code", b."Customer_Class", --c.*, ci.*
            CASE WHEN (b."Selling_Currency" = 'CAD') THEN 'Canada' ELSE 'Barbados' END AS company
            FROM airfare a
            JOIN public.service s ON a."ServiceID" = s."ServiceID"
            JOIN public.booking b ON s."BookingNo" = b."BookingNo"
            LEFT JOIN public.contact c ON c.id = to_number(substring(b."Customer_Code" FROM 4 for 8), '99999999')
            LEFT JOIN gp_contact_ids ci ON (c.id = ci.contact_id
            				AND b."Selling_Currency" = ci.currency
            				AND ci.company = CASE WHEN (b."Selling_Currency" = 'CAD') THEN 'Canada' ELSE 'BARBADOS' END)
            WHERE b."Customer_Code" ~ 'BD-'
            AND a."Last_Modified" >= '2016-11-15'

            UNION

            SELECT b."Customer_Code", b."Customer_Class", --c.*, ci.*
            CASE WHEN (b."Selling_Currency" = 'CAD') THEN 'Canada' ELSE 'Barbados' END AS company
            FROM service s
            JOIN public.booking b ON s."BookingNo" = b."BookingNo"
            LEFT JOIN public.contact c ON c.id = to_number(substring(b."Customer_Code" FROM 4 for 8), '99999999')
            LEFT JOIN gp_contact_ids ci ON (c.id = ci.contact_id
            				AND b."Selling_Currency" = ci.currency
            				AND ci.company = CASE WHEN (b."Selling_Currency" = 'CAD') THEN 'Canada' ELSE 'BARBADOS' END)
            WHERE b."Customer_Code" ~ 'BD-'
            AND s."Modified_Date" >= '2016-11-15'

            UNION

            SELECT b."Customer_Code", b."Customer_Class", --c.*, ci.*
            CASE WHEN (b."Selling_Currency" = 'CAD') THEN 'Canada' ELSE 'Barbados' END AS company
            FROM payment p
            JOIN booking b ON p."BookingId" = b."BookingNo"
            LEFT JOIN contact c ON c.id = to_number(substring(b."Customer_Code" FROM 4 for 8), '99999999')
            LEFT JOIN gp_contact_ids ci ON (c.id = ci.contact_id
            				AND b."Selling_Currency" = ci.currency
            				AND ci.company = CASE WHEN (b."Selling_Currency" = 'CAD') THEN 'Canada' ELSE 'BARBADOS' END)
            WHERE b."Customer_Code" ~ 'BD-'
            AND p."Modified_Date" >= '2016-11-15'

            UNION

            SELECT b."Customer_Code", b."Customer_Class", --c.*, ci.*
            CASE WHEN (b."Selling_Currency" = 'CAD') THEN 'Canada' ELSE 'Barbados' END AS company
            FROM refund r
            JOIN booking b ON r."BookingNo" = b."BookingNo"
            LEFT JOIN contact c ON c.id = to_number(substring(b."Customer_Code" FROM 4 for 8), '99999999')
            LEFT JOIN gp_contact_ids ci ON (c.id = ci.contact_id
            				AND b."Selling_Currency" = ci.currency
            				AND ci.company = CASE WHEN (b."Selling_Currency" = 'CAD') THEN 'Canada' ELSE 'BARBADOS' END)
            WHERE b."Customer_Code" ~ 'BD-'
            AND r."Modified_Date" >= '2016-11-15'

            UNION

            SELECT b."Customer_Code", b."Customer_Class", --c.*, ci.*
            CASE WHEN (b."Selling_Currency" = 'CAD') THEN 'Canada' ELSE 'Barbados' END AS company
            FROM customer_changes cc
            JOIN public.booking b ON cc."BookingNo" = b."BookingNo"
            LEFT JOIN public.contact c ON c.id = to_number(substring(b."Customer_Code" FROM 4 for 8), '99999999')
            LEFT JOIN gp_contact_ids ci ON (c.id = ci.contact_id
            				AND b."Selling_Currency" = ci.currency
            				AND ci.company = CASE WHEN (b."Selling_Currency" = 'CAD') THEN 'Canada' ELSE 'BARBADOS' END)
            WHERE b."Customer_Code" ~ 'BD-'
            AND cc."Date_Changed" >= '2016-11-15'
            order by 1, 2, 3
            """

src_datamart_customers = etl.records(etl.fromdb(src_datamart_conn, src_query))
# src_datamart_customers = etl.records(src_datamart_customers)

for row in src_datamart_customers:
    print row['Customer_Code'] + ' | ' + row['Customer_Class'] + ' | ' + row['company']
