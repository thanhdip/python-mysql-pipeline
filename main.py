import mysql.connector
import sys
import csv


column_names = [
    'ticket_id',
    'trans_date',
    'event_id',
    'event_name',
    'event_date',
    'event_type',
    'event_city',
    'customer_id',
    'price',
    'num_tickets'
]


def gen_insert_satement(table):
    stmnt1 = f"INSERT INTO {table} "
    stmnt2 = " VALUES "

    stmnt1 += '('
    stmnt2 += '('
    for name in column_names:
        stmnt1 += f"{name}, "
        stmnt2 += "%s, "
    stmnt1 = stmnt1[:-2]
    stmnt2 = stmnt2[:-2]
    stmnt1 += ')'
    stmnt2 += ')'
    return stmnt1 + stmnt2


def gen_insert_data(data):
    return dict(zip(column_names, data))


def get_db_connection(pwd, user='root', host='localhost', port='3306', database='third_party_sales'):
    connection = None
    try:
        connection = mysql.connector.connect(user=user,
                                             password=pwd,
                                             host=host,
                                             port=port,
                                             database=database)
    except Exception as error:
        print('Error while connecting to database for job tracker', error)

    return connection


def load_third_party(connection, file_path_csv, table='third_party_sales'):
    cursor = connection.cursor()
    # [Iterate through the CSV file and execute insert statement]
    with open(file_path_csv) as csv_file:
        csv_data = csv.reader(csv_file, delimiter=',')
        insert_stmnt = gen_insert_satement(table)
        for r in csv_data:
            print(insert_stmnt)
            print(r)
            cursor.execute(insert_stmnt, r)
    connection.commit()
    cursor.close()
    return


def query_popular_tickets(connection):
    # Get the most popular ticket in the past month
    # WHERE statement would need to change if the data is actually updated
    sql_statement = ('SELECT event_name, COUNT(ticket_id) AS num_tickets '
                     'FROM third_party_sales.third_party_sales '
                     'WHERE month(trans_date) = 8 '
                     'GROUP BY event_name '
                     'ORDER BY num_tickets DESC;')
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return records


def main():
    args = sys.argv[1:]
    print("Connecting to DB...")
    conn = get_db_connection(str(args[0]))
    print("Insert!")
    load_third_party(conn, str(args[1]))
    print("Querying...")
    res = query_popular_tickets(conn)
    print("Here are the most popular tickets in the past month:")
    for r in res:
        print(f'- {r[0]}')
    conn.close()


if __name__ == "__main__":
    main()
