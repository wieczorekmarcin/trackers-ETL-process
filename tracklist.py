import csv
import string
import psycopg2
import time

data_listens = "resources/triplets_sample_20p.txt"
data_tracks = "resources/unique_tracks.txt"

query_most_popular_artist = "SELECT track_artist, sum(listen) as total FROM tracks GROUP BY track_artist ORDER BY sum(listen) DESC LIMIT 1;"
query_most_popular_songs = "SELECT track_name, listen FROM tracks ORDER BY listen DESC LIMIT 5;"


def prepareFile(file_path, shouldFilterNonAscii):
    print("Preparing to process file: " + file_path)
    fin = open(file_path, "rt", encoding='latin-1')
    data = fin.read()
    data = data.replace('<SEP>', '\t')
    data = data.replace('\'', '\'\'')

    if shouldFilterNonAscii:
        print("Filtering Non-ASCII characters: " + file_path)
        filtered_string = filter(lambda x: x in string.printable, data)
        data = ''.join(filtered_string)

    fin = open(file_path, "wt")
    fin.write(data)
    fin.close()


def toList(file_path):
    f = open(file_path, 'rt')
    reader = csv.reader(f, delimiter='\t')
    print("Converting to list: " + file_path)
    data = [row for row in reader]
    f.close()

    return data


def reduceData(data):
    print("Reducing data...")
    result = {}
    for i in data:
        if i[1] in result:
            result[i[1]] += 1
        else:
            result[i[1]] = 0
    return result


def connect():
    print("Conncting to database...")
    connection = psycopg2.connect(user="python_lab",
                                  password="python_lab",
                                  host="localhost",
                                  port="5432",
                                  database="python_lab")

    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("Connected to database: ", record, "\n")

    return connection


def createTable(tracks, listens):
    connection = connect()
    cursor = connection.cursor()
    print("Creating table in database...")
    cursor.execute('DROP TABLE IF EXISTS tracks');
    cursor.execute("CREATE TABLE tracks(id SERIAL PRIMARY KEY,"
                   "artist_id TEXT,"
                   "track_id TEXT,"
                   "track_artist TEXT,"
                   "track_name TEXT,"
                   "listen INTEGER);")

    print("Inserting tracks...")
    for track in tracks:
        trackId = track[1]
        if trackId in listens:
            limit = str(listens.get(trackId))
        else:
            limit = "0"
        query = "INSERT INTO tracks (artist_id, track_id, track_artist, track_name, listen) VALUES (" \
                "'" + track[0] + "', '" + track[1] + "', '" + track[2] + "', '" + track[3] + "', " + limit + ");"
        cursor.execute(query)
    connection.commit()


def executeQuery(query):
    connection = connect()
    cursor = connection.cursor()
    print("Executing query: " + query)
    cursor.execute(query)
    result = cursor.fetchall()

    counter = 1
    for row in result:
        print(counter, "place: " + row[0] + " with ", row[1], " listens.")
        counter += 1
    print("\n")
    print("___________________________________________________________________________________________________________")
    print("\n")


def main():
    program_tic = time.clock()
    print("Starting program...")

    prepare_tic = time.clock()
    prepareFile(data_tracks, True)
    prepareFile(data_listens, False)

    tracks = toList(data_tracks)
    listens = toList(data_listens)
    reduced_listens = reduceData(listens)
    prepare_toc = time.clock()
    print("Files was prepared in: ", prepare_toc - prepare_tic, "s.", "\n")

    sql_tic = time.clock()
    createTable(tracks, reduced_listens)
    executeQuery(query_most_popular_artist)
    executeQuery(query_most_popular_songs)
    sql_toc = time.clock()
    print("SQL operations took: ", sql_toc - sql_tic, "s.", "\n")

    program_toc = time.clock()
    print("Program processed in: ", program_toc - program_tic, "s.")


if __name__ == '__main__':
    main()
