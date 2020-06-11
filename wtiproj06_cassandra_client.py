from cassandra.cluster import Cluster
from cassandra.query import dict_factory

from wtiproj03_ETL import lab4JoinTables

KEYSPACE = "ratings_keyspace"
USER_TABLE = "user_rated_movies"
AVG_RATINGS = "avg_genre_ratings_for_user"


class Cass:

    def __init__(self):
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect()
        self.session.row_factory = dict_factory
        self.create_keyspace(KEYSPACE)
        self.session.set_keyspace(KEYSPACE)
        self.genres, self.merged_table = lab4JoinTables()

    def create_user_rated_movies_table(self):
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS """ + KEYSPACE + """.""" + USER_TABLE + """ (
            user_id int,
            movie_id int,
            rating float,
            genre text,
            PRIMARY KEY(user_id, movie_id)
            )
            """)

    def push_data_user_rated_table(self, user_id, movie_id, rating, genre):
        self.session.execute(
            """
            INSERT INTO """ + KEYSPACE + """.""" + USER_TABLE + """ (user_id, movie_id, rating, genre)
            VALUES (%(user_id)s, %(movie_id)s, %(rating)s, %(genre)s)
            """,
            {
                'user_id': user_id,
                'movie_id': movie_id,
                'rating': rating,
                'genre': genre
            }
        )

    def push_data_average_rating_table(self, user_id, data):
        self.session.execute(
            """
            INSERT INTO """ + KEYSPACE + """.""" + AVG_RATINGS + """(user_id, genre_Action, genre_Adventure, 
            genre_Animation,genre_Children,genre_Comedy, genre_Crime, genre_Documentary, genre_Drama, genre_Fantasy,
            genre_Film_Noir, genre_Horror,genre_IMAX, genre_Musical, genre_Mystery, genre_Romance, genre_Sci_Fi, 
            genre_Thriller,genre_War,genre_Western) 
            VALUES (%(user_id)s, %(genre_Action)s, %(genre_Adventure)s, %(genre_Animation)s,%(genre_Children)s,%(genre_Comedy)s,
            %(genre_Crime)s, %(genre_Documentary)s,
            %(genre_Drama)s,  %(genre_Fantasy)s, %(genre_Film_Noir)s,  %(genre_Horror)s, %(genre_IMAX)s,  %(genre_Musical)s, 
            %(genre_Mystery)s,  %(genre_Romance)s,  %(genre_Sci_Fi)s,  %(genre_Thriller)s, %(genre_War)s,
            %(genre_Western)s) """,
            {
                'user_id': user_id,
                'genre_Action': data[0],
                'genre_Adventure': data[1],
                'genre_Animation': data[2],
                'genre_Children': data[3],
                'genre_Comedy': data[4],
                'genre_Crime': data[5],
                'genre_Documentary': data[6],
                'genre_Drama': data[7],
                'genre_Fantasy': data[8],
                'genre_Film_Noir': data[9],
                'genre_Horror': data[10],
                'genre_IMAX': data[11],
                'genre_Musical': data[12],
                'genre_Mystery': data[13],
                'genre_Romance': data[14],
                'genre_Sci_Fi': data[15],
                'genre_Thriller': data[16],
                'genre_War': data[17],
                'genre_Western': data[18]
            }
        )

    def create_average_user_rating_table(self):
        newGenres = [genre.replace("-", "_") for genre in self.genres]
        execute = """
            CREATE TABLE IF NOT EXISTS """ + KEYSPACE + """.""" + AVG_RATINGS + """(
                user_id int,
            """
        for genre in newGenres:
            execute += genre + """ float,"""

        execute += """
            PRIMARY KEY(user_id)
            )    
        """
        self.session.execute(execute)

        return execute

    def create_keyspace(self, keyspace):
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS """ + keyspace + """
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
            """)

    def delete_table(self, keyspace, table):
        self.session.execute("DROP TABLE " + keyspace + "." + table + ";")

    def clear_table(self, keyspace, table):
        self.session.execute("TRUNCATE " + keyspace + "." + table + ";")

    def get_data_table(self, keyspace, table):
        rows = self.session.execute("SELECT * FROM " + keyspace + "." + table + ";")
        return rows

    def get_data_table_byId(self, keyspace, table, userID):
        rows = self.session.execute("SELECT * FROM " + keyspace + "." + table + " WHERE user_id=" + userID + ";")
        return rows
