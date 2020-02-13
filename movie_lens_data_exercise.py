#!/usr/bin/env python

import pandas
import logging


INPUT_RATINGS_FILE = './input/ratings.dat'
OUTPUT_USER_RATINGS = './output/A_user_ratings.csv'
INPUT_GENRES_FILE = './input/movies.dat'
OUTPUT_GENRES_FILE = './output/B_genre_groupings.csv'
OUTPUT_TOP_100_MOVIES = './output/C_top_100_movies.parquet'


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s -- %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    )
logger = logging.getLogger(__name__)


def read_src_file(input_csv):
    """Reads CSV file into a Pandas dataframe object

    :return Dataframe
    """
    logger.info(f'Reading into dataframe from file: {input_csv} ...')
    df = pandas.read_csv(
        filepath_or_buffer=input_csv,
        delimiter='::',
        compression=None,
        header=None,
        engine='python'
    )
    return df


def process_user_ratings(df_obj):
    """Produces list of users with the number or movies they rated and the average rating for each user

    :return Dataframe
    """
    logger.info('Processing user ratings ...')
    df_obj.columns = ['UserID', 'MovieID', 'Rating', 'Timestamp']
    df_user = df_obj.groupby('UserID').agg({'Rating': ['count', 'mean']})
    df_user = df_user.reset_index()
    df_user.columns = ['UserID', 'No. Of Movies Rated', 'Average Rating']
    return df_user


def process_movies_genres(df_obj):
    """Produces a list of unique movie genres and the number of movies associated to that genre.

    :return: Dataframe
    """
    logger.info('Processing movie genres ...')
    df_obj.columns = ['MovieID', 'Title', 'Genres']

    # The genres column consists of values stored as for example "Animation|Children's|Comedy",
    # split the genres column into separate columns and store them into a dataframe
    df_obj = df_obj['Genres'].str.split('|', expand=True)

    # get all the genre values into a single array-like object by using flatten
    df_genres = pandas.DataFrame({'Genres': df_obj.values.flatten()})
    df_genres = df_genres.groupby('Genres').agg({'Genres': ['count']})
    df_genres = df_genres.reset_index()
    df_genres.columns = ['Genres', 'No.of Movies']
    return df_genres


def process_top_100_movies(ratings_df, genres_df):
    """Generates the top 100 movies based on the average user rating.
    And ranks the movies from highest to lowest.

    :return Dataframe
    """
    logger.info('Processing top 100 movies ...')
    ratings_df.columns = ['UserID', 'MovieID', 'Rating', 'Timestamp']
    genres_df.columns = ['MovieID', 'Title', 'Genres']

    # merge the 2 dataframes that contain data on the ratings and the corresponding movie ID and title,
    # based on the common column between the 2 - MovieID
    df_obj = pandas.merge(ratings_df, genres_df, on='MovieID', how='inner')

    df_obj = df_obj[['MovieID', 'Title', 'Rating']]                 # select the 3 columns required
    df_obj = df_obj.groupby(['MovieID', 'Title']).agg({'Rating': ['mean']})
    df_obj = df_obj.reset_index()
    df_obj.columns = ['MovieID', 'Title', 'Average Rating']
    df_obj['Rank'] = df_obj['Average Rating'].rank(ascending=False, method='dense').astype(int)
    df_obj = df_obj.sort_values(by='Rank', ascending=True).reset_index(drop=True)
    df_obj = df_obj[['Rank', 'MovieID', 'Title', 'Average Rating']]

    # NOTE: movies with the same average rating will have the same rank

    return df_obj.iloc[0:100]


def write_df_to_csv(df_obj, file_path):
    """Writes the contents to a dataframe object to a CSV file.

    :return None
    """
    logger.info(f'Output written to: {file_path}')
    df_obj.to_csv(file_path, index=False, encoding='utf-8', sep=',')
    return None


def write_df_to_parquet(df_obj, file_path):
    """Writes the contents of a dataframe object to a Parquet file.

    :return None
    """
    logger.info(f'Output written to: {file_path}')
    df_obj.to_parquet(file_path, compression=None, engine='pyarrow')
    return None


def main():
    # Q1)
    df_user_ratings_raw = read_src_file(INPUT_RATINGS_FILE)
    df_users = process_user_ratings(df_user_ratings_raw)
    write_df_to_csv(df_users, OUTPUT_USER_RATINGS)

    # Q2)
    df_genres_raw = read_src_file(INPUT_GENRES_FILE)
    df_genres = process_movies_genres(df_genres_raw)
    write_df_to_csv(df_genres, OUTPUT_GENRES_FILE)

    # Q3
    df_top_100_movies = process_top_100_movies(ratings_df=df_user_ratings_raw, genres_df=df_genres_raw)
    write_df_to_parquet(df_top_100_movies, OUTPUT_TOP_100_MOVIES)


if __name__ == '__main__':
    main()
