import mock
import pandas
import pytest
import movie_lens_data_exercise as movieslens


@pytest.fixture()
def example_input_src_file_movie_genres(tmpdir):
    """ Fixture creates a .dat source file with data.

    Returns file path.
    """
    src_file = tmpdir.mkdir('sub').join('example_input_data.dat')
    # MovieID::Title::Genres
    src_file.write(
        "1::Toy Story (1995)::Animation|Children's|Comedy\n"
        "2::Jumanji (1995)::Adventure|Children's|Fantasy\n"
        "3::Grumpier Old Men (1995)::Comedy|Romance\n"
        )
    return str(src_file)


@pytest.fixture()
def example_df_movie_genres():
    data = [
        # MovieID, Title, Genres
        [1, "Toy Story (1995)",   "Animation|Children's|Comedy"],
        [2, "Jumanji (1995)",  "Adventure|Children's|Fantasy"],
        [3, "Grumpier Old Men (1995)", "Comedy|Romance"]
    ]
    df = pandas.DataFrame(data)
    return df


@pytest.fixture()
def example_df_user_ratings():
    data = [
        # UserID, MovieID, Rating, Timestamp (secs since epoch)
        [1, 1, 4, 978302174],
        [1, 2, 4, 978301398],
        [1, 3, 4, 978302091],
        [2, 1, 5, 978298709],
        [2, 2, 3, 978295609],
        [3, 3, 5, 986729870],
        [4, 1, 3, 986782870]
    ]
    df = pandas.DataFrame(data)
    return df


class TestMovieLensExercise(object):

    def test_read_src_file(self, example_input_src_file_movie_genres, example_df_movie_genres):
        outcome_df = movieslens.read_src_file(example_input_src_file_movie_genres)
        assert outcome_df.equals(example_df_movie_genres)

    def test_process_user_ratings(self, example_df_user_ratings):
        outcome_df = movieslens.process_user_ratings(example_df_user_ratings)
        expected_df = pandas.DataFrame(
            [
                [1, 3, 4],
                [2, 2, 4],
                [3, 1, 5],
                [4, 1, 3]
            ],
            columns=['UserID', 'No. Of Movies Rated', 'Average Rating']
        )
        assert outcome_df.equals(expected_df)

    def test_process_movies_genres(self, example_df_movie_genres):
        outcome_df = movieslens.process_movies_genres(example_df_movie_genres)
        expected_df = pandas.DataFrame(
            [
                ["Adventure", 1],
                ["Animation", 1],
                ["Children's", 2],
                ["Comedy", 2],
                ["Fantasy", 1],
                ["Romance", 1]
            ],
            columns=['Genres', 'No.of Movies']
        )
        assert outcome_df.equals(expected_df)

    def test_process_top_100_movies(self, example_df_user_ratings, example_df_movie_genres):
        outcome_df = movieslens.process_top_100_movies(
            ratings_df=example_df_user_ratings, genres_df=example_df_movie_genres
        )
        expected_df = pandas.DataFrame(
            [
                [1, 3, "Grumpier Old Men (1995)", 4.5],
                [2, 1, "Toy Story (1995)", 4],
                [3, 2, "Jumanji (1995)", 3.5]
            ],
            columns=['Rank', 'MovieID', 'Title', 'Average Rating']
        )
        assert outcome_df.equals(expected_df)

    @mock.patch.object(pandas.DataFrame, 'to_csv')
    def test_write_df_to_csv(self, mock_to_csv, tmp_path):
        df = pandas.DataFrame(
            [
                ["Adventure", 1],
                ["Animation", 1],
                ["Children's", 2],
                ["Comedy", 2],
                ["Fantasy", 1],
                ["Romance", 1]
            ],
            columns=['Genres', 'No.of Movies']
        )
        movieslens.write_df_to_csv(df, tmp_path)
        mock_to_csv.assert_called_once()

    @mock.patch.object(pandas.DataFrame, 'to_parquet')
    def test_write_df_to_parquet(self, mock_to_parquet, tmp_path):
        df = pandas.DataFrame(
            [
                [1, 3, "Grumpier Old Men (1995)", 4.5],
                [2, 1, "Toy Story (1995)", 4],
                [3, 2, "Jumanji (1995)", 3.5]
            ],
            columns=['Rank', 'MovieID', 'Title', 'Average Rating']
        )
        movieslens.write_df_to_parquet(df, tmp_path)
        mock_to_parquet.assert_called_once()
