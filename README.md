# Data Wrangling Technical Task

As part of the tech task, this Python script is designed to produce 3 output files based on the data from MovieLens.

1) A CSV file containing list of users with the number of movies they rated and the average rating per user.
2) A CSV file containing list of unique Genres and the number of movies under each genre.
3) A Parquet file containing the top 100 movies based on their ratings. 
This should have fields, Rank (1-100), Movie Id, Title, Average Rating. Where Rank 1 is the most popular movie.
## Set up your virtualenv with Python 3.7

Assuming virtualenv is already installed on your system.  
If using a virtualenvwrapper then set up a virtual environment for this project 
eg.
```
mkvirtualenv -p /usr/bin/python3.7 -a [path to project] [virtualenv name]
```
Otherwise set up your virtual environment as you normally would.  

Once your virtualenv is active, from the project root dir install the necessary dependencies

```
pip3 install -r requirements.txt
```

## MovieLens Input Data
This exercise was based off MovieLens' 1M Dataset. It can be downloaded from  http://files.grouplens.org/datasets/movielens/ml-1m.zip
  
Download and extract the data files into the `input` directory, found in the same location as this `README` in the project root dir. 

## Run script
```bash
python movie_lens_data_exercise.py
```

## Run unittests
From the project root ...
```bash 
pytest -v test_movie_lens_data_exercise.py
```

Data set courtesy of  
`F. Maxwell Harper and Joseph A. Konstan. 2015. The MovieLens Datasets: History
and Context. ACM Transactions on Interactive Intelligent Systems (TiiS) 5, 4,
Article 19 (December 2015), 19 pages. DOI=http://dx.doi.org/10.1145/2827872`
