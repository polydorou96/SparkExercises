# Requirements
* Install Python 3.11 (https://www.python.org/downloads/release/python-3119/)
* Install Poetry (https://python-poetry.org/docs/)

# Installing dependencies
The repository has been configured to run with python 3.11 and the dependencies can all be handled via poetry’s `poetry.lock` and `pyproject.toml` files that are present in the repository, as long as we set a poetry interpreter to handle our dependencies.

Alternatively, the repository contains a requirements.txt file present in the repository which we can use to install all dependencies from there before running our script by calling `pip install -r requirements.txt`

# Running the script
The end point which we should call to give us our outputs called `main.py`. We can run this script through our terminal by calling `python main.py` which should run the necessary functions to give the output for each exercise.
This will also create a log file called SparkInterview.log which will allow us to view the outputs for each of our exercises.