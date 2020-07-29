from db import Score, Population, create_tables

create_tables()
s = Population.population_from_csv("demographics.csv")