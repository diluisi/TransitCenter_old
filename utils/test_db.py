from db import Score, Population, create_tables, BlockGroup

# create_tables()
# s = Score.score_from_csv("sample_accesibility_V2.csv")
# p = Population.population_from_csv("demographics1.csv")

BlockGroup.tag_bg_from_csv("boston_msa_onlly.csv", 'boston-msa')