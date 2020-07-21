from peewee import Model, SqliteDatabase, TextField, ForeignKeyField

database = SqliteDatabase('results.db')


class BaseModel(Model):
    class Meta:
        database = database
        legacy_table_names = False


class BlockGroup(BaseModel):
    name = TextField()
    description = TextField()


class ScoreType(BaseModel):
    name = TextField()
    description = TextField()


class Score(BaseModel):
    block_group = ForeignKeyField(BlockGroup, backref='scores')
    score_type = ForeignKeyField(ScoreType, backref='scores')


class PopulationType(BaseModel):
    name = TextField()
    description = TextField()


class Population(BaseModel):
    block_group = ForeignKeyField(BlockGroup, backref='populations')
    population_type = ForeignKeyField(PopulationType, backref='populations')


class Tag(BaseModel):
    name = TextField()


class BlockGroupTag(BaseModel):
    block_group = ForeignKeyField(BlockGroup, backref='block_group_tags')
    tag = ForeignKeyField(Tag, backref='block_group_tags')


if __name__ == "__main__":
    database.connect()
    database.create_tables([BlockGroup, ScoreType, Score, PopulationType, Population, Tag, BlockGroupTag])