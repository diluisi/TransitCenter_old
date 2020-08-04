"""
Results database Peewee model library. STILL UNDER DEVELOPMENT

This file contains database table defintions using the Peewee database
object-relational-mapper, along with helper functions to assist data management.
"""
from datetime import date, datetime

from peewee import Model, SqliteDatabase, TextField, ForeignKeyField, IntegerField, FloatField, DateField, chunked
import pandas as pd

database = SqliteDatabase(r'C:\Users\Willem\Documents\Project\TransitCenter\TransitCenter\results.db') # Temporary sqlite DB instance

class BaseModel(Model):
    class Meta:
        database = database
        legacy_table_names = False


class BlockGroup(BaseModel):
    id = IntegerField(primary_key=True)

    @staticmethod
    def by_tag(tag):
        return (BlockGroup.select(BlockGroup.id)
                    .join(BlockGroupTag).join(Tag)
                    .where(Tag.name == tag))

    @staticmethod
    def tag_bg_from_csv(filepath, tag):
        df = pd.read_csv(filepath, dtype={'block_group_id': 'Int64'})
        t, new = Tag.get_or_create(name=tag)
        to_insert = []
        for idx, bg in df.iterrows():
            to_tag = dict()
            to_tag['block_group_id'] = bg.block_group_id
            to_tag['tag_id'] = t.id
            to_insert.append(to_tag)

        with database.atomic():
            for batch in chunked(to_insert, 100):
                BlockGroupTag.insert_many(batch).execute()

class ScoreType(BaseModel):
    name = TextField()
    measure = TextField()
    destination = TextField()
    function = TextField()
    date = DateField()
    period = TextField()
    description = TextField(null=True)


class Score(BaseModel):
    block_group = ForeignKeyField(BlockGroup, backref='scores')
    score_type = ForeignKeyField(ScoreType, backref='scores')
    score = FloatField(null=True)

    @staticmethod
    def score_from_csv(filepath):
        df = pd.read_csv(filepath, dtype={'block_group_id': 'Int64'})
        # We'll build a dictionary of types to add and make sure they're
        score_types = dict()
        for score_column in df.columns[1:]:
            print(score_column)
            s_split = score_column.split('_')
            if s_split[0] == 'A':
                measure = 'Access to'
            else:
                raise TypeError # Temporary until we add all the possibilities

            destination = s_split[1]
            if s_split[2] == 'c30':
                metric = '30 minute cumulative'
            elif s_split[2] == 'c45':
                metric = '45 minute cumulative'
            elif s_split[2] == 'c60':
                metric = '60 minute cumulative'
            elif s_split[2] == 'time3':
                metric = 'time to closest 3 destinations'
            elif s_split[2] == 'time1':
                metric = 'Time to closest destination'
            else:
                raise TypeError # Temporary to flag errors.
            
            if s_split[4]:
                period = 'morning peak'
            else:
                raise TypeError # Temporary to flag errors

            score_type, new = ScoreType.get_or_create(
                name = score_column,
                measure = s_split[0],
                destination = s_split[1],
                function = s_split[2],
                date = datetime.strptime(s_split[3], '%d%m%Y'),
                period = s_split[4]
                )
            description = f"{measure} {s_split[1]} using a {metric} measure for {period} period on {s_split[3]}"
            score_type.description = description
            score_type.save()
            score_types[score_column] = score_type

            # Now we can package the column into a list of dictionaries and do a bulk insert
            to_insert = []
            for idx, score_row in df.iterrows():
                insert_row = dict()
                if score_row['block_group_id'] < 0:
                    print(score_row)
                else:
                    insert_row['block_group_id'] = score_row['block_group_id']
                    insert_row['score_type_id'] = score_type.id
                    insert_row['score'] = score_row[score_column]
                    to_insert.append(insert_row)

            with database.atomic():
                for batch in chunked(to_insert, 100):
                    Score.insert_many(batch).execute()
        
    @staticmethod
    def by_tag_type(tag, score_type, date=False):
        if date:
            return (Score.select(Score.score, BlockGroup.id, ScoreType.date)
                    .join(BlockGroup).join(BlockGroupTag).join(Tag)
                    .where(Tag.name == tag)
                    .switch(Score)
                    .join(ScoreType)
                    .where(ScoreType.name == score_type))
        else:
            return (Score.select(Score.score, BlockGroup.id)
                    .join(BlockGroup).join(BlockGroupTag).join(Tag)
                    .where(Tag.name == tag)
                    .switch(Score)
                    .join(ScoreType)
                    .where(ScoreType.name == score_type))
    
    @staticmethod
    def by_tag_type_no_date(tag, score_type):
        # Parse the score key
        s_split = score_type.split("_")
        return (Score.select(Score.score, BlockGroup.id, ScoreType.date)
                .join(BlockGroup).join(BlockGroupTag).join(Tag)
                .where(Tag.name == tag)
                .switch(Score)
                .join(ScoreType)
                .where((ScoreType.measure == s_split[0])
                 & (ScoreType.destination == s_split[1])
                 & (ScoreType.function == s_split[2])
                 & (ScoreType.period == s_split[3]))
                )
    
    @staticmethod
    def weighted_average(tag, score_type, pop_type):
        # Start by getting the appropriate score
        score = (Score.select(Score.score, BlockGroup.id)
                .join(BlockGroup).join(BlockGroupTag).join(Tag)
                .where(Tag.name == tag)
                .switch(Score)
                .join(ScoreType)
                .where(ScoreType.name == score_type)
                .switch(BlockGroup)
                .join(Population)
                .join(PopulationType)
                .where(PopulationType.name == pop_type))
        return score


class PopulationType(BaseModel):
    name = TextField()
    description = TextField(null=True)


class Population(BaseModel):
    block_group = ForeignKeyField(BlockGroup, backref='populations')
    population_type = ForeignKeyField(PopulationType, backref='populations')
    value = FloatField()

    @staticmethod
    def population_from_csv(filepath):
        df = pd.read_csv(filepath, dtype={'block_group_id': 'Int64'})
        # We'll build a dictionary of types to add and make sure they're
        pop_types = dict()
        for pop_column in df.columns[1:]:
            print(pop_column)

            pop_type, new = PopulationType.get_or_create(name=pop_column)
            description = pop_column
            pop_type.description = description
            pop_type.save()
            pop_types[pop_column] = pop_type

            # Now we can package the column into a list of dictionaries and do a bulk insert
            to_insert = []
            for idx, pop_row in df.iterrows():
                insert_row = dict()
                if pop_row['block_group_id'] < 0:
                    print(pop_row)
                else:
                    insert_row['block_group_id'] = pop_row['block_group_id']
                    insert_row['population_type_id'] = pop_type.id
                    insert_row['value'] = pop_row[pop_column]
                    to_insert.append(insert_row)

            with database.atomic():
                for batch in chunked(to_insert, 100):
                    Population.insert_many(batch).execute()
    @staticmethod
    def by_tag_type(tag, pop_type):
        return (Population.select(Population.value, BlockGroup.id)
                .join(BlockGroup).join(BlockGroupTag).join(Tag)
                .where(Tag.name == tag)
                .switch(Population)
                .join(PopulationType)
                .where(PopulationType.name == pop_type))


class Tag(BaseModel):
    name = TextField()


class BlockGroupTag(BaseModel):
    block_group = ForeignKeyField(BlockGroup, backref='block_group_tags')
    tag = ForeignKeyField(Tag, backref='block_group_tags')

def create_tables():
    database.connect()
    database.create_tables([BlockGroup, ScoreType, Score, PopulationType, Population, Tag, BlockGroupTag])

if __name__ == "__main__":
    database.connect()
    database.create_tables([BlockGroup, ScoreType, Score, PopulationType, Population, Tag, BlockGroupTag])