import pymongo
import spacy
import decouple
import glob
import datetime
from PIL import Image
from IPython.display import display
from tqdm.notebook import tqdm
from pytrends.request import TrendReq
import tweepy

config = decouple.AutoConfig(' ')
nlp = spacy.load("en_core_web_lg")
pytrends = TrendReq(hl='en-US', tz=360)


def main(request):
    gsearch = 'photo'
    pytrends.build_payload([gsearch], timeframe='now 4-H')
    qrys = pytrends.related_queries()[gsearch]['rising']['query'].values
    trend = ' '.join([_q.replace(gsearch, '').replace('  ', ' ').strip() for _q in qrys])

    # client = pymongo.MongoClient(config('MONGODB_URI'))
    # client.list_database_names()
    # db = client.Cluster0
    # return db.list_collection_names()
    return str(nlp(trend).similarity(nlp('test cluster noise')))