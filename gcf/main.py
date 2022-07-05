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
import os
from io import BytesIO
from google.cloud import storage

config = decouple.AutoConfig(' ')
nlp = spacy.load("en_core_web_lg")

bucket_name = 'sideb-proejct.appspot.com'
bucket_dir = 'piixur/'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.abspath(os.path.join(os.getcwd(), 'sideb-proejct-0e33d8c0b0a9.json'))


def get_gtrend(kw):
    pytrends = TrendReq(hl='en-US', tz=360)
    pytrends.build_payload([kw], timeframe='now 1-d')
    qrys = pytrends.related_queries()[kw]['rising']['query'].values
    return ' '.join([_q.replace(kw, '').replace('  ', ' ').strip() for _q in qrys])


def get_mongodb_collection(colname):
    client = pymongo.MongoClient(config('MONGODB_URI'))
    return client.Cluster0[colname]


def get_tweepy_api():
    bearer_token = config('TWITTER_BEARER_TOKEN')
    consumer_key = config('TWITTER_CONSUMER_KEY')
    consumer_secret = config('TWITTER_CONSUMER_SECRET')
    access_token = config('TWITTER_ACCESS_TOKEN')
    access_token_secret = config('TWITTER_ACCESS_TOKEN_SECRET')

    auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret, access_token, access_token_secret)
    return tweepy.API(auth)


def main():
    log = {}
    
    try:
        gtrend = get_gtrend('photo')
        log['gtrend'] = gtrend

        colpix = get_mongodb_collection('pix')
        pixs = list(colpix.find({ 'feed_id': {'$exists': False}}))
        log['target_docs'] = len(pixs)

        sims = []
        for _p in tqdm(pixs):
            sims.append((_p['hash'], nlp(gtrend).similarity(nlp(_p['labels'] + ' ' + _p['colors']))))

        sims = sorted(sims, key=lambda x: -x[1])
        log['scores'] = sims[:3]

        _hash = sims[0][0]
        pix = colpix.find_one({ 'hash': _hash })
        url = pix['urls'][0]
        log['pix_url'] = url

        storage_client = storage.Client()
        blobs = list(storage_client.list_blobs(bucket_name, prefix=bucket_dir+_hash))
        blob = blobs[0]
        log['storage_file_exists'] = True

        api = get_tweepy_api()
        file = BytesIO(blob.download_as_bytes())
        media = api.media_upload(filename='0', file=file)
        log['twitter_media_id'] = media.media_id

        api.create_media_metadata(media.media_id_string, url)
        log['twitter_media_metadata_created'] = True

        status = api.update_status(status='', media_ids=[media.media_id_string])
        log['feed_id'] = status.id

        colpix.update_one(
            { 'hash': _hash }, 
            { '$set': { 'feed_id': status.id } }
        )
        log['mongodb_updated'] = True
        
    except:
        log['mongodb_updated'] = False
    
    finally:
        # print(log)
        feedlog = get_mongodb_collection('feedlog')
        feedlog.insert_one(log)    