import threading
import base64
import json
import os
import time
from multiprocessing import Process, Pipe
import cloudpickle as pickle
from Inspector import Inspector
from ast import literal_eval

import boto3
import ROOT

bucket = os.getenv('bucket')
monitor = (os.getenv('monitor', 'False') == 'True')
results_fname = os.getenv('results_fname', 'results.txt')


def the_monitor(pipe):
    def inspect_me():
        inspector = Inspector()
        inspector.inspectAll()
        return inspector.finish()
    # results_fname = 'results.txt'
    # f = open("/tmp/{results_fname}", "a+")
    # f.write("[")
    # f.close()
    while True:
        os.nice(0)
        pipe.send(inspect_me())
        # f = open("/tmp/{results_fname}", "a+")
        # f.write(json.dumps(inspect_me()))
        # f.close()
        time.sleep(1)


def lambda_handler(event, context):
    thread = None
    pipe_in, pipe_out = Pipe()
    if monitor:
        thread = Process(target=the_monitor, args=(pipe_in,))
        thread.start()
        print('monitoring started!')

    print('event', event)
    s3 = boto3.client('s3')

    start = int(event['start'])
    end = int(event['end'])

    range = base64.b64decode(event['range'][2:-1])
    mapper = base64.b64decode(event['script'][2:-1])
    cert_file = base64.b64decode(event['cert'][2:-1])

    mapper = pickle.loads(mapper)
    range = pickle.loads(range)

    with open("/tmp/certs", "wb") as handle:
        pickle.dump(cert_file, handle)

    try:
        hist = mapper(range)
    except Exception as e:
        return {
            'statusCode': 500,
            'errorType': json.dumps(type(e).__name__),
            'errorMessage': json.dumps(str(e)),
        }

    with open('/tmp/out.pickle', 'wb') as handle:
        pickle.dump(hist, handle)

    filename = f'partial_{str(start)}_{str(end)}_{str(int(time.time()*1000.0))}.pickle'
    s3.upload_file(f'/tmp/out.pickle', bucket, filename)

    if monitor:
        print('monitoring stopping!')
        thread.terminate()
        thread.join()
        print('monitoring finished!')
    
    results=[]
    while pipe_out.poll():
        results.append(pipe_out.recv())

    # f = open("/tmp/{results_fname}", "a")
    # f.write("]")
    # f.close()
    # f = open(f'/tmp/{results_fname}', 'r')
    # results = f.read()
    # f.close()

    return {
        'statusCode': 200,
        'body': json.dumps(results),
        'filename': json.dumps(filename)
    }
