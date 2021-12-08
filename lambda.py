import ROOT
import base64
import boto3
import cloudpickle as pickle
import json
import os
import time
from multiprocessing import Process, Pipe

from Inspector import Inspector

bucket = os.getenv('bucket')
debug_command = os.getenv('debug_command', '')
return_after_debug = os.getenv('return_after_debug', 'False')

krb5ccname = os.getenv('KRB5CCNAME', '/tmp/certs')
monitor = (os.getenv('monitor', 'False') == 'True')
results_fname = os.getenv('results_fname', 'results.txt')


def the_monitor(pipe):
    def network_measurement():
        memInfo = ""
        with open('/proc/net/dev', 'r') as file:
            memInfo = file.read()
        my_dict = {}
        for line in memInfo.split('\n')[2:-1]:
            splitted = line.split(':')
            my_dict[splitted[0].strip()] = int(
                splitted[1].lstrip().split(maxsplit=1)[0]
            )
        return my_dict

    def inspect_me():
        inspector = Inspector()
        inspector.inspectAll()
        inspector.addAttribute("network_rx_bytes", network_measurement())

        return inspector.finish()

    while True:
        os.nice(0)
        with open("/tmp/readings.txt","a") as f:
            f.write(json.dumps(inspect_me()))
            f.write("\n")
        # pipe.send(inspect_me())
        time.sleep(1)


def lambda_handler(event, context):
    thread = None

    if event.get('debug_command'):
        debug_command = event.get('debug_command', debug_command)
        if return_after_debug:
            return {
                'statusCode': 500,
                'command': debug_command,
                'command_output': os.popen(f'{debug_command}').read()
            }
        else:
            print(debug_command)

    pipe_in, pipe_out = Pipe()
    if monitor:
        thread = Process(target=the_monitor, args=(pipe_in,))
        thread.start()
        print('monitoring started!')

    print('event', event)
    s3 = boto3.client('s3')

    range = base64.b64decode(event['range'][2:-1])
    mapper = base64.b64decode(event['script'][2:-1])
    cert_file = base64.b64decode(event['cert'][2:-1])
    headers = pickle.loads(base64.b64decode(event['headers'][2:-1]))

    mapper = pickle.loads(mapper)
    range = pickle.loads(range)

    with open(f'{krb5ccname}', "wb") as handle:
        handle.write(cert_file)

    for header_name, header_content in headers:
        with open('/tmp/' + header_name, 'w') as f:
            f.write(header_content)
        print(header_name)
        try:
            ROOT.gInterpreter.Declare(f'#include "/tmp/{header_name}"')
        except Exception:
            pass

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

    filename = f'partial_{range.id}_{str(int(time.time() * 1000.0))}.pickle'
    s3.upload_file(f'/tmp/out.pickle', bucket, filename)

    if monitor:
        print('monitoring stopping!')
        thread.terminate()
        thread.join()
        print('monitoring finished!')

    results = []
    with open("/tmp/readings.txt", "r") as f:
        lines = f.readlines()
        for line in lines:
            results.append(json.loads(line))
    # while pipe_out.poll():
    #     results.append(pipe_out.recv())

    return {
        'statusCode': 200,
        'body': json.dumps(results),
        'filename': json.dumps(filename)
    }
