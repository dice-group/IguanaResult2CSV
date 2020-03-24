import csv
import json
import os
from typing import List, Iterator

import dateutil.parser
import rdflib as rdf

fieldnames = ["starttime", "benchmarkID", "format", "dataset", "triplestore", "noclients", "queryID", "qps",
              "succeeded", "failed", "timeouts", "unknownExceptions", "wrongCodes", "totaltime", "resultsize",
              "penalizedtime"]


class TaskMetaData:
    def __init__(self, benchmarkID: str, format: str, dataset: str, noclients: int, triplestore: str, starttime: str,
                 runtime: float):
        self.benchmarkID = str(benchmarkID)
        self.format = str(format)
        self.dataset = str(dataset)
        self.noclients = int(noclients)
        self.triplestore = str(triplestore)
        self.starttime = dateutil.parser.parse(starttime)
        self.runtime = float(runtime)


def extract_meta_data(rdf_graph) -> List[TaskMetaData]:
    raw_tasks = list(rdf_graph.query(
        ''' PREFIX iguanac: <http://iguana-benchmark.eu/class/>
            PREFIX iguana: <http://iguana-benchmark.eu/properties/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT DISTINCT ?benchmarkID ?format ?dataset ?noclients ?triplestore ?starttime ?runtime WHERE
            {
            ?benchmarkID rdfs:Class iguanac:Task .
            ?benchmarkID rdfs:startDate ?starttime .
            ?benchmarkID iguana:timeLimit ?runtime.
            # experiment
            ?experiment iguana:task ?benchmarkID .
            ?experiment iguana:dataset ?ds.
            ?ds rdfs:label ?dataset.
            # triplestore label
            ?benchmarkID iguana:connection ?conn .
            ?conn rdfs:label ?triplestore.
            # clients
            ?benchmarkID iguana:noOfWorkers ?noclients .
            # worker results
            ?benchmarkID iguana:workerResult ?wr .
            ?wr iguana:workerType ?workerType .
            BIND( IF(CONTAINS(?workerType, "CLI"),"CLI","HTTP") AS ?format )
            } '''
    ))
    return [TaskMetaData(**raw_task.asdict()) for raw_task in raw_tasks]


def convert_result_file(rdf_file: str, input_dir: str, output_dir: str) -> Iterator[str]:
    """
    Converts a input file
    :param rdf_file: the IGUANA output file to be processed
    :return: the file where the result was written to
    """

    # load the file
    iguana_result_graph = rdf.Graph()
    iguana_result_graph.parse(os.path.join(input_dir, rdf_file), format="nt")

    tasks_meta_data: List[TaskMetaData] = extract_meta_data(iguana_result_graph)

    for task_meta_data in tasks_meta_data:

        query_results = iguana_result_graph.query(
            ''' PREFIX iguanac: <http://iguana-benchmark.eu/class/>
                PREFIX iguana: <http://iguana-benchmark.eu/properties/>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                SELECT ?starttime ?benchmarkID ?format ?dataset ?triplestore ?noclients ?queryID ?qps ?succeeded ?failed ?timeouts ?unknownExceptions ?wrongCodes ?totaltime ?resultsize WHERE
                {{
                BIND( {benchmarkID} AS ?benchmarkID )
                {benchmarkID} rdfs:Class iguanac:Task .
                {benchmarkID} rdfs:startDate ?starttime .
                # experiment
                ?experiment iguana:task {benchmarkID} .
                ?experiment iguana:dataset ?ds.
                ?ds rdfs:label ?dataset.
                # triplestore label
                {benchmarkID} iguana:connection ?conn .
                ?conn rdfs:label ?triplestore.
                # clients
                {benchmarkID} iguana:noOfWorkers ?noclients .
                # worker results
                {benchmarkID} iguana:workerResult ?wr .
                ?wr iguana:queryID ?queryIDURI.
                ?queryIDURI rdfs:ID ?queryID .
                ?wr iguana:queriesPerSecond ?qps.
                ?wr iguana:succeeded ?succeeded.
                ?wr iguana:failed ?failed.
                ?wr iguana:timeouts ?timeouts.
                ?wr iguana:unknownExceptions ?unknownExceptions.
                ?wr iguana:wrongCodes ?wrongCodes.
                ?wr iguana:totalTime ?totaltime.
                ?wr iguana:resultSize ?resultsize.
                BIND( "{bindformat}" AS ?format )
                }} '''.format(benchmarkID="<{}>".format(task_meta_data.benchmarkID), bindformat=task_meta_data.format)
        )

        outputfile: str = "{}_{}_{:02d}-clients_{}_{}".format(task_meta_data.format, task_meta_data.dataset,
                                                              int(task_meta_data.noclients), task_meta_data.triplestore,
                                                              task_meta_data.starttime.strftime("%Y-%m-%d_%H-%M-%S"))
        os.makedirs(output_dir, exist_ok=True)

        with open(os.path.join(output_dir, outputfile + ".json"), "w") as jsonfile:
            jsonfile.write(json.dumps({'benchmarkID': task_meta_data.benchmarkID,
                                       'starttime': str(task_meta_data.starttime),
                                       'runtime': task_meta_data.runtime,
                                       "format": task_meta_data.format,
                                       'dataset': task_meta_data.dataset,
                                       'noclients': task_meta_data.noclients,
                                       'triplestore': task_meta_data.triplestore},
                                      sort_keys=True,
                                      indent=4),
                           )

        output_csv = os.path.join(output_dir, outputfile + ".csv")
        with open(output_csv, 'w') as csvfile:
            csvwriter = csv.DictWriter(csvfile,
                                       fieldnames=fieldnames)
            csvwriter.writeheader()
            for binding in sorted(list(query_results), key=lambda x: int(x.queryID)):
                # TODO: make penalty time configurable
                penalty_time = 180000

                penalized_time = float(binding.totaltime)
                if int(binding.failed) > 0 and float(binding.totaltime) < penalty_time * int(binding.failed):
                    penalized_time = penalized_time + penalty_time * int(binding.failed)

                csvwriter.writerow({
                    "starttime": binding.starttime,
                    "benchmarkID": binding.benchmarkID,
                    "format": binding.format,
                    "dataset": binding.dataset,
                    "triplestore": binding.triplestore,
                    "noclients": int(binding.noclients),
                    "queryID": binding.queryID,
                    "qps": binding.qps,
                    "succeeded": int(binding.succeeded),
                    "failed": int(binding.failed),
                    "wrongCodes": int(binding.wrongCodes),
                    "unknownExceptions": int(binding.unknownExceptions),
                    "timeouts": int(binding.timeouts),
                    "totaltime": float(binding.totaltime),
                    "resultsize": int(binding["resultsize"]) if str(binding["resultsize"]) != '?' else '',
                    "penalizedtime": penalized_time
                })
        yield os.path.join(output_dir, outputfile)
