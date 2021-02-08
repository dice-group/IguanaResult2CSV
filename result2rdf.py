import csv
import json
import os
from pathlib import Path
from string import Template
from typing import List, Iterator

from rdflib.query import ResultRow

import dateutil.parser
import rdflib as rdf

fieldnames = ["starttime", "benchmarkID", "format", "dataset", "triplestore", "noclients", "queryID", "qps",
              "penalizedQps",
              "succeeded", "failed", "timeouts", "unknownExceptions", "wrongCodes", "totaltime", "resultsize",
              "penalizedtime"]

with open('sparql/get_tasks.sparql', 'r') as file:
    get_experiments_sparql = file.read()

with open('sparql/task_meta_data.sparql', 'r') as file:
    task_meta_data_template = file.read()

with open('sparql/task_data.sparql', 'r') as file:
    task_data_template = file.read()


def extract_tasks(rdf_graph) -> List[rdf.URIRef]:
    query_result = list(rdf_graph.query(get_experiments_sparql
                                        ))
    return [result["task"] for result in query_result]


def extract_task_meta_data(rdf_graph, task: rdf.URIRef) -> ResultRow:
    query_result = rdf_graph.query(
        Template(task_meta_data_template).substitute(task=task.n3())
    )
    assert len(query_result) == 1
    return next(iter(query_result))


def extract_task_data(rdf_graph, task: rdf.URIRef):
    query_result = rdf_graph.query(
        Template(task_data_template).substitute(task=task.n3())
    )
    assert len(query_result) > 0
    return query_result


def convert_result_file(rdf_file: Path, input_dir: Path, output_dir: Path) -> Iterator[str]:
    """
    Converts a input file
    :param rdf_file: the IGUANA output file to be processed
    :return: the file where the result was written to
    """

    # load the file
    iguana_result_graph = rdf.Graph()
    iguana_result_graph.parse(str(rdf_file), format="ttl")

    tasks = extract_tasks(iguana_result_graph)

    for task in tasks:
        task_meta_data = extract_task_meta_data(iguana_result_graph, task)

        query_results = extract_task_data(iguana_result_graph, task)

        outputfile: str = "{}_{}_{:02d}-clients_{}_{}".format(task_meta_data.format.toPython(),
                                                              task_meta_data.dataset.toPython(),
                                                              int(task_meta_data.noclients.toPython()),
                                                              # flaw in iguana result file
                                                              task_meta_data.triplestore.toPython(),
                                                              task_meta_data.startDate.toPython().strftime(
                                                                  "%Y-%m-%d_%H-%M-%S"))
        os.makedirs(output_dir, exist_ok=True)

        task_meta_data.PenalizedAvgQPS = 0

        output_csv = os.path.join(output_dir, outputfile + ".csv")
        fieldnames = query_results.vars + ["penalized_time"]
        with open(output_csv, 'w') as csvfile:
            csvwriter = csv.DictWriter(csvfile,
                                       fieldnames=fieldnames,
                                       quoting=csv.QUOTE_NONNUMERIC)
            csvwriter.writeheader()
            for result_row in sorted(list(query_results), key=lambda x: x.queryID):
                # TODO: make penalty time configurable
                penalty_time = 180000

                task_meta_data.PenalizedAvgQPS += float(result_row.penalizedQPS)

                penalized_time = float(result_row.totaltime)
                if result_row.failed.toPython() > 0 and result_row.totaltime.toPython() < penalty_time * result_row.failed.toPython():
                    penalized_time = penalized_time + penalty_time * result_row.failed.toPython()

                csv_row = dict(zip(fieldnames, [entry.toPython() for entry in result_row] + [penalized_time]))
                csvwriter.writerow(csv_row)
        task_meta_data.PenalizedAvgQPS = task_meta_data.PenalizedAvgQPS / len(
            query_results) if task_meta_data.PenalizedAvgQPS > 0 else 0

        with open(os.path.join(output_dir, outputfile + ".json"), "w") as jsonfile:
            jsonfile.write(json.dumps(task_meta_data.asdict(),
                                      sort_keys=True,
                                      indent=4),
                           )
        yield os.path.join(output_dir, outputfile)
