import csv
import json
import os
import pkg_resources

from pathlib import Path
from string import Template
from typing import List, Iterator, Tuple

import rdflib as rdf
import rdflib.query as sparql
from rdflib import Graph, URIRef

sparql_folder = Path(pkg_resources.resource_filename('iguanaresult2csv', 'sparql/'))

print("sparql folder mod: {}".format(sparql_folder))

with open(sparql_folder.joinpath('get_tasks.sparql'), 'r') as file:
    get_experiments_sparql = file.read()

with open(sparql_folder.joinpath('task_meta_data.sparql'), 'r') as file:
    task_meta_data_template = file.read()

with open(sparql_folder.joinpath('task_data.sparql'), 'r') as file:
    task_data_template = file.read()

with open(sparql_folder.joinpath('task_data_each_query.sparql'), 'r') as file:
    task_data_each_query_template = file.read()


def extract_tasks(rdf_graph) -> List[URIRef]:
    query_result = list(rdf_graph.query(get_experiments_sparql))
    return [result["task"] for result in query_result]


def extract_task_meta_data(rdf_graph: Graph, task: URIRef) -> sparql.ResultRow:
    query_result: sparql.Result = rdf_graph.query(
        Template(task_meta_data_template).substitute(task=task.n3())
    )
    assert len(query_result) == 1
    return next(iter(query_result))


def extract_task_data(rdf_graph: Graph, task: URIRef) -> sparql.Result:
    query_result: sparql.Result = rdf_graph.query(
        Template(task_data_template).substitute(task=task.n3())
    )
    assert len(query_result) > 0
    return query_result


def extract_task_data_each_query(rdf_graph, task: URIRef) -> sparql.Result:
    query_result: sparql.Result = rdf_graph.query(
        Template(task_data_each_query_template).substitute(task=task.n3())
    )
    assert len(query_result) > 0
    return query_result


def convert_result_file(rdf_file: Path, output_dir: Path) -> Iterator[Tuple[Path, Path, Path]]:
    """
    Converts a input file
    :param rdf_file: the IGUANA output file to be processed
    :param output_dir: dir where the files are created/overwritten
    :return: the file where the result was written to
    """

    # load the file
    iguana_result_graph: Graph = Graph()
    iguana_result_graph.parse(str(rdf_file), format="ttl")

    tasks: List[URIRef] = extract_tasks(iguana_result_graph)

    for task in tasks:
        task_meta_data: sparql.ResultRow = extract_task_meta_data(iguana_result_graph, task)

        query_results: sparql.Result = extract_task_data(iguana_result_graph, task)

        output_filename: str = "{}_{}_{:02d}-clients_{}_{}".format(
            task_meta_data.format.toPython(),
            task_meta_data.dataset.toPython(),
            int(task_meta_data.noclients.toPython()),
            # flaw in iguana result file
            task_meta_data.triplestore.toPython(),
            task_meta_data.startDate.toPython().strftime(
                "%Y-%m-%d_%H-%M-%S"))
        os.makedirs(output_dir, exist_ok=True)

        # write csv
        # task's PenalizedAvgQPS is calculated along with writing the csv
        task_meta_data.PenalizedAvgQPS = 0

        output_csv: Path = output_dir.joinpath(output_filename + ".csv")
        fieldnames: List[str] = query_results.vars + ["penalizedTime"]
        with open(output_csv, 'w') as csvfile:
            csvwriter = csv.DictWriter(csvfile,
                                       fieldnames=fieldnames,
                                       quoting=csv.QUOTE_NONNUMERIC)
            csvwriter.writeheader()
            for result_row in query_results:
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

        # write json
        output_json = output_dir.joinpath(output_filename + ".json")
        with open(output_json, "w") as jsonfile:
            jsonfile.write(json.dumps(task_meta_data.asdict(),
                                      sort_keys=True,
                                      indent=4))

        # write extra CSV with each query stats if EachQuery metric was activated
        output_csv_eq = None
        if task_meta_data.EachQuery.toPython():
            query_results_eq: sparql.Result = extract_task_data_each_query(iguana_result_graph, task)
            fieldnames_eq: List[str] = query_results_eq.vars
            output_csv_eq = output_dir.joinpath(output_filename + "_each_query.csv")
            qm_bug: bool = False
            with open(output_csv_eq, 'w') as csvfile_eq:
                csvwriter = csv.DictWriter(csvfile_eq,
                                           fieldnames=fieldnames_eq,
                                           quoting=csv.QUOTE_NONNUMERIC)
                csvwriter.writeheader()

                for result_row in query_results_eq:
                    # TODO: workaround for IGUANA bug where too many runs are executed for NumberOfQueryMixes mode
                    if task_meta_data.numberOfQueryMixes.toPython() is not None \
                            and result_row.run.toPython() > int(task_meta_data.numberOfQueryMixes):
                        if qm_bug is False:
                            print(
                                "WARNING: Runs after numberOfQueryMixes = {} are not reported in the each query output."
                                " Consequently, aggregated metrics in {} and {} do not apply to {}.".format(
                                    task_meta_data.numberOfQueryMixes,
                                    output_csv.name,
                                    output_json.name,
                                    output_csv_eq.name
                                ))
                            qm_bug = True
                        continue
                    csv_row = dict(zip(fieldnames_eq, [entry.toPython() for entry in result_row]))
                    csvwriter.writerow(csv_row)

        yield output_csv, output_json, output_csv_eq
