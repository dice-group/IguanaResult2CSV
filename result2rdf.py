import csv
import json
import os

import dateutil.parser
import rdflib as rdf

fieldnames = ["benchmarkID", "format", "dataset", "triplestore", "noclients", "queryID", "qps", "succeeded",
              "failed", "totaltime", "resultsize"]


def repair_result_file(rdf_file, cleaned_rdf_file, input_dir):
    # clean the file
    with open(os.path.join(input_dir, cleaned_rdf_file), 'w') as out_file:
        with open(os.path.join(input_dir, rdf_file)) as in_file:

            line = in_file.readline()
            while line:
                if not ("<http://www.w3.org/2000/01/rdf-schema#label>" in line and "sparql" in line):
                    out_file.write(line)
                line = in_file.readline()


def extract_meta_data(rdf_graph):
    global dataset, triplestore, ID, no_clients, format
    r = rdf_graph.query(
        "SELECT DISTINCT ?starttime WHERE {[] <http://www.w3.org/2000/01/rdf-schema#startDate> ?starttime.}")
    starttime = dateutil.parser.parse(next(iter(r))["starttime"])

    r = rdf_graph.query(
        "SELECT DISTINCT ?runtime WHERE {[] <http://iguana-benchmark.eu/properties/timeLimit> ?runtime.}")
    runtime = next(iter(r))["runtime"]

    r = rdf_graph.query("SELECT DISTINCT ?dataset WHERE {[] <http://iguana-benchmark.eu/properties/dataset> ?ds. "
                        "?ds <http://www.w3.org/2000/01/rdf-schema#label> ?dataset }")
    dataset = str(next(iter(r))["dataset"])
    r = rdf_graph.query(
        "SELECT DISTINCT ?triplestore WHERE {[] <http://iguana-benchmark.eu/properties/connection>  ?ts. "
        "?ts <http://www.w3.org/2000/01/rdf-schema#label> ?triplestore }")
    triplestore = str(next(iter(r))["triplestore"])
    r = rdf_graph.query("SELECT DISTINCT ?task ?clients WHERE {[] <http://iguana-benchmark.eu/properties/task> ?task. "
                        "?task <http://iguana-benchmark.eu/properties/noOfWorkers> ?clients }")
    task_clients = next(iter(r))
    split = str(task_clients["task"]).split("/")
    ID = "{}/{}/{}".format(split[-3], split[-2], split[-1])
    no_clients = task_clients["clients"]
    format = "HTTP"
    return ID, format, dataset, no_clients, triplestore, starttime, runtime


def convert_result_file(rdf_file: str, input_dir: str, output_dir: str) -> str:
    """
    Converts a input file
    :param rdf_file: the IGUANA output file to be processed
    :return: the file where the result was written to
    """
    cleaned_rdf_file = "cleaned_{}".format(rdf_file)

    repair_result_file(rdf_file, cleaned_rdf_file, input_dir)

    # load the file
    data = rdf.Graph()
    data.parse(os.path.join(input_dir, cleaned_rdf_file), format="nt")

    ID, format, dataset, no_clients, triplestore, starttime, runtime = extract_meta_data(data)

    benchmark_logs = data.query(
        "SELECT ?query ?queryID ?qps ?succeeded ?failed ?totaltime ?resultsize "
        "WHERE {"
        "?query <http://iguana-benchmark.eu/properties/queriesPerSecond> ?qps ."
        "?query <http://iguana-benchmark.eu/properties/id> ?queryID . "
        "?query <http://iguana-benchmark.eu/properties/failed> ?failed ."
        "?query <http://iguana-benchmark.eu/properties/succeded> ?succeeded ."
        "?query <http://iguana-benchmark.eu/properties/totalTime> ?totaltime ."
        "?query <http://iguana-benchmark.eu/properties/resultSize> ?resultsize ."
        "}")

    outputfile = "{}_{}_{:02d}-clients_{}_{}".format(format, dataset, int(no_clients), triplestore,
                                                     starttime.strftime("%Y-%m-%d_%H-%M-%S"))
    os.makedirs(output_dir, exist_ok=True)

    with open(os.path.join(output_dir, outputfile + ".json"), "w") as jsonfile:
        jsonfile.write(json.dumps({'benchmarkID': ID,
                                   'starttime': str(starttime),
                                   'runtime': runtime,
                                   "format": format,
                                   'dataset': dataset,
                                   'noclients': no_clients,
                                   'triplestore': triplestore},
                                  sort_keys=True,
                                  indent=4),
                       )

    output_csv = os.path.join(output_dir, outputfile + ".csv")
    with open(output_csv, 'w') as csvfile:
        csvwriter = csv.DictWriter(csvfile,
                                   fieldnames=fieldnames)
        csvwriter.writeheader()
        for binding in benchmark_logs:
            csvwriter.writerow({
                "benchmarkID": ID,
                "format": format,
                "dataset": dataset,
                "triplestore": triplestore,
                "noclients": no_clients,
                "queryID": binding["queryID"].split("sparql")[1],
                "qps": binding["qps"],
                "succeeded": binding["succeeded"],
                "failed": binding["failed"],
                "totaltime": binding["totaltime"],
                "resultsize": binding["resultsize"]
            })
    os.remove(os.path.join(input_dir, cleaned_rdf_file))
    return os.path.join(output_dir, outputfile)
