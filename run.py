import csv
import json
import os
from pathlib import Path
from typing import List

import click


# from visualize import visualize


@click.command()
@click.argument('output_dir', type=click.Path(), default=lambda: os.getcwd())
@click.argument('input_dir', type=click.Path(exists=True), default=lambda: os.getcwd())
def run(output_dir, input_dir):
    output_dir = Path(output_dir)
    input_dir = Path(input_dir)
    click.echo("output directory: {}".format(output_dir))
    click.echo("input directory: {}".format(input_dir))

    # find files that are Iguana result files
    files = [file for file in Path(input_dir).iterdir() if file.suffix in {".nt", ".ttl"}]
    click.echo("\nFiles for conversion: \n{}".format("\n".join([file.name for file in files])))
    output_csvs = list()
    output_jsons = list()
    click.echo("\nConverted files:")
    for file in files:
        for output_files in result2rdf.convert_result_file(file, output_dir):
            click.echo("{}.csv".format(output_files[0].name))
            output_csvs.append(output_files[0])
            click.echo("{}.json".format(output_files[1].name))
            output_jsons.append(output_files[1])

    click.echo("\nConcatenating all output files ... ")
    concatenated_csv_path = output_dir.joinpath("all_results.csv")

    with open(concatenated_csv_path, 'w') as concatenated_csv:
        csv_writer = None

        for output_csv in output_csvs:
            with open(output_csv, 'r') as input_file:
                csv_reader = csv.DictReader(input_file, )
                if csv_writer is None:
                    csv_writer = csv.DictWriter(concatenated_csv, fieldnames=csv_reader.fieldnames,
                                                quoting=csv.QUOTE_NONNUMERIC)
                    csv_writer.writeheader()
                for row in csv_reader:
                    csv_writer.writerow(row)

    concatenated_json_path = output_dir.joinpath("all_results.json")
    with open(concatenated_json_path, 'w') as concatenated_json:
        entries = list()
        for output_json in output_jsons:
            with open(output_json, 'r') as input_file:
                json_obj = json.load(input_file)
                entries.append(json_obj)
        concatenated_json.write(json.dumps({"benchmarks": entries},
                                     sort_keys=True,
                                     indent=4
                                     ))
    click.echo("Done\n")
    click.echo("Generating plots ...")
    # visualize(os.path.join(output_dir, "all_results.json"), os.path.join(output_dir, "all_results.csv"))
    click.echo("Done")


import result2rdf

if __name__ == '__main__':
    run()
