import csv
import json
import os
from pathlib import Path
from typing import List

import click

from iguanaresult2csv.processing import convert_result_file


@click.command()
@click.argument('output_dir', type=click.Path(), default=lambda: os.getcwd())
@click.argument('input_dir', type=click.Path(exists=True), default=lambda: os.getcwd())
def cli(output_dir, input_dir):
    output_dir = Path(output_dir)
    input_dir = Path(input_dir)
    click.echo("output directory: {}".format(output_dir))
    click.echo("input directory: {}".format(input_dir))

    # find files that are Iguana result files
    files = [file for file in Path(input_dir).iterdir() if file.suffix in {".nt", ".ttl"}]
    click.echo("\nFiles for conversion: \n{}".format("\n".join([file.name for file in files])))
    output_csvs: List[Path] = list()
    output_jsons: List[Path] = list()
    output_each_query_csvs: List[Path] = list()
    click.echo("\nConverted files:")
    for file in files:
        for output_files in convert_result_file(file, output_dir):
            click.echo("{}".format(output_files[0].name))
            output_csvs.append(output_files[0])
            click.echo("{}".format(output_files[1].name))
            output_jsons.append(output_files[1])
            if output_files[1] is not None:
                click.echo("{}".format(output_files[2].name))
                output_each_query_csvs.append(output_files[2])

    click.echo("\nConcatenating all output files ... ")

    concatenated_csv_path = output_dir.joinpath("all_results.csv")
    combine_csv_files(concatenated_csv_path, output_csvs)

    concatenated_json_path = output_dir.joinpath("all_results.json")
    combine_json_files(concatenated_json_path, output_jsons)

    concatenated_csv_eq_path = output_dir.joinpath("all_results_each_query.csv")
    combine_csv_files(concatenated_csv_eq_path, output_each_query_csvs)

    click.echo("Done\n")


def combine_json_files(concatenated_json_path: Path, json_files: List[Path]):
    with open(concatenated_json_path, 'w') as concatenated_json:
        entries = list()
        for output_json in json_files:
            with open(output_json, 'r') as input_file:
                json_obj = json.load(input_file)
                entries.append(json_obj)
        concatenated_json.write(json.dumps({"benchmarks": entries},
                                           sort_keys=True,
                                           indent=4
                                           ))


def combine_csv_files(concatenated_csv_path: Path, csv_files: List[Path]):
    with open(concatenated_csv_path, 'w') as concatenated_csv:
        csv_writer = None

        for output_csv in csv_files:
            with open(output_csv, 'r') as input_file:
                csv_reader = csv.DictReader(input_file, )
                if csv_writer is None:
                    csv_writer = csv.DictWriter(concatenated_csv, fieldnames=csv_reader.fieldnames,
                                                quoting=csv.QUOTE_NONNUMERIC)
                    csv_writer.writeheader()
                for row in csv_reader:
                    csv_writer.writerow(row)


if __name__ == '__main__':
    cli()
