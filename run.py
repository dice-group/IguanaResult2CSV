import csv
import json
import os
from pathlib import Path

import click


@click.command()
@click.argument('output_dir', type=click.Path(), default=lambda: os.getcwd())
@click.argument('input_dir', type=click.Path(exists=True), default=lambda: os.getcwd())
def run(output_dir, input_dir):
    click.echo("output directory: {}".format(output_dir))
    click.echo("input directory: {}".format(input_dir))

    # find files that are Iguana result files
    files = list(filter(
        lambda path: path.is_file() and
                     path.suffix == ".nt" and
                     not path.name.startswith("cleaned_"),
        [Path(os.path.join(input_dir, path)) for path in os.listdir(input_dir)]
    ))
    click.echo("\nFiles for conversion: \n{}".format("\n".join([file.name for file in files])))
    output_files = list()
    click.echo("\nConverted files:")
    for file in files:
        output_file = result2rdf.convert_result_file(file.name, input_dir, output_dir)
        click.echo("{}.csv".format(Path(output_file).name))
        output_files.append(output_file)

    click.echo("\nConcatenating all output files ... ")
    concat_output_file_path = os.path.join(output_dir, "all_results")
    with open(concat_output_file_path + ".csv", 'w') as output_file:
        csv_writer = csv.DictWriter(output_file, fieldnames=result2rdf.fieldnames)
        csv_writer.writeheader()
        for input_file_path in output_files:
            with open(input_file_path + ".csv", 'r') as input_file:
                csv_reader = csv.DictReader(input_file, )
                for row in csv_reader:
                    csv_writer.writerow(row)

    with open(concat_output_file_path + ".json", 'w') as output_file:
        entries = list()
        for input_file_path in output_files:
            with open(input_file_path + ".json", 'r') as input_file:
                json_obj = json.load(input_file)
                entries.append(json_obj)
        output_file.write(json.dumps({"benchmarks": entries},
                                     sort_keys=True,
                                     indent=4
                                     ))
    click.echo("Done")


import result2rdf

if __name__ == '__main__':
    run()
