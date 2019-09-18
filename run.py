import string

import click
import os
from pathlib import Path

import result2rdf


@click.command()
@click.argument('input_dir', type=click.Path(exists=True), default=lambda: os.getcwd())
@click.argument('output_dir', type=click.Path(exists=True), default=lambda: os.getcwd())
def run(input_dir, output_dir):
    files = list(filter(
        lambda path: os.path.isfile(path) and
                     Path(path).suffix == ".nt" and
                     not Path(path).name.startswith("cleaned_"),
        os.listdir(input_dir)
    ))
    click.echo("Files for conversion: \n{}".format("\n".join(files)))
    for file in files:
        result2rdf.convert_result_file(file, output_dir)


if __name__ == '__main__':
    run()
