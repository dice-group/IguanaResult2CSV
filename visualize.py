import numpy as np

from bokeh.layouts import gridplot
from bokeh.models import LabelSet, ColumnDataSource
from bokeh.plotting import figure, output_file, show
from pathlib import Path
from bokeh.plotting import figure, output_file, show, output_notebook



import json
import csv



def visualize(json_path: str, csv_path:str):
    meta_data = None
    with open(json_path, 'r') as f:
        meta_data = json.load(f)
    meta_data = meta_data["benchmarks"]
    data = None
    with open(csv_path, 'r') as input_file:
        reader = csv.DictReader(input_file, quoting=csv.QUOTE_NONNUMERIC)
        data = [line for line in reader]


    datasets = sorted({run["dataset"] for run in meta_data})
    triplestores = sorted({run["triplestore"] for run in meta_data})
    numbersofclients = sorted({run["noclients"] for run in meta_data})

    output_file('vbar.html')
    PenalizedAvgQPS = {run["triplestore"]: run["PenalizedAvgQPS"]
                       for run in meta_data}

    x = triplestores
    y = [round(PenalizedAvgQPS[triplestore]) if PenalizedAvgQPS[triplestore] else 0 for triplestore in triplestores]
    print(PenalizedAvgQPS)

    x_ticks = [i +.5 for i in range(len(x))]

    p = figure(x_range=triplestores,
               tools="pan,wheel_zoom,box_zoom,reset,zoom_in,zoom_out,save")
    source = ColumnDataSource(data=dict(x=x_ticks, x_label=x, y=y))
    labels = LabelSet(x='x', y='y', text='y', source=source,
                      #render_mode='css',
                      y_offset=10, text_font_size = '8pt',text_font_style='bold', text_color = 'black', text_baseline ='middle', text_align="center")

    p.vbar(source=source, x='x', top='y', width=0.9)
    p.add_layout(labels)
    show(p)




path = "/home/me/sync/Work/2020-11 Tentris Compression Benchmarks/selected_result_files_csv"
visualize(path + "/all_results.json", path + "/all_results.csv")



