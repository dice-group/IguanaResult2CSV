from collections import defaultdict
from typing import Dict

import numpy as np

from bokeh.layouts import gridplot, column, row
from bokeh.models import LabelSet, ColumnDataSource
from bokeh.plotting import figure, output_file, show
from pathlib import Path
from bokeh.plotting import figure, output_file, show, output_notebook
import pandas as pd

import json
import csv


def visualize(json_path: str, csv_path: str):
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

    dataset = "swdf"
    noclients = 1

    aggreagting_metrics = []
    for metric in ["PenalizedAvgQPS", "QMPH"]:
        x_y_map = {run["triplestore"]: run[metric]
                   for run in meta_data
                   if run["dataset"] == dataset
                   and run["noclients"] == noclients}
        plot = generate_plot(metric, triplestores, x_y_map)
        aggreagting_metrics.append(plot)

    layout = row(aggreagting_metrics)

    metric = "qps"
    cat_yy_map = defaultdict(list)
    for data_point in data:
        if data_point["dataset"] == dataset and int(data_point["noclients"]) == noclients:
            cat_yy_map[data_point["triplestore"]].append(float(data_point[metric]))

    p = box_plot("testtitle", triplestores, cat_yy_map)
    layout = column(layout, p)

    show(layout)


def box_plot(title, cats, cat_yy_map: Dict, x_label="", y_label=""):
    df = pd.DataFrame(
        data=[(score, cat) for [cat, scores] in cat_yy_map.items() for score in scores],
        columns=["score", "group"]
    )

    # find the quartiles and IQR for each category
    groups = df.groupby('group')
    q1 = groups.quantile(q=0.25)
    q2 = groups.quantile(q=0.5)
    q3 = groups.quantile(q=0.75)
    iqr = q3 - q1
    upper = q3 + 1.5 * iqr
    lower = q1 - 1.5 * iqr

    # find the outliers for each category
    def outliers(group):
        cat = group.name
        return group[(group.score > upper.loc[cat]['score']) | (group.score < lower.loc[cat]['score'])]['score']

    out = groups.apply(outliers).dropna()

    # prepare outlier data for plotting, we need coordinates for every outlier.
    if not out.empty:
        outx = []
        outy = []
        for keys in out.index:
            outx.append(keys[0])
            outy.append(out.loc[keys[0]].loc[keys[1]])

    p = figure(tools="",
               y_axis_type="log",
               # background_fill_color="#efefef",
               x_range=cats, toolbar_location=None)

    # if no outliers, shrink lengths of stems to be no longer than the minimums or maximums
    qmin = groups.quantile(q=0.00)
    qmax = groups.quantile(q=1.00)
    upper.score = [min([x, y]) for (x, y) in zip(list(qmax.loc[:, 'score']), upper.score)]
    lower.score = [max([x, y]) for (x, y) in zip(list(qmin.loc[:, 'score']), lower.score)]

    # stems
    p.segment(cats, upper.score, cats, q3.score, line_color="black")
    p.segment(cats, lower.score, cats, q1.score, line_color="black")

    # boxes
    p.vbar(cats, 0.7, q2.score, q3.score, fill_alpha=50, fill_color="#E08E79", line_color="black")
    p.vbar(cats, 0.7, q1.score, q2.score, fill_color="#3B8686", line_color="black")

    # whiskers (almost-0 height rects simpler than segments)
    p.rect(cats, lower.score, 0.2, 0.01, line_color="black")
    p.rect(cats, upper.score, 0.2, 0.01, line_color="black")

    # outliers
    if not out.empty:
        p.circle(outx, outy, size=6, color="#F38630", fill_alpha=0.6)

    # p.xgrid.grid_line_color = None
    # p.ygrid.grid_line_color = "white"
    # p.grid.grid_line_width = 2
    # p.xaxis.major_label_text_font_size = "16px"
    return p


def generate_plot(title, x, x_y_map, x_label="", y_label=""):
    triplestores = x
    y = [round(x_y_map[triplestore]) if x_y_map[triplestore] else 0 for triplestore in triplestores]
    x_ticks = [i + .5 for i in range(len(x))]
    p = figure(x_range=triplestores,
               title=title,
               tools="save",
               toolbar_location=None
               #               tools="pan,wheel_zoom,box_zoom,reset,zoom_in,zoom_out,save",
               )
    source = ColumnDataSource(data=dict(x=x_ticks, x_label=x, y=y))
    labels = LabelSet(x='x', y='y', text='y', source=source,
                      y_offset=10, text_baseline='middle', text_align="center")
    p.xaxis.axis_label = x_label
    p.yaxis.axis_label = y_label
    p.vbar(source=source, x='x', top='y', width=0.7)
    p.add_layout(labels)
    return p


path = "/home/me/sync/Work/2020-11 Tentris Compression Benchmarks/selected_result_files_csv"
visualize(path + "/all_results.json", path + "/all_results.csv")
