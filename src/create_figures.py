#%%

import datetime as dt
from pathlib import Path

import pandas as pd
import yaml
from mouffet import file_utils
from plotnine import (
    aes,
    coord_flip,
    element_text,
    facet_grid,
    geom_bar,
    geom_point,
    ggplot,
    scale_color_discrete,
    scale_fill_brewer,
    scale_size,
    scale_size_continuous,
    scale_x_continuous,
    scale_y_continuous,
    theme,
    theme_classic,
    xlab,
    ylab,
)

## Paths

dest_root = Path(
    "/mnt/win/UMoncton/OneDrive - Université de Moncton/Data/Reference/Arctic/arctic_songs"
)
tmp_dest_root = file_utils.ensure_path_exists(dest_root / "tmp")
archive_dest_root = dest_root / "DataS1"
arctic_infos_df = pd.read_csv(tmp_dest_root / "arctic_infos.csv")
all_tags = pd.read_csv(tmp_dest_root / "all_tags.csv")
bird_code_reference = Path("../resources/IBP-AOS-LIST22.csv")
fig_dest_root = file_utils.ensure_path_exists(dest_root / "figures")
tmp_dest_root = file_utils.ensure_path_exists(dest_root / "tmp")

arctic_summary = {}

arctic_summary["n_files"] = arctic_infos_df.shape[0]
arctic_summary["file_duration"] = 30
arctic_summary["total_duration"] = (
    arctic_summary["file_duration"] * arctic_summary["n_files"]
)
for col in ["year", "site", "plot", "date"]:
    arctic_summary[f"n_unique_{col}s"] = len(arctic_infos_df[col].unique())
    arctic_summary[f"{col}s"] = arctic_infos_df[col].unique().tolist()

arctic_summary["n_plots_total"] = (
    arctic_infos_df.groupby(["year", "plot"]).count().shape[0]
)


arctic_summary["n_annotations"] = all_tags.shape[0]
arctic_summary["n_classes"] = len(all_tags.tag.unique())

arctic_summary["n_dates_by_year"] = {}

with open(tmp_dest_root / "arctic_summary.yaml", "w", encoding="UTF-8") as summary_file:
    yaml.dump(arctic_summary, summary_file, default_flow_style=False)


#%%

# Tag details

tag_summary = {}

birds_df = pd.read_csv(bird_code_reference)


tag_summary["n_annotations"] = all_tags.shape[0]
tag_summary["n_classes"] = len(all_tags.tag.unique())

tag_details = all_tags[["tag", "related"]].drop_duplicates().sort_values("tag")
tag_details = (
    tag_details.merge(
        birds_df[["SPEC", "COMMONNAME", "SCINAME"]],
        left_on="tag",
        right_on="SPEC",
        how="left",
    )
    .drop(columns=["SPEC"])
    .rename(columns={"COMMONNAME": "common_name", "SCINAME": "latin_name"})
)

tag_details.to_csv(archive_dest_root / "annotations_details.csv", index=False)


#%%


# Table 1
arctic_sites = (
    arctic_infos_df[
        [
            "plot",
            "year",
            "site",
            "deployment_start",
            "deployment_end",
            "latitude",
            "longitude",
            "substrate",
            "humidity",
        ]
    ]
    .drop_duplicates(subset=["year", "plot", "site"])
    .reset_index(drop=True)
)
arctic_sites.to_csv(archive_dest_root / "site_infos.csv", index=False)

tmp_df = arctic_sites[["year", "site", "plot", "latitude", "longitude"]].reset_index(
    drop=True
)
tmp_df = tmp_df.rename(
    columns={
        "year": "Year",
        "site": "Site",
        "plot": "Plot",
        "latitude": "Latitude",
        "longitude": "Longitude",
    }
)
# with open(fig_dest_root / "table1.txt", "w", encoding="utf8") as f:

tmp_df.style.hide(axis="index").to_latex(
    buf=fig_dest_root / "table1.txt",
    column_format="ccccc",
    caption="Study site locations, years, and plot names where acoustic recordings were collected across the Arctic",
    hrules=True,
    # header=["Year", "Site", "Plot", "Latitude", "Longitude"],
    position="h!",
    label="table_sites",
)


#%%
legend_title_margin = {"b": 15}

arctic_infos_df["date"] = pd.to_datetime(arctic_infos_df["date"])
arctic_infos_df["year"] = arctic_infos_df.date.dt.year
arctic_infos_df["julian"] = arctic_infos_df.date.dt.dayofyear


plot_df = (
    arctic_infos_df[["year", "plot", "julian", "date_hour"]]
    .groupby(["year", "plot", "julian"])
    .agg(n_files=("date_hour", lambda x: x.unique().shape[0]))
    .reset_index()
)

plot_df["plot"] = plot_df["plot"].astype("category")
plot_df["plot"] = plot_df["plot"].cat.set_categories(
    plot_df["plot"].cat.categories.sort_values(ascending=False)
)


def label_x(dates):
    res = [(dt.datetime(2018, 1, 1) + dt.timedelta(x)).strftime("%d-%m") for x in dates]
    print(res)
    return res


plt = (
    ggplot(data=plot_df, mapping=aes(x="julian", y="plot", color="plot"))
    + geom_point(aes(size="n_files"))
    + xlab("Day")
    + ylab("Plot")
    + facet_grid(". ~ year")
    # + theme(legend_position="none")
    + scale_color_discrete(
        guide=False,
    )
    + scale_size_continuous(name="Number of\nrecordings")
    + scale_x_continuous(labels=label_x)
    + theme(legend_title=element_text(margin=legend_title_margin))
)

plt.save(fig_dest_root / "plots_days.png", width=10, height=8)
print(plt)

#%%

# Tags repartition

tags_plot_df = all_tags.copy()
tags_plot_df["tag"] = tags_plot_df["tag"].astype("category")
tag_order = tags_plot_df["tag"].value_counts().index.tolist()
tag_order.reverse()
tags_plot_df["tag"].cat.set_categories(tag_order, inplace=True)


all_tags_plot = (
    ggplot(data=tags_plot_df, mapping=aes(x="tag", fill="site"))
    + geom_bar()  # width=0.8, position=position_dodge(width=0.9))
    + coord_flip()
    + ylab("Count")
    + xlab("Class")
    # + scale_x_discrete(expand=(0, 0))
    + scale_fill_brewer(
        name="Deployment site", type="div", palette="RdYlBu", direction=-1
    )
    + scale_y_continuous(expand=(0, 0, 0.1, 0))
    + theme_classic()
    + theme(
        # axis_text_y=element_text(vjust=1, hjust=1),
        figure_size=(20, 8),
        legend_position=((0.8, 0.3)),
        legend_title=element_text(margin=legend_title_margin),
    )
)

all_tags_plot.save(fig_dest_root / "tag_repartition.png", width=10, height=8)

print(all_tags_plot)


#%%
tags_plot_df["duration"] = tags_plot_df["end"] - tags_plot_df["start"]

tags_dur_df = (
    tags_plot_df.groupby(["site", "tag"]).agg({"duration": "sum"}).reset_index()
)

dur_order = (
    tags_dur_df.groupby("tag")
    .agg({"duration": "sum"})
    .reset_index()
    .sort_values("duration")
    .tag.tolist()
)

tags_dur_df["tag"] = tags_dur_df["tag"].cat.set_categories(dur_order)
tags_dur_df.duration = tags_dur_df.duration.astype("float")
tags_dur_plot = (
    ggplot(data=tags_dur_df, mapping=aes(x="tag", y="duration", fill="site"))
    + geom_bar(stat="sum")  # width=0.8, position=position_dodge(width=0.9))
    + coord_flip()
    + theme_classic()
    + ylab("Duration (s)")
    + xlab("Class")
    # + scale_x_discrete(expand=(0, 0))
    + scale_y_continuous(expand=(0, 0, 0.1, 0))
    + scale_fill_brewer(
        name="Deployment site", type="div", palette="RdYlBu", direction=-1
    )
    + scale_size(guide=None)
    + theme(
        # axis_text_y=element_text(vjust=1, hjust=1),
        figure_size=(20, 8),
        legend_position=((0.8, 0.3)),
        legend_title=element_text(margin=legend_title_margin),
    )
)

tags_dur_plot.save(fig_dest_root / "tag_duration.png", width=10, height=8)

print(tags_dur_plot)
