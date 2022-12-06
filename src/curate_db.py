#%%
import shutil
from datetime import datetime
import datetime as dt
from pathlib import Path

import pandas as pd
import yaml
from mouffet import file_utils, common_utils

from flac_converter import FlacConverter
import pyflac
from plotnine import *

arctic_root_path = Path(
    # "/mnt/win/UMoncton/OneDrive - Université de Moncton/Data/Reference/Arctic/Complete"
    "/mnt/win/UMoncton/Doctorat/data/acoustic/reference/Final"
)

# summer_root_path = Path(
#     "/mnt/win/UMoncton/Doctorat/data/dl_training/raw/full_summer_subset1"
# )

dest_root = Path(
    "/mnt/win/UMoncton/OneDrive - Université de Moncton/Data/Reference/Arctic/arctic_songs"
)

arch_dest_root = dest_root / "DataS1"
fig_dest_root = file_utils.ensure_path_exists(dest_root / "figures")
tmp_dest_root = file_utils.ensure_path_exists(dest_root / "tmp")


deployment_root_dir = Path("/mnt/win/UMoncton/OneDrive - Université de Moncton/Data")

reference_classes_path = Path(
    "/mnt/win/UMoncton/Doctorat/dev/dlbd/resources/reference_classes.csv"
)
bird_code_reference = Path(
    "/mnt/win/UMoncton/Doctorat/data/Bird codes/IBP-AOS-LIST22.csv"
)


sites = {
    "2018": [
        "BARW_0",
        "BARW_5",
        "CORI_1",
        "EABA_1",
        "IGLO_A",
        "IGLO_B",
        "IGLO_D",
        "IGLO_E",
        "IGLO_F",
        "PBPS_1",
        "PBPS_2",
    ],
    "2019": [
        "BARW_0",
        "BARW_2",
        "BARW_5",
        "BARW_8",
        "EABA_MC",
        "PBPS_1",
        "PBPS_2",
        "ZACK_1",
    ],
}

sites_rename = {"Barrow": "Utqiaġvik", "Polar Bear Pass": "Nanuit Ittilinga"}

plots_rename = {
    "BARW_0": "UTQI-0",
    "BARW_5": "UTQI-5",
    "BARW_2": "UTQI-2",
    "BARW_8": "UTQI-8",
    "PBPS_1": "NAIT-1",
    "PBPS_2": "NAIT-2",
}

exclude_sites = {"2018": ["BARW_8", "ZACK_2", "IGLO_H"], "2019": ["PCIS_1", "CABA_3"]}
exclude_files = {"2018": {"IGLO_E": ["5B344F30"]}}
TAG_EXCLUDE = ["Wind", "Rain"]
# exclude_sites = {"2018": [], "2019": []}

COLUMNS_DROP = [
    "Label",
    "tag",
    "Related",
    "background",
    "noise",
    "LabelTimeStamp",
    "Spec_NStep",
    "Spec_NWin",
    "LabelArea_DataPoints",
]
COLUMN_NAMES = {
    "LabelStartTime_Seconds": "start",
    "LabelEndTime_Seconds": "end",
    "overlap": "overlap",
    "Filename": "file_name",
    "tag_global": "tag",
    "related_global": "related",
    "MinimumFreq_Hz": "frequency_min",
    "MaximumFreq_Hz": "frequency_max",
    "MaxAmp": "amplitude_max",
    "MinAmp": "amplitude_min",
    "MeanAmp": "amplitude_mean",
    "AmpSD": "amplitude_sd",
}
COLUMN_ORDER = [
    "id",
    "file_name",
    "tag",
    "start",
    "end",
    "related",
    "overlap",
    "frequency_min",
    "frequency_max",
    "amplitude_min",
    "amplitude_max",
    "amplitude_mean",
    "amplitude_sd",
]


def extract_infos_2019(file_path):
    infos = {}
    date, time, rec_id = file_path.stem.split("_")
    full_date = datetime.strptime(f"{date}_{time}", "%Y%m%d_%H%M%S")
    infos["full_date"] = full_date
    infos["date"] = datetime.strptime(date, "%Y%m%d")
    infos["date_hour"] = datetime.strptime(full_date.strftime("%Y%m%d_%H"), "%Y%m%d_%H")
    infos["rec_id"] = rec_id
    infos["path"] = file_path
    return infos


def extract_infos_2018(file_path):
    infos = {}
    timestamp, rec_id = file_path.stem.split("_")
    full_date = datetime.fromtimestamp(int(timestamp, 16))
    infos["full_date"] = full_date
    infos["date"] = datetime.strptime(full_date.strftime("%Y%m%d"), "%Y%m%d")
    infos["date_hour"] = datetime.strptime(full_date.strftime("%Y%m%d_%H"), "%Y%m%d_%H")
    infos["rec_id"] = rec_id
    infos["path"] = file_path
    return infos


def clean_tags(
    tags_src_path,
    audio_file_name,
    ref_df,
    column_names=COLUMN_NAMES,
    to_drop=COLUMNS_DROP,
    exclude_tags=TAG_EXCLUDE,
):
    new_df = None
    if tags_src_path.exists():
        tags_df = pd.read_csv(tags_src_path)
        new_df = tags_df.merge(ref_df, left_on="Label", right_on="tag")
        new_df = new_df.loc[~new_df.tag.isin(exclude_tags)]
        new_df = (
            new_df.drop(
                to_drop,
                axis=1,
            )
            .rename(columns=column_names)
            .round(4)
            .sort_values(by=["start"])[COLUMN_ORDER]
        )
        new_df["file_name"] = audio_file_name

    else:
        common_utils.print_warning(
            f"Tag file {tags_src_path} does not exists. Creating empty one"
        )
        return

    return new_df


funcs = {"2018": extract_infos_2018, "2019": extract_infos_2019}

compress = True
overwrite = False
dest_dir = arch_dest_root
if compress:
    dest_dir /= "audio_annots"
    converter = FlacConverter()


years = [x for x in arctic_root_path.iterdir() if x.is_dir()]
tag_list = []

reference_df = pd.read_csv(reference_classes_path)[
    ["tag", "tag_global", "related_global"]
]

tmp_infos = []
for year in years:
    deployment_info = pd.read_excel(
        deployment_root_dir / f"sites_deployment_{year.name}.xlsx"
    )
    plots = [x for x in year.iterdir() if x.is_dir()]
    for plot in plots:
        if plot.stem not in exclude_sites[year.stem]:
            wav_list = []
            for ext in ["*.WAV", "*.wav"]:
                wav_list += list(plot.glob(ext))
            plot_info = (
                deployment_info.loc[deployment_info["plot"] == plot.name]
                .iloc[0]
                .to_dict()
            )
            for wav_file in wav_list:
                exclude = False
                if year.name in exclude_files:
                    if plot.name in exclude_files[year.name]:
                        for excl in exclude_files[year.name][plot.name]:
                            if excl in wav_file.stem:
                                common_utils.print_warning(f"Excluding file {wav_file}")
                                exclude = True
                if exclude:
                    continue
                print(wav_file)
                func = funcs[year.stem]
                infos = func(wav_file)
                if plot.name in plots_rename:
                    infos["plot"] = plots_rename[plot.name]
                else:
                    infos["plot"] = plot.name.replace("_", "-")

                infos["year"] = year.name
                site = plot_info["Site"]
                if site in sites_rename:
                    site = sites_rename[site]
                infos["site"] = site
                infos["deployment_start"] = plot_info["depl_start"]
                infos["deployment_end"] = plot_info["depl_end"]
                infos["latitude"] = round(float(plot_info["lat"]), 5)
                infos["longitude"] = round(float(plot_info["lon"]), 5)
                infos["substrate"] = plot_info["substrate"]
                infos["humidity"] = plot_info["humidity"]
                if plot_info["depl_start"] and not pd.isna(plot_info["depl_start"]):
                    if not (
                        (infos["full_date"] >= plot_info["depl_start"])
                        & (infos["full_date"] <= plot_info["depl_end"])
                    ):
                        raise ValueError("BLORP")
                tmp_infos.append(infos)
                if compress:
                    ext = "flac"
                else:
                    ext = "wav"
                audio_file_name = f'{year.name}_{infos["plot"]}_{infos["full_date"].strftime("%Y%m%d-%H%M%S")}_{infos["rec_id"]}.{ext}'
                wav_copy_dest = file_utils.ensure_path_exists(
                    dest_dir / audio_file_name,
                    is_file=True,
                )
                tags_src_path = wav_file.parent / f"{wav_file.stem}-sceneRect.csv"
                tags_dest_path = (
                    dest_dir
                    / f'{year.name}_{infos["plot"]}_{infos["full_date"].strftime("%Y%m%d-%H%M%S")}_{infos["rec_id"]}-tags.csv'
                )

                if not tags_dest_path.exists() or overwrite:
                    tags_df = clean_tags(
                        tags_src_path, audio_file_name, reference_df, COLUMN_NAMES
                    )
                    if tags_df is not None:
                        tags_df.to_csv(tags_dest_path, index=False)
                else:
                    tags_df = pd.read_csv(tags_dest_path)

                tags_df["site"] = infos["site"]

                tag_list.append(tags_df)

                if not wav_copy_dest.exists() or overwrite:
                    if compress:
                        print(f"Compressing {wav_file} into {wav_copy_dest}")
                        flac_converter = pyflac.FileEncoder(wav_file, wav_copy_dest)
                        flac_converter.process()
                    else:
                        shutil.copy(wav_file, wav_copy_dest)

arctic_infos_df = pd.DataFrame(tmp_infos)

all_tags = pd.concat(tag_list)

arctic_infos_df.to_csv(tmp_dest_root / "arctic_infos.csv", index=False)
all_tags.to_csv(tmp_dest_root / "all_tags.csv", index=False)

#%%


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

with open(tmp_dest_root / "arctic_summary.yaml", "w") as summary_file:
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

tag_details.to_csv(arch_dest_root / "annotations_details.csv", index=False)


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
arctic_sites.to_csv(arch_dest_root / "site_infos.csv", index=False)

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
    res = [(datetime(2018, 1, 1) + dt.timedelta(x)).strftime("%d-%m") for x in dates]
    print(res)
    return res


plt = (
    ggplot(data=plot_df, mapping=aes(x="julian", y="plot", color="plot"))
    + geom_point(aes(size="n_files"))
    + xlab("Day")
    + ylab("Plot")
    + facet_grid(". ~ year")
    # + theme(legend_position="none")
    + scale_colour_discrete(
        guide=False,
    )
    + scale_size_continuous(name="Number of\nrecordings")
    + scale_x_continuous(labels=label_x)
    + theme(legend_title=element_text(margin=legend_title_margin))
)

plt.save(fig_dest_root / "plots_days.png", width=10, height=8)
plt

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

all_tags_plot


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

tags_dur_plot
