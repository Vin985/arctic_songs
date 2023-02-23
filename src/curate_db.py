#%%
import shutil
from pathlib import Path

import pandas as pd
import pyflac
from utils import (
    ensure_path_exists,
    print_warning,
    extract_infos_2018,
    extract_infos_2019,
)
from tag_utils import clean_tags, COLUMN_NAMES

## Paths
src_root_path = Path("/mnt/win/UMoncton/Doctorat/data/acoustic/reference/Final")
dest_root = ensure_path_exists(Path("results/arctic_songs"))
deployment_root_dir = Path("/mnt/win/UMoncton/OneDrive - Université de Moncton/Data")
reference_classes_path = Path("../resources/reference_classes.csv")
bird_code_reference = Path("../resources/IBP-AOS-LIST22.csv")


archive_dest_root = dest_root / "DataS1"
fig_dest_root = ensure_path_exists(dest_root / "figures")
tmp_dest_root = ensure_path_exists(dest_root / "tmp")


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

# exclude_sites = {"2018": [], "2019": []}

funcs = {"2018": extract_infos_2018, "2019": extract_infos_2019}

compress = True
overwrite = False
dest_dir = archive_dest_root
if compress:
    dest_dir /= "audio_annots"


years = [x for x in src_root_path.iterdir() if x.is_dir()]
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
                                print_warning(f"Excluding file {wav_file}")
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
                wav_copy_dest = ensure_path_exists(
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
