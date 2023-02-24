#%%
import shutil
import pyflac
from pathlib import Path

import pandas as pd
from pysoundplayer.audio import Audio

from tag_utils import COLUMN_NAMES, clean_tags
from utils import (
    ensure_path_exists,
    extract_infos_2018,
    extract_infos_2019,
    print_warning,
    print_info,
)

## Paths
src_root_path = Path("/mnt/win/UMoncton/Doctorat/data/acoustic/reference/Final")
dest_root = ensure_path_exists(
    "/mnt/win/UMoncton/OneDrive - Université de Moncton/Data/Reference/Arctic/arctic_songs"
)
deployment_root_dir = Path("/mnt/win/UMoncton/OneDrive - Université de Moncton/Data")
reference_classes_path = Path("resources/reference_classes.csv")
bird_code_reference = Path("resources/IBP-AOS-LIST22.csv")


archive_dest_root = dest_root / "DataS1"
fig_dest_root = ensure_path_exists(dest_root / "figures")
tmp_dest_root = ensure_path_exists(dest_root / "tmp")
uncompressed_dest_root = ensure_path_exists(dest_root / "uncompressed")


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

overwrite = False
dest_dir = archive_dest_root / "audio_annots"
uncompressed_dir = ensure_path_exists(dest_root / "uncompressed")


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
            plot_depl_info = (
                deployment_info.loc[deployment_info["plot"] == plot.name]
                .iloc[0]
                .to_dict()
            )
            # * Rename sites if needed
            site = plot_depl_info["Site"]
            if site in sites_rename:
                site = sites_rename[site]

            # * Compile information about the site from the deployment information
            plot_infos = {}
            plot_infos["site"] = site
            plot_infos["deployment_start"] = plot_depl_info["depl_start"]
            plot_infos["deployment_end"] = plot_depl_info["depl_end"]
            plot_infos["latitude"] = round(float(plot_depl_info["lat"]), 5)
            plot_infos["longitude"] = round(float(plot_depl_info["lon"]), 5)
            plot_infos["substrate"] = plot_depl_info["substrate"]
            plot_infos["humidity"] = plot_depl_info["humidity"]
            # * Rename plots if needed
            if plot.name in plots_rename:
                plot_infos["plot"] = plots_rename[plot.name]
            else:
                plot_infos["plot"] = plot.name.replace("_", "-")

            plot_infos["year"] = year.name

            # if plot_depl_info["depl_start"] and not pd.isna(
            #     plot_depl_info["depl_start"]
            # ):
            #     if not (
            #         (plot_infos["full_date"] >= plot_depl_info["depl_start"])
            #         & (plot_infos["full_date"] <= plot_depl_info["depl_end"])
            #     ):
            #         raise ValueError("BLORP")
            for wav_file in wav_list:
                # * Check if the current file is in the excluded list
                exclude = False
                if year.name in exclude_files:
                    if plot.name in exclude_files[year.name]:
                        for excl in exclude_files[year.name][plot.name]:
                            if excl in wav_file.stem:
                                print_warning(f"Excluding file {wav_file}")
                                exclude = True
                                break
                if exclude:
                    # * If excluded, skip to next one
                    continue
                print_info(f"Processing {wav_file}")

                # * Get the date extraction function based on the year the data was collected
                func = funcs[year.stem]
                file_infos = func(wav_file)
                file_infos.update(plot_infos)

                tmp_infos.append(file_infos)
                audio_file_name_root = f'{year.name}_{file_infos["plot"]}_{file_infos["full_date"].strftime("%Y%m%d-%H%M%S")}_{file_infos["rec_id"]}'
                flac_file_name = audio_file_name_root + ".flac"
                wav_file_name = audio_file_name_root + ".wav"

                flac_copy_dest = ensure_path_exists(
                    dest_dir / flac_file_name,
                    is_file=True,
                )
                wav_copy_dest = ensure_path_exists(
                    uncompressed_dest_root / wav_file_name,
                    is_file=True,
                )
                tags_src_path = wav_file.parent / f"{wav_file.stem}-sceneRect.csv"
                tags_file_name = f'{year.name}_{file_infos["plot"]}_{file_infos["full_date"].strftime("%Y%m%d-%H%M%S")}_{file_infos["rec_id"]}-tags.csv'
                tags_dest_path = dest_dir / tags_file_name

                if not tags_dest_path.exists() or overwrite:
                    tags_df = clean_tags(
                        tags_src_path, flac_file_name, reference_df, COLUMN_NAMES
                    )
                    if tags_df is not None:
                        tags_df.to_csv(tags_dest_path, index=False)
                else:
                    tags_df = pd.read_csv(tags_dest_path)

                tags_df["site"] = file_infos["site"]

                tag_list.append(tags_df)

                if not wav_copy_dest.exists() or overwrite:
                    # * Remove human voices
                    human_tags = tags_df[tags_df.tag == "Human"]
                    if not human_tags.empty:
                        print(f"Humans in {wav_file}")
                        audio = Audio(wav_file)
                        for humans in human_tags.itertuples():
                            duration = humans.end - humans.start
                            noise_start = 0
                            noise_end = noise_start + duration
                            for tag in tags_df.itertuples():
                                print(f"tag_eng: {tag.end} noise_end: {noise_end}")
                                is_overlapping = (
                                    min(tag.end, noise_end)
                                    - max(tag.start, noise_start)
                                ) > 0
                                if is_overlapping:
                                    print_warning("overlapping")
                                    noise_start = tag.end
                                    noise_end = noise_start + duration
                            if not is_overlapping and noise_end < audio.duration:
                                # * Replace human voices by random noise from the same extract
                                audio.set_extract(
                                    audio.get_extract(
                                        noise_start, noise_end, seconds=True
                                    ),
                                    humans.start,
                                    humans.end,
                                    seconds=True,
                                )
                                audio.write(
                                    wav_copy_dest,
                                )
                                # * Change tags to make sure no humans are present anymore
                                tags_df.tag.loc[
                                    tags_df.tag == "Human"
                                ] = "Artificial Noise"
                                tags_df.to_csv(
                                    Path("results/no_humans/") / tags_file_name
                                )
                            # shutil.copy(
                            #     wav_file,
                            #     ensure_path_exists(
                            #         Path("results/no_humans/")
                            #         / (audio_file_name_root + "_original.wav"),
                            #         is_file=True,
                            #     ),
                            # )
                    else:
                        shutil.copy(wav_file, wav_copy_dest)

                if not flac_copy_dest.exists() or overwrite:
                    print(f"Compressing {wav_copy_dest} into {flac_copy_dest}")
                    flac_converter = pyflac.FileEncoder(wav_copy_dest, flac_copy_dest)
                    flac_converter.process()


arctic_infos_df = pd.DataFrame(tmp_infos)

all_tags = pd.concat(tag_list)

arctic_infos_df.to_csv(tmp_dest_root / "arctic_infos.csv", index=False)
all_tags.to_csv(tmp_dest_root / "all_tags.csv", index=False)
