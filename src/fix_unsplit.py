######
### This file is used to split annotation files that were previously not correctly split into 30 seconds chunks
######


import math
from pathlib import Path

import pandas as pd
from pysoundplayer.audio import Audio


src_root_dir = Path(
    "/mnt/win/UMoncton/OneDrive - Université de Moncton/Data/Reference/Arctic/Complete/"
)

audio_files = [
    src_root_dir / "2019/PBPS_2/20190622_160000.WAV",
    src_root_dir / "2019/PBPS_2/20190622_080000.WAV",
]

dest_dir = Path("split_folder")

duration = 30

for audio_file in audio_files:
    count = 1
    start = 0
    # * Load audio file
    audio = Audio(str(audio_file), sr=None, mono=True)

    # * Load annotation file
    tags_path = audio_file.parent / f"{audio_file.stem}-sceneRect.csv"
    tags_df = pd.read_csv(tags_path)

    if audio.duration > duration:
        # * Split file into the specified duration chunks
        for start in range(0, math.ceil(audio.duration), duration):
            # * Create destination paths for audio chunks and annotations files
            audio_dest_path = dest_dir / f"{audio_file.stem}_{count}.wav"
            tags_dest_path = dest_dir / f"{audio_file.stem}_{count}-sceneRect.csv"

            print("Writing file: ", audio_dest_path)
            end = min(start + duration, audio.duration)
            # * Get audio extract and save it
            extract = audio.get_extract(start, end, seconds=True)
            audio_dest_path.parent.mkdir(parents=True, exist_ok=True)
            audio.write(str(audio_dest_path), data=extract, sr=audio.sr)

            # * Get annotations and save them
            tmp_df = tags_df.loc[
                (tags_df.LabelStartTime_Seconds >= start)
                & (tags_df.LabelEndTime_Seconds < end)
            ].copy()
            tmp_df.LabelStartTime_Seconds -= start
            tmp_df.LabelEndTime_Seconds -= start
            tmp_df.to_csv(tags_dest_path, index=False)
            count += 1
