import pandas as pd
from utils import print_warning

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
    "MaxAmp",
    "MinAmp",
    "MeanAmp",
    "AmpSD",
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
]

TAG_EXCLUDE = ["Wind", "Rain"]


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
        print_warning(f"Tag file {tags_src_path} does not exists. Creating empty one")
        return

    return new_df
