#%%
from pathlib import Path

import librosa
import numpy as np
from PIL import Image

import pandas as pd

# file_path = Path(
#     "/mnt/win/UMoncton/OneDrive - Université de Moncton/Data/Reference/Arctic/Complete/2018/BARW_0/5B29A710_1.WAV"
# )

# tags_path = Path(
#     "/mnt/win/UMoncton/OneDrive - Université de Moncton/Data/Reference/Arctic/Complete/2018/BARW_0/5B29A710_1-sceneRect.csv"
# )

file_path = Path(
    "//mnt/win/UMoncton/OneDrive - Université de Moncton/Data/Reference/Macauley/Priority/3114_SESA.wav"
)

tags_path = Path(
    "//mnt/win/UMoncton/OneDrive - Université de Moncton/Data/Reference/Macauley/Priority/labels/3114_SESA-sceneRect.csv"
)


tags_df = pd.read_csv(tags_path)


wav, sr = librosa.load(str(file_path), sr=None)


def resize_spectrogram(spectrogram, size, resample_method="bicubic"):
    img = Image.fromarray(spectrogram)
    if hasattr(Image, resample_method.upper()):
        resample_method = getattr(Image, resample_method.upper())
    else:
        resample_method = Image.BICUBIC
    img = img.resize(size, resample=resample_method)
    return np.array(img)


spec = librosa.stft(wav)
spec = librosa.power_to_db(np.abs(spec) ** 2, ref=np.max)
resize_width = int(100 * (len(wav) / sr))

rspec = resize_spectrogram(spec, (resize_width, spec.shape[0]))

#%%
import librosa.display
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

fig, ax = plt.subplots()
librosa.display.specshow(rspec, ax=ax)


def freq2pixels(freq, sr, height):
    res = 0
    max_freq = sr / 2
    freq_step = height / max_freq
    res = freq * freq_step
    return res


res = []

for tag in tags_df.itertuples():
    print(freq2pixels(tag.MinimumFreq_Hz, sr, rspec.shape[0]))
    xstart = int(tag.LabelStartTime_Seconds * 100)
    xend = int(tag.LabelEndTime_Seconds * 100)
    ystart = int(freq2pixels(tag.MinimumFreq_Hz, sr, rspec.shape[0]))
    yend = int(freq2pixels(tag.MaximumFreq_Hz, sr, rspec.shape[0]))
    tag_spec = rspec[ystart:yend, xstart:xend]
    print(
        f"Processing tag {tag.Label} with background={tag.background} and mean sound level {tag_spec.mean()}"
    )
    res.append(
        {
            "tag": tag.Label,
            "bg": tag.background,
            "mean": tag_spec.mean(),
            "max": tag_spec.max(),
            "median": np.median(tag_spec),
        }
    )
    ax.add_patch(
        Rectangle(
            (
                xstart,
                ystart,
            ),
            xend - xstart,
            yend - ystart,
            edgecolor="yellow",
            fill=False,
        )
    )
    # TODO: remove overlaps


bg_summary = pd.DataFrame(res)

# ax.add_patch(Rectangle((200, freq2pixels(4000, sr, rspec.shape[0])), 1000, 50))

# plt.colorbar()
