import pyflac

src_wav = "../resources/test.flac"

dest_wav = "test.wav"

flac_decoder = pyflac.FileDecoder(src_wav, dest_wav)
flac_decoder.process()
