import pyvips
import glob

# export VIPS_DISC_THRESHOLD=100
# export VIPS_PROGRESS=1
# export VIPS_CONCURRENCY=1

files = sorted(glob.glob("/Users/jj/desktop/cddaraster/*.png"))
tiles = [pyvips.Image.new_from_file(f, access="sequential") for f in files]
mosaic = pyvips.Image.arrayjoin(tiles, across=43)
mosaic.write_to_file("/Users/jj/desktop/cddaraster-final/x.png")