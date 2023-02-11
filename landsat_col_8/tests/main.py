import argparse
import sys

from test1.export_samples_by_scene import run

'''
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--checkpoint_path', type=str,
                        help="Path to the dir where the checkpoints are stored")
    parser.add_argument('--image_path', type=str, help="Path to the input GeoTIFF image")
    parser.add_argument('--save_path', type=str, help="Path where the output map will be saved")
    args = parser.parse_args()
    #main(args.checkpoint_path, args.image_path, args.save_path)
'''

if __name__ == '__main__':
    globals()[sys.argv[1]]()