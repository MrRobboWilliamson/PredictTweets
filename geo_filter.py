import pandas as pd
import os

'''
This script opens the geo twitter data and filters for the tweets by a bounding box
'''

COLS = ['id', 'time_stamp', 'coordinates', 'bbox']
LOCCOLS = ['coordinates', 'bbox']
COORD_PATTERN = r"(-?\d+\.\d+)"

class ConsoleBar:
    def __init__(self, num_ticks):
        # check that end - start is greater than one and that they are both integers
        if type(num_ticks) is not int:
            raise TypeError('arg "num_ticks" must be type int')
        if num_ticks < 1:
            raise ValueError("num_ticks not > 0")

        #  get the absolute size and normalise
        self.num_ticks = num_ticks
        self.ticker = 0

        # start the ticker
        print('\r   {:>3.0f}%[{: <101}]'.format(0, '='*0+'>'), end='\r')

    def tick(self, step=1):
        print_end = '\r'
        self.ticker += step
        if self.ticker > self.num_ticks:
            self.ticker = self.num_ticks
            # print a warning
            print('Warning: The ticker is overreaching and has been capped at the end point', end='\r')
        elif self.ticker + step == self.num_ticks:
            print_end = ''

        progress = int((self.ticker/self.num_ticks)*100)
        print('\r   {:>3.0f}%[{: <101}]'.format(progress, '='*progress+'>'), end=print_end)


def read_data(filename, columns=COLS):
    '''
    read the data as a csv
    path is the location of the file and file name
    '''
    # read in the data add the column headers and set the id as the index
    df = pd.read_csv(filename, sep='|', header=None)
    df.columns = columns
    return df.set_index('id', drop=True)

def get_coords(df, col='coordinates'):
    '''
    Use regex pattern and extract to get the coordinates from the data
    '''
    
    try:
        coords = df[col].str.extractall(COORD_PATTERN).unstack(['match']).astype(float)
        coords.columns = ['longitude', 'latitude']
        coords['type'] = 'point'

        # concatenate with the original data and drop the location columns
        df = df.drop(LOCCOLS, axis=1)
        return pd.concat([df, coords], axis=1)

    except Exception as ex:
        print("Error in get_coords:")        
        print(ex)

def get_bbox_centroid(df, col='bbox'):
    '''
    Making a big, but convenient assumption here that the centroid of the bbox is good enough

    From observing the strings the bbox has long-lat coordinates with the following points:
    - 0 bottom-left
    - 1 top-left
    - 2 top-right
    - 3 bottom-right
    '''
    try:
        # get the coords from bbox string
        columns = ["bottom_left_long", "bottom_left_lat",
            "top_left_long", "top_left_lat",
            "top_right_long", "top_right_lat",
            "bottom_right_long", "bottom_right_lat"]
        box_bounds = df[col].str.extractall(COORD_PATTERN).unstack(['match']).astype(float)

        box_bounds.columns = columns
        
        # calculate the centriods as the average of the bottom left and top right
        longs = box_bounds.loc[:, ['bottom_left_long', 'top_right_long']]
        lats = box_bounds.loc[:, ['bottom_left_lat', 'top_right_lat']]
        centroids = dict()
        centroids['longitude'] = longs.mean(axis=1)
        centroids['latitude'] = lats.mean(axis=1)
        centroids = pd.DataFrame(centroids)
        centroids['type'] = 'centroid'

        # concatenate with the original data and drop the location columns
        df = df.drop(LOCCOLS, axis=1)
        return pd.concat([df, centroids], axis=1)

    except Exception as ex:
        print("Error in get_bbox_centroid:")
        print(ex)

def get_preference(df, preference=LOCCOLS[0]):
    '''
    To speed up processing first assess if there are LOCCOLS[0] use those.
    Only use LOCCOLS[1] where LOCCOLS[0] is null.
    '''
    bbox = df[df[preference].isnull()]
    coords = df[df[preference].notnull()]

    # process and concatenate these
    bbox_coords = get_bbox_centroid(bbox)
    coordinates = get_coords(coords)
    return pd.concat([bbox_coords, coordinates])

def filter_bybox(df, longcol, latcol, bounds):
    """
    return the df filtered by a bounds
    bounds is a tuple with two tuples containing the lower-left and upper-right bounds
    longcol and latcol are the columns in df
    """
    return df[(df[longcol]>bounds[0][0]) &
              (df[longcol]<bounds[1][0]) &
              (df[latcol]>bounds[0][1]) &
              (df[latcol]<bounds[1][1])]


def main():
    # do things
    current_loc = os.getcwd()
    print("You are working from", current_loc)
    input_path = input("Where are the files to process? ")
    full_input = os.path.join(current_loc, input_path)
    if not os.path.exists(full_input):
        print(full_input)
        raise Exception("Location does not exist!")

    output_path = input("Where do you want to save the results? ")
    full = os.path.join(current_loc, output_path)
    if not os.path.exists(full):
        if input("  - path does not exist, would you like to create it? [y/other] ").lower()[0] == 'y':
            os.mkdir(full)
        else:
            raise Exception("bye!!")

    print("Please enter the lower left and upper right corners of the bounds you desire:")    
    ll_lng = float(input("  - lower left longitude: "))
    ll_lat = float(input("  - lower left latitude: "))
    ur_lng = float(input("  - upper right longitude: "))
    ur_lat = float(input("  - upper right latitude: "))
    bounds = ((ll_lng, ll_lat), (ur_lng, ur_lat))

    # go through the list of files in the input path and filter for the geo location input

    # lets use the console progress bar to see our progress
    # get the number of files to process for the progress bar
    files = os.listdir(input_path)
    bar = ConsoleBar(len(files))
    for filename in files:
        # only use the "part* files"    
        try:
            # read the file
            if filename[:4] == "part":
                df = read_data(os.path.join(input_path, filename))

                # get the coordinates from the data
                coords = get_preference(df)
                
                # filter for the des
                filtered = filter_bybox(coords, 'longitude', 'latitude', bounds)

                # save the result in the output folder
                out_name = filename + "_filtered.csv"
                filtered.to_csv(os.path.join(output_path, out_name))
                bar.tick()

        except Exception as ex:
            print(ex)
            print(os.path.join(current_loc, input_path, filename))
            break

if __name__ == "__main__":
    main()




    