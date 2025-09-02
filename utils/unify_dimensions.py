import os
import xarray as xr
import argparse

"""
This script is designed to take a netcdf file that has duplicate dimensions with different names and unify them.  For example, if a dataset has dimensions dum1 and dum2, where the name differ but the values of their corresponding coordinates are the same, this script replaces all instances of dum2 with dum1.
"""

parser = argparse.ArgumentParser(description='Provide files to input/output and dimension names to use')
parser.add_argument('--input_file', type=str,
                    help='a string specifying the input file')
parser.add_argument('--output_file', type=str, default=None,
                    help='a string specifying the output file')
parser.add_argument('--new_dim', type=str, default=None,
                    help='specify the dimension name to use across variables')
parser.add_argument('--old_dim',   type=str, default=None,
                    help='specify the dimension name to purge')
parser.add_argument('--drop_vars', type=str, nargs='+', default=None,
                    help='specify a list of variables to drop')
args = parser.parse_args()

if args.output_file is None:
    output_file = args.input_file
    dirname     = os.path.dirname(args.input_file)
    input_file  = os.path.join(dirname, 'temp.nc')
    os.system('mv ' + output_file + ' ' + input_file)
else:
    input_file  = args.input_file
    output_file = args.output_file

if args.new_dim is None:
    new_dim = 'cell'
else:
    new_dim = args.new_dim

if args.old_dim is None:
    old_dim = 'ncol'
else:
    old_dim = args.old_dim

if args.drop_vars is None:
    drop_vars = list()
else:
    drop_vars = args.drop_vars

def unify_dimensions(input_file, new_dim='cell', old_dim='ncol',
                     drop_variables=list()):
    with xr.open_dataset(input_file) as ds:
        # create new dataset to output
        new_ds = xr.Dataset()
        # set the new
        new_dim_coord = ds[new_dim]
        # loop through the variables and replace the old_dim with the new_dim
        for variable in ds.variables:
            if old_dim in ds[variable].dims:
                new_coords = dict()
                new_dims   = list()
                new_attrs  = ds[variable].attrs
                for dimension in ds[variable].dims:
                    if dimension == old_dim:
                        new_coords[new_dim] = new_dim_coord
                        new_dims.append(new_dim)
                    else:
                        new_coords[dimension] = ds[variable].coords[dimension]
                        new_dims.append(dimension)
                new_ds[variable] = xr.DataArray(ds[variable].values,
                                                coords=new_coords,
                                                dims=new_dims,
                                                attrs=new_attrs)
            else:
                # Include an option to drop variables that aren't needed.
                if variable not in drop_variables:
                    new_ds[variable] = ds[variable]
        new_ds.attrs.update(ds.attrs)
    return new_ds


new_ds = unify_dimensions(input_file, new_dim=new_dim, old_dim=old_dim,
                          drop_variables=drop_vars)

new_ds.to_netcdf(output_file, mode='w')

temporary_file = os.path.join(os.path.dirname(args.input_file), 'temp.nc')
if os.path.isfile(temporary_file):
    os.system('rm -f ' + temporary_file)


