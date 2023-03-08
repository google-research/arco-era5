#!/usr/bin/env python
"""Script to merge AR ML datasets into CO ML dataset

This script contains the Metview workflow that will merge the 
AR ML datasets into a single ML dataset.  It will also create several 
commonly used derived variables (u/v winds, dewpoint temperature, relative humidity, 
geopotential, and pressure) on the model levels).  This script produces 
a Gaussian gridded dataset and a regular lat-lon dataset.  
"""
import xarray

def attribute_fix(ds):
    """Needed to fix a low-level bug in ecCodes.
    
    Sometimes, shortNames get overloaded in ecCodes's table. 
    To eliminate ambiguity in their string matching, we
    force ecCodes to make use of the paramId, which is a
    consistent source-of-truth.
    """
    for var in ds:
        attrs = ds[var].attrs
        result = attrs.pop('GRIB_cfName', None)
        result = attrs.pop('GRIB_cfVarName', None)
        result = attrs.pop('GRIB_shortName', None)
        ds[var].attrs.update(attrs)
    return ds


def compute_gg_ll_dataset(ml_surface, ml_wind, ml_moisture, datestring):
   """Merges/processes AR ML dataset to a CO ML dataset
   
   This function takes a datestamp and the three AR model-level ERA5 
   datasets to produce a unified model-level dataset on a Gaussian Grid
   and a regular lat-lon grid for a single hour.
   """
   import metview as mv

   surface_slice = ml_surface.sel(time=slice(datestring,datestring)).compute()
   wind_slice = ml_wind.sel(time=slice(datestring,datestring)).compute()
   moisture_slice = ml_moisture.sel(time=slice(datestring,datestring)).compute()

   # Fix the fieldsets
   # WARNING:  Metview creates temporary files for each fieldset variable.  This
   # can use a lot of local disk space.  It's important to delete fieldset
   # variables once they are no longer necessary to free up disk space.
   wind_fieldset = mv.dataset_to_fieldset(attribute_fix(wind_slice).squeeze())
   surface_fieldset = mv.dataset_to_fieldset(attribute_fix(surface_slice).squeeze())
   moist_fieldset = mv.dataset_to_fieldset(attribute_fix(moisture_slice).squeeze())

   # Translate spectral surface fields into Gaussian Grid
   surface_gg = mv.read(data=surface_fieldset,grid='N320')
   surface_gg.describe()
   lnsp_gg = surface_gg.select(shortName="lnsp")
   zs_gg = surface_gg.select(shortName="z")
  
   # Compute pressure values at model levels
   pres_gg = mv.unipressure(surface_gg.select(shortName="lnsp"))

   # Convert spectral model level data to Gaussian Grids
   wind_gg = mv.read(data=wind_fieldset,grid='N320')
   t_gg = wind_gg.select(shortName='t')

   q_gg = moist_fieldset.select(shortName='q')
   r_gg = mv.relative_humidity_from_specific_humidity(t_gg,q_gg,lnsp_gg)
   td_gg = mv.dewpoint_from_specific_humidity(q_gg,lnsp_gg)

   moist_fieldset = mv.merge(moist_fieldset,r_gg,td_gg)
   # Deleting unnecessary fieldset variables to save space.
   del r_gg
   del td_gg


   # Compute geopotential on model levels
   zm_gg = mv.mvl_geopotential_on_ml(t_gg, q_gg, lnsp_gg, zs_gg)
   pz_fieldset = mv.merge(pres_gg, zm_gg)
   # Merging fieldset variables creates a new temporary file, so we should delete
   # the individual variables now to save space.  
   del surface_fieldset
   del t_gg
   del q_gg
   del lnsp_gg
   del zs_gg
   del pres_gg
   del zm_gg


   # Compute the u/v wind elements from d/vo and then translate to a Gaussian grid
   uv_wind_spectral = mv.uvwind(data=wind_fieldset,truncation=639)
   uv_wind_gg = mv.read(data=uv_wind_spectral,grid='N320')
   del uv_wind_spectral
   del wind_fieldset

   u_wind_gg = uv_wind_gg.select(shortName='u')
   v_wind_gg = uv_wind_gg.select(shortName='v')

   speed_gg = mv.speed(u_wind_gg,v_wind_gg)
   del u_wind_gg
   del v_wind_gg

   # The next step creates the largest temporary file in this workflow since it's bringing
   # all of the fieldset variables together.  We'll delete the individual fieldsets once the 
   # merge is complete to save space.  
   gg_fieldset = mv.merge(wind_gg, speed_gg, uv_wind_gg, pz_fieldset, moist_fieldset)
   del wind_gg
   del speed_gg
   del uv_wind_gg
   del pz_fieldset
   del moist_fieldset
   
   print("About to gg_fieldset.to_dataset()")
   gg_dataset = gg_fieldset.to_dataset()

   
   # Interpolate the Gaussian Grid fieldset to a regular lat-lon grid
   ll_fieldset = mv.read(data=gg_fieldset, grid=[0.25, 0.25])
   ll_dataset = ll_fieldset.to_dataset()

    
   # To err on the side of caution, we're deleting all of the remaining 
   # fieldset variables to save disk space.  
   del surface_gg
   del ll_fieldset
   del gg_fieldset
   
   return gg_dataset, ll_dataset
   

def main(): 
   ml_surface = xarray.open_zarr(
       'gs://gcp-public-data-arco-era5/co/single-level-surface.zarr/',
       chunks={'time': 48},
       consolidated=True,
   )
   ml_wind = xarray.open_zarr(
       'gs://gcp-public-data-arco-era5/co/model-level-wind.zarr/',
       chunks={'time': 48},
       consolidated=True,
   )
   ml_moisture = xarray.open_zarr(
       'gs://gcp-public-data-arco-era5/co/model-level-moisture.zarr/',
       chunks={'time': 48},
       consolidated=True,
   )

   datestring="2017-10-09T06"

   gg_dataset, ll_dataset = compute_gg_ll_dataset(ml_surface, ml_wind, ml_moisture, datestring)

   print(gg_dataset)

if __name__ == "__main__":
    main()
