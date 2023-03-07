#!/usr/bin/env python

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
   """
   This function takes a datestamp and the three AR model-level ERA5 
   datasets to produce a unified model-level dataset on a Gaussian Grid
   and a regular lat-lon grid for a single hour.
   """
   import metview as mv

   surface_slice = ml_surface.sel(time=slice(datestring,datestring)).compute()
   wind_slice = ml_wind.sel(time=slice(datestring,datestring)).compute()
   moisture_slice = ml_moisture.sel(time=slice(datestring,datestring)).compute()

   #fix the fieldsets
   wind_fieldset = mv.dataset_to_fieldset(attribute_fix(wind_slice).squeeze())
   surface_fieldset = mv.dataset_to_fieldset(attribute_fix(surface_slice).squeeze())
   moist_fieldset = mv.dataset_to_fieldset(attribute_fix(moisture_slice).squeeze())

   #Translate spectral surface fields into Gaussian Grid
   surface_gg = mv.read(data=surface_fieldset,grid='N320')
   surface_gg.describe()
   lnsp_gg = surface_gg.select(shortName="lnsp")
   zs_gg = surface_gg.select(shortName="z")
  
   #compute pressure values at model levels
   pres_gg = mv.unipressure(surface_gg.select(shortName="lnsp"))

   #convert spectral model level data to Gaussian Grids
   wind_gg = mv.read(data=wind_fieldset,grid='N320')
   t_gg = wind_gg.select(shortName='t')

   q_gg = moist_fieldset.select(shortName='q')
   r_gg = mv.relative_humidity_from_specific_humidity(t_gg,q_gg,lnsp_gg)
   td_gg = mv.dewpoint_from_specific_humidity(q_gg,lnsp_gg)

   moist_fieldset = mv.merge(moist_fieldset,r_gg,td_gg)
   """
   Metview fieldsets are stored as GRIB files in a temporary directory.
   We should delete fieldset variables once we are done with
   them.
   """
   del r_gg
   del td_gg


   #compute geopotential on model levels
   zm_gg = mv.mvl_geopotential_on_ml(t_gg, q_gg, lnsp_gg, zs_gg)
   pz_fieldset = mv.merge(pres_gg, zm_gg)

   del surface_fieldset
   del t_gg
   del q_gg
   del lnsp_gg
   del zs_gg
   del pres_gg
   del zm_gg


   #Compute the u/v wind elements from d/vo and then translate to a Gaussian grid
   uv_wind_spectral = mv.uvwind(data=wind_fieldset,truncation=639)
   uv_wind_gg = mv.read(data=uv_wind_spectral,grid='N320')
   del uv_wind_spectral
   del wind_fieldset

   u_wind_gg = uv_wind_gg.select(shortName='u')
   v_wind_gg = uv_wind_gg.select(shortName='v')

   speed_gg = mv.speed(u_wind_gg,v_wind_gg)
   del u_wind_gg
   del v_wind_gg

   gg_fieldset = mv.merge(wind_gg, speed_gg, uv_wind_gg, pz_fieldset, moist_fieldset)
   print("About to gg_fieldset.to_dataset()")
   gg_dataset = gg_fieldset.to_dataset()

   del wind_gg
   del speed_gg
   del uv_wind_gg
   del pz_fieldset
   del moist_fieldset

   #Interpolate the Gaussian Grid fieldset to a regular lat-lon grid
   ll_fieldset = mv.read(data=gg_fieldset, grid=[0.25, 0.25])
   ll_dataset = ll_fieldset.to_dataset()

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
