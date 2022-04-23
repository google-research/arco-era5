# Converting Era 5 to Zarr

Implementation of go/era5-conventions.

```shell
export PROJECT=ai-for-weather
export REGION=us-central1
export BUCKET=ai4wx-beam
```

See top of source files for instructions on how to run.

## Data Description

As of 2022-04-27, all data spans the dates `1979-01-01/to/2021-07-01` (inclusive).

### Model Level Wind

* _Levels_: `1/to/137`
* _Times_: `00/to/23`
* _Grid_: `Spectral Harmonic Coefficients`

| name                 | short name | units   | docs                                              | config          |
|----------------------|------------|---------|---------------------------------------------------|-----------------|
| vorticity (relative) | vo         | s^-1    | https://apps.ecmwf.int/codes/grib/param-db?id=138 | era5_ml_dv.cfg  |
| divergence           | d          | s^-1    | https://apps.ecmwf.int/codes/grib/param-db?id=155 | era5_ml_dv.cfg  |
| temperature          | t          | K       | https://apps.ecmwf.int/codes/grib/param-db?id=130 | era5_ml_tw.cfg  |
| vertical velocity    | d          | Pa s^-1 | https://apps.ecmwf.int/codes/grib/param-db?id=135 | era5_ml_tw.cfg  |

### Model Level Moisture

* _Levels_: `1/to/137`
* _Times_: `00/to/23`
* _Grid_: `N320` (Reduced Gaussian Grid)

| name                                | short name | units    | docs                                              | config           |
|-------------------------------------|------------|----------|---------------------------------------------------|------------------|
| specific humidity                   | q          | kg kg^-1 | https://apps.ecmwf.int/codes/grib/param-db?id=133 | era5_ml_o2q.cfg  |
| ozone mass mixing ratio             | o3         | kg kg^-1 | https://apps.ecmwf.int/codes/grib/param-db?id=203 | era5_ml_o2q.cfg  | 
| specific cloud liquid water content | clwc       | kg kg^-1 | https://apps.ecmwf.int/codes/grib/param-db?id=246 | era5_ml_o2q.cfg  | 
| specific cloud ice water content    | ciwc       | kg kg^-1 | https://apps.ecmwf.int/codes/grib/param-db?id=247 | era5_ml_o2q.cfg  |
| fraction of cloud cover             | cc         | (0 - 1)  | https://apps.ecmwf.int/codes/grib/param-db?id=248 | era5_ml_o2q.cfg  |
| specific rain water content         | crwc       | kg kg^-1 | https://apps.ecmwf.int/codes/grib/param-db?id=75  | era5_ml_qrqs.cfg |
| specific snow water content         | cswc       | kg kg^-1 | https://apps.ecmwf.int/codes/grib/param-db?id=76  | era5_ml_qrqs.cfg |

### Single Level Reanalysis

* _Times_: `00/to/23`
* _Grid_: `N320` (Reduced Gaussian Grid)

| name                                                       | short name | units        | docs                                                 | config             |
|------------------------------------------------------------|------------|--------------|------------------------------------------------------|--------------------|
| convective available potential energy                      | cape       | J kg^-1      | https://apps.ecmwf.int/codes/grib/param-db?id=59     | era5_sfc_cape.cfg  |
| total column cloud ice water                               | tciw       | kg m^-2      | https://apps.ecmwf.int/codes/grib/param-db?id=79     | era5_sfc_cape.cfg  |
| vertical integral of divergence of cloud frozen water flux | wiiwd      | kg m^-2 s^-1 | https://apps.ecmwf.int/codes/grib/param-db?id=162080 | era5_sfc_cape.cfg  |
| 100 metre U wind component                                 | 100u       | m s^-1       | https://apps.ecmwf.int/codes/grib/param-db?id=228246 | era5_sfc_cape.cfg  |
| 100 metre V wind component                                 | 100v       | m s^-1       | https://apps.ecmwf.int/codes/grib/param-db?id=228247 | era5_sfc_cape.cfg  |
| sea ice area fraction                                      | ci         | (0 - 1)      | https://apps.ecmwf.int/codes/grib/param-db?id=31     | era5_sfc_cisst.cfg | 
| sea surface temperature                                    | sst        | Pa           | https://apps.ecmwf.int/codes/grib/param-db?id=34     | era5_sfc_cisst.cfg |
| skin temperature                                           | skt        | K            | https://apps.ecmwf.int/codes/grib/param-db?id=235    | era5_sfc_cisst.cfg |
| soil temperature level 1                                   | stl1       | K            | https://apps.ecmwf.int/codes/grib/param-db?id=139    | era5_sfc_soil.cfg  | 
| soil temperature level 2                                   | stl2       | K            | https://apps.ecmwf.int/codes/grib/param-db?id=170    | era5_sfc_soil.cfg  |
| soil temperature level 3                                   | stl3       | K            | https://apps.ecmwf.int/codes/grib/param-db?id=183    | era5_sfc_soil.cfg  |
| soil temperature level 4                                   | stl4       | K            | https://apps.ecmwf.int/codes/grib/param-db?id=236    | era5_sfc_soil.cfg  |
| temperature of snow layer                                  | tsn        | K            | https://apps.ecmwf.int/codes/grib/param-db?id=238    | era5_sfc_soil.cfg  |
| volumetric soil water layer 1                              | swvl1      | m^3 m^-3     | https://apps.ecmwf.int/codes/grib/param-db?id=39     | era5_sfc_soil.cfg  |
| volumetric soil water layer 2                              | swvl2      | m^3 m^-3     | https://apps.ecmwf.int/codes/grib/param-db?id=40     | era5_sfc_soil.cfg  |
| volumetric soil water layer 3                              | swvl3      | m^3 m^-3     | https://apps.ecmwf.int/codes/grib/param-db?id=41     | era5_sfc_soil.cfg  |
| volumetric soil water layer 4                              | swvl4      | m^3 m^-3     | https://apps.ecmwf.int/codes/grib/param-db?id=42     | era5_sfc_soil.cfg  |
| ice temperature layer 1                                    | istl1      | K            | https://apps.ecmwf.int/codes/grib/param-db?id=35     | era5_sfc_soil.cfg  |
| ice temperature layer 2                                    | istl2      | K            | https://apps.ecmwf.int/codes/grib/param-db?id=36     | era5_sfc_soil.cfg  |
| ice temperature layer 3                                    | istl3      | K            | https://apps.ecmwf.int/codes/grib/param-db?id=37     | era5_sfc_soil.cfg  |
| ice temperature layer 4                                    | istl4      | K            | https://apps.ecmwf.int/codes/grib/param-db?id=38     | era5_sfc_soil.cfg  |
| total column cloud liquid water                            | tclw       | kg m^-2      | https://apps.ecmwf.int/codes/grib/param-db?id=78     | era5_sfc_tcol.cfg  | 
| total column cloud ice water                               | tciw       | kg m^-2      | https://apps.ecmwf.int/codes/grib/param-db?id=79     | era5_sfc_tcol.cfg  |
| total column cloud ice water                               | tciw       | kg m^-2      | https://apps.ecmwf.int/codes/grib/param-db?id=79     | era5_sfc_tcol.cfg  |
| total column rain water                                    | tcrw       | kg m^-2      | https://apps.ecmwf.int/codes/grib/param-db?id=228089 | era5_sfc_tcol.cfg  |
| total column snow water                                    | tcsw       | kg m^-2      | https://apps.ecmwf.int/codes/grib/param-db?id=228090 | era5_sfc_tcol.cfg  |
| total column water                                         | tcsw       | kg m^-2      | https://apps.ecmwf.int/codes/grib/param-db?id=136    | era5_sfc_tcol.cfg  |
| total column vertically-integrated water vapour            | tcsw       | kg m^-2      | https://apps.ecmwf.int/codes/grib/param-db?id=137    | era5_sfc_tcol.cfg  |

### Single Level Forecast

* _Times_: `06:00/18:00`
* _Steps_: `0/1/2/3/4/5/6/7/8/9/10/11/12/13/14/15/16/17/18`
* _Grid_: `N320` (Reduced Gaussian Grid)

| name                                       | short name | units                 | docs                                                 | config           |
|--------------------------------------------|------------|-----------------------|------------------------------------------------------|------------------|
| snow density                               | rsn        | kg m^-3               | https://apps.ecmwf.int/codes/grib/param-db?id=33     | era5_sfc_pcp.cfg | 
| snow evaporation                           | es         | m of water equivalent | https://apps.ecmwf.int/codes/grib/param-db?id=44     | era5_sfc_pcp.cfg | 
| snow melt                                  | smlt       | m of water equivalent | https://apps.ecmwf.int/codes/grib/param-db?id=45     | era5_sfc_pcp.cfg | 
| large-scale precipitation fraction         | lspf       | s                     | https://apps.ecmwf.int/codes/grib/param-db?id=50     | era5_sfc_pcp.cfg | 
| snow depth                                 | sd         | m of water equivalent | https://apps.ecmwf.int/codes/grib/param-db?id=141    | era5_sfc_pcp.cfg |
| large-scale precipitation                  | lsp        | m                     | https://apps.ecmwf.int/codes/grib/param-db?id=142    | era5_sfc_pcp.cfg |
| convective precipitation                   | cp         | m                     | https://apps.ecmwf.int/codes/grib/param-db?id=143    | era5_sfc_pcp.cfg |
| snowfall                                   | sf         | m of water equivalent | https://apps.ecmwf.int/codes/grib/param-db?id=144    | era5_sfc_pcp.cfg |
| convective rain rate                       | crr        | kg m^-2 s^-1          | https://apps.ecmwf.int/codes/grib/param-db?id=228218 | era5_sfc_pcp.cfg |
| large scale rain rate                      | lsrr       | kg m^-2 s^-1          | https://apps.ecmwf.int/codes/grib/param-db?id=228219 | era5_sfc_pcp.cfg |
| convective snowfall rate water equivalent  | csfr       | kg m^-2 s^-1          | https://apps.ecmwf.int/codes/grib/param-db?id=228220 | era5_sfc_pcp.cfg |
| large scale snowfall rate water equivalent | lssfr      | kg m^-2 s^-1          | https://apps.ecmwf.int/codes/grib/param-db?id=228221 | era5_sfc_pcp.cfg |
| total precipitation                        | tp         | m                     | https://apps.ecmwf.int/codes/grib/param-db?id=228    | era5_sfc_pcp.cfg | 
| convective snowfall                        | csf        | m of water equivalent | https://apps.ecmwf.int/codes/grib/param-db?id=239    | era5_sfc_pcp.cfg | 
| large-scale snowfall                       | lsf        | m of water equivalent | https://apps.ecmwf.int/codes/grib/param-db?id=240    | era5_sfc_pcp.cfg |
| precipitation type                         | ptype      | code table (4.201)    | https://apps.ecmwf.int/codes/grib/param-db?id=260015 | era5_sfc_pcp.cfg |
| surface solar radiation downwards          | ssrd       | J m^-2                | https://apps.ecmwf.int/codes/grib/param-db?id=169    | era5_sfc_rad.cfg | 
| top net thermal radiation                  | ttr        | J m^-2                | https://apps.ecmwf.int/codes/grib/param-db?id=179    | era5_sfc_rad.cfg |
| gravity wave dissipation                   | gwd        | J m^-2                | https://apps.ecmwf.int/codes/grib/param-db?id=197    | era5_sfc_rad.cfg |
| surface thermal radiation downwards        | strd       | J m^-2                | https://apps.ecmwf.int/codes/grib/param-db?id=175    | era5_sfc_rad.cfg |
| surface net thermal radiation              | str        | J m^-2                | https://apps.ecmwf.int/codes/grib/param-db?id=177    | era5_sfc_rad.cfg |


## License

This is not an official Google product.

```
Copyright 2022 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```