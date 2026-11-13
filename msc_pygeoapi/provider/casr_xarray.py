# =================================================================
#
# Author: Xavier Lafrance <Xavier.Lafrance@ec.gc.ca>
#
# Copyright (c) 2026 Xavier Lafrance
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================
#
# CaSR xarray provider for pygeoapi
#
# =================================================================

import logging
import tempfile

import numpy as np
import xarray as xr

from pyproj import CRS

from pygeoapi.provider.base import (
    BaseProvider,
    ProviderConnectionError,
    ProviderNoDataError,
    ProviderQueryError,
)
from pygeoapi.provider.xarray_ import (
    XarrayProvider,
    _convert_float32_to_float64,
    _get_zarr_data,
)
from pygeoapi.util import read_data

LOGGER = logging.getLogger(__name__)


class CasrProvider(XarrayProvider):
    """Provider for Canadian Surface Reanalysis Zarr datacubes."""

    def __init__(self, provider_def):
        """
        Initialize the CaSR provider.

        :param provider_def: pygeoapi provider configuration
        """

        BaseProvider.__init__(self, provider_def)

        try:
            # Only opens a zarr.
            self._data = open_data(
                self.data,
                options=provider_def.get("options", {}),
            )

            self._set_axis_fields()
            self._set_crs()

            self.period = self.get_time_resolution()

            self._coverage_properties = self._get_coverage_properties()

            self.axes = [
                self._coverage_properties["x_axis_label"],
                self._coverage_properties["y_axis_label"],
            ]

            if self.time_field is not None:
                self.axes.append(self._coverage_properties["time_axis_label"])

            self.get_fields()

        except ProviderConnectionError:
            raise

        except Exception as err:
            LOGGER.exception("Could not initialize CaSR provider")
            raise ProviderConnectionError(str(err)) from err

    def _set_axis_fields(self):
        """
        Determinate the spatial and temporal dimension names.

        Explicit values from the pygeoapi configuration YAML are preferred.
        Otherwise, the provider derives them from CF metadata.
        """
        # For the CaSR zarr cubes the results will be self.x_field = "rlon"
        # self.y_field = "rlat" and self.time_field = "time"

        # rlon
        if self.x_field is None:
            self.x_field = self._find_axis(
                axis="X",
                standard_names=("grid_longitude",),
                fallback_names=("rlon",),
            )

        # rlat
        if self.y_field is None:
            self.y_field = self._find_axis(
                axis="Y",
                standard_names=("grid_latitude",),
                fallback_names=("rlat",),
            )

        # time
        if self.time_field is None:
            self.time_field = self._find_axis(
                axis="T",
                standard_names=("time",),
                fallback_names=("time",),
            )

        # Verifies that the spatial axis were found.
        if self.x_field is None or self.y_field is None:
            raise ProviderConnectionError(
                "Could not determine the CaSR spatial dimensions"
            )

        # Verifies that the axis are really in the datacube dimensions.
        if self.x_field not in self._data.dims:
            raise ProviderConnectionError(
                f"X field '{self.x_field}' is not a dataset dimension"
            )

        if self.y_field not in self._data.dims:
            raise ProviderConnectionError(
                f"Y field '{self.y_field}' is not a dataset dimension"
            )

        if self.time_field is not None and self.time_field not in self._data.dims:
            raise ProviderConnectionError(
                f"Time field '{self.time_field}' is not a dataset dimension"
            )

    def _find_axis(
        self,
        axis,
        standard_names=(),
        fallback_names=(),
    ):
        """
        Find a one-dimensional coordinate representing an axis.

        :param axis: CF axis name, for example X, Y or T
        :param standard_names: accepted CF standard_name values
        :param fallback_names: accepted coordinate names
        :returns: coordinate name or None
        """

        # Looks for all coord in the datacube that have only 1 dimension.
        # For example, lat and lon are coords but have 2 dims each (rlat,rlon).
        for name, coord in self._data.coords.items():
            if coord.ndim != 1:
                continue

            # Finds the axis name by looking in the coord attributes.
            if coord.attrs.get("axis", "").upper() == axis:
                return name

        for name, coord in self._data.coords.items():
            if coord.ndim != 1:
                continue

            if coord.attrs.get("standard_name") in standard_names:
                return name

        for name in fallback_names:
            if name in self._data.coords:
                if self._data.coords[name].ndim == 1:
                    return name

        return None

    def query(
        self,
        properties=[],
        subsets={},
        bbox=[],
        datetime_=None,
        format_="json",
        **kwargs,
    ):
        """
        Extract data from collection collection

        :param properties: list of data variables to return (all if blank)
        :param subsets: Not supported with CasrProvider
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param format_: data format of output
        :param **kwargs: Only there to keep the compatibility the xarray_.py provider

        :returns: coverage data as dict of CoverageJSON or native format
        """

        # If no properties are specified then return all variables.
        if len(properties) < 1:
            properties = self.fields.keys()

        # data become a Xarray.Dataset containing only the selected data.
        data = self._data[[*properties]]

        # CaSR does not support OGC API Coverages subsets.
        if subsets:
            msg = (
                "Subsetting is not supported by CasrProvider. "
                "Use bbox for spatial extraction and datetime for temporal extraction."
            )
            LOGGER.error(msg)
            raise ProviderQueryError(msg)

        query_params = {}

        # Determining how different values of bbox and datetime can slice the datacube.
        if bbox or datetime_ is not None:

            LOGGER.debug("Creating spatio-temporal subset")

            if bbox:
                # Transform longitude from 0 - 360 degres to -180 - 180 degres
                lon = ((data["lon"] + 180) % 360) - 180
                lat = data["lat"]

                mask = (
                    (lon >= bbox[0])
                    & (lon <= bbox[2])
                    & (lat >= bbox[1])
                    & (lat <= bbox[3])
                )

                mask_values = mask.compute()

                if not mask_values.any():
                    raise ProviderNoDataError("No data found within requested bbox")

                # rows = rlat INDEX and cols = rlon INDEX
                # iterates every cell within the bbox
                rows, cols = np.where(mask_values)

                LOGGER.debug(
                    f"bbox selected {len(rows)} cells "
                    f"({rows.min()}:{rows.max()}, {cols.min()}:{cols.max()})"
                )

                # Determining the minimal rlat/rlon INDEXES rectangle
                rlon_min = cols.min()
                rlon_max = cols.max()

                rlat_min = rows.min()
                rlat_max = rows.max()

                # Converting indexes to rlat/rlon coordinates
                x_vals = data.coords[self.x_field].values
                y_vals = data.coords[self.y_field].values

                if x_vals[0] > x_vals[-1]:
                    query_params[self.x_field] = slice(
                        x_vals[rlon_max], x_vals[rlon_min]
                    )
                else:
                    query_params[self.x_field] = slice(
                        x_vals[rlon_min], x_vals[rlon_max]
                    )

                if y_vals[0] > y_vals[-1]:
                    query_params[self.y_field] = slice(
                        y_vals[rlat_max], y_vals[rlat_min]
                    )
                else:
                    query_params[self.y_field] = slice(
                        y_vals[rlat_min], y_vals[rlat_max]
                    )

                LOGGER.debug("bbox_crs is not currently handled")

            if datetime_ is not None:
                # Checks if a time field is in the dataset
                if self._coverage_properties["time_axis_label"] is None:
                    msg = "Dataset does not contain a time axis"
                    LOGGER.error(msg)
                    raise ProviderQueryError(msg)
                else:
                    if "/" in datetime_:
                        begin, end = datetime_.split("/")

                        nearest_start = (
                            data.sel({self.time_field: [begin]}, method="nearest")
                            .coords[self.time_field]
                            .values[0]
                        )
                        nearest_end = (
                            data.sel({self.time_field: [end]}, method="nearest")
                            .coords[self.time_field]
                            .values[0]
                        )

                        if nearest_start > nearest_end:
                            nearest_start, nearest_end = nearest_end, nearest_start

                        query_params[self.time_field] = slice(
                            nearest_start, nearest_end
                        )

                    else:
                        nearest_time = (
                            data.sel({self.time_field: [datetime_]}, method="nearest")
                            .coords[self.time_field]
                            .values[0]
                        )
                        query_params[self.time_field] = [nearest_time]

            LOGGER.debug(f"Query parameters: {query_params}")
            try:
                data = data.sel(query_params)

            except Exception as err:
                LOGGER.warning(err)
                raise ProviderQueryError(err)

        if any(size == 0 for size in data.sizes.values()):
            msg = "No data found"
            LOGGER.warning(msg)
            raise ProviderNoDataError(msg)

        if format_ == "json":
            # json does not support float32
            data = _convert_float32_to_float64(data)

        # Returns the metadata of the filtered dataset. The bbox is in the rotated grid coordinates.
        # bbox_crs returns the projjson of the dataset.
        out_meta = {
            "bbox": [
                data.coords[self.x_field].values[0],
                data.coords[self.y_field].values[0],
                data.coords[self.x_field].values[-1],
                data.coords[self.y_field].values[-1],
            ],
            "bbox_crs": self._coverage_properties["bbox_crs"],
            "crs_type": self._coverage_properties["crs_type"],
            "driver": "xarray",
            "height": data.sizes[self.y_field],
            "width": data.sizes[self.x_field],
            "variables": {
                var_name: var.attrs for var_name, var in data.variables.items()
            },
        }

        LOGGER.warning(type(data.coords[self.time_field].values))
        LOGGER.warning(data.coords[self.time_field].values)
        if self.time_field is not None:

            time_values = np.atleast_1d(data.coords[self.time_field].values)

            out_meta["time"] = [
                str(time_values[0]),
                str(time_values[-1]),
            ]

            out_meta["time_steps"] = len(time_values)

        LOGGER.debug(f"time = {out_meta}")

        # No utility until there's a * in the filename
        # self.filename = self.data.split("/")[-1].replace("*", "-".join(properties))

        ####
        LOGGER.debug(type(data))
        LOGGER.warning(f"format_ = {format_}")
        LOGGER.warning(f"filename = {self.filename}")
        LOGGER.debug(f"properties = {properties}")

        # Download all the data with the link "Coverage data as NetCDF (application/x-netcdf)""
        # Determining in which format to return the data depending on the asked format.
        LOGGER.debug("Serializing data in memory")
        if format_ == "json":
            LOGGER.debug("Creating output in CoverageJSON")
            LOGGER.warning(self._coverage_properties)
            return self.gen_covjson(out_meta, data, properties)
        elif format_ == "zarr":
            for var in data.variables:
                data[var].encoding.pop("chunks", None)
            LOGGER.debug("Returning data in native zarr format")
            return _get_zarr_data(data)
        elif format_ == "NetCDF":  # return data in native format
            self.filename = self.data.split("/")
            self.split_layername = self.filename[-1].split(".")
            self.split_layername[-1] = "nc"
            self.filename = ".".join(self.split_layername)
            with tempfile.TemporaryFile() as fp:
                LOGGER.debug("Returning in NetCDF format")
                fp.write(
                    data.to_netcdf()
                )  # we need to pass a string to be able to use the "netcdf4" engine  # noqa
                fp.seek(0)
                return fp.read()
        else:
            raise ProviderQueryError(f"Unsupported format: {format_}")

    def gen_covjson(self, metadata, data, range_type):
        """
        Generate coverage as CoverageJSON representation

        :param metadata: coverage metadata
        :param data: rasterio DatasetReader object
        :param range_type: range type list

        :returns: dict of CoverageJSON representation
        """

        LOGGER.debug("Creating CoverageJSON domain")
        # bbox extraction
        minx, miny, maxx, maxy = metadata["bbox"]

        try:
            # Reading the first value of the Y axis
            tmp_min = data.coords[self.y_field].values[0]
        except IndexError:
            # Supports the case where data.coords[self.y_field].values is a scalar instead of an array([self.y_field])
            tmp_min = data.coords[self.y_field].values
        try:
            # Reading the last value of the Y axis
            tmp_max = data.coords[self.y_field].values[-1]
        except IndexError:
            tmp_max = data.coords[self.y_field].values

        if tmp_min > tmp_max:
            # Reversing miny and maxy because CoverageJSON expects miny < maxy
            LOGGER.debug(f"Reversing direction of {self.y_field}")
            miny = tmp_max
            maxy = tmp_min

        # Determines how the CoverageJSON is structured
        cj = {
            "type": "Coverage",
            "domain": {
                "type": "Domain",
                "domainType": "Grid",
                "axes": {
                    "x": {"start": minx, "stop": maxx, "num": metadata["width"]},
                    "y": {"start": miny, "stop": maxy, "num": metadata["height"]},
                },
                "referencing": [
                    {
                        "coordinates": ["x", "y"],
                        "system": {
                            "type": metadata["crs_type"],
                            "id": metadata["bbox_crs"],
                        },
                    }
                ],
            },
            "parameters": {},
            "ranges": {},
        }

        if self.time_field in data.coords:
            cov_tfor = "%Y"
            if self.period == "P1M":
                cov_tfor = "%Y-%m"
            date_coord = (
                data.coords[self.time_field].dt.strftime(cov_tfor).values
            )  # noqa
            cj["domain"]["axes"]["t"] = {"values": list(date_coord)}

        for var in range_type:
            pm = self._get_parameter_metadata(var, self._data[var].attrs)

            parameter = {
                "type": "Parameter",
                "description": {"en": pm["description"]},
                "unit": {"symbol": pm["unit_label"]},
                "observedProperty": {
                    "id": pm["observed_property_id"],
                    "label": {"en": pm["observed_property_name"]},
                },
            }

            cj["parameters"][pm["id"]] = parameter

        data = data.fillna(None)
        data = _convert_float32_to_float64(data)

        try:
            for key in cj["parameters"].keys():
                cj["ranges"][key] = {
                    "type": "NdArray",
                    # dataType must be either float, integer or string
                    "dataType": self._fields[key]["type"],
                    "axisNames": ["y", "x"],
                    "shape": [metadata["height"], metadata["width"]],
                }
                if "t" in cj["domain"]["axes"]:
                    cj["ranges"][key]["axisNames"].append("t")
                    cj["ranges"][key]["shape"].append(metadata["time_steps"])
                cj["ranges"][key]["values"] = (
                    data[key].values.flatten().tolist()
                )  # noqa
        except IndexError as err:
            LOGGER.warning(err)
            raise ProviderQueryError("Invalid query parameter")

        return cj

    def _find_grid_mapping_name(self):
        """
        Find the name of the CF grid-mapping variable.

        The search is performed in this order:

        1. grid_mapping attribute on a spatial data variable;
        2. grid_mapping value stored in the variable encoding;
        3. coordinate having a grid_mapping_name attribute.

        :returns: grid-mapping variable name or None
        """

        """for name, variable in self._data.data_vars.items():
            if not {
                self.x_field,
                self.y_field,
            }.issubset(variable.dims):
                continue

            # First mecanism looking for the "grid_mapping" name in the data variables.
            grid_mapping = variable.attrs.get("grid_mapping")

            if grid_mapping:
                LOGGER.debug(
                    "Grid mapping '%s' found in attributes of %s",
                    grid_mapping,
                    name,
                )
                return grid_mapping

            # Second mecanism looking for the "grid_mapping" name in the variable encoding.
            grid_mapping = variable.encoding.get("grid_mapping")

            if grid_mapping:
                LOGGER.debug(
                    "Grid mapping '%s' found in encoding of %s",
                    grid_mapping,
                    name,
                )
                return grid_mapping"""

        # Third mecanism which found any variable containing "grid_mapping_name"
        for name, variable in self._data.variables.items():
            if "grid_mapping_name" in variable.attrs:
                LOGGER.debug(
                    "Grid-mapping variable found from CF metadata: %s",
                    name,
                )
                return name

        return None

    def _set_crs(self):
        """
        Determine the native CRS from CF grid-mapping metadata.
        Creates the PROJJSON.

        No storage_crs configuration value is required.
        """

        grid_mapping_name = self._find_grid_mapping_name()

        if grid_mapping_name is None:
            raise ProviderConnectionError(
                "Could not determine the CaSR grid-mapping variable"
            )

        if grid_mapping_name not in self._data.variables:
            raise ProviderConnectionError(
                f"Grid-mapping variable '{grid_mapping_name}' "
                "does not exist in the dataset"
            )

        grid_mapping = self._data[grid_mapping_name]

        # Verifying if it's a CF variable
        if "grid_mapping_name" not in grid_mapping.attrs:
            raise ProviderConnectionError(
                f"Variable '{grid_mapping_name}' does not contain "
                "a CF grid_mapping_name attribute"
            )

        # Reading the CF attributes with Pyproj and creating the pyproj.CRS object.
        try:
            native_crs = CRS.from_cf(dict(grid_mapping.attrs))
        except Exception as err:
            LOGGER.exception(
                "Could not derive the CaSR CRS from %s",
                grid_mapping_name,
            )
            raise ProviderConnectionError(
                f"Could not derive the CaSR CRS from " f"'{grid_mapping_name}': {err}"
            ) from err

        # grid_mapping_field will contains "rotated_pole"
        # self.crs will contains the pyproj.CRS object containing the rotated grid.
        # self.crs is the CRS logic exposed.
        # storage_crs is the CRS in which the data are stocked.
        self.grid_mapping_field = grid_mapping_name
        self.crs = native_crs
        self.storage_crs = native_crs

        # authority ask: is there a known official id for this CRS?
        authority = native_crs.to_authority()

        if authority is not None:
            authority_name, authority_code = authority
            self.crs_identifier = (
                "http://www.opengis.net/def/crs/" f"{authority_name}/0/{authority_code}"
            )
        else:
            # self.crs_identifier = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
            # Produce a PROJJSON :
            self.crs_identifier = native_crs.to_json()

        LOGGER.info(
            "CaSR native CRS derived from '%s'",
            self.grid_mapping_field,
        )

        LOGGER.debug(
            "CaSR CRS: %s",
            self.crs.to_wkt(),
        )

    def get_fields(self):
        """
        Provide coverage fields.

        Only actual data variables having the spatial dimensions are
        exposed as coverage fields.
        Determinates the variables that can be ask with /coverage?f=json&properties=... .
        Determinates the variables exposed in the "Schema of collection".
        """
        # The variables for example : AirTemp-Avg, AirTemp-Max, ...

        self._fields = {}

        for name, variable in self._data.data_vars.items():
            print(name, variable.dims)
            LOGGER.debug(
                "Determining range type for %s",
                name,
            )

            # Exposes a variable only if it has rlat and rlon as it's dimensions (.dims).
            if not {
                self.x_field,
                self.y_field,
            }.issubset(variable.dims):
                LOGGER.debug(
                    "Ignoring auxiliary variable %s with dimensions %s",
                    name,
                    variable.dims,
                )
                continue

            data_type = self._get_field_data_type(variable.dtype)

            units = variable.attrs.get("units", "1")

            title = (
                variable.attrs.get("long_name")
            )

            # Construction of the metadata for each valid variables.
            self._fields[name] = {
                "title": title,
                "type": data_type,
                "x-ogc-unit": units,
                "_meta": {
                    "tags": dict(variable.attrs),
                    "uom": {
                        "id": ("http://www.opengis.net/" f"def/uom/UCUM/{units}"),
                        "type": "UnitReference",
                        "code": units,
                    },
                    "encodingInfo": {
                        "dataType": (
                            "http://www.opengis.net/"
                            "def/dataType/OGC/0/"
                            f"{variable.dtype}"
                        )
                    },
                    "nodata": "null",
                },
            }

        if not self._fields:
            raise ProviderConnectionError("No CaSR coverage fields were found")

        return self._fields

    @staticmethod
    def _get_field_data_type(dtype):
        """
        Convert a NumPy dtype to an OGC-compatible data type.
        Convert NumPy types to the expected type in the coverage schema.

        :param dtype: NumPy dtype
        :returns: float, integer, string or boolean
        """

        dtype = np.dtype(dtype)

        # np.issubdtype
        if np.issubdtype(dtype, np.floating):
            return "float"

        if np.issubdtype(dtype, np.integer):
            return "integer"

        if np.issubdtype(dtype, np.bool_):
            return "boolean"

        if np.issubdtype(dtype, np.str_):
            return "string"

        return "string"

    def get_coverage_domainset(self):
        """
        Provide additional non-spatial coverage axes.

        CaSR currently has no scenario, percentile or season axis
        comparable to the specialized climate provider.
        """

        return {}

    def _get_coverage_properties(self):
        """
        Normalize CaSR coverage properties.
        """

        x_coord = self._data.coords[self.x_field]
        y_coord = self._data.coords[self.y_field]


        if x_coord.size < 2 or y_coord.size < 2:
            raise ProviderConnectionError(
                "Spatial axes must contain at least two values"
            )

        properties = {
            "bbox": [
                float(x_coord.values[0]),
                float(y_coord.values[0]),
                float(x_coord.values[-1]),
                float(y_coord.values[-1]),
            ],
            "bbox_crs": self.crs_identifier,
            "crs_type": ("ProjectedCRS" if self.crs.is_projected else "GeographicCRS"),
            "x_axis_label": self.x_field,
            "y_axis_label": self.y_field,
            "time_axis_label": self.time_field,
            "width": self._data.sizes[self.x_field],
            "height": self._data.sizes[self.y_field],
            "resx": float(np.abs(x_coord.values[1] - x_coord.values[0])),
            "resy": float(np.abs(y_coord.values[1] - y_coord.values[0])),
            "uad": self.get_coverage_domainset(),
        }

        if self.time_field is not None and self._data.coords[self.time_field].size > 0:
            time_coord = self._data.coords[self.time_field]

            properties["restime"] = self.period
            properties["time_range"] = [
                self._format_time_range_value(time_coord.values[0]),
                self._format_time_range_value(time_coord.values[-1]),
            ]

        LOGGER.warning(properties["time_range"])
        return properties

    def get_time_resolution(self):
        """
        Derive the temporal resolution (monthly or yearly) from the time coordinate.

        :returns: ISO 8601 duration such as P1M, P1D or PT1H
        """

        if self.time_field is None:
            return None

        time_coord = self._data.coords[self.time_field]

        if time_coord.size < 2:
            return None

        first = np.datetime64(time_coord.values[0])
        second = np.datetime64(time_coord.values[1])

        months = second.astype("datetime64[M]").astype(int) - first.astype(
            "datetime64[M]"
        ).astype(int)

        if months == 1:
            return "P1M"

        if months == 12:
            return "P1Y"

        raise ProviderConnectionError(
            f"Unsupported temporal resolution: {months} month(s)"
        )

    def _format_time_range_value(self, value):
        """
        Formats the dates in the temporal extent.
        Months as "year-month" and years as "years"
        """

        value = np.datetime64(value)

        if self.period == "P1Y":
            return np.datetime_as_string(value, unit="Y")

        if self.period == "P1M":
            return np.datetime_as_string(value, unit="M")

        return np.datetime_as_string(value, unit="s")


def open_data(data, options=None):
    """
    Open a consolidated CaSR Zarr dataset.

    :param data: local path, mounted path or supported Zarr store
    :param options: provider options from the pygeoapi configuration
    :returns: xarray.Dataset
    """

    options = options or {}
    zarr_options = options.get("zarr", {})

    open_options = {
        "consolidated": zarr_options.get(
            "consolidated",
            True,
        ),
        "decode_coords": zarr_options.get(
            "decode_coords",
            "all",
        ),
    }

    try:
        dataset = xr.open_zarr(
            data,
            **open_options,
        )

        return dataset

    except Exception as err:
        LOGGER.exception(
            "Could not open CaSR Zarr dataset: %s",
            data,
        )
        raise ProviderConnectionError(str(err)) from err
