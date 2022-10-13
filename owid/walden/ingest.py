"""Tools to ingest to Walden and Catalog."""

import inspect
import tempfile
from pathlib import Path
from typing import Any, Optional, Union

import pandas as pd

from owid.walden import files

from .catalog import Dataset
from .ui import log


########################################################################################################################
# TODO: These functions should be removed, and imported from owid.datautils.dataframes once they exist.
def has_index(df: pd.DataFrame) -> bool:
    """Return True if a dataframe has an index, and False if it does not (i.e. if it has a dummy index).

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe whose index will be checked.

    Returns
    -------
    df_has_index : bool
        True if dataframe has a non-dummy (single- or multi-) index.

    """
    # Dataframes always have an attribute index.names, which is a frozen list.
    # If the dataframe has no set index (i.e. if it has a dummy index), that list contains only [None].
    # In any other case, the frozen list contains one or more elements different than None.
    df_has_index = True if df.index.names[0] is not None else False

    return df_has_index


def to_file(df: pd.DataFrame, file_path: Union[str, Path], overwrite: bool = True, **kwargs: Any) -> None:
    """Save dataframe to file.

    This function wraps all pandas df.to_* methods, e.g. df.to_csv() or df.to_parquet(), with the following advantages:
    * The output file will have the format determined by the extension of file_path. Hence, to_file(df, "data.csv") will
    create a csv file, and to_file(df, "data.parquet") will create a parquet file.
    * If file_path is with one or more subfolders that do not exist, the full path will be created.
    * It can overwrite an existing file (if overwrite is True), or raise an error if the file already exists.
    * It will avoid creating an index column if the dataframe has a dummy index (which would be equivalent to doing
    df.to_csv(file_path, index=False)), but it will include the index if the dataframe has one.
    * Any additional keyword argument that would be passed on to the method to write a file can be safely added. For
    example, to_file(df, "data.csv", na_rep="TEST") will replace missing data by "TEST" (analogous to
    df.to_csv("data.csv", na_rep="TEST")).

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to be stored in a file.
    file_path : Union[str, Path]
        Path to file to be created.
    overwrite : bool, optional
        True to overwrite file if it already exists. False to raise an error if file already exists.

    """
    # Ensure file_path is a Path object.
    file_path = Path(file_path)

    # Ensure extension is lower case and does not start with '.'.
    extension = file_path.suffix.lstrip(".").lower()

    # Ensure output directory exists.
    if not file_path.parent.exists():
        file_path.parent.mkdir(parents=True)

    # Avoid overwriting an existing file unless explicitly stated.
    if file_path.is_file() and not overwrite:
        raise FileExistsError("Failed to save dataframe because file exists and 'overwrite' is False.")

    # Available output methods (some of them may need additional dependencies to work).
    output_methods = {
        "csv": df.to_csv,
        "dta": df.to_stata,
        "feather": df.to_feather,
        "hdf": df.to_hdf,
        "html": df.to_html,
        "json": df.to_json,
        "md": df.to_markdown,
        "parquet": df.to_parquet,
        "pickle": df.to_pickle,
        "pkl": df.to_pickle,
        "tex": df.to_latex,
        "txt": df.to_string,
        "xlsx": df.to_excel,
        "xml": df.to_xml,
    }
    if extension not in output_methods:
        raise ValueError(f"Failed saving dataframe because of an unknown file extension: {extension}")
    # Select the appropriate storing method.
    save_function = output_methods[extension]

    # Decide whether dataframe should be stored with or without an index, if:
    # * The storing method allows for an 'index' argument.
    # * The argument "index" is not explicitly given.
    if ("index" in inspect.signature(save_function).parameters) and ("index" not in kwargs):
        # Make 'index' False to avoid storing index if dataframe has a dummy index.
        kwargs["index"] = has_index(df=df)

    # Save file using the chosen save function and the appropriate arguments.
    save_function(file_path, **kwargs)


########################################################################################################################


def add_to_catalog(
    metadata: Union[dict, Dataset],
    filename: Optional[Union[str, Path]] = None,
    dataframe: Optional[pd.DataFrame] = None,
    upload: bool = False,
    public: bool = True,
) -> None:
    """Add dataset with metadata to catalog, where the data is either a local file, or a dataframe in memory.

    Additionally, it computes the md5 hash of the file, which is added to the metadata file.

    TODO: Add checks of fields.

    Args:
        metadata (dict): Dictionary with metadata.
        filename (str or None): Path to local data file (if dataframe is not given).
        dataframe (pd.DataFrame or None): Dataframe to upload (if filename is not given).
        upload (bool): True to upload data to Walden bucket.
        public (bool): True to make file public.
    """
    if (filename is not None) and (dataframe is None):
        # checksum happens in here, copy to cache happens here
        dataset = Dataset.copy_and_create(str(filename), metadata)

        if upload:
            # add it to our DigitalOcean Space and set `owid_cache_url`
            dataset.upload(public=public)

        # save the JSON to the local index
        dataset.save()
        log("ADDED TO CATALOG", f"{dataset.relative_base}.json")
    elif (dataframe is not None) and (filename is None):
        # Get output file extension from metadata.
        if type(metadata) == dict:
            file_extension = metadata["file_extension"]
        else:
            file_extension = metadata.file_extension  # type: ignore

        with tempfile.TemporaryDirectory() as temp_dir:
            # Save dataframe in a temporary file.
            # Use the extension specified in the metadata, so that the file is stored in the right format.
            temp_file = Path(temp_dir) / f"temp.{file_extension}"
            to_file(dataframe, file_path=temp_file)
            # Add file checksum to metadata.
            metadata.md5 = files.checksum(temp_file)  # type: ignore
            # Run the function again, but now fetching the data from the temporary file instead of the dataframe.
            # This time the function will create the walden index file and upload to s3 (if upload is True).
            add_to_catalog(metadata=metadata, filename=temp_file, upload=upload)
    else:
        raise ValueError("Use either 'filename' or 'dataframe' argument, but not both.")
