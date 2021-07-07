# walden

A prototype catalog of data sources that OWID's datasets are built from.

## Catalog structure

This repo contains a catalog of JSON metadata, representing datasets provided as is by large institutions or by researchers. For example, suppose the UN FAO provides a file `VeryImportantData.xlsx`. Then inside the `index/un_fao/` folder there will be a file `very_important_data.json` that looks like this:

```
{
  "md5": "a0dc1033f5d8952739497fb0932ff240",
  "namespace": "un_fao",
  "short_name": "very_important_data",
  "description": "...",
  "publication_year": 2019,
  "owid_data_url": "https://walden.nyc.digitalocean.com/un_fao/2019/VeryImportantData.xlsx",
  ...
}
```

To get the data locally, you can run "make fetch" to download everything to `data/`, or programmatically read the catalog and fetch just what you need.

## Working with the codebase

### The basics

You need Python 3.8+ to use this repository, with `poetry` installed (`pip install poetry`).

You many then run tests with:

`make test`

Fetch all data locally with:

`make fetch`

Or simply run `make` to see available commands.

### Working with DigitalOcean (OWID staff only)

Ask a colleague to give you access to DigitalOcean, where you can create an access token pair for Spaces.

Once you have that, install `s3cmd` globally (`pip install s3cmd`) and configure it with `s3cmd --configure`. When asked for your S3 endpoint, set it to `nyc3.digitaloceanspaces.com`. If it is working, you should be able to run `s3cmd ls s3://walden/` and see the contents of that bucket.

## Adding to the catalog

1. **Create a stub JSON entry.** To add a new file to the catalog, firstly decide on whether it has a `namespace`, what its `short_name` should be, and identify its `publication_date` or `publication_year`. With these as your guide, create a JSON description at `index/<namespace>/<publication_year>/<short_name>.json`, starting with these fields.
2. **Calculate and add the checksum.** You should download your file locally, calculate its md5 checksum, and that to the metadata too (e.g. `md5 -q myfilename.xlsx`).
3. **Upload the a copy of the file.** Finally, you should upload the file to `s3://walden/<namespace>/<publication_year>/<filename>` (e.g. `s3cmd put -P myfilename s3://walden/un_fao/2019/`), and then add the resulting public URL to the metadata as the `owid_data_url` field.
4. **Check it's correct.** Run `make test` to check your file against the schema, and if necessary, `make format` to reformat it.
5. **Ship it!** Commit and push.

## TODO

First prototype

- [X] (Lars) Validate entries against the schema with one command
- [X] (Lars) One command to fetch all the data
- [X] (Lars) A dedicated space for our source files
- [ ] (Lars) Access for all data team members to the dedicated space
- [ ] (Lucas) Trial using the index for FAO - food security
    - [ ] Manually add JSON files for each data file
    - [ ] Upload copies of data files to our cache
- [ ] Make an interactive helper script for adding a new file to the catalog

Break, share with the team. If it's good, continue:

- [ ] (Ed/Lucas) Add our existing importers datasets to the index
    - [ ] faostat
    - [ ] ihme_sdg
    - [ ] povcal
    - [ ] un_wpp
    - [ ] vdem
    - [ ] who_gho
    - [ ] worldbank
    - [ ] worldbank_wdi
